#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TOI - Coletor Temporal Multi-Fila V3 - RESILIENTE / SOMENTE LEITURA

Melhorias sobre o V2:
- worker independente por fila;
- uma fila lenta não bloqueia as demais;
- sem sobreposição por fila;
- retry curto e controlado;
- circuit breaker por fila/fonte;
- infraestrutura independente por instância;
- discador e últimas chamadas em cadências próprias;
- SQLite + JSONL;
- após login: SOMENTE GET;
- nenhuma ação corretiva.

Uso recomendado:
  python teste_toi_coletor_temporal_multifila_readonly_v3.py ^
    --config toi_demo_scope_v1.json ^
    --duracao-min 15

Coleta longa:
  python teste_toi_coletor_temporal_multifila_readonly_v3.py ^
    --config toi_demo_scope_v1.json ^
    --duracao-min 240

Cadências padrão:
- fila/P34/P135: 10s
- infraestrutura: 15s
- últimas chamadas: 60s
- discador: 180s
- canais travados: 60s

Circuit breaker:
- 3 falhas consecutivas => cooldown de 60s daquela fonte/fila
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

DEFAULT_BASE_URL = "http://10.90.10.101:8181"


def now_iso():
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def frontend_base(base_url, instancia):
    base_url = base_url.rstrip("/")
    if str(instancia) == "1":
        return f"{base_url}/fornecedor/appatmaacme/fornecedortechsuitefrontendweb/"
    return f"{base_url}/fornecedor/appatmaacme/fornecedortechsuitefrontendweb_inst{instancia}/"


def parse_num(v):
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("%", "")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def list_from(doc, *keys):
    if not isinstance(doc, dict):
        return []
    for k in keys:
        v = doc.get(k)
        if isinstance(v, list):
            return v
    return []


def extract_status(doc):
    out = {34: (0.0, 0), 135: (0.0, 0)}
    arr = list_from(
        doc,
        "callStatisticsList",
        "CallStatisticsList",
        "callstatisticslist",
    )
    for x in arr:
        m = re.match(r"\s*(\d+)\s*-", str(x.get("NmStatusIpbx", "")))
        if not m:
            continue
        code = int(m.group(1))
        if code not in out:
            continue
        pct = x.get("VlPercCalls")
        if pct is None:
            pct = x.get("VlPercCallsToDisplay")
        out[code] = (parse_num(pct), int(x.get("QtCalls") or 0))
    return out


def qfield(doc, *keys, default=0):
    if not isinstance(doc, dict):
        return default
    for k in keys:
        if k in doc:
            return doc[k]
    return default


class PortalSession:
    def __init__(self, inst, user, pwd, base_url, timeout=20):
        self.inst = str(inst)
        self.user = user
        self.pwd = pwd
        self.timeout = timeout
        self.base = frontend_base(base_url, self.inst)
        self.login_url = urljoin(self.base, "start/account/login")
        self.cookies = None
        self.cookie_lock = threading.Lock()
        self.auth_lock = threading.Lock()

    def auth(self):
        s = requests.Session()
        r = s.get(self.login_url, timeout=self.timeout)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        tok = soup.find("input", {"name": "__RequestVerificationToken"})
        if not tok or not tok.get("value"):
            raise RuntimeError("token antiforgery ausente")

        payload = [
            ("ReturnUrl", "/"),
            ("__RequestVerificationToken", tok["value"]),
            ("DeLogin", self.user),
            ("DePassword", self.pwd),
            ("rememberMe", "true"),
            ("rememberMe", "false"),
        ]

        # ÚNICO POST permitido: autenticação.
        r = s.post(
            self.login_url,
            data=payload,
            headers={"Referer": self.login_url},
            allow_redirects=False,
            timeout=self.timeout,
        )
        if r.status_code not in (301, 302, 303):
            raise RuntimeError(f"login HTTP {r.status_code}")

        with self.cookie_lock:
            self.cookies = s.cookies.get_dict()

    def reauth(self, expected_cookies=None):
        # Uma lane pode ter renovado a sessão enquanto outra recebia 401.
        # Evita logins sequenciais que invalidariam o cookie recém-criado.
        with self.auth_lock:
            with self.cookie_lock:
                current = dict(self.cookies or {})
            if expected_cookies is not None and current != expected_cookies:
                return
            self.auth()

    def get_json(self, path, params=None, referer=None, timeout=None, retries=1):
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "User-Agent": "TOI-Live-Fidelity-v1.10",
        }
        if referer:
            headers["Referer"] = referer

        last_exc = None
        attempts = []
        regular_failures = 0
        reauth_used = False
        attempt = 0

        # retries controla falhas normais; sessão expirada ganha uma tentativa
        # adicional exclusiva após reautenticação.
        while regular_failures <= retries:
            attempt += 1
            t0 = time.perf_counter()
            try:
                with self.cookie_lock:
                    cookies = dict(self.cookies or {})
                r = requests.get(
                    urljoin(self.base, path), params=params, headers=headers,
                    cookies=cookies, timeout=timeout or self.timeout,
                    allow_redirects=True,
                )
                latency = round(time.perf_counter() - t0, 3)
                attempts.append({"attempt": attempt, "http": r.status_code, "latency_s": latency})
                # O Portal também pode sinalizar sessão vencida diretamente com
                # 401/403. Renovar uma vez antes do retry normal; raise_for_status
                # aqui impediria a reautenticação e criaria lacunas até o restart.
                if r.status_code in (401, 403):
                    raise PermissionError(f"sessão HTTP {r.status_code}")
                r.raise_for_status()
                if "account/login" in r.url.lower() or 'type="password"' in (r.text or "").lower():
                    raise PermissionError("sessão expirada")
                return r.json(), latency, attempts
            except PermissionError as exc:
                latency = round(time.perf_counter() - t0, 3)
                attempts.append({"attempt": attempt, "error": f"PermissionError: {exc}", "latency_s": latency})
                last_exc = exc
                if reauth_used:
                    break
                self.reauth(cookies)
                reauth_used = True
                # não consome retry normal
                continue
            except Exception as exc:
                latency = round(time.perf_counter() - t0, 3)
                attempts.append({"attempt": attempt, "error": f"{type(exc).__name__}: {exc}", "latency_s": latency})
                last_exc = exc
                regular_failures += 1
                if regular_failures <= retries:
                    time.sleep(min(1.0 * regular_failures, 2.0))
                    continue
                break

        raise RuntimeError(f"{type(last_exc).__name__}: {last_exc}")


class Store:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS queue_snapshot(
          ts TEXT,
          instancia TEXT,
          projeto_id TEXT,
          codigo TEXT,
          nome TEXT,
          motivo TEXT,
          p34_pct REAL,
          p34_qtd INTEGER,
          p135_pct REAL,
          p135_qtd INTEGER,
          agents_available INTEGER,
          agents_talking INTEGER,
          agents_acw INTEGER,
          agents_paused INTEGER,
          agents_other INTEGER,
          agents_dialing INTEGER,
          calls_dialing INTEGER,
          calls_in_attendance INTEGER,
          calls_last5 INTEGER,
          hit_rate5 REAL,
          hit_rate15 REAL,
          dialing_rate REAL,
          buffer REAL,
          process_status TEXT,
          active TEXT,
          paused TEXT,
          latency_queue REAL,
          latency_stats REAL,
          cycle_lag_s REAL,
          raw_queue TEXT,
          raw_stats TEXT
        );

        CREATE INDEX IF NOT EXISTS ix_qs
        ON queue_snapshot(instancia,codigo,ts);

        CREATE TABLE IF NOT EXISTS infra_snapshot(
          ts TEXT,
          instancia TEXT,
          source TEXT,
          entity TEXT,
          total REAL,
          busy REAL,
          idle REAL,
          occupancy REAL,
          invalid REAL,
          latency REAL,
          raw_json TEXT
        );

        CREATE INDEX IF NOT EXISTS ix_inf
        ON infra_snapshot(instancia,source,ts);

        CREATE TABLE IF NOT EXISTS dialer_snapshot(
          ts TEXT,
          instancia TEXT,
          projeto_id TEXT,
          codigo TEXT,
          nome TEXT,
          latency REAL,
          raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS lastcalls_snapshot(
          ts TEXT,
          instancia TEXT,
          projeto_id TEXT,
          codigo TEXT,
          nome TEXT,
          latency REAL,
          raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS stuck_channels_snapshot(
          ts TEXT,
          instancia TEXT,
          status TEXT,
          quantity INTEGER,
          raw_json TEXT
        );

        CREATE INDEX IF NOT EXISTS ix_stuck
        ON stuck_channels_snapshot(instancia,ts);

        CREATE TABLE IF NOT EXISTS collector_event(
          ts TEXT,
          level TEXT,
          worker TEXT,
          entity TEXT,
          message TEXT
        );

        CREATE TABLE IF NOT EXISTS worker_health(
          ts TEXT,
          worker TEXT,
          entity TEXT,
          state TEXT,
          consecutive_failures INTEGER,
          cooldown_until TEXT,
          last_success TEXT,
          last_error TEXT
        );
        """)
        self.conn.commit()

    def exec(self, sql, args):
        with self.lock:
            self.conn.execute(sql, args)
            self.conn.commit()

    def many(self, sql, rows):
        if not rows:
            return
        with self.lock:
            self.conn.executemany(sql, rows)
            self.conn.commit()

    def event(self, level, worker, entity, msg):
        self.exec(
            "INSERT INTO collector_event VALUES(?,?,?,?,?)",
            (now_iso(), level, worker, entity, str(msg)),
        )


@dataclass
class CircuitState:
    failures: int = 0
    cooldown_until: float = 0.0
    last_success: str | None = None
    last_error: str | None = None

    def can_run(self):
        return time.time() >= self.cooldown_until

    def success(self):
        self.failures = 0
        self.cooldown_until = 0.0
        self.last_success = now_iso()
        self.last_error = None

    def fail(self, threshold, cooldown_s, error):
        self.failures += 1
        self.last_error = str(error)
        if self.failures >= threshold:
            self.cooldown_until = time.time() + cooldown_s

    def state_name(self):
        if time.time() < self.cooldown_until:
            return "COOLDOWN"
        if self.failures > 0:
            return "DEGRADED"
        return "OK"


def save_health(store, worker, entity, state):
    cd = None
    if state.cooldown_until > time.time():
        cd = datetime.fromtimestamp(
            state.cooldown_until
        ).astimezone().isoformat(timespec="seconds")

    store.exec(
        "INSERT INTO worker_health VALUES(?,?,?,?,?,?,?,?)",
        (
            now_iso(),
            worker,
            entity,
            state.state_name(),
            state.failures,
            cd,
            state.last_success,
            state.last_error,
        ),
    )


def queue_worker(
    stop_event,
    ps,
    q,
    store,
    raw_jsonl,
    interval_s,
    timeout_s,
    retry,
    breaker_threshold,
    cooldown_s,
):
    worker_name = "queue"
    entity = f"inst{q['instancia']}:{q['codigo']}"
    state = CircuitState()
    next_run = time.monotonic()

    while not stop_event.is_set():
        now = time.monotonic()
        if now < next_run:
            stop_event.wait(min(next_run - now, 0.5))
            continue

        if not state.can_run():
            save_health(store, worker_name, entity, state)
            next_run = time.monotonic() + interval_s
            continue

        scheduled = next_run
        next_run += interval_s

        params = {
            "pCodeQueue": q["codigo"],
            "pIdProject": q["projeto_id"],
        }
        ref = (
            urljoin(ps.base, "metrics/dashboardqueuedetail")
            + f"?pCodeQueue={q['codigo']}&pIdProject={q['projeto_id']}"
        )

        try:
            d1, l1, _ = ps.get_json(
                "metrics/dashboardqueuedetail/getqueuedetaildata",
                params,
                ref,
                timeout=timeout_s,
                retries=retry,
            )
            d2, l2, _ = ps.get_json(
                "metrics/dashboardqueuedetail/getcallstatisticsdata",
                params,
                ref,
                timeout=timeout_s,
                retries=retry,
            )

            st = extract_status(d2)
            ts = now_iso()
            lag = max(0.0, time.monotonic() - scheduled)

            row = (
                ts,
                q["instancia"],
                q["projeto_id"],
                q["codigo"],
                q["nome"],
                q["motivo_prioridade"],
                st[34][0],
                st[34][1],
                st[135][0],
                st[135][1],
                int(qfield(d1, "QtAgentsAvailable", default=0) or 0),
                int(qfield(d1, "QtAgentsTalking", default=0) or 0),
                int(qfield(d1, "QtAgentsAcw", default=0) or 0),
                int(qfield(d1, "QtAgentsPaused", default=0) or 0),
                int(qfield(d1, "QtAgentsOther", default=0) or 0),
                int(qfield(d1, "QtAgentsDialing", default=0) or 0),
                int(qfield(d1, "QtCallsDialing", default=0) or 0),
                int(qfield(d1, "QtCallsInAttend", default=0) or 0),
                int(qfield(d1, "QtCallsLast5", default=0) or 0),
                parse_num(qfield(d1, "PercHitRateLast5", default=0)),
                parse_num(qfield(d1, "PercHitRateLast15", default=0)),
                parse_num(qfield(d1, "VlRatioCalls", default=0)),
                parse_num(
                    qfield(
                        d1,
                        "QtMailingOnScreen",
                        "QtGetBufferTelephone",
                        default=0,
                    )
                ),
                str(qfield(d1, "DeDsState", default="")),
                str(qfield(d1, "DeIsActiveDs", default="")),
                str(qfield(d1, "DeIsPausedDs", default="")),
                l1,
                l2,
                round(lag, 3),
                json.dumps(d1, ensure_ascii=False),
                json.dumps(d2, ensure_ascii=False),
            )

            store.exec(
                "INSERT INTO queue_snapshot VALUES("
                + ",".join(["?"] * 31)
                + ")",
                row,
            )

            with raw_jsonl.open("a", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "ts": ts,
                            "type": "queue",
                            "queue": q,
                            "queue_detail": d1,
                            "stats": d2,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

            state.success()
            save_health(store, worker_name, entity, state)

            print(
                f"[FILA] {q['codigo']} "
                f"P34={st[34][0]:.2f}% "
                f"P135={st[135][0]:.2f}% "
                f"({l1+l2:.2f}s)"
            )

        except Exception as exc:
            state.fail(
                breaker_threshold,
                cooldown_s,
                exc,
            )
            store.event("ERROR", worker_name, entity, exc)
            save_health(store, worker_name, entity, state)

            print(
                f"[FILA] {q['codigo']} ERRO "
                f"({state.failures}/{breaker_threshold}) "
                f"{type(exc).__name__}: {exc}"
            )

            # Evita fila "atrasada" tentar recuperar ciclos passados.
            if next_run < time.monotonic():
                next_run = time.monotonic() + interval_s


def infra_entities(doc, source):
    if source == "gateway":
        arr = list_from(
            doc,
            "gatewayDataList",
            "GatewayDataList",
            "gatewayList",
            "Data",
            "data",
        )
        if not arr and isinstance(doc, list):
            arr = doc

        for x in arr:
            yield (
                str(x.get("NmGateway") or "").strip().upper(),
                parse_num(x.get("QtValidsTotal")),
                parse_num(x.get("QtValidsBusy")),
                parse_num(x.get("QtValidsIdle")),
                parse_num(x.get("VlPercValidsBusy")),
                parse_num(x.get("QtInvalidsTotal")),
            )
    else:
        arr = list_from(
            doc,
            "trunkDataList",
            "TrunkDataList",
            "trunkList",
            "Data",
            "data",
        )
        if not arr and isinstance(doc, list):
            arr = doc

        for x in arr:
            yield (
                str(
                    x.get("DeNmTrunkGroup")
                    or x.get("NmTrunkGroup")
                    or ""
                ),
                parse_num(x.get("QtValidsTotal")),
                parse_num(x.get("QtValidsBusy")),
                parse_num(x.get("QtValidsIdle")),
                parse_num(x.get("VlPercValidsBusy")),
                parse_num(x.get("QtInvalidsTotal")),
            )


def infra_worker(
    stop_event,
    ps,
    store,
    interval_s,
    timeout_s,
    breaker_threshold,
    cooldown_s,
):
    worker_name = "infra"
    entity = f"inst{ps.inst}"
    state = CircuitState()
    next_run = time.monotonic()

    while not stop_event.is_set():
        now = time.monotonic()
        if now < next_run:
            stop_event.wait(min(next_run - now, 0.5))
            continue

        next_run = time.monotonic() + interval_s

        if not state.can_run():
            save_health(store, worker_name, entity, state)
            continue

        try:
            ts = now_iso()

            for source, path in [
                ("gateway", "metrics/dashboardtrunk/getgatewaydata"),
                ("trunk", "metrics/dashboardtrunk/gettrunkdata"),
            ]:
                doc, lat, _ = ps.get_json(
                    path,
                    referer=urljoin(ps.base, "metrics/dashboardtrunk"),
                    timeout=timeout_s,
                    retries=0,
                )

                rows = []
                for ent, total, busy, idle, occ, invalid in infra_entities(
                    doc, source
                ):
                    rows.append(
                        (
                            ts,
                            ps.inst,
                            source,
                            ent,
                            total,
                            busy,
                            idle,
                            occ,
                            invalid,
                            lat,
                            json.dumps(doc, ensure_ascii=False),
                        )
                    )

                if not rows:
                    rows = [
                        (
                            ts,
                            ps.inst,
                            source,
                            "__RAW__",
                            0,
                            0,
                            0,
                            0,
                            0,
                            lat,
                            json.dumps(doc, ensure_ascii=False),
                        )
                    ]

                store.many(
                    "INSERT INTO infra_snapshot VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    rows,
                )

            state.success()
            save_health(store, worker_name, entity, state)

        except Exception as exc:
            state.fail(
                breaker_threshold,
                cooldown_s,
                exc,
            )
            store.event("ERROR", worker_name, entity, exc)
            save_health(store, worker_name, entity, state)


def slow_worker(
    stop_event,
    ps,
    q,
    store,
    kind,
    interval_s,
    timeout_s,
    breaker_threshold,
    cooldown_s,
):
    worker_name = kind
    entity = f"inst{q['instancia']}:{q['codigo']}"
    state = CircuitState()
    next_run = time.monotonic()

    if kind == "dialer":
        path = "metrics/dashboardqueuedetail/getdialerstatisticsdata"
        table = "dialer_snapshot"
    else:
        path = "metrics/dashboardqueuedetail/getlastcallsdata"
        table = "lastcalls_snapshot"

    while not stop_event.is_set():
        now = time.monotonic()
        if now < next_run:
            stop_event.wait(min(next_run - now, 0.5))
            continue

        next_run = time.monotonic() + interval_s

        if not state.can_run():
            save_health(store, worker_name, entity, state)
            continue

        params = {
            "pCodeQueue": q["codigo"],
            "pIdProject": q["projeto_id"],
        }
        ref = (
            urljoin(ps.base, "metrics/dashboardqueuedetail")
            + f"?pCodeQueue={q['codigo']}&pIdProject={q['projeto_id']}"
        )

        try:
            doc, lat, _ = ps.get_json(
                path,
                params,
                ref,
                timeout=timeout_s,
                retries=0,
            )

            store.exec(
                f"INSERT INTO {table} VALUES(?,?,?,?,?,?,?)",
                (
                    now_iso(),
                    q["instancia"],
                    q["projeto_id"],
                    q["codigo"],
                    q["nome"],
                    lat,
                    json.dumps(doc, ensure_ascii=False),
                ),
            )

            state.success()
            save_health(store, worker_name, entity, state)

        except Exception as exc:
            state.fail(
                breaker_threshold,
                cooldown_s,
                exc,
            )
            store.event("ERROR", worker_name, entity, exc)
            save_health(store, worker_name, entity, state)


def read_stuck(status_path, inst, store):
    p = Path(status_path)
    if not p.exists():
        return

    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        x = (doc.get("canais_travados") or {}).get(str(inst))
        qty = None

        if isinstance(x, dict):
            for k in ("quantidade", "qtd", "atual"):
                if k in x:
                    qty = int(x[k])
                    break

        store.exec(
            "INSERT INTO stuck_channels_snapshot VALUES(?,?,?,?,?)",
            (
                now_iso(),
                str(inst),
                "OK" if x is not None else "SEM_DADO",
                qty,
                json.dumps(x, ensure_ascii=False),
            ),
        )

    except Exception as exc:
        store.event("ERROR", "stuck", f"inst{inst}", exc)


def stuck_worker(stop_event, insts, status_path, store, interval_s):
    while not stop_event.is_set():
        for inst in insts:
            read_stuck(status_path, inst, store)
        stop_event.wait(interval_s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="toi_demo_scope_v1.json")
    ap.add_argument("--duracao-min", type=float, default=15)

    ap.add_argument("--fila-seg", type=float, default=10)
    ap.add_argument("--infra-seg", type=float, default=15)
    ap.add_argument("--lastcalls-seg", type=float, default=60)
    ap.add_argument("--discador-seg", type=float, default=180)
    ap.add_argument("--stuck-seg", type=float, default=60)

    ap.add_argument("--timeout-fila", type=int, default=20)
    ap.add_argument("--timeout-infra", type=int, default=20)
    ap.add_argument("--timeout-lastcalls", type=int, default=25)
    ap.add_argument("--timeout-discador", type=int, default=90)

    ap.add_argument("--retry-fila", type=int, default=1)
    ap.add_argument("--breaker-falhas", type=int, default=3)
    ap.add_argument("--cooldown-seg", type=int, default=60)

    ap.add_argument(
        "--status-canais",
        default="dados/status_portal_orbe.json",
    )
    ap.add_argument(
        "--saida",
        default="dados_toi/demo_v3",
    )
    args = ap.parse_args()

    load_dotenv()
    user = (os.getenv("ORBE_LOGIN") or "").strip()
    pwd = os.getenv("ORBE_PASSWORD") or ""
    base = (os.getenv("ORBE_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")

    if not user or not pwd:
        print("ERRO: ORBE_LOGIN/ORBE_PASSWORD ausentes")
        return 2

    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    queues = cfg["queues"]
    insts = sorted(
        {str(q["instancia"]) for q in queues},
        key=int,
    )

    out = Path(args.saida) / datetime.now().strftime("%Y%m%d_%H%M%S")
    out.mkdir(parents=True, exist_ok=True)

    db_path = out / "toi_demo.sqlite"
    raw_path = out / "timeline_raw.jsonl"
    store = Store(str(db_path))

    print("=" * 96)
    print("TOI - COLETOR TEMPORAL MULTI-FILA V3 - RESILIENTE / SOMENTE LEITURA")
    print("=" * 96)
    print(f"Filas       : {len(queues)}")
    print(f"Instâncias  : {','.join(insts)}")
    print(f"Duração     : {args.duracao_min} min")
    print(f"Fila        : {args.fila_seg}s")
    print(f"Infra       : {args.infra_seg}s")
    print(f"Last Calls  : {args.lastcalls_seg}s")
    print(f"Discador    : {args.discador_seg}s")
    print(f"Breaker     : {args.breaker_falhas} falhas / cooldown {args.cooldown_seg}s")
    print(f"SQLite      : {db_path}")
    print()
    print("SEGURANÇA:")
    print("- único POST por instância: autenticação;")
    print("- após login: SOMENTE GET;")
    print("- sem Telegram, Ticketdesk, limpeza ou DynamicQuery.")
    print()

    sessions = {}

    for inst in insts:
        print(f"[LOGIN] Instância {inst}...")
        ps = PortalSession(
            inst,
            user,
            pwd,
            base,
            timeout=max(
                args.timeout_fila,
                args.timeout_infra,
            ),
        )
        ps.auth()
        sessions[inst] = ps
        print("        OK")

    stop_event = threading.Event()
    threads = []

    # Worker independente por fila.
    for q in queues:
        t = threading.Thread(
            target=queue_worker,
            args=(
                stop_event,
                sessions[str(q["instancia"])],
                q,
                store,
                raw_path,
                args.fila_seg,
                args.timeout_fila,
                args.retry_fila,
                args.breaker_falhas,
                args.cooldown_seg,
            ),
            daemon=True,
            name=f"queue-{q['codigo']}",
        )
        t.start()
        threads.append(t)

    # Infra independente por instância.
    for inst in insts:
        t = threading.Thread(
            target=infra_worker,
            args=(
                stop_event,
                sessions[inst],
                store,
                args.infra_seg,
                args.timeout_infra,
                args.breaker_falhas,
                args.cooldown_seg,
            ),
            daemon=True,
            name=f"infra-{inst}",
        )
        t.start()
        threads.append(t)

    # Detalhes pesados apenas onde full_detail=True.
    for q in queues:
        if not q.get("full_detail"):
            continue

        t1 = threading.Thread(
            target=slow_worker,
            args=(
                stop_event,
                sessions[str(q["instancia"])],
                q,
                store,
                "lastcalls",
                args.lastcalls_seg,
                args.timeout_lastcalls,
                args.breaker_falhas,
                args.cooldown_seg,
            ),
            daemon=True,
            name=f"lastcalls-{q['codigo']}",
        )
        t1.start()
        threads.append(t1)

        t2 = threading.Thread(
            target=slow_worker,
            args=(
                stop_event,
                sessions[str(q["instancia"])],
                q,
                store,
                "dialer",
                args.discador_seg,
                args.timeout_discador,
                args.breaker_falhas,
                args.cooldown_seg * 2,
            ),
            daemon=True,
            name=f"dialer-{q['codigo']}",
        )
        t2.start()
        threads.append(t2)

    # Canais travados.
    ts = threading.Thread(
        target=stuck_worker,
        args=(
            stop_event,
            insts,
            args.status_canais,
            store,
            args.stuck_seg,
        ),
        daemon=True,
        name="stuck",
    )
    ts.start()
    threads.append(ts)

    started = time.time()
    deadline = started + args.duracao_min * 60

    try:
        while time.time() < deadline:
            remaining = max(0, deadline - time.time())
            time.sleep(min(1.0, remaining))
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário.")
    finally:
        stop_event.set()

        print()
        print("Aguardando finalização dos workers...")
        for t in threads:
            t.join(timeout=5)

        # Resumo rápido do banco.
        with store.lock:
            cur = store.conn.cursor()
            counts = {}
            for table in [
                "queue_snapshot",
                "infra_snapshot",
                "dialer_snapshot",
                "lastcalls_snapshot",
                "stuck_channels_snapshot",
                "collector_event",
            ]:
                counts[table] = cur.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]

            errs = cur.execute(
                "SELECT COUNT(*) FROM collector_event WHERE level='ERROR'"
            ).fetchone()[0]

            qstats = cur.execute("""
                SELECT instancia,codigo,COUNT(*) n,
                       ROUND(AVG(latency_queue+latency_stats),2) avg_latency,
                       ROUND(MAX(latency_queue+latency_stats),2) max_latency
                FROM queue_snapshot
                GROUP BY instancia,codigo
                ORDER BY CAST(instancia AS INTEGER),codigo
            """).fetchall()

        print()
        print("=" * 96)
        print("RESUMO V3")
        print("=" * 96)
        print(f"Queue snapshots : {counts['queue_snapshot']}")
        print(f"Infra rows      : {counts['infra_snapshot']}")
        print(f"Discador        : {counts['dialer_snapshot']}")
        print(f"Last calls      : {counts['lastcalls_snapshot']}")
        print(f"Canais travados : {counts['stuck_channels_snapshot']}")
        print(f"Erros registrados: {errs}")
        print()
        print("AMOSTRAS POR FILA:")
        for inst, code, n, avg_l, max_l in qstats:
            print(
                f"Inst {inst} | {code} | n={n} | "
                f"latência média={avg_l}s | máx={max_l}s"
            )

        print()
        print(f"Banco: {db_path}")
        print(f"Raw  : {raw_path}")
        print("✅ Nenhuma ação corretiva foi executada.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
