#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""TOI Live Collector v1.10.6 — Dual-Lane / Deadline Coalescing / Protected Slow-Path.

Motivação
=========
A homologação mostrou:
- fidelidade Portal -> TOI = 100%;
- Instância 6 com cadência saudável;
- Instância 4 sensível a concorrência;
- fast-concurrency=1 reduz drasticamente a duração individual dos GETs,
  porém múltiplas threads serializadas podem gerar starvation e gaps.

A v1.10.6 divide a Instância 4 em DUAS lanes de aquisição independentes,
compartilhando apenas a autenticação/cookies. Cada lane possui seu próprio
dispatcher EDF e aplica deadline coalescing para nunca formar backlog.
Infra e Discador ficam em lanes de background protegidas pela freshness
do fast-path, de modo que uma fonte lenta não bloqueie ALFA_COB_2/ALFA_COB_3.

Princípios
==========
1. Duas lanes de fila na Instância 4; nenhuma fila executa em mais de uma lane.
2. EDF absoluto + deadline coalescing: slots vencidos são descartados, nunca acumulados.
3. ALFA_COB_2 e ALFA_COB_3 recebem SLA mais curto e ficam em lanes diferentes.
4. Infra e Discador usam lanes independentes e só rodam com headroom do fast-path.
5. Discador é background serial/protegido; pode ser habilitado por fila sem obrigar Last Calls.
6. Nenhuma mudança em parsing/fórmulas homologados.
7. Segurança preservada: sem Telegram, Ticketdesk, limpeza ou Confirmar canais.
8. ML não atua aqui: aquisição continua determinística e somente leitura.
"""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
from collections import defaultdict
from contextlib import nullcontext
from pathlib import Path
from urllib.parse import urljoin

from dotenv import load_dotenv

import toi_collector_core_v3 as core
from toi_ipbx_readonly import IPBXStuckReader, env_config


APP = Path(__file__).resolve().parent


def load_project_env():
    for p in [Path.cwd(), APP, APP.parent, APP.parent.parent]:
        env = p / ".env"
        if env.exists():
            load_dotenv(env, override=False)
            return env
    load_dotenv(override=False)
    return None


def add_live_tables(store):
    with store.lock:
        store.conn.executescript("""
        CREATE TABLE IF NOT EXISTS live_heartbeat(
          ts TEXT,
          source TEXT,
          instancia TEXT,
          codigo TEXT,
          status TEXT,
          latency REAL,
          detail TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_live_hb
          ON live_heartbeat(source,instancia,codigo,ts);

        CREATE TABLE IF NOT EXISTS scheduler_health(
          ts TEXT,
          source TEXT,
          instancia TEXT,
          codigo TEXT,
          state TEXT,
          scheduled_interval_s REAL,
          start_lag_s REAL,
          duration_s REAL,
          recovery_delay_s REAL,
          detail TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_scheduler_health
          ON scheduler_health(source,instancia,codigo,ts);

        CREATE TABLE IF NOT EXISTS adaptive_dispatcher_health(
          ts TEXT,
          instancia TEXT,
          job_type TEXT,
          codigo TEXT,
          target_s REAL,
          age_before_s REAL,
          urgency REAL,
          priority INTEGER,
          duration_s REAL,
          result TEXT,
          p34_pct REAL,
          p135_pct REAL,
          detail TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_adaptive_health
          ON adaptive_dispatcher_health(instancia,job_type,codigo,ts);

        CREATE TABLE IF NOT EXISTS dual_lane_health(
          ts TEXT,
          instancia TEXT,
          lane TEXT,
          codigo TEXT,
          target_s REAL,
          deadline_lag_s REAL,
          age_before_s REAL,
          duration_s REAL,
          coalesced_slots INTEGER,
          result TEXT,
          detail TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_dual_lane_health
          ON dual_lane_health(instancia,lane,codigo,ts);

        CREATE TABLE IF NOT EXISTS slow_path_health(
          ts TEXT,
          instancia TEXT,
          kind TEXT,
          codigo TEXT,
          target_s REAL,
          duration_s REAL,
          result TEXT,
          fast_age_s REAL,
          detail TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_slow_path_health
          ON slow_path_health(instancia,kind,codigo,ts);
        """)
        store.conn.commit()


def heartbeat(store, source, instancia, codigo, status, latency=None, detail=""):
    store.exec(
        "INSERT INTO live_heartbeat VALUES(?,?,?,?,?,?,?)",
        (
            core.now_iso(), str(source), str(instancia), str(codigo or ""),
            str(status), latency, str(detail or "")
        )
    )


def scheduler_event(
    store, source, instancia, codigo, state,
    interval_s, start_lag_s, duration_s, recovery_delay_s=0.0, detail=""
):
    store.exec(
        "INSERT INTO scheduler_health VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            core.now_iso(), str(source), str(instancia), str(codigo or ""),
            str(state), float(interval_s), round(float(start_lag_s), 3),
            round(float(duration_s), 3), round(float(recovery_delay_s), 3),
            str(detail or "")
        )
    )


def adaptive_event(
    store, instancia, job_type, codigo, target_s, age_before_s,
    urgency, priority, duration_s, result, p34=None, p135=None, detail=""
):
    store.exec(
        "INSERT INTO adaptive_dispatcher_health VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            core.now_iso(), str(instancia), str(job_type), str(codigo or ""),
            float(target_s), round(float(age_before_s), 3),
            round(float(urgency), 5), int(priority),
            round(float(duration_s), 3), str(result),
            p34, p135, str(detail or "")
        )
    )


def dual_lane_event(
    store, instancia, lane, codigo, target_s, deadline_lag_s, age_before_s,
    duration_s, coalesced_slots, result, detail=""
):
    store.exec(
        "INSERT INTO dual_lane_health VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (
            core.now_iso(), str(instancia), str(lane), str(codigo or ""),
            float(target_s), round(float(deadline_lag_s), 3),
            round(float(age_before_s), 3), round(float(duration_s), 3),
            int(coalesced_slots or 0), str(result), str(detail or "")
        )
    )


def slow_path_event(
    store, instancia, kind, codigo, target_s, duration_s, result,
    fast_age_s=None, detail=""
):
    store.exec(
        "INSERT INTO slow_path_health VALUES(?,?,?,?,?,?,?,?,?)",
        (
            core.now_iso(), str(instancia), str(kind), str(codigo or ""),
            float(target_s), round(float(duration_s), 3), str(result),
            None if fast_age_s is None else round(float(fast_age_s), 3),
            str(detail or "")
        )
    )


class FastPathRegistry:
    """Estado em memória usado somente para proteger workers de background."""
    def __init__(self):
        self._lock = threading.Lock()
        self._rows = {}

    def seed(self, instancia, codigo, target_s, max_age_s):
        now = time.monotonic()
        with self._lock:
            self._rows[(str(instancia), str(codigo))] = {
                "last_success": now - float(target_s),
                "target_s": float(target_s),
                "max_age_s": float(max_age_s),
            }

    def success(self, instancia, codigo, when=None):
        key=(str(instancia),str(codigo))
        with self._lock:
            row=self._rows.get(key)
            if row is not None:
                row["last_success"] = float(when if when is not None else time.monotonic())

    def worst_age(self, instancia, codes=None):
        now=time.monotonic(); wanted=set(str(x) for x in codes) if codes else None
        ages=[]
        with self._lock:
            for (inst,code),row in self._rows.items():
                if inst!=str(instancia) or (wanted is not None and code not in wanted):
                    continue
                ages.append(max(0.0, now-row["last_success"]))
        return max(ages) if ages else None

    def healthy_for_background(self, instancia, max_fast_age_s, codes=None):
        age=self.worst_age(instancia, codes=codes)
        return (True, age) if age is None else (age <= float(max_fast_age_s), age)


class PortalLane:
    """Semáforo para instâncias que continuam no scheduler tradicional."""
    def __init__(self, ps, semaphore, lane_name):
        self._ps = ps
        self._sem = semaphore
        self.lane_name = lane_name

    @property
    def base(self):
        return self._ps.base

    @property
    def inst(self):
        return self._ps.inst

    def get_json(self, *args, **kwargs):
        with self._sem:
            return self._ps.get_json(*args, **kwargs)


class PortalNamedLane:
    """
    Serializa apenas as leituras pertencentes a UMA lane lógica.

    O RLock permite que uma coleta de fila mantenha queue+stats como uma
    transação temporal única, ao mesmo tempo em que get_json continua seguro.
    Cada lane possui lock próprio. As lanes compartilham a mesma autenticação/cookies,
    porém podem executar GETs simultaneamente. Não há login adicional.
    """
    def __init__(self, ps, lane_name="lane"):
        self.lane_name = str(lane_name)
        self._ps = ps
        self._lock = threading.RLock()

    @property
    def base(self):
        return self._ps.base

    @property
    def inst(self):
        return self._ps.inst

    def transaction(self):
        return self._lock

    def get_json(self, *args, **kwargs):
        with self._lock:
            return self._ps.get_json(*args, **kwargs)


def wait_initial(stop_event, delay_s):
    if delay_s <= 0:
        return not stop_event.is_set()
    return not stop_event.wait(delay_s)


def grouped_queues(queues):
    by = defaultdict(list)
    for q in queues:
        by[str(q["instancia"])].append(q)
    return by


def merge_catalog_queues(curated, catalog):
    queues = list(curated)
    known = {(str(q["instancia"]), str(q["codigo"])) for q in queues}
    for item in catalog.get("queues", []):
        inst = str(item.get("instancia", "")).strip()
        code = str(item.get("codigo", "")).strip()
        project = str(item.get("projeto_id", "")).strip()
        if not (inst.isdigit() and code.isdigit() and project.isdigit()):
            continue
        if (inst, code) in known:
            continue
        queues.append({"instancia": inst, "codigo": code,
                       "projeto_id": project, "nome": str(item.get("nome") or code),
                       "motivo_prioridade": "portal_catalog", "full_detail": False,
                       "dialer_detail": False, "lastcalls_detail": False})
        known.add((inst, code))
    return queues


def catalog_policy(queues):
    """Spread inventory polls across five minutes without starving curated queues."""
    count = max(1, len(queues))
    return {"queue_timeout_s": 12, "queue_retries": 0,
            "error_retry_s": 15, "cooldown_s": 60,
            "queues": {q["codigo"]: {"target_s": 300, "max_age_s": 900,
                "priority": 10, "phase_s": i * 300 / count}
                for i, q in enumerate(queues)}}


def queue_offsets(queues_for_inst, interval_s):
    n = max(1, len(queues_for_inst))
    step = float(interval_s) / n
    return {
        q["codigo"]: round(i * step, 3)
        for i, q in enumerate(queues_for_inst)
    }


def slow_offsets(full_queues, interval_s, base_delay):
    n = max(1, len(full_queues))
    usable = max(1.0, float(interval_s) - float(base_delay))
    step = usable / n
    return {
        q["codigo"]: round(float(base_delay) + i * step, 3)
        for i, q in enumerate(full_queues)
    }


def queue_worker_legacy(
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
    initial_delay_s,
    overrun_recovery_s,
):
    """Scheduler v1.10.2 preservado para instâncias NÃO adaptativas."""
    worker_name = "queue"
    entity = f"inst{q['instancia']}:{q['codigo']}"
    state = core.CircuitState()

    if not wait_initial(stop_event, initial_delay_s):
        return

    next_run = time.monotonic()

    while not stop_event.is_set():
        now = time.monotonic()
        if now < next_run:
            stop_event.wait(min(next_run - now, 0.25))
            continue

        scheduled = next_run
        start = time.monotonic()
        start_lag = max(0.0, start - scheduled)

        if not state.can_run():
            core.save_health(store, worker_name, entity, state)
            next_run = time.monotonic() + interval_s
            scheduler_event(
                store, "queue", q["instancia"], q["codigo"], "COOLDOWN",
                interval_s, start_lag, 0.0, interval_s,
                f"falhas={state.failures}"
            )
            continue

        params = {"pCodeQueue": q["codigo"], "pIdProject": q["projeto_id"]}
        ref = (
            urljoin(ps.base, "metrics/dashboardqueuedetail")
            + f"?pCodeQueue={q['codigo']}&pIdProject={q['projeto_id']}"
        )
        ok = False
        err_text = ""
        l1 = l2 = 0.0

        try:
            d1, l1, _ = ps.get_json(
                "metrics/dashboardqueuedetail/getqueuedetaildata",
                params, ref, timeout=timeout_s, retries=retry
            )
            d2, l2, _ = ps.get_json(
                "metrics/dashboardqueuedetail/getcallstatisticsdata",
                params, ref, timeout=timeout_s, retries=retry
            )
            write_queue_snapshot(
                store, raw_jsonl, q, d1, d2,
                l1, l2, start_lag,
                scheduler_meta={
                    "mode": "legacy",
                    "initial_delay_s": initial_delay_s,
                    "start_lag_s": round(start_lag, 3),
                    "duration_s": round(time.monotonic()-start, 3),
                }
            )
            st = core.extract_status(d2)
            state.success()
            core.save_health(store, worker_name, entity, state)
            ok = True
            print(
                f"[FILA] {q['codigo']} "
                f"P34={st[34][0]:.2f}% P135={st[135][0]:.2f}% "
                f"GET={l1+l2:.2f}s lag={start_lag:.2f}s"
            )

        except Exception as exc:
            err_text = f"{type(exc).__name__}: {exc}"
            state.fail(breaker_threshold, cooldown_s, exc)
            store.event("ERROR", worker_name, entity, exc)
            core.save_health(store, worker_name, entity, state)
            print(f"[FILA] {q['codigo']} ERRO {err_text}")

        finish = time.monotonic()
        nominal_next = scheduled + interval_s
        recovery = 0.0
        state_name = "OK" if ok else ("ERROR_OVERRUN" if finish >= nominal_next else "ERROR")
        if not ok:
            # Uma falha curta não deve esperar o ciclo inteiro. O breaker
            # continua limitando tentativas durante indisponibilidade longa.
            recovery = min(3.0, max(1.0, interval_s / 4.0))
            next_run = finish + recovery
        elif finish < nominal_next:
            next_run = nominal_next
        else:
            recovery = min(max(0.5, overrun_recovery_s), interval_s/2.0)
            next_run = finish + recovery
            state_name = "OVERRUN" if ok else "ERROR_OVERRUN"

        scheduler_event(
            store, "queue", q["instancia"], q["codigo"], state_name,
            interval_s, start_lag, finish-start, recovery,
            f"get={l1+l2:.3f}s" if ok else err_text
        )


def write_queue_snapshot(
    store, raw_jsonl, q, d1, d2, l1, l2, cycle_lag_s,
    scheduler_meta=None
):
    st = core.extract_status(d2)
    ts = core.now_iso()

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
        int(core.qfield(d1, "QtAgentsAvailable", default=0) or 0),
        int(core.qfield(d1, "QtAgentsTalking", default=0) or 0),
        int(core.qfield(d1, "QtAgentsAcw", default=0) or 0),
        int(core.qfield(d1, "QtAgentsPaused", default=0) or 0),
        int(core.qfield(d1, "QtAgentsOther", default=0) or 0),
        int(core.qfield(d1, "QtAgentsDialing", default=0) or 0),
        int(core.qfield(d1, "QtCallsDialing", default=0) or 0),
        int(core.qfield(d1, "QtCallsInAttend", default=0) or 0),
        int(core.qfield(d1, "QtCallsLast5", default=0) or 0),
        core.parse_num(core.qfield(d1, "PercHitRateLast5", default=0)),
        core.parse_num(core.qfield(d1, "PercHitRateLast15", default=0)),
        core.parse_num(core.qfield(d1, "VlRatioCalls", default=0)),
        core.parse_num(
            core.qfield(d1, "QtMailingOnScreen", "QtGetBufferTelephone", default=0)
        ),
        str(core.qfield(d1, "DeDsState", default="")),
        str(core.qfield(d1, "DeIsActiveDs", default="")),
        str(core.qfield(d1, "DeIsPausedDs", default="")),
        l1,
        l2,
        round(float(cycle_lag_s), 3),
        json.dumps(d1, ensure_ascii=False),
        json.dumps(d2, ensure_ascii=False),
    )
    store.exec(
        "INSERT INTO queue_snapshot VALUES(" + ",".join(["?"]*31) + ")",
        row
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
                    "scheduler": scheduler_meta or {},
                },
                ensure_ascii=False
            ) + "\n"
        )

    return st


def collect_queue_once(ps, q, store, raw_jsonl, timeout_s, retry, scheduler_meta):
    params = {"pCodeQueue": q["codigo"], "pIdProject": q["projeto_id"]}
    ref = (
        urljoin(ps.base, "metrics/dashboardqueuedetail")
        + f"?pCodeQueue={q['codigo']}&pIdProject={q['projeto_id']}"
    )
    t0 = time.monotonic()
    tx = ps.transaction() if hasattr(ps, "transaction") else nullcontext()
    # queue detail + call statistics pertencem ao MESMO snapshot lógico.
    # Na lane exclusiva nenhum outro endpoint da Instância 4 entra entre eles.
    with tx:
        d1, l1, _ = ps.get_json(
            "metrics/dashboardqueuedetail/getqueuedetaildata",
            params, ref, timeout=timeout_s, retries=retry
        )
        d2, l2, _ = ps.get_json(
            "metrics/dashboardqueuedetail/getcallstatisticsdata",
            params, ref, timeout=timeout_s, retries=retry
        )
    duration = time.monotonic() - t0
    meta = dict(scheduler_meta)
    meta.update({
        "latency_queue_s": round(l1, 3),
        "latency_stats_s": round(l2, 3),
        "duration_s": round(duration, 3),
    })
    st = write_queue_snapshot(
        store, raw_jsonl, q, d1, d2, l1, l2, 0.0, meta
    )
    return st, duration, l1, l2


def collect_infra_source_once(ps, store, source, timeout_s):
    path = (
        "metrics/dashboardtrunk/getgatewaydata"
        if source == "gateway"
        else "metrics/dashboardtrunk/gettrunkdata"
    )
    t0 = time.monotonic()
    doc, lat, _ = ps.get_json(
        path,
        referer=urljoin(ps.base, "metrics/dashboardtrunk"),
        timeout=timeout_s,
        retries=0,
    )
    ts = core.now_iso()
    rows = []
    for ent, total, busy, idle, occ, invalid in core.infra_entities(doc, source):
        rows.append(
            (
                ts, ps.inst, source, ent, total, busy, idle,
                occ, invalid, lat, json.dumps(doc, ensure_ascii=False)
            )
        )
    if not rows:
        rows = [
            (
                ts, ps.inst, source, "__RAW__", 0, 0, 0, 0, 0,
                lat, json.dumps(doc, ensure_ascii=False)
            )
        ]
    store.many(
        "INSERT INTO infra_snapshot VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        rows
    )
    return time.monotonic()-t0, lat


class DeadlineDispatcher:
    """EDF com deadlines absolutos, sem deriva fim+target e sem catch-up storm."""
    def __init__(
        self, stop_event, ps, queues, store, raw_jsonl, policy,
        timeout_queue, retry, breaker_threshold, cooldown_s,
        lane_id="A", registry=None
    ):
        self.stop = stop_event
        self.ps = ps
        self.queues = {q["codigo"]: q for q in queues}
        self.store = store
        self.raw = raw_jsonl
        self.policy = policy
        self.timeout_queue = float(policy.get("queue_timeout_s", timeout_queue))
        self.retry = int(policy.get("queue_retries", retry))
        self.breaker_threshold = breaker_threshold
        self.cooldown_s = float(policy.get("cooldown_s", cooldown_s))
        self.error_retry_s = float(policy.get("error_retry_s", 15))
        self.lane_id = str(lane_id)
        self.registry = registry

        now = time.monotonic()
        self.state = {}
        qpol = policy.get("queues", {})
        for code, q in self.queues.items():
            p = qpol.get(code, {})
            target = float(p.get("target_s", 20))
            max_age = float(p.get("max_age_s", max(40, target*2)))
            priority = int(p.get("priority", 50))
            phase = float(p.get("phase_s", 0.0))
            self.state[code] = {
                "target": target,
                "max_age": max_age,
                "priority": priority,
                "phase": phase,
                "next_deadline": now + phase,
                "last_success": now - target,
                "last_p34": 0.0,
                "last_p135": 0.0,
                "ewma": 2.5,
                "errors": 0,
                "cooldown_until": 0.0,
            }
            if self.registry is not None:
                self.registry.seed(self.ps.inst, code, target, max_age)

    @staticmethod
    def _coalesce_due(state, now):
        """Move deadline antigo para o slot vencido mais recente; retorna slots descartados."""
        target=max(1.0,state["target"])
        if now < state["next_deadline"] + target:
            return 0
        skipped=int((now-state["next_deadline"])//target)
        if skipped>0:
            state["next_deadline"] += skipped*target
        return skipped

    @staticmethod
    def _advance_deadline(state, after):
        target = max(1.0, state["target"])
        while state["next_deadline"] <= after:
            state["next_deadline"] += target

    @classmethod
    def _schedule_after_attempt(cls, state, after, failed, error_retry_s):
        cls._advance_deadline(state, after)
        if failed:
            # Um erro no catálogo de 5 min precisa de nova tentativa em
            # segundos; nenhum valor é inventado para preencher a lacuna.
            retry_at = max(state["cooldown_until"], after + max(1.0, error_retry_s))
            state["next_deadline"] = min(state["next_deadline"], retry_at)

    def choose_due(self):
        now = time.monotonic()
        due = []
        for code, state in self.state.items():
            if now < state["cooldown_until"]:
                continue
            coalesced=self._coalesce_due(state, now)
            if now >= state["next_deadline"]:
                lateness = max(0.0, now - state["next_deadline"])
                # EDF primeiro; prioridade somente no desempate.
                due.append((state["next_deadline"], -state["priority"], code, lateness, coalesced))
        if not due:
            return None
        due.sort()
        _, neg_prio, code, lateness, coalesced = due[0]
        state = self.state[code]
        age = max(0.0, now - state["last_success"])
        urgency = age/max(1.0, state["target"]) + state["priority"]/1000.0
        if age >= state["max_age"]:
            urgency += 10.0
        return code, lateness, age, urgency, -neg_prio, coalesced

    def sleep_until_next(self):
        now = time.monotonic()
        waits = [max(0.0, max(s["next_deadline"], s["cooldown_until"])-now)
                 for s in self.state.values()]
        self.stop.wait(max(0.03, min(0.40, min(waits) if waits else 0.40)))

    def run(self):
        inst = self.ps.inst
        print(f"\n[DUAL-LANE {self.lane_id}] Instância {inst} — EDF + coalescing iniciado.")
        print(f"[DUAL-LANE {self.lane_id}] timeout={self.timeout_queue:.0f}s retries={self.retry}")
        print(f"[DUAL-LANE {self.lane_id}] Política:")
        for code, state in sorted(self.state.items(), key=lambda kv: (-kv[1]["priority"], kv[0])):
            q = self.queues[code]
            print(
                f"  {code} {q['nome'][:28]:28s} target={state['target']:4.0f}s "
                f"max_age={state['max_age']:4.0f}s phase={state['phase']:4.0f}s prio={state['priority']}"
            )

        while not self.stop.is_set():
            job = self.choose_due()
            if not job:
                self.sleep_until_next()
                continue

            code, deadline_lag, age_before, urgency, priority, coalesced = job
            q = self.queues[code]
            state = self.state[code]
            t0 = time.monotonic()
            result = "OK"
            detail = ""
            try:
                st, duration, lq, ls = collect_queue_once(
                    self.ps, q, self.store, self.raw,
                    self.timeout_queue, self.retry,
                    {
                        "mode": "dual_lane_edf_coalesced",
                        "lane": self.lane_id,
                        "coalesced_slots": int(coalesced),
                        "target_s": state["target"],
                        "max_age_s": state["max_age"],
                        "deadline_lag_s": round(deadline_lag, 3),
                        "age_before_s": round(age_before, 3),
                        "priority": priority,
                    }
                )
                finished = time.monotonic()
                state["last_success"] = finished
                if self.registry is not None:
                    self.registry.success(inst, code, finished)
                state["last_p34"] = float(st[34][0] or 0)
                state["last_p135"] = float(st[135][0] or 0)
                state["ewma"] = 0.75*state["ewma"] + 0.25*duration
                state["errors"] = 0
                detail = (
                    f"queue={lq:.3f}s stats={ls:.3f}s ewma={state['ewma']:.3f}s "
                    f"deadline_lag={deadline_lag:.3f}s"
                )
                adaptive_event(
                    self.store, inst, "queue", code, state["target"],
                    age_before, urgency, priority, duration, result,
                    state["last_p34"], state["last_p135"], detail
                )
                scheduler_event(
                    self.store, "deadline_queue", inst, code, result,
                    state["target"], deadline_lag, duration, 0.0, detail
                )
                dual_lane_event(
                    self.store, inst, self.lane_id, code, state["target"],
                    deadline_lag, age_before, duration, coalesced, result, detail
                )
                print(
                    f"[LANE {self.lane_id}/FILA] {code} P34={state['last_p34']:.2f}% "
                    f"P135={state['last_p135']:.2f}% age={age_before:.1f}s "
                    f"lag={deadline_lag:.1f}s dur={duration:.2f}s"
                )
            except Exception as exc:
                duration = time.monotonic()-t0
                state["errors"] += 1
                result = "ERROR"
                detail = f"{type(exc).__name__}: {exc} | deadline_lag={deadline_lag:.3f}s"
                if state["errors"] >= self.breaker_threshold:
                    state["cooldown_until"] = time.monotonic()+self.cooldown_s
                    result = "COOLDOWN"
                adaptive_event(
                    self.store, inst, "queue", code, state["target"],
                    age_before, urgency, priority, duration, result,
                    state["last_p34"], state["last_p135"], detail
                )
                scheduler_event(
                    self.store, "deadline_queue", inst, code, result,
                    state["target"], deadline_lag, duration, 0.0, detail
                )
                dual_lane_event(
                    self.store, inst, self.lane_id, code, state["target"],
                    deadline_lag, age_before, duration, coalesced, result, detail
                )
                self.store.event("ERROR", "deadline_queue", f"inst{inst}:{code}", exc)
                print(f"[LANE {self.lane_id}/FILA] {code} ERRO: {detail}")
            finally:
                # Avança a grade mesmo em erro: fail-fast não pode monopolizar o EDF.
                self._schedule_after_attempt(
                    state, time.monotonic(), result != "OK", self.error_retry_s
                )


def instance_infra_policy(policy_doc, instancia, default_interval_s=15):
    """Apply common gateway cadence to every instance without changing queue scheduling."""
    defaults=policy_doc.get('infra_defaults') or {}
    specific=(policy_doc.get('instancias',{}).get(str(instancia),{}).get('infra') or {})
    policy={'gateway_target_s':default_interval_s,'trunk_target_s':default_interval_s}
    policy.update(defaults)
    policy.update(specific)
    if 'gateway_phase_s' not in policy:
        policy['gateway_phase_s']=float(int(instancia)%max(1,int(policy['gateway_target_s'])))
    return policy


class AdaptiveInfraWorker:
    """Infra possui scheduler próprio, mas compartilha a lane HTTP exclusiva."""
    def __init__(self, stop_event, ps, store, policy, default_timeout=8, registry=None):
        self.stop = stop_event
        self.ps = ps
        self.store = store
        self.policy = policy or {}
        self.timeout = float(self.policy.get("timeout_s", default_timeout))
        self.max_age = float(self.policy.get("max_age_s", 120))
        self.registry = registry
        self.protect_fast_age = float(self.policy.get("protect_fast_if_age_gt_s", 24))
        self.protect_codes = [str(x) for x in self.policy.get("protect_codes", [])]
        priority = int(self.policy.get("priority", 15))
        now = time.monotonic()
        self.state = {
            "gateway": {
                "target": float(self.policy.get("gateway_target_s", 60)),
                "phase": float(self.policy.get("gateway_phase_s", 12)),
                "priority": priority,
            },
            "trunk": {
                "target": float(self.policy.get("trunk_target_s", 60)),
                "phase": float(self.policy.get("trunk_phase_s", 42)),
                "priority": priority,
            },
        }
        for state in self.state.values():
            state["next_deadline"] = now + state["phase"]
            state["last_success"] = now - state["target"]
            state["ewma"] = 1.0

    @staticmethod
    def _advance(state, after):
        while state["next_deadline"] <= after:
            state["next_deadline"] += max(1.0, state["target"])

    def run(self):
        inst = self.ps.inst
        print(
            f"[INFRA-WORKER] Instância {inst}: independente, lane exclusiva compartilhada, "
            f"gateway={self.state['gateway']['target']:.0f}s trunk={self.state['trunk']['target']:.0f}s"
        )
        while not self.stop.is_set():
            now=time.monotonic()
            due=[(s["next_deadline"],src) for src,s in self.state.items() if now>=s["next_deadline"]]
            if not due:
                wait=min(max(0.0,s["next_deadline"]-now) for s in self.state.values())
                self.stop.wait(max(0.05,min(0.50,wait)))
                continue
            due.sort()
            _,source=due[0]
            state=self.state[source]
            if self.registry is not None:
                healthy,fast_age=self.registry.healthy_for_background(inst,self.protect_fast_age,self.protect_codes or None)
                if not healthy:
                    heartbeat(self.store,"infra_guard",inst,source,"DEFERRED",0.0,f"fast_age={fast_age:.1f}s")
                    # tenta novamente em breve, sem criar backlog de infra
                    state["next_deadline"]=time.monotonic()+min(5.0,max(1.0,state["target"]/10.0))
                    self.stop.wait(0.2)
                    continue
            lag=max(0.0,time.monotonic()-state["next_deadline"])
            age=max(0.0,time.monotonic()-state["last_success"])
            t0=time.monotonic()
            result="OK"
            try:
                duration,lat=collect_infra_source_once(self.ps,self.store,source,self.timeout)
                state["last_success"]=time.monotonic()
                state["ewma"]=0.75*state["ewma"]+0.25*duration
                detail=f"lat={lat:.3f}s ewma={state['ewma']:.3f}s deadline_lag={lag:.3f}s"
                print(f"[INFRA-WORKER] {source} age={age:.1f}s lag={lag:.1f}s dur={duration:.2f}s")
            except Exception as exc:
                duration=time.monotonic()-t0
                result="ERROR"
                detail=f"{type(exc).__name__}: {exc} | deadline_lag={lag:.3f}s"
                self.store.event("ERROR",f"deadline_infra_{source}",f"inst{inst}",exc)
                print(f"[INFRA-WORKER] {source} ERRO: {detail}")
            adaptive_event(
                self.store,inst,f"infra_{source}","",state["target"],age,
                age/max(1.0,state["target"]),state["priority"],duration,result,
                None,None,detail
            )
            scheduler_event(
                self.store,"deadline_infra",inst,source,result,state["target"],lag,duration,0.0,detail
            )
            self._advance(state,time.monotonic())


class ProtectedSlowDispatcher:
    """Slow-path serial/coalescido por instância, protegido pela freshness do fast-path."""
    def __init__(self, stop_event, ps, queues, store, policy, registry,
                 timeout_lastcalls=25, timeout_dialer=90,
                 breaker_threshold=3, cooldown_s=120):
        self.stop=stop_event; self.ps=ps; self.queues=list(queues); self.store=store
        self.policy=policy or {}; self.registry=registry
        self.timeout_last=float(self.policy.get("lastcalls_timeout_s",timeout_lastcalls))
        self.timeout_dial=float(self.policy.get("dialer_timeout_s",timeout_dialer))
        self.protect_fast_age=float(self.policy.get("protect_fast_if_age_gt_s",24))
        self.protect_codes=[str(x) for x in self.policy.get("protect_codes",[])]
        self.defer_step=float(self.policy.get("defer_step_s",5))
        self.breaker_threshold=int(breaker_threshold); self.cooldown_s=float(cooldown_s)
        self.states={}
        now=time.monotonic()
        last_target=float(self.policy.get("lastcalls_s",300))
        dial_target=float(self.policy.get("dialer_s",600))
        # Espalha fases. As fontes podem ser habilitadas independentemente por fila.
        # Isso permite coletar Estatísticas do Discador em todo o escopo sem obrigar
        # a coleta de Last Calls (mais uma fonte pesada) onde ela não é necessária.
        last_i = 0
        dial_i = 0
        for q in self.queues:
            last_enabled = bool(q.get("lastcalls_detail", q.get("full_detail", False)))
            dial_enabled = bool(q.get("dialer_detail", q.get("full_detail", False)))
            if last_enabled:
                self.states[("lastcalls", q["codigo"])]=dict(
                    q=q, target=last_target, next_deadline=now+30+last_i*17,
                    errors=0, cooldown_until=0.0
                )
                last_i += 1
            if dial_enabled:
                self.states[("dialer", q["codigo"])]=dict(
                    q=q, target=dial_target, next_deadline=now+60+dial_i*37,
                    errors=0, cooldown_until=0.0
                )
                dial_i += 1

    @staticmethod
    def _advance(st, after):
        target=max(1.0,float(st["target"]))
        while st["next_deadline"]<=after:
            st["next_deadline"]+=target

    def _collect(self, kind, q, timeout):
        path=("metrics/dashboardqueuedetail/getdialerstatisticsdata" if kind=="dialer"
              else "metrics/dashboardqueuedetail/getlastcallsdata")
        table="dialer_snapshot" if kind=="dialer" else "lastcalls_snapshot"
        params={"pCodeQueue":q["codigo"],"pIdProject":q["projeto_id"]}
        ref=urljoin(self.ps.base,"metrics/dashboardqueuedetail")+f"?pCodeQueue={q['codigo']}&pIdProject={q['projeto_id']}"
        t0=time.monotonic()
        doc,lat,_=self.ps.get_json(path,params,ref,timeout=timeout,retries=0)
        self.store.exec(
            f"INSERT INTO {table} VALUES(?,?,?,?,?,?,?)",
            (core.now_iso(),q["instancia"],q["projeto_id"],q["codigo"],q["nome"],lat,json.dumps(doc,ensure_ascii=False))
        )
        return time.monotonic()-t0,lat

    def run(self):
        inst=self.ps.inst
        if not self.states:
            return
        print(f"[SLOW-PATH] Instância {inst}: background protegido / {len(self.queues)} filas.")
        while not self.stop.is_set():
            now=time.monotonic()
            due=[(st["next_deadline"],0 if kind=="lastcalls" else 1,kind,code,st)
                 for (kind,code),st in self.states.items()
                 if now>=st["next_deadline"] and now>=st["cooldown_until"]]
            if not due:
                nxt=min(st["next_deadline"] for st in self.states.values())
                self.stop.wait(max(0.1,min(1.0,nxt-now)))
                continue
            due.sort(key=lambda x:(x[0],x[1]))
            _,_,kind,code,st=due[0]
            healthy,fast_age=self.registry.healthy_for_background(inst,self.protect_fast_age,self.protect_codes or None) if self.registry else (True,None)
            if not healthy:
                st["next_deadline"]=time.monotonic()+self.defer_step
                slow_path_event(self.store,inst,kind,code,st["target"],0.0,"DEFERRED",fast_age,
                                f"fast-path protegido: age={fast_age:.1f}s")
                self.stop.wait(0.2)
                continue
            q=st["q"]; timeout=self.timeout_dial if kind=="dialer" else self.timeout_last
            t0=time.monotonic(); result="OK"; detail=""
            try:
                dur,lat=self._collect(kind,q,timeout)
                st["errors"]=0
                detail=f"lat={lat:.3f}s"
                heartbeat(self.store,kind,inst,code,"OK",lat,detail)
                print(f"[SLOW/{kind.upper()}] {code} OK {dur:.2f}s")
            except Exception as exc:
                dur=time.monotonic()-t0; st["errors"]+=1; result="ERROR"
                detail=f"{type(exc).__name__}: {exc}"
                if st["errors"]>=self.breaker_threshold:
                    st["cooldown_until"]=time.monotonic()+self.cooldown_s
                    result="COOLDOWN"
                self.store.event("ERROR",f"slow_{kind}",f"inst{inst}:{code}",exc)
                heartbeat(self.store,kind,inst,code,result,dur,detail)
                print(f"[SLOW/{kind.upper()}] {code} {result}: {detail}")
            slow_path_event(self.store,inst,kind,code,st["target"],dur,result,fast_age,detail)
            self._advance(st,time.monotonic())


def delayed_core_worker(
    stop_event, delay_s, store, source_name, instancia, codigo,
    target, *target_args
):
    if not wait_initial(stop_event, delay_s):
        return
    heartbeat(
        store, f"{source_name}_scheduler", instancia, codigo,
        "STARTED", 0.0, f"initial_delay={delay_s:.2f}s"
    )
    try:
        target(*target_args)
    except Exception as exc:
        heartbeat(
            store, source_name, instancia, codigo, "FATAL", None,
            f"{type(exc).__name__}: {exc}"
        )
        store.event("ERROR", source_name, f"inst{instancia}:{codigo}", exc)
        raise


def stuck_direct_worker(stop_event, reader, store, interval_s, initial_delay_s=0.0):
    inst = reader.instancia
    if not wait_initial(stop_event, initial_delay_s):
        return

    next_run = time.monotonic()
    while not stop_event.is_set():
        now = time.monotonic()
        if now < next_run:
            stop_event.wait(min(0.25, next_run-now))
            continue

        scheduled = next_run
        t0 = time.perf_counter()
        try:
            result = reader.consultar()
            latency = round(time.perf_counter()-t0, 3)
            store.exec(
                "INSERT INTO stuck_channels_snapshot VALUES(?,?,?,?,?)",
                (
                    core.now_iso(), inst, "OK", int(result["quantidade"]),
                    json.dumps(result, ensure_ascii=False)
                )
            )
            heartbeat(
                store, "stuck_direct", inst, "", "OK",
                latency, f"qty={result['quantidade']}"
            )
            print(f"[TRAVADOS] inst{inst}={result['quantidade']} ({latency:.2f}s)")
        except Exception as exc:
            latency = round(time.perf_counter()-t0, 3)
            store.exec(
                "INSERT INTO stuck_channels_snapshot VALUES(?,?,?,?,?)",
                (
                    core.now_iso(), inst, "ERRO", None,
                    json.dumps({"erro": str(exc)}, ensure_ascii=False)
                )
            )
            heartbeat(
                store, "stuck_direct", inst, "", "ERROR",
                latency, f"{type(exc).__name__}: {exc}"
            )
            store.event("ERROR", "stuck_direct", f"inst{inst}", exc)
            print(f"[TRAVADOS] inst{inst} ERRO: {exc}")

        finish = time.monotonic()
        nominal_next = scheduled + interval_s
        next_run = nominal_next if finish < nominal_next else finish+0.5


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="toi_demo_scope_v1.json")
    ap.add_argument("--policy", default="toi_adaptive_policy_v1.json")
    ap.add_argument("--db", default="../dados_toi/live/toi_live.sqlite")
    ap.add_argument("--raw", default="../dados_toi/live/timeline_raw.jsonl")

    ap.add_argument("--fila-seg", type=float, default=10)
    ap.add_argument("--infra-seg", type=float, default=15)
    ap.add_argument("--lastcalls-seg", type=float, default=60)
    ap.add_argument("--discador-seg", type=float, default=180)
    ap.add_argument("--stuck-seg", type=float, default=15)

    ap.add_argument("--timeout-fila", type=int, default=12)
    ap.add_argument("--timeout-infra", type=int, default=8)
    ap.add_argument("--timeout-lastcalls", type=int, default=25)
    ap.add_argument("--timeout-discador", type=int, default=90)

    ap.add_argument("--retry-fila", type=int, default=0)
    ap.add_argument("--breaker-falhas", type=int, default=3)
    ap.add_argument("--cooldown-seg", type=int, default=60)

    ap.add_argument("--fast-concurrency", type=int, default=2)
    ap.add_argument("--slow-concurrency", type=int, default=1)
    ap.add_argument("--overrun-recovery-seg", type=float, default=1.0)
    ap.add_argument("--sem-ipbx-direto", action="store_true")
    args = ap.parse_args()

    env_path = load_project_env()
    user = (os.getenv("ORBE_LOGIN") or "").strip()
    pwd = os.getenv("ORBE_PASSWORD") or ""
    base = (os.getenv("ORBE_BASE_URL") or core.DEFAULT_BASE_URL).rstrip("/")

    if not user or not pwd:
        print("ERRO: ORBE_LOGIN/ORBE_PASSWORD ausentes no .env")
        return 2

    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = APP / cfg_path
    policy_path = Path(args.policy)
    if not policy_path.is_absolute():
        policy_path = APP / policy_path

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    policy_doc = json.loads(policy_path.read_text(encoding="utf-8"))
    adaptive_cfg = policy_doc.get("instancias", {})

    # Persist time series for every queue shown by the Portal inventory.
    # Curated settings retain precedence for the original demonstration scope.
    catalog = json.loads((APP / "portal_queue_catalog.json").read_text(encoding="utf-8"))
    queues = merge_catalog_queues(cfg["queues"], catalog)
    by_inst = grouped_queues(queues)
    insts = sorted(by_inst.keys(), key=int)

    db_path = Path(args.db)
    if not db_path.is_absolute():
        db_path = (APP / db_path).resolve()
    raw_path = Path(args.raw)
    if not raw_path.is_absolute():
        raw_path = (APP / raw_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    store = core.Store(str(db_path))
    add_live_tables(store)

    print("="*104)
    print("TOI LIVE COLLECTOR v1.10.6 — DUAL-LANE / DEADLINE COALESCING / PROTECTED SLOW-PATH")
    print("="*104)
    print(f"Config      : {cfg_path}")
    print(f"Policy      : {policy_path}")
    print(f"SQLite LIVE : {db_path}")
    print(f"Raw         : {raw_path}")
    print(f"Instâncias  : {','.join(insts)} | Filas: {len(queues)}")
    print(f".env        : {env_path or 'busca padrão'}")
    print("SEGURANÇA   : sem Telegram, sem Ticketdesk, sem limpeza, sem Confirmar canais.")
    print()

    sessions = {}
    fast_lanes = {}
    slow_lanes = {}
    named_lanes = {}
    registry = FastPathRegistry()

    for inst in insts:
        print(f"[LOGIN ORBE] Instância {inst}...")
        ps = core.PortalSession(
            inst, user, pwd, base,
            timeout=max(args.timeout_fila, args.timeout_infra)
        )
        ps.auth()
        sessions[inst] = ps
        fast_lanes[inst] = PortalLane(
            ps, threading.BoundedSemaphore(max(1, args.fast_concurrency)), "fast"
        )
        slow_lanes[inst] = PortalLane(
            ps, threading.BoundedSemaphore(max(1, args.slow_concurrency)), "slow"
        )
        print("             OK")

    stop_event = threading.Event()
    threads = []
    adaptive_instances = {
        inst for inst, p in adaptive_cfg.items()
        if p.get("enabled") and inst in sessions
    }

    # ---------------------------------------------------------------
    # Queue / infra
    # ---------------------------------------------------------------
    for inst in insts:
        if inst in adaptive_instances:
            ipol=adaptive_cfg[inst]
            dual=ipol.get("dual_lane") or {}
            lanes=(dual.get("lanes") or {}) if dual.get("enabled",True) else {"A":[q["codigo"] for q in by_inst[inst]]}
            assigned=set()
            for lane_id,codes in lanes.items():
                codes=[str(c) for c in codes]
                lane_q=[q for q in by_inst[inst] if q["codigo"] in codes]
                if not lane_q:
                    continue
                overlap=assigned.intersection(q["codigo"] for q in lane_q)
                if overlap:
                    raise RuntimeError(f"Fila em mais de uma lane na instância {inst}: {sorted(overlap)}")
                assigned.update(q["codigo"] for q in lane_q)
                lane=PortalNamedLane(sessions[inst],f"fast-{inst}-{lane_id}")
                named_lanes[(inst,lane_id)]=lane
                dispatcher=DeadlineDispatcher(
                    stop_event,lane,lane_q,store,raw_path,ipol,
                    args.timeout_fila,args.retry_fila,args.breaker_falhas,args.cooldown_seg,
                    lane_id=lane_id,registry=registry
                )
                t=threading.Thread(target=dispatcher.run,daemon=True,name=f"dual-{inst}-{lane_id}")
                t.start();threads.append(t)
            missing={q["codigo"] for q in by_inst[inst]}-assigned
            if missing:
                inventory=[q for q in by_inst[inst] if q["codigo"] in missing]
                lane=PortalNamedLane(sessions[inst],f"catalog-{inst}")
                dispatcher=DeadlineDispatcher(stop_event,lane,inventory,store,raw_path,
                    catalog_policy(inventory),args.timeout_fila,args.retry_fila,
                    args.breaker_falhas,args.cooldown_seg,lane_id="CATALOG",registry=registry)
                t=threading.Thread(target=dispatcher.run,daemon=True,name=f"catalog-{inst}")
                t.start();threads.append(t)

        else:
            curated=[q for q in by_inst[inst] if q.get("motivo_prioridade") != "portal_catalog"]
            inventory=[q for q in by_inst[inst] if q.get("motivo_prioridade") == "portal_catalog"]
            offsets = queue_offsets(curated, args.fila_seg)
            for q in curated:
                delay = offsets[q["codigo"]]
                t = threading.Thread(
                    target=queue_worker_legacy,
                    args=(
                        stop_event, fast_lanes[inst], q, store, raw_path,
                        args.fila_seg, args.timeout_fila, args.retry_fila,
                        args.breaker_falhas, args.cooldown_seg,
                        delay, args.overrun_recovery_seg
                    ),
                    daemon=True,
                    name=f"queue-{q['codigo']}"
                )
                t.start()
                threads.append(t)

            if inventory:
                lane=PortalNamedLane(sessions[inst],f"catalog-{inst}")
                dispatcher=DeadlineDispatcher(stop_event,lane,inventory,store,raw_path,
                    catalog_policy(inventory),args.timeout_fila,args.retry_fila,
                    args.breaker_falhas,args.cooldown_seg,lane_id="CATALOG",registry=registry)
                t=threading.Thread(target=dispatcher.run,daemon=True,name=f"catalog-{inst}")
                t.start();threads.append(t)

        # Exactly one infra scheduler per configured instance, independent of
        # its queue mode. Slow trunk reads cannot change the gateway deadline.
        infra_policy=instance_infra_policy(policy_doc,inst,args.infra_seg)
        infra_lane=PortalNamedLane(sessions[inst],f"infra-{inst}")
        iw=AdaptiveInfraWorker(
            stop_event,infra_lane,store,infra_policy,args.timeout_infra,
            registry=registry if inst in adaptive_instances else None)
        it=threading.Thread(target=iw.run,daemon=True,name=f"infra-bg-inst{inst}")
        it.start();threads.append(it)

    # ---------------------------------------------------------------
    # Slow path
    # ---------------------------------------------------------------
    for inst in insts:
        # Compatibilidade: full_detail=True continua habilitando as duas fontes.
        # Novos flags permitem ativá-las separadamente.
        detail = [
            q for q in by_inst[inst]
            if q.get("lastcalls_detail", q.get("full_detail", False))
            or q.get("dialer_detail", q.get("full_detail", False))
        ]

        if inst in adaptive_instances:
            sp = adaptive_cfg[inst].get("slow_path", {})
            if not bool(sp.get("enabled", True)):
                print(f"[SLOW-PATH] Instância {inst}: DESABILITADO por política.")
                continue
            allowed = set(str(x) for x in sp.get("only_codes", []))
            if allowed:
                detail = [q for q in detail if q["codigo"] in allowed]
            if not detail:
                continue
            slow_lane = PortalNamedLane(sessions[inst], f"slow-bg-{inst}")
            sd = ProtectedSlowDispatcher(
                stop_event, slow_lane, detail, store, sp, registry,
                args.timeout_lastcalls, args.timeout_discador,
                args.breaker_falhas, args.cooldown_seg * 2,
            )
            t = threading.Thread(
                target=sd.run, daemon=True, name=f"slow-bg-inst{inst}"
            )
            t.start()
            threads.append(t)
            continue

        last_interval = args.lastcalls_seg
        dial_interval = args.discador_seg
        last_q = [
            q for q in detail
            if q.get("lastcalls_detail", q.get("full_detail", False))
        ]
        dial_q = [
            q for q in detail
            if q.get("dialer_detail", q.get("full_detail", False))
        ]

        last_offsets = slow_offsets(last_q, last_interval, base_delay=20.0) if last_q else {}
        dial_offsets = slow_offsets(dial_q, dial_interval, base_delay=45.0) if dial_q else {}

        for q in last_q:
            t = threading.Thread(
                target=delayed_core_worker,
                args=(
                    stop_event, last_offsets[q["codigo"]], store,
                    "lastcalls", inst, q["codigo"], core.slow_worker,
                    stop_event, slow_lanes[inst], q, store, "lastcalls",
                    last_interval, args.timeout_lastcalls,
                    args.breaker_falhas, args.cooldown_seg,
                ),
                daemon=True,
                name=f"lastcalls-{q['codigo']}",
            )
            t.start()
            threads.append(t)

        for q in dial_q:
            t = threading.Thread(
                target=delayed_core_worker,
                args=(
                    stop_event, dial_offsets[q["codigo"]], store,
                    "dialer", inst, q["codigo"], core.slow_worker,
                    stop_event, slow_lanes[inst], q, store, "dialer",
                    dial_interval, args.timeout_discador,
                    args.breaker_falhas, args.cooldown_seg * 2,
                ),
                daemon=True,
                name=f"dialer-{q['codigo']}",
            )
            t.start()
            threads.append(t)

    # ---------------------------------------------------------------
    # IPBX stuck channels (sistema separado)
    # ---------------------------------------------------------------
    if not args.sem_ipbx_direto:
        ipbx_cfg = env_config(insts)
        for idx, inst in enumerate(insts):
            c = ipbx_cfg.get(inst)
            if not c:
                continue
            delay = round((idx*2.0) % max(1.0, args.stuck_seg), 3)
            reader = IPBXStuckReader(
                user, pwd, inst, c["projeto"], c["menu"]
            )
            t = threading.Thread(
                target=stuck_direct_worker,
                args=(stop_event, reader, store, args.stuck_seg, delay),
                daemon=True,
                name=f"stuck-{inst}"
            )
            t.start()
            threads.append(t)

    print("\nLIVE v1.10.6 iniciado. CTRL+C para encerrar.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nEncerrando coletor LIVE...")
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=5)
        with store.lock:
            try:
                store.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                store.conn.commit()
            except Exception:
                pass
            store.conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
