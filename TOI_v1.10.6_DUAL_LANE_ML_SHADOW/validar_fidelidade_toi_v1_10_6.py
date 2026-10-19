#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
TOI — Validação de Fidelidade v1.10.6

Objetivo:
1) Separar FIDELIDADE de FRESHNESS.
2) Não chamar "DIF" quando Portal e TOI representam instantes diferentes.
3) Validar se o snapshot TOI preserva corretamente o payload bruto que recebeu.
4) Medir intervalo real entre snapshots do TOI.
5) Comparar Portal x TOI de forma estrita SOMENTE quando o cache/fonte do Portal coincide.
6) Validar Canais Travados diretamente no IPBX.

Uso:
  python validar_fidelidade_toi_v1_10_6.py ^
      --db "C:\Projetos\MGWMonitor\dados_toi\live\toi_live.sqlite" ^
      --instancia 4 --fila 0249700009 --ciclos 10

Somente leitura.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

import toi_collector_core_v3 as core
from toi_ipbx_readonly import IPBXStuckReader, env_config

APP = Path(__file__).resolve().parent


def load_env():
    for p in (Path.cwd(), APP, APP.parent, APP.parent.parent):
        env = p / ".env"
        if env.exists():
            load_dotenv(env)
            return env
    load_dotenv()
    return None


def dt_age_seconds(ts):
    if not ts:
        return None
    d = datetime.fromisoformat(ts)
    n = datetime.now().astimezone()
    if d.tzinfo is None:
        d = d.astimezone()
    return (n - d).total_seconds()


def parse_source_pair(v):
    """'13:55:01/13:55:14' -> ('13:55:01','13:55:14')"""
    s = str(v or "").strip()
    if "/" not in s:
        return (None, None)
    a, b = s.split("/", 1)
    return (a.strip() or None, b.strip() or None)


def clock_seconds(hms):
    if not hms:
        return None
    try:
        h, m, s = [int(x) for x in hms.split(":")[:3]]
        return h * 3600 + m * 60 + s
    except Exception:
        return None


def clock_delta(a, b):
    """Diferença absoluta entre HH:MM:SS, tolerando virada de dia."""
    sa, sb = clock_seconds(a), clock_seconds(b)
    if sa is None or sb is None:
        return None
    d = abs(sa - sb)
    return min(d, 86400 - d)


def fnum(v):
    if v is None:
        return None
    return float(v)


def eq_num(a, b, tol=0.011):
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) < tol


def raw_values(raw_queue, raw_stats):
    d1 = json.loads(raw_queue or "{}")
    d2 = json.loads(raw_stats or "{}")
    st = core.extract_status(d2)
    values = {
        "p34_pct": st[34][0],
        "p135_pct": st[135][0],
        "calls_dialing": int(core.qfield(d1, "QtCallsDialing", default=0) or 0),
        "calls_in_attendance": int(core.qfield(d1, "QtCallsInAttend", default=0) or 0),
        "hit_rate5": core.parse_num(core.qfield(d1, "PercHitRateLast5", default=0)),
        "hit_rate15": core.parse_num(core.qfield(d1, "PercHitRateLast15", default=0)),
        "dialing_rate": core.parse_num(core.qfield(d1, "VlRatioCalls", default=0)),
    }
    return d1, d2, values


def portal_values(d1, d2):
    st = core.extract_status(d2)
    return {
        "p34_pct": st[34][0],
        "p135_pct": st[135][0],
        "calls_dialing": int(core.qfield(d1, "QtCallsDialing", default=0) or 0),
        "calls_in_attendance": int(core.qfield(d1, "QtCallsInAttend", default=0) or 0),
        "hit_rate5": core.parse_num(core.qfield(d1, "PercHitRateLast5", default=0)),
        "hit_rate15": core.parse_num(core.qfield(d1, "PercHitRateLast15", default=0)),
        "dialing_rate": core.parse_num(core.qfield(d1, "VlRatioCalls", default=0)),
    }


def latest_row(con, instancia, fila):
    return con.execute(
        """SELECT * FROM queue_snapshot
           WHERE instancia=? AND codigo=?
           ORDER BY ts DESC LIMIT 1""",
        (str(instancia), str(fila)),
    ).fetchone()


def wait_new_row(con, instancia, fila, previous_ts, timeout):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        r = latest_row(con, instancia, fila)
        if r and r["ts"] != previous_ts:
            return r
        time.sleep(0.20)
    return None


def print_metric_table(portal, db, aligned_queue, aligned_stats):
    queue_fields = {
        "calls_dialing", "calls_in_attendance", "hit_rate5",
        "hit_rate15", "dialing_rate",
    }
    print("\nCampo".ljust(28), "Portal".rjust(14), "TOI".rjust(14), "Resultado".rjust(18))
    print("-" * 78)
    passed = 0
    tested = 0
    for k, pv in portal.items():
        tv = db.get(k)
        aligned = aligned_queue if k in queue_fields else aligned_stats
        if aligned:
            tested += 1
            ok = eq_num(pv, tv)
            passed += int(ok)
            result = "OK ALINHADO" if ok else "FALHA REAL"
        else:
            result = "TEMPORAL"
        print(k.ljust(28), str(pv).rjust(14), str(tv).rjust(14), result.rjust(18))
    return passed, tested


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(APP / "toi_demo_scope_v1.json"))
    ap.add_argument("--db", default=str((APP / "../dados_toi/live/toi_live.sqlite").resolve()))
    ap.add_argument("--instancia", required=True)
    ap.add_argument("--fila", required=True)
    ap.add_argument("--ciclos", type=int, default=10)
    ap.add_argument("--report-json", default="", help="Grava resultado estruturado da homologação.")
    ap.add_argument("--timeout-snapshot", type=float, default=45.0)
    args = ap.parse_args()

    env = load_env()
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    q = next(
        (
            x for x in cfg["queues"]
            if str(x["instancia"]) == str(args.instancia)
            and str(x["codigo"]) == str(args.fila)
        ),
        None,
    )
    if not q:
        raise SystemExit("Fila não encontrada no config.")

    user = (os.getenv("ORBE_LOGIN") or "").strip()
    pwd = os.getenv("ORBE_PASSWORD") or ""
    base = (os.getenv("ORBE_BASE_URL") or core.DEFAULT_BASE_URL).rstrip("/")

    ps = core.PortalSession(args.instancia, user, pwd, base, timeout=30)
    ps.auth()

    con = sqlite3.connect(args.db, timeout=10)
    con.row_factory = sqlite3.Row

    print("=" * 96)
    print("TOI — HOMOLOGAÇÃO DE TEMPO REAL v1.10.6")
    print("=" * 96)
    print(f"Fila       : inst{args.instancia} {q['nome']} ({q['codigo']})")
    print(f"SQLite LIVE: {args.db}")
    print(f"Ciclos     : {args.ciclos}")
    print(f".env       : {env or 'busca padrão'}")
    print()
    print("Regra:")
    print("- DIFERENÇA com cache/fonte diferente = TEMPORAL, não falha de fidelidade.")
    print("- Cache/fonte igual + valor diferente = FALHA REAL.")
    print("- DB x payload bruto do mesmo snapshot deve ser sempre idêntico.")
    print()

    intervals = []
    source_deltas_queue = []
    source_deltas_stats = []
    aligned_tests = 0
    aligned_passes = 0
    integrity_failures = 0
    ipbx_tests = 0
    ipbx_passes = 0

    previous = latest_row(con, args.instancia, args.fila)
    previous_ts = previous["ts"] if previous else None
    prev_dt = datetime.fromisoformat(previous_ts) if previous_ts else None

    params = {"pCodeQueue": q["codigo"], "pIdProject": q["projeto_id"]}
    ref = ps.base + (
        f"metrics/dashboardqueuedetail?"
        f"pCodeQueue={q['codigo']}&pIdProject={q['projeto_id']}"
    )

    ipcfg = env_config([args.instancia]).get(str(args.instancia))
    ip_reader = (
        IPBXStuckReader(user, pwd, args.instancia, ipcfg["projeto"], ipcfg["menu"])
        if ipcfg else None
    )

    for cycle in range(1, args.ciclos + 1):
        print("\n" + "=" * 96)
        print(f"CICLO {cycle}/{args.ciclos} — aguardando PRÓXIMO snapshot da fila...")
        row = wait_new_row(
            con, args.instancia, args.fila, previous_ts, args.timeout_snapshot
        )
        if not row:
            print("FALHA SLA: nenhum novo snapshot dentro do timeout.")
            previous_ts = latest_row(con, args.instancia, args.fila)["ts"]
            continue

        snap_dt = datetime.fromisoformat(row["ts"])
        if prev_dt is not None:
            intervals.append((snap_dt - prev_dt).total_seconds())
        prev_dt = snap_dt
        previous_ts = row["ts"]

        # Integridade interna: coluna do DB x raw do MESMO snapshot.
        toi_d1, toi_d2, toi_raw = raw_values(row["raw_queue"], row["raw_stats"])
        db = dict(row)
        bad_internal = []
        for k, rv in toi_raw.items():
            if not eq_num(rv, db.get(k)):
                bad_internal.append((k, rv, db.get(k)))
        if bad_internal:
            integrity_failures += 1

        # Imediatamente após detectar o novo snapshot, consulta o Portal.
        d1, lat1, _ = ps.get_json(
            "metrics/dashboardqueuedetail/getqueuedetaildata",
            params, ref, timeout=30, retries=1
        )
        d2, lat2, _ = ps.get_json(
            "metrics/dashboardqueuedetail/getcallstatisticsdata",
            params, ref, timeout=30, retries=1
        )
        portal = portal_values(d1, d2)

        tq_cache, tq_server = parse_source_pair(
            toi_d1.get("dtCacheAndServerQueueDetailData")
        )
        pq_cache, pq_server = parse_source_pair(
            d1.get("dtCacheAndServerQueueDetailData")
        )
        ts_cache, ts_server = parse_source_pair(
            toi_d2.get("dtCacheAndServerCallStatisticsData")
        )
        ps_cache, ps_server = parse_source_pair(
            d2.get("dtCacheAndServerCallStatisticsData")
        )

        dq = clock_delta(tq_server, pq_server)
        ds = clock_delta(ts_server, ps_server)
        if dq is not None:
            source_deltas_queue.append(dq)
        if ds is not None:
            source_deltas_stats.append(ds)

        aligned_queue = bool(tq_cache and pq_cache and tq_cache == pq_cache)
        aligned_stats = bool(ts_cache and ps_cache and ts_cache == ps_cache)

        print(f"Snapshot TOI : {row['ts']} | idade após consulta Portal={dt_age_seconds(row['ts']):.1f}s")
        print(f"Latência GET : queue={lat1:.2f}s | stats={lat2:.2f}s")
        print(
            f"Fonte QUEUE  : TOI cache/server={tq_cache}/{tq_server} | "
            f"Portal={pq_cache}/{pq_server} | delta_server={dq if dq is not None else '—'}s | "
            f"{'ALINHADO' if aligned_queue else 'TEMPORAL'}"
        )
        print(
            f"Fonte STATS  : TOI cache/server={ts_cache}/{ts_server} | "
            f"Portal={ps_cache}/{ps_server} | delta_server={ds if ds is not None else '—'}s | "
            f"{'ALINHADO' if aligned_stats else 'TEMPORAL'}"
        )

        if bad_internal:
            print("\nINTEGRIDADE INTERNA: FALHA")
            for k, rv, dv in bad_internal:
                print(f"  {k}: RAW={rv} DB={dv}")
        else:
            print("\nINTEGRIDADE INTERNA: OK — DB reproduz o payload bruto do mesmo snapshot.")

        p, t = print_metric_table(
            portal, db, aligned_queue=aligned_queue, aligned_stats=aligned_stats
        )
        aligned_passes += p
        aligned_tests += t

        if ip_reader:
            try:
                live = ip_reader.consultar()["quantidade"]
                sr = con.execute(
                    """SELECT ts,quantity,status
                       FROM stuck_channels_snapshot
                       WHERE instancia=?
                       ORDER BY ts DESC LIMIT 1""",
                    (str(args.instancia),),
                ).fetchone()
                if sr:
                    ipbx_tests += 1
                    ip_age = dt_age_seconds(sr["ts"])
                    ok = (sr["quantity"] == live and ip_age <= 25)
                    ipbx_passes += int(ok)
                    print(
                        f"\nIPBX TRAVADOS: Portal={live} | TOI={sr['quantity']} | "
                        f"idade={ip_age:.1f}s | {'OK' if ok else 'VERIFICAR'}"
                    )
            except Exception as exc:
                print(f"\nIPBX TRAVADOS: validação falhou: {type(exc).__name__}: {exc}")

    print("\n" + "=" * 96)
    print("RESUMO DA HOMOLOGAÇÃO")
    print("=" * 96)

    p95_i = None
    if intervals:
        sorted_i = sorted(intervals)
        p95_i = sorted_i[min(len(sorted_i)-1, round((len(sorted_i)-1)*0.95))]
        print(
            f"Intervalo entre snapshots da fila: "
            f"média={statistics.mean(intervals):.1f}s | "
            f"P95={p95_i:.1f}s | máx={max(intervals):.1f}s"
        )
    else:
        print("Intervalo entre snapshots da fila: sem amostras suficientes.")

    if source_deltas_queue:
        print(
            f"Defasagem da fonte QUEUE vs Portal no instante de validação: "
            f"média={statistics.mean(source_deltas_queue):.1f}s | "
            f"máx={max(source_deltas_queue):.1f}s"
        )
    if source_deltas_stats:
        print(
            f"Defasagem da fonte STATS vs Portal no instante de validação: "
            f"média={statistics.mean(source_deltas_stats):.1f}s | "
            f"máx={max(source_deltas_stats):.1f}s"
        )

    print(
        f"Integridade DB x RAW: "
        f"{'OK' if integrity_failures == 0 else 'FALHA'} "
        f"({args.ciclos - integrity_failures}/{args.ciclos} ciclos íntegros)"
    )

    if aligned_tests:
        print(
            f"Comparações com cache ALINHADO: "
            f"{aligned_passes}/{aligned_tests} campos idênticos "
            f"({aligned_passes/aligned_tests*100:.1f}%)"
        )
    else:
        print(
            "Comparações com cache ALINHADO: nenhuma coincidência de cache neste teste; "
            "isso não é falha, use as defasagens de fonte."
        )

    if ipbx_tests:
        print(
            f"Canais travados IPBX: {ipbx_passes}/{ipbx_tests} ciclos OK "
            f"({ipbx_passes/ipbx_tests*100:.1f}%)"
        )

    integrity_pct = round((args.ciclos-integrity_failures)/max(args.ciclos,1)*100.0, 2)
    aligned_pct = round(aligned_passes/aligned_tests*100.0, 2) if aligned_tests else None
    ipbx_pct = round(ipbx_passes/ipbx_tests*100.0, 2) if ipbx_tests else None
    interval_limit = 30.0 if str(args.instancia)=="4" and str(args.fila) in {"0249700009","0249700010"} else 40.0
    approved = (
        integrity_pct == 100.0
        and aligned_tests > 0
        and aligned_pct == 100.0
        and (p95_i is not None and p95_i <= interval_limit)
        and (ipbx_pct is None or ipbx_pct == 100.0)
    )
    report = {
        "version":"1.10.6",
        "generated_at":datetime.now().astimezone().isoformat(timespec="seconds"),
        "instancia":str(args.instancia),
        "fila":str(args.fila),
        "ciclos":args.ciclos,
        "integrity_pct":integrity_pct,
        "aligned_tests":aligned_tests,
        "aligned_fields_pct":aligned_pct,
        "snapshot_interval_p95_s":None if p95_i is None else round(float(p95_i),3),
        "snapshot_interval_limit_s":interval_limit,
        "ipbx_pct":ipbx_pct,
        "approved":approved,
        "note":"Diferenças com caches diferentes são temporais; fidelidade é exigida quando cache/fonte coincide."
    }
    if args.report_json:
        rp=Path(args.report_json)
        if not rp.is_absolute():
            rp=(APP/rp).resolve()
        rp.parent.mkdir(parents=True,exist_ok=True)
        rp.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
        print(f"\nRelatório estruturado: {rp}")
    print(f"RESULTADO v1.10.6: {'APROVADO' if approved else 'NÃO APROVADO'}")

    print("\nCritério recomendado para aprovar TEMPO REAL:")
    print("- Integridade DB x RAW = 100%;")
    print(f"- intervalo P95 da fila <= {interval_limit:.0f}s para esta fila;")
    print("- fonte QUEUE/STATS normalmente <= 10–15s do Portal;")
    print("- quando cache coincide, campos = 100% idênticos;")
    print("- Canais Travados: igualdade e idade <=25s.")
    print()
    con.close()
    return 0 if approved else 3


if __name__ == "__main__":
    raise SystemExit(main())
