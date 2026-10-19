#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TOI System Health READ-ONLY v1.2

Objetivo
--------
Separar duas perguntas que a v1 misturava:

1) CURRENT HEALTH
   "O TOI está funcionando AGORA?"

2) HISTORICAL RELIABILITY
   "Houve interrupções/degradações na janela analisada?"

Também:
- detecta gaps por fila;
- tenta identificar interrupções transversais;
- agrupa erros de collector por host:porta;
- inspeciona tabelas de heartbeat/health existentes sem assumir schema;
- preserva modo estritamente read-only.

Não altera aquisição, SQLite, policy, diagnóstico, recomendações ou ML.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

APP = Path(__file__).resolve().parent
HEALTH_TABLES = (
    "live_heartbeat",
    "dual_lane_health",
    "scheduler_health",
    "slow_path_health",
    "worker_health",
    "adaptive_dispatcher_health",
)


def locate_db(explicit=None):
    if explicit:
        return Path(explicit).expanduser().resolve()
    for p in [
        (APP / "../../dados_toi/live/toi_live.sqlite").resolve(),
        (APP / "../../../dados_toi/live/toi_live.sqlite").resolve(),
        (Path.cwd() / "dados_toi/live/toi_live.sqlite").resolve(),
        (Path.cwd() / "../dados_toi/live/toi_live.sqlite").resolve(),
    ]:
        if p.exists():
            return p
    raise FileNotFoundError("toi_live.sqlite não localizado. Informe --db.")


def connect_ro(db):
    con = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    return con


def table_exists(con, name):
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def columns(con, name):
    if not table_exists(con, name):
        return []
    return [r[1] for r in con.execute(f"PRAGMA table_info({name})")]


def parse_ts(v):
    try:
        return datetime.fromisoformat(str(v))
    except Exception:
        return None


def age_seconds(ts):
    d = parse_ts(ts)
    if d is None:
        return None
    now = datetime.now(d.tzinfo) if d.tzinfo else datetime.now()
    return max(0.0, (now - d).total_seconds())


def percentile(vals, p):
    if not vals:
        return None
    vals = sorted(vals)
    k = (len(vals) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return vals[int(k)]
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def latest_ts(con, table):
    if not table_exists(con, table):
        return None
    cols = columns(con, table)
    for candidate in ("ts", "timestamp", "updated_at", "created_at", "last_ts"):
        if candidate in cols:
            row = con.execute(f"SELECT MAX({candidate}) v FROM {table}").fetchone()
            return row["v"] if row else None
    return None


def latest_row_generic(con, table):
    if not table_exists(con, table):
        return {"exists": False}
    cols = columns(con, table)
    ts_col = next((c for c in ("ts","timestamp","updated_at","created_at","last_ts")
                   if c in cols), None)
    result = {"exists": True, "columns": cols, "ts_column": ts_col}
    if ts_col:
        row = con.execute(
            f"SELECT * FROM {table} ORDER BY {ts_col} DESC LIMIT 1"
        ).fetchone()
    else:
        row = con.execute(f"SELECT * FROM {table} LIMIT 1").fetchone()
    if row:
        d = dict(row)
        result["latest"] = d
        if ts_col:
            result["age_s"] = round(age_seconds(d.get(ts_col)), 1)
    return result


def queue_gap_details(rows, gap_threshold_s):
    pairs = []
    prev = None
    gaps = []
    for r in rows:
        t = parse_ts(r["ts"])
        if t is None:
            continue
        if prev is not None and t >= prev:
            g = (t - prev).total_seconds()
            gaps.append(g)
            if g >= gap_threshold_s:
                pairs.append({
                    "start": prev,
                    "end": t,
                    "gap_s": round(g, 2),
                })
        prev = t
    return gaps, pairs


def cluster_transversal(gap_events, queue_count, tolerance_s=90, min_share=0.50):
    events = sorted(gap_events, key=lambda x: x["start"])
    clusters = []
    for ev in events:
        placed = None
        for c in reversed(clusters):
            if abs((ev["start"] - c["anchor"]).total_seconds()) <= tolerance_s:
                placed = c
                break
            if (ev["start"] - c["anchor"]).total_seconds() > tolerance_s:
                break
        if placed is None:
            placed = {
                "anchor": ev["start"],
                "events": [],
            }
            clusters.append(placed)
        placed["events"].append(ev)

    out = []
    for c in clusters:
        unique = {(e["instancia"], e["codigo"]) for e in c["events"]}
        share = len(unique) / queue_count if queue_count else 0
        if share >= min_share:
            starts = [e["start"] for e in c["events"]]
            ends = [e["end"] for e in c["events"]]
            out.append({
                "start": min(starts).isoformat(),
                "end": max(ends).isoformat(),
                "impacted_queues": len(unique),
                "queue_share": round(share, 4),
                "max_gap_s": max(e["gap_s"] for e in c["events"]),
                "classification": "TRANSVERSAL",
            })
    return out


def extract_hostport(text):
    if not text:
        return None
    s = str(text)
    host = re.search(r"host=['\"]?(\d{1,3}(?:\.\d{1,3}){3})", s)
    port = re.search(r"port[=:\s]+(\d+)", s)
    if host:
        return host.group(1) + (f":{port.group(1)}" if port else "")
    m = re.search(r"(?<!\d)(\d{1,3}(?:\.\d{1,3}){3})(?::(\d+))?", s)
    if not m:
        return None
    return m.group(1) + (f":{m.group(2)}" if m.group(2) else "")



def health_activity_in_interval(con, table, start_iso, end_iso):
    if not table_exists(con, table):
        return {"exists": False, "rows": 0}
    cols = columns(con, table)
    ts_col = next((c for c in ("ts","timestamp","updated_at","created_at","last_ts") if c in cols), None)
    if not ts_col:
        return {"exists": True, "rows": None}
    row = con.execute(
        f"SELECT COUNT(*) n, MIN({ts_col}) min_ts, MAX({ts_col}) max_ts "
        f"FROM {table} WHERE {ts_col}>? AND {ts_col}<?",
        (start_iso, end_iso)
    ).fetchone()
    return {
        "exists": True,
        "rows": int(row["n"] or 0),
        "min_ts": row["min_ts"],
        "max_ts": row["max_ts"],
    }


def classify_transversal_with_platform_health(con, interruptions):
    proof_tables = ("live_heartbeat","scheduler_health","worker_health","dual_lane_health")
    classified = []
    for x in interruptions:
        item = dict(x)
        activity = {}
        total_rows = 0
        observed = 0
        for table in proof_tables:
            a = health_activity_in_interval(con, table, x["start"], x["end"])
            activity[table] = a
            if a.get("exists") and a.get("rows") is not None:
                observed += 1
                total_rows += a.get("rows", 0)
        if observed and total_rows == 0:
            cause = "PLATFORM_OR_HOST_SILENCE"
        elif total_rows > 0:
            cause = "EXTERNAL_SOURCE_OR_PIPELINE"
        else:
            cause = "UNDETERMINED"
        item["cause_class"] = cause
        item["internal_health_activity"] = activity
        item["interval_note"] = "Gap observado; não prova o instante exato da causa."
        classified.append(item)
    return classified


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db")
    ap.add_argument("--hours", type=float, default=6.0)
    ap.add_argument("--gap-threshold-s", type=float, default=120.0)
    ap.add_argument("--json-out")
    args = ap.parse_args()

    db = locate_db(args.db)
    now = datetime.now().astimezone()
    start = now - timedelta(hours=args.hours)
    start_iso = start.isoformat()

    out = {
        "tool": "toi_system_health_readonly_v1_2",
        "generated_at": now.isoformat(timespec="seconds"),
        "db": str(db),
        "hours": args.hours,
        "gap_threshold_s": args.gap_threshold_s,
    }

    with connect_ro(db) as con:
        quick = con.execute("PRAGMA quick_check").fetchone()[0]
        out["sqlite"] = {
            "quick_check": quick,
            "db_bytes": db.stat().st_size if db.exists() else None,
            "wal_bytes": Path(str(db)+"-wal").stat().st_size if Path(str(db)+"-wal").exists() else 0,
            "shm_bytes": Path(str(db)+"-shm").stat().st_size if Path(str(db)+"-shm").exists() else 0,
        }

        source_tables = {
            "queue": "queue_snapshot",
            "infra": "infra_snapshot",
            "ipbx": "stuck_channels_snapshot",
            "dialer": "dialer_snapshot",
            "ml_shadow": "ml_shadow_prediction",
        }
        sources = {}
        for name, table in source_tables.items():
            ts = latest_ts(con, table)
            sources[name] = {
                "table": table,
                "exists": table_exists(con, table),
                "last_ts": ts,
                "age_s": round(age_seconds(ts), 1) if ts else None,
            }
        out["sources"] = sources

        # CURRENT HEALTH: estado agora, separado do histórico.
        current_reasons = []
        if quick != "ok":
            current_reasons.append("SQLite quick_check != ok")

        current_limits = {
            "queue": 120.0,
            "infra": 180.0,
            "ipbx": 60.0,
            "dialer": 1200.0,  # slow-path
            "ml_shadow": 180.0,
        }
        current_components = {}
        required = ("queue", "infra", "ipbx", "ml_shadow")
        for name, info in sources.items():
            age = info["age_s"]
            lim = current_limits[name]
            if not info["exists"] or age is None:
                state = "SEM_DADOS"
            elif age <= lim:
                state = "LIVE"
            else:
                state = "STALE"
            current_components[name] = {
                "status": state, "age_s": age, "current_limit_s": lim,
            }
            if name in required and state != "LIVE":
                current_reasons.append(f"{name} {state}")

        current_status = "SAUDÁVEL" if not current_reasons else "DEGRADADO"
        out["current_health"] = {
            "status": current_status,
            "components": current_components,
            "reasons": current_reasons,
            "note": (
                "Limites desta ferramenta são de saúde da plataforma, não substituem "
                "freshness/policy por fila do Dashboard."
            )
        }

        # Queue historical reliability.
        queues = []
        all_gap_events = []
        if table_exists(con, "queue_snapshot"):
            qrows = con.execute(
                "SELECT instancia,codigo,MAX(ts) last_ts,COUNT(*) total "
                "FROM queue_snapshot GROUP BY instancia,codigo "
                "ORDER BY CAST(instancia AS INT),codigo"
            ).fetchall()

            for q in qrows:
                rows = con.execute(
                    "SELECT ts FROM queue_snapshot "
                    "WHERE instancia=? AND codigo=? AND ts>=? ORDER BY ts",
                    (q["instancia"], q["codigo"], start_iso)
                ).fetchall()
                gaps, gap_events = queue_gap_details(rows, args.gap_threshold_s)
                for e in gap_events:
                    all_gap_events.append({
                        **e,
                        "instancia": str(q["instancia"]),
                        "codigo": str(q["codigo"]),
                    })

                lat = None
                cols = columns(con, "queue_snapshot")
                if "latency_queue" in cols and "latency_stats" in cols:
                    lat = con.execute(
                        "SELECT AVG(COALESCE(latency_queue,0)+COALESCE(latency_stats,0)) avg_lat,"
                        "MAX(COALESCE(latency_queue,0)+COALESCE(latency_stats,0)) max_lat "
                        "FROM queue_snapshot WHERE instancia=? AND codigo=? AND ts>=?",
                        (q["instancia"], q["codigo"], start_iso)
                    ).fetchone()

                queues.append({
                    "instancia": str(q["instancia"]),
                    "codigo": str(q["codigo"]),
                    "last_ts": q["last_ts"],
                    "age_s": round(age_seconds(q["last_ts"]), 1) if q["last_ts"] else None,
                    "samples_window": len(rows),
                    "gap_p50_s": round(percentile(gaps, .50), 2) if gaps else None,
                    "gap_p95_s": round(percentile(gaps, .95), 2) if gaps else None,
                    "gap_max_s": round(max(gaps), 2) if gaps else None,
                    "gaps_over_threshold": len(gap_events),
                    "avg_latency_s": round(lat["avg_lat"] or 0, 3) if lat else None,
                    "max_latency_s": round(lat["max_lat"] or 0, 3) if lat else None,
                })
        out["queues"] = queues

        transversal_raw = cluster_transversal(
            all_gap_events, len(queues), tolerance_s=90, min_share=0.50
        )

        # Collector events + host/port concentration.
        collector = {
            "exists": table_exists(con, "collector_event"),
            "errors_window": None,
            "warnings_window": None,
            "top_targets": [],
            "recent": [],
        }
        if collector["exists"]:
            cols = columns(con, "collector_event")
            if "ts" in cols:
                wanted = [c for c in (
                    "ts","level","entity","event","message","detail","duration_s"
                ) if c in cols]
                where = "ts>=?"
                params = [start_iso]
                rows = con.execute(
                    f"SELECT {','.join(wanted)} FROM collector_event "
                    f"WHERE {where} ORDER BY ts DESC",
                    params
                ).fetchall()

                levels = Counter()
                targets = Counter()
                recent = []
                for row in rows:
                    d = dict(row)
                    level = str(d.get("level") or "").upper()
                    if level:
                        levels[level] += 1
                    text = " ".join(str(d.get(k) or "") for k in ("message","detail","event","entity"))
                    hp = extract_hostport(text)
                    if hp:
                        targets[hp] += 1
                    if len(recent) < 50 and level in ("ERROR","WARN","WARNING"):
                        recent.append(d)

                collector["errors_window"] = levels.get("ERROR", 0)
                collector["warnings_window"] = levels.get("WARN", 0) + levels.get("WARNING", 0)
                collector["top_targets"] = [
                    {"target": k, "events": v} for k, v in targets.most_common(10)
                ]
                collector["recent"] = recent
        out["collector_events"] = collector

        # Tabelas internas já existentes: inspeção genérica e segura.
        internal_health = {}
        for table in HEALTH_TABLES:
            internal_health[table] = latest_row_generic(con, table)
        out["internal_health_tables"] = internal_health

        transversal = classify_transversal_with_platform_health(con, transversal_raw)
        out["transversal_interruptions"] = transversal

    # HISTORICAL RELIABILITY.
    hist_reasons = []
    err_count = out["collector_events"].get("errors_window")
    if err_count and err_count >= 3:
        hist_reasons.append(f"{err_count} erros de collector na janela")
    if transversal:
        hist_reasons.append(
            f"{len(transversal)} interrupção(ões) transversal(is) detectada(s)"
        )
    queues_with_large_gap = sum(1 for q in queues if q["gaps_over_threshold"] > 0)
    if queues_with_large_gap:
        hist_reasons.append(
            f"{queues_with_large_gap}/{len(queues)} filas com gap >= {args.gap_threshold_s:.0f}s"
        )

    if not hist_reasons:
        reliability = "ESTÁVEL"
    elif transversal:
        reliability = "DEGRADADA"
    else:
        reliability = "ATENÇÃO"

    out["historical_reliability"] = {
        "status": reliability,
        "window_hours": args.hours,
        "reasons": hist_reasons,
        "current_recovered": out["current_health"]["status"] == "SAUDÁVEL",
    }

    out_path = Path(args.json_out) if args.json_out else (
        Path.cwd() / f"toi_system_health_v1_2_{datetime.now():%Y%m%d_%H%M%S}.json"
    )
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 104)
    print("TOI SYSTEM HEALTH — READ ONLY v1.2")
    print("=" * 104)
    print("DB:", db)
    print("")
    print("CURRENT HEALTH")
    print("  Status:", out["current_health"]["status"])
    for k, v in out["current_health"]["components"].items():
        print(f"  {k:10s} {v['status']:10s} age={v['age_s']}s")
    print("  SQLite:", out["sqlite"]["quick_check"])
    print("")
    print(f"HISTORICAL RELIABILITY — {args.hours:g}h")
    print("  Status:", out["historical_reliability"]["status"])
    for r in out["historical_reliability"]["reasons"]:
        print("  -", r)
    if transversal:
        print("")
        print("INTERRUPÇÕES TRANSVERSAIS")
        for x in transversal[:10]:
            print(
                f"  {x['start']} -> {x['end']} | "
                f"{x['impacted_queues']} filas ({x['queue_share']*100:.0f}%) | "
                f"max_gap={x['max_gap_s']:.1f}s | {x.get('cause_class','UNDETERMINED')}"
            )
    targets = out["collector_events"].get("top_targets") or []
    if targets:
        print("")
        print("ALVOS MAIS PRESENTES NOS EVENTOS")
        for x in targets[:5]:
            print(f"  {x['target']:22s} {x['events']} eventos")
    print("")
    print("Relatório JSON:", out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
