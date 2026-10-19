"""Read-only, payload-free collection audit for every configured TOI instance."""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

APP = Path("/var/www/toi/TOI_v1.10.6_DUAL_LANE_ML_SHADOW")
DB = Path("/var/www/toi/dados_toi/live/toi_live.sqlite")


def category(detail):
    value = (detail or "").lower()
    for marker, name in (
        ("read timed out", "read_timeout"), ("readtimeout", "read_timeout"),
        ("connect timed out", "connect_timeout"), ("connecttimeout", "connect_timeout"),
        ("permissionerror", "authentication"), ("401", "authentication"),
        ("403", "authentication"), ("connectionerror", "connection"),
        ("connection refused", "connection"), ("database is locked", "sqlite_lock"),
        ("jsondecodeerror", "invalid_json"), ("http 5", "http_5xx"),
    ):
        if marker in value:
            return name
    return "other"


def series_report(rows, now, target):
    points = [datetime.fromisoformat(row[0]) for row in rows]
    threshold = max(target * 2, target + 30)
    gaps = [
        {"start": a.isoformat(), "end": b.isoformat(),
         "duration_s": round((b-a).total_seconds(), 1)}
        for a, b in zip(points, points[1:])
        if (b-a).total_seconds() > threshold
    ]
    return {
        "target_s": target, "threshold_s": threshold, "samples": len(points),
        "latest_age_s": round((now-points[-1]).total_seconds(), 1) if points else None,
        "worst_gap_s": max((x["duration_s"] for x in gaps), default=0),
        "gap_count": len(gaps),
        "largest_gaps": sorted(gaps, key=lambda x: -x["duration_s"])[:5],
    }


def audit(hours=4):
    now = datetime.now().astimezone()
    since = (now-timedelta(hours=hours)).isoformat()
    catalog = json.loads((APP / "portal_queue_catalog.json").read_text())
    scope = json.loads((APP / "toi_demo_scope_v1.json").read_text())
    policy = json.loads((APP / "toi_adaptive_policy_v1.json").read_text())
    curated = {(str(q["instancia"]), str(q["codigo"])) for q in scope["queues"]}
    queues = {(str(q["instancia"]), str(q["codigo"])) for q in catalog["queues"]} | curated
    con = sqlite3.connect(DB.resolve().as_uri()+"?mode=ro", uri=True, timeout=10)
    con.execute("PRAGMA query_only=ON")
    result = {"checked_at": now.isoformat(), "hours": hours, "instances": {},
              "error_minutes": {}, "error_categories": {}}
    try:
        for inst in sorted({i for i, _ in queues}, key=int):
            entry = {"queues": {}, "infra": {}}
            inst_policy = policy.get("instancias", {}).get(inst, {})
            for _, code in sorted(k for k in queues if k[0] == inst):
                target = float(inst_policy.get("queues", {}).get(code, {}).get(
                    "target_s", 10 if (inst, code) in curated else 300))
                rows = con.execute(
                    "SELECT ts FROM queue_snapshot WHERE instancia=? AND codigo=? AND ts>=? ORDER BY ts",
                    (inst, code, since)).fetchall()
                item = series_report(rows, now, target)
                source = "deadline_queue" if inst_policy.get("queues", {}).get(code) or (inst, code) not in curated else "queue"
                state_rows = con.execute(
                    "SELECT state, COUNT(*), MAX(duration_s) FROM scheduler_health "
                    "WHERE source=? AND instancia=? AND codigo=? AND ts>=? GROUP BY state",
                    (source, inst, code, since)).fetchall()
                item["scheduler"] = {state: {"count": count, "max_duration_s": duration}
                                     for state, count, duration in state_rows}
                entry["queues"][code] = item
            for source in ("trunk", "gateway"):
                rows = con.execute(
                    "SELECT DISTINCT ts FROM infra_snapshot WHERE instancia=? AND source=? AND ts>=? ORDER BY ts",
                    (inst, source, since)).fetchall()
                entry["infra"][source] = series_report(rows, now, 60 if inst == "4" else 15)
            result["instances"][inst] = entry
        rows = con.execute(
            "SELECT ts,instancia,source,state,duration_s,detail FROM scheduler_health "
            "WHERE ts>=? AND state NOT IN ('OK','OVERRUN')", (since,)).fetchall()
        by_minute = Counter()
        by_category = Counter()
        for ts, inst, source, state, duration, detail in rows:
            by_minute[(inst, ts[:16])] += 1
            by_category[(inst, source, state, category(detail))] += 1
        result["error_minutes"] = {
            inst: [{"minute": minute, "errors": n} for (i, minute), n in sorted(by_minute.items()) if i == inst]
            for inst in result["instances"]
        }
        result["error_categories"] = [
            {"instance": inst, "source": source, "state": state,
             "category": cat, "count": n}
            for (inst, source, state, cat), n in sorted(by_category.items())
        ]
    finally:
        con.close()
    return result


if __name__ == "__main__":
    print(json.dumps(audit(), ensure_ascii=False))
