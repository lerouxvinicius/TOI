"""Read-only cadence audit for one TOI queue, without raw Portal payloads."""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path


def error_type(detail: str | None) -> str | None:
    match = re.match(r"^[A-Za-z_][A-Za-z_0-9]{0,59}", detail or "")
    return match.group(0) if match else None


def gap_events(times: list[datetime], threshold: float) -> list[dict]:
    return [
        {"start": a.isoformat(), "end": b.isoformat(),
         "duration_s": round((b - a).total_seconds(), 1)}
        for a, b in zip(times, times[1:]) if (b - a).total_seconds() > threshold
    ]


def audit(db: Path, instancia: str, codigo: str, hours: float, target_s: float) -> dict:
    if not db.is_file():
        raise FileNotFoundError(db)
    now = datetime.now().astimezone()
    since = (now - timedelta(hours=hours)).isoformat()
    connection = sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        rows = connection.execute(
            """SELECT ts,p34_pct,p135_pct,hit_rate15,calls_dialing,calls_in_attendance
               FROM queue_snapshot
               WHERE instancia=? AND codigo=? AND ts>=? ORDER BY ts""",
            (instancia, codigo, since),
        ).fetchall()
        times = [datetime.fromisoformat(row["ts"]) for row in rows]
        gaps = [round((b - a).total_seconds(), 1) for a, b in zip(times, times[1:])]
        first_ever = connection.execute(
            """SELECT ts FROM queue_snapshot WHERE instancia=? AND codigo=?
               ORDER BY ts LIMIT 1""", (instancia, codigo)
        ).fetchone()
        schedule = connection.execute(
            """SELECT state,COUNT(*) AS n,ROUND(MAX(start_lag_s),2) AS worst_lag_s,
                      ROUND(MAX(duration_s),2) AS worst_duration_s
               FROM scheduler_health
               WHERE source IN ('queue','deadline_queue')
                 AND instancia=? AND codigo=? AND ts>=?
               GROUP BY state ORDER BY n DESC""",
            (instancia, codigo, since),
        ).fetchall()
        non_ok = connection.execute(
            """SELECT ts,state,start_lag_s,duration_s,detail
               FROM scheduler_health
               WHERE source IN ('queue','deadline_queue')
                 AND instancia=? AND codigo=? AND ts>=? AND state!='OK'
               ORDER BY ts DESC LIMIT 30""",
            (instancia, codigo, since),
        ).fetchall()
        infra = {}
        for source in ("trunk", "gateway"):
            infra_rows = connection.execute(
                """SELECT DISTINCT ts FROM infra_snapshot
                   WHERE instancia=? AND source=? AND ts>=? ORDER BY ts""",
                (instancia, source, since),
            ).fetchall()
            infra_times = [datetime.fromisoformat(row["ts"]) for row in infra_rows]
            infra[source] = {
                "samples": len(infra_times),
                "last_ts": infra_times[-1].isoformat() if infra_times else None,
                "gaps_over_120_s": gap_events(infra_times, 120),
            }
    finally:
        connection.close()
    threshold = max(2 * target_s, target_s + 30)
    queue_gaps = gap_events(times, threshold)
    return {
        "checked_at": now.isoformat(),
        "queue": {"instancia": instancia, "codigo": codigo},
        "window_hours": hours,
        "target_s": target_s,
        "first_ever_ts": first_ever["ts"] if first_ever else None,
        "first_in_window": times[0].isoformat() if times else None,
        "last_in_window": times[-1].isoformat() if times else None,
        "latest_age_s": round((now - times[-1]).total_seconds(), 1) if times else None,
        "samples": len(times),
        "null_metric_counts": {
            field: sum(row[field] is None for row in rows)
            for field in ("p34_pct", "p135_pct", "hit_rate15",
                          "calls_dialing", "calls_in_attendance")
        },
        "worst_gap_s": max(gaps, default=None),
        "gaps_over_threshold_s": [gap for gap in gaps if gap > threshold],
        "gap_events": queue_gaps,
        "gap_threshold_s": threshold,
        "scheduler_states": [dict(row) for row in schedule],
        "scheduler_non_ok": [
            {"ts": row["ts"], "state": row["state"],
             "start_lag_s": row["start_lag_s"], "duration_s": row["duration_s"],
             "error_type": error_type(row["detail"])}
            for row in non_ok
        ],
        "infra": infra,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("/var/www/toi/dados_toi/live/toi_live.sqlite"))
    parser.add_argument("--instancia", default="6")
    parser.add_argument("--codigo", default="0255800015")
    parser.add_argument("--hours", type=float, default=6)
    parser.add_argument("--target-s", type=float, default=10)
    args = parser.parse_args()
    print(json.dumps(audit(args.db, args.instancia, args.codigo, args.hours, args.target_s),
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
