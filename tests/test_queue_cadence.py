"""Regressions for the collector cadence shown by the live dashboard."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path


APP = Path(__file__).resolve().parents[1] / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"
sys.path.insert(0, str(APP))
from toi_dashboard_server_v1_10_6 import DataService  # noqa: E402
sys.path.insert(0, str(APP.parent / "deploy"))
from diagnose_queue_cadence import audit  # noqa: E402


class QueueCadenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = DataService(Path("unused.sqlite"), APP / "toi_adaptive_policy_v1.json")

    def test_queue_in_screenshot_uses_realtime_collector(self):
        scope = json.loads((APP / "toi_demo_scope_v1.json").read_text(encoding="utf-8"))
        selected = [q for q in scope["queues"] if q["instancia"] == "6" and q["codigo"] == "0255800015"]
        self.assertEqual(len(selected), 1)
        self.assertEqual(self.service.queue_policy("6", "0255800015")["target_s"], 10)

    def test_catalog_freshness_matches_five_minute_schedule(self):
        self.assertEqual(self.service.queue_policy("6", "0255800014"),
                         {"target_s": 300, "max_age_s": 900})
        now = datetime.now().astimezone()
        for age, expected in ((26, "LIVE"), (310, "ATRASO"), (901, "STALE")):
            latest = {"ts": (now - timedelta(seconds=age)).isoformat()}
            result = self.service.freshness("6", "0255800014", latest, {}, None, None)
            self.assertEqual(result["sources"]["queue"]["status"], expected)
            self.assertEqual(result["sources"]["queue"]["target_s"], 300)

    def test_adaptive_policy_takes_precedence(self):
        self.assertEqual(self.service.queue_policy("4", "0249700009")["target_s"], 18)

    def test_period_coverage_detects_interior_gap(self):
        end = datetime.now().astimezone()
        full = [{"ts": (end - timedelta(seconds=18 * i)).isoformat()}
                for i in range(200, -1, -1)]
        gapped = [row for row in full
                  if not (end - timedelta(minutes=55) < datetime.fromisoformat(row["ts"])
                          < end - timedelta(minutes=52))]
        normal = self.service.period_summary("4", "0249700009", 60, full)["coverage"]
        missing = self.service.period_summary("4", "0249700009", 60, gapped)["coverage"]
        self.assertTrue(normal["complete"])
        self.assertFalse(missing["complete"])
        self.assertGreaterEqual(missing["gap_count"], 1)
        self.assertLess(missing["coverage_pct"], normal["coverage_pct"])

    def test_audit_distinguishes_short_history_from_interior_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "live.sqlite"
            now = datetime.now().astimezone()
            times = [now - timedelta(minutes=m) for m in (50, 45, 40, 15, 10, 5)]
            with closing(sqlite3.connect(db)) as conn:
                conn.executescript("""
                    CREATE TABLE queue_snapshot(ts TEXT, instancia TEXT, codigo TEXT,
                        p34_pct REAL, p135_pct REAL, hit_rate15 REAL,
                        calls_dialing INTEGER, calls_in_attendance INTEGER);
                    CREATE INDEX ix_qs ON queue_snapshot(instancia,codigo,ts);
                    CREATE TABLE scheduler_health(ts TEXT, source TEXT, instancia TEXT,
                        codigo TEXT, state TEXT, start_lag_s REAL, duration_s REAL,
                        detail TEXT);
                    CREATE TABLE infra_snapshot(ts TEXT, instancia TEXT, source TEXT);
                """)
                conn.executemany("INSERT INTO queue_snapshot VALUES(?,?,?,?,?,?,?,?)",
                                 [(ts.isoformat(), "6", "0255800015", 0, 0, 0, 0, 0)
                                  for ts in times])
                conn.execute("INSERT INTO scheduler_health VALUES(?,?,?,?,?,?,?,?)",
                             (times[-1].isoformat(), "deadline_queue", "6", "0255800015", "OK", 1, 2, ""))
                conn.commit()
            result = audit(db, "6", "0255800015", 6, 300)
            self.assertEqual(result["samples"], 6)
            self.assertEqual(result["gaps_over_threshold_s"], [1500.0])
            self.assertEqual(result["scheduler_states"][0]["state"], "OK")


if __name__ == "__main__":
    unittest.main()
