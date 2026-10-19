"""Testes do pipeline de ML: score shadow, freeze da aquisição e rótulo/split sem vazamento."""
import hashlib
import json
import random
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"))
sys.path.insert(0, str(ROOT / "demo"))

import pandas as pd  # noqa: E402

import ml_case_study as case  # noqa: E402
import toi_collector_core_v3 as core  # noqa: E402
import toi_ml_homologation_audit_v1 as audit  # noqa: E402
import toi_ml_shadow_v1_10_6 as shadow  # noqa: E402

BRT = timezone(timedelta(hours=-3))


def baseline(n=120, seed=1):
    rng = random.Random(seed)
    return {
        "p34_pct": [rng.gauss(4, 1) for _ in range(n)],
        "p135_pct": [rng.gauss(2, 0.5) for _ in range(n)],
        "hit_rate15": [rng.gauss(30, 2) for _ in range(n)],
        "calls_dialing": [rng.gauss(40, 4) for _ in range(n)],
        "dialing_rate": [rng.gauss(2, 0.2) for _ in range(n)],
    }


class ShadowScoreTest(unittest.TestCase):
    def test_robust_z_needs_minimum_history(self):
        self.assertEqual(shadow.robust_z(100, [1, 2, 3]), 0.0)

    def test_robust_z_is_not_dragged_by_outliers(self):
        vals = [10.0] * 50 + [11.0] * 50 + [500.0]
        self.assertLess(abs(shadow.robust_z(10.5, vals)), 1)

    def test_normal_point_is_low_risk(self):
        cur = {"p34_pct": 4, "p135_pct": 2, "hit_rate15": 30, "calls_dialing": 40, "dialing_rate": 2}
        s = shadow.score_from_baseline(cur, baseline(), trunk=50)
        self.assertEqual(s["risk_class"], "BAIXO")
        self.assertIn("Sem anomalia", s["reasons"][0])

    def test_degradation_raises_risk_with_explanation(self):
        cur = {"p34_pct": 12, "p135_pct": 9, "hit_rate15": 18, "calls_dialing": 70, "dialing_rate": 3.5}
        s = shadow.score_from_baseline(cur, baseline(), trunk=92)
        self.assertEqual(s["risk_class"], "ALTO")
        self.assertTrue(any("P135" in r for r in s["reasons"]))
        self.assertTrue(any("troncos" in r for r in s["reasons"]))


class FreezeManifestTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.app = Path(self.tmp.name)
        self.file = self.app / "collector.py"
        self.file.write_text("print('v1')", encoding="utf-8")
        self.old_app, shadow.APP = shadow.APP, self.app
        self.doc = {"version": "1.10.6", "files": {
            "collector.py": {"sha256": hashlib.sha256(self.file.read_bytes()).hexdigest()}}}

    def tearDown(self):
        shadow.APP = self.old_app
        self.tmp.cleanup()

    def test_intact_acquisition_authorizes_model(self):
        self.assertTrue(shadow.verify_freeze_manifest(self.doc)[0])

    def test_any_change_in_acquisition_revokes_model(self):
        self.file.write_text("print('v2')", encoding="utf-8")
        ok, reason = shadow.verify_freeze_manifest(self.doc)
        self.assertFalse(ok)
        self.assertIn("revogado", reason)


class LabelAndSplitTest(unittest.TestCase):
    def _db(self, rows):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        db = Path(tmp.name) / "t.sqlite"
        core.Store(str(db)).conn.close()
        con = sqlite3.connect(db)
        shadow.ensure(con)
        case.write_queue(con, "4", "0249700009", rows)
        con.commit()
        return tmp, con

    def test_target_only_looks_at_the_next_15_minutes(self):
        start = datetime(2026, 9, 1, 8, tzinfo=BRT)
        normal = {"p34_pct": 3.0, "p135_pct": 1.0, "hit_rate15": 30, "calls_dialing": 40,
                  "dialing_rate": 2, "agents_available": 5, "calls_in_attendance": 20}
        rows = [(start + timedelta(minutes=m), dict(normal, p34_pct=25.0) if 300 <= m < 320 else normal, 50)
                for m in range(600)]
        tmp, con = self._db(rows)
        try:
            frame = audit._queue_frame(con, "4", "0249700009", start.isoformat())
        finally:
            con.close()
            tmp.cleanup()
        onset = pd.Timestamp(start + timedelta(minutes=300)).tz_convert("UTC")
        before = frame[frame.index < onset]
        minutes_to_onset = (onset - before.index).total_seconds() / 60
        self.assertTrue((before[minutes_to_onset <= 15]["target"] == 1).all())
        self.assertTrue((before[minutes_to_onset > 15]["target"] == 0).all())
        congested = frame[(frame.index >= onset) & (frame.index < onset + pd.Timedelta(minutes=20))]
        self.assertTrue(congested.empty, "minutos já congestionados não entram como exemplo")

    def test_chronological_split_keeps_a_15_minute_embargo(self):
        rows, _ = case.simulate_queue(random.Random(3), datetime.now(BRT).replace(
            hour=0, minute=0, second=0, microsecond=0) - timedelta(days=case.DAYS))
        tmp, con = self._db(rows)
        try:
            result = audit.audit_queue(con, "4", "0249700009", (rows[0][0] - timedelta(days=1)).isoformat())
        finally:
            con.close()
            tmp.cleanup()
        gap = datetime.fromisoformat(result["test_start"]) - datetime.fromisoformat(result["train_end"])
        self.assertGreaterEqual(gap, timedelta(minutes=15))
        self.assertIn("candidate_hist_gradient_boosting", result)


if __name__ == "__main__":
    unittest.main()
