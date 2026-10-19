"""Bounded recovery after a failed queue poll."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] /
                       "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"))
from toi_live_collector_v1_10_6 import DeadlineDispatcher, catalog_policy  # noqa: E402


class FailureRecoveryTests(unittest.TestCase):
    def test_catalog_failure_retries_before_five_minute_slot(self):
        policy = catalog_policy([{"codigo": "123"}])
        self.assertEqual(policy["error_retry_s"], 15)
        state = {"target": 300.0, "next_deadline": 300.0, "cooldown_until": 0.0}
        DeadlineDispatcher._schedule_after_attempt(state, 312.0, True, 15.0)
        self.assertEqual(state["next_deadline"], 327.0)

    def test_cooldown_limits_retries_during_long_outage(self):
        state = {"target": 18.0, "next_deadline": 18.0, "cooldown_until": 44.0}
        DeadlineDispatcher._schedule_after_attempt(state, 30.0, True, 3.0)
        self.assertEqual(state["next_deadline"], 36.0)
        self.assertLess(state["next_deadline"], state["cooldown_until"])
        DeadlineDispatcher._schedule_after_attempt(state, 45.0, False, 3.0)
        self.assertEqual(state["next_deadline"], 54.0)


if __name__ == "__main__":
    unittest.main()
