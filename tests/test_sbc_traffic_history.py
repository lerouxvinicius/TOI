import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

APP = Path(__file__).resolve().parents[1] / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"
sys.path.insert(0, str(APP))
from sbc_traffic_history import live_report, traffic_report  # noqa: E402
from sbc_card_bridge import card_data  # noqa: E402


class SBCRealmHistoryTests(unittest.TestCase):
    def test_day_hour_peaks_and_missing_reading(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "dados" / "sip_capacity" / "sip_capacity_2026-09-27.jsonl"
            path.parent.mkdir(parents=True)
            rows = [
                {"timestamp": "2026-09-27T09:05:00", "sbc_active": {"ip": "10.90.11.132"},
                 "realms": {"a-alfa-2": {"inbound_active": 2, "outbound_active": 1}}},
                {"timestamp": "2026-09-27T09:15:00", "sbc_active": {"ip": "10.90.11.132"},
                 "realms": {"a-alfa-2": {"inbound_active": 7, "outbound_active": 4},
                            "a-alfa-4-stir": {"inbound_active": None, "outbound_active": 0}}},
            ]
            path.write_text("\n".join(json.dumps(item) for item in rows) + "\n", encoding="utf-8")
            live = root / "dados" / "sbc_live" / "norte.json"
            live.parent.mkdir(parents=True)
            live.write_text(json.dumps({**rows[-1], "timestamp": "2026-09-27T09:15:45-03:00",
                                        "source_ok": True, "nodes": []}), encoding="utf-8")
            report = traffic_report(root, "norte", "2026-09-27", 9,
                                    now=datetime.fromisoformat("2026-09-27T09:16:00-03:00"))
            self.assertEqual(len(report["realms"]), 5)
            self.assertEqual(report["hourly"][9]["realms"]["a-alfa-2"]["inbound"]["peak"], 7)
            self.assertEqual(report["hourly"][9]["realms"]["a-alfa-2"]["inbound"]["at"],
                             "2026-09-27T09:15:00")
            self.assertIsNone(report["current"]["realms"]["a-alfa-4-stir"]["inbound"])
            self.assertEqual(report["current"]["realms"]["a-alfa-4-stir"]["outbound"], 0)
            self.assertEqual(len(report["readings"]), 2)

    def test_stale_current_does_not_become_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "dados" / "sip_capacity" / "sul" / "sip_capacity_2026-09-27.jsonl"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"timestamp": "2026-09-27T08:00:00",
                                        "realms": {"a-beta-link1": {"inbound_active": 5,
                                                                    "outbound_active": 2}}}) + "\n")
            live = root / "dados" / "sbc_live" / "sul.json"
            live.parent.mkdir(parents=True)
            live.write_text(json.dumps({"timestamp": "2026-09-27T09:58:00-03:00",
                                        "source_ok": True, "sbc_active": {"ip": "10.90.13.10"},
                                        "realms": {"a-beta-link1": {"inbound_active": 9,
                                                                      "outbound_active": 1}},
                                        "nodes": []}), encoding="utf-8")
            report = traffic_report(root, "sul", "2026-09-27",
                                    now=datetime.fromisoformat("2026-09-27T10:00:00-03:00"))
            self.assertIsNone(report["current"])
            self.assertFalse(report["current_fresh"])
            self.assertEqual(report["daily"]["a-beta-link1"]["inbound"]["peak"], 5)

    def test_hour_consolidation_precedes_realm_filter_for_norte_and_sul(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for pair, folder, first, second in (
                ("norte", "", "a-alfa-2", "a-alfa-4"),
                ("sul", "sul", "a-beta-link1", "a-Epsilon"),
            ):
                path = root / "dados" / "sip_capacity" / folder / "sip_capacity_2026-09-27.jsonl"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("\n".join(json.dumps(row) for row in (
                    {"timestamp": "2026-09-27T08:30:00", "realms": {
                        first: {"inbound_active": 99, "outbound_active": 1}}},
                    {"timestamp": "2026-09-27T09:05:00", "realms": {
                        first: {"inbound_active": 2, "outbound_active": 1},
                        second: {"inbound_active": 7, "outbound_active": 4}}},
                    {"timestamp": "2026-09-27T09:15:00", "realms": {
                        first: {"inbound_active": 3, "outbound_active": 5},
                        second: {"inbound_active": 1, "outbound_active": 2}}},
                )) + "\n", encoding="utf-8")
                report = traffic_report(
                    root, pair, "2026-09-27", 9, first,
                    now=datetime.fromisoformat("2026-09-27T10:00:00-03:00"))
                self.assertEqual(report["consolidated"]["samples"], 2)
                self.assertEqual(report["consolidated"]["inbound"]["peak"], 7)
                self.assertEqual(report["consolidated"]["inbound"]["realm"], second)
                self.assertEqual(report["consolidated"]["realms_with_traffic"], 2)
                self.assertEqual(report["selected_realms"], [first])
                self.assertEqual(report["realm_summary"][first]["inbound"]["peak"], 3)
                self.assertTrue(all(set(row["realms"]) == {first} for row in report["readings"]))
                all_realms = traffic_report(
                    root, pair, "2026-09-27", 9,
                    now=datetime.fromisoformat("2026-09-27T10:00:00-03:00"))
                self.assertEqual(len(all_realms["selected_realms"]), len(all_realms["realms"]))
                with self.assertRaisesRegex(ValueError, "Realm desconhecido"):
                    traffic_report(root, pair, "2026-09-27", 9, "a-inexistente")

    def test_norte_and_sul_have_independent_live_clocks(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            live_dir = root / "dados" / "sbc_live"
            live_dir.mkdir(parents=True)
            for pair, stamp, realm in (
                ("norte", "2026-09-27T10:00:00-03:00", "a-alfa-2"),
                ("sul", "2026-09-27T09:58:00-03:00", "a-beta-link1"),
            ):
                (live_dir / f"{pair}.json").write_text(json.dumps({
                    "timestamp": stamp, "source_ok": True,
                    "sbc_active": {"ip": "10.90.11.132"},
                    "realms": {realm: {"inbound_active": 3, "outbound_active": 1}},
                    "nodes": [],
                }), encoding="utf-8")
            now = datetime.fromisoformat("2026-09-27T10:00:20-03:00")
            self.assertTrue(live_report(root, "norte", now=now)["current_fresh"])
            self.assertFalse(live_report(root, "sul", now=now)["current_fresh"])
            # The Portal period/date selection cannot change the current SBC clock.
            historical = traffic_report(root, "norte", "2026-09-26", now=now)
            self.assertTrue(historical["current_fresh"])
            self.assertEqual(historical["current_age_s"], 20)

    def test_selected_agents_keep_missing_metrics_unknown(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "dados" / "status_ha_sbcs.json"
            path.parent.mkdir(parents=True)
            now = datetime.now().astimezone()
            path.write_text(json.dumps({"atualizado_em": now.isoformat(), "pares": {
                "SBCs SUL": {"membros": [{"ip": "10.90.13.10", "hostname": "SBC-1",
                    "ha": "ACTIVE", "respondendo_snmp": True, "health": 100,
                    "cpu": 1, "memoria": 20, "sip_sessions_ativas": 3, "cps": 0,
                    "agents": {"10.90.15.211": {"status_bruto": "3", "inbound_active": None,
                                                   "outbound_active": 0}}}]}}}), encoding="utf-8")
            feed = card_data(root, now=now.timestamp())
            sul = next(pair for pair in feed["pairs"] if pair["name"] == "SBCs SUL")
            agent = next(agent for agent in sul["nodes"][0]["agents"]
                         if agent["ip"] == "10.90.15.211")
            self.assertIsNone(agent["inbound_active"])
            self.assertEqual(agent["outbound_active"], 0)
            self.assertTrue(agent["observed"])

    def test_card_uses_fresh_live_sbc_source_even_when_alert_snapshot_is_old(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            live = root / "dados" / "sbc_live"
            live.mkdir(parents=True)
            now = datetime.now().astimezone()
            (live / "norte.json").write_text(json.dumps({
                "timestamp": now.isoformat(), "source_ok": True,
                "nodes": [
                    {"ip": "10.90.11.132", "hostname": "M1", "ha": "ACTIVE",
                     "health": 100, "cpu": 1, "memoria": 20, "sip_sessions_ativas": 9,
                     "cps": 0, "agents": {"172.16.10.150": {
                         "status_bruto": "3", "inbound_active": 1, "outbound_active": 2}}},
                    {"ip": "10.90.11.133", "hostname": "M2", "ha": "STANDBY",
                     "health": 100, "cpu": 1, "memoria": 20, "sip_sessions_ativas": 9,
                     "cps": 0, "agents": {}},
                ],
            }), encoding="utf-8")
            feed = card_data(root, now=now.timestamp())
            norte = next(pair for pair in feed["pairs"] if pair["name"] == "SBCs NORTE")
            sul = next(pair for pair in feed["pairs"] if pair["name"] == "SBCs SUL")
            self.assertTrue(norte["fresh"])
            self.assertEqual(norte["observed_ha"], "NORMAL_OBSERVADO")
            self.assertEqual(norte["nodes"][0]["agents"][0]["inbound_active"], 1)
            self.assertFalse(sul["fresh"])


if __name__ == "__main__":
    unittest.main()
