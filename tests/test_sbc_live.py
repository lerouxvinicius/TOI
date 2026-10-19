import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from mgw import sbc_live
from mgw.sip_capacity import AMBIENTES


class LivePersistenceTests(unittest.TestCase):
    def test_pair_histories_are_independent_and_failure_is_not_a_zero_sample(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            norte = AMBIENTES["SBCs NORTE"]
            sul = AMBIENTES["SBCs SUL"]
            base = {"timestamp": "2026-09-27T15:00:00-03:00", "source_ok": True,
                    "realms": {"a-alfa-2": {"inbound_active": 2, "outbound_active": 1}},
                    "nodes": [], "sbc_active": {"ip": "10.90.11.132"}}
            sbc_live.save_pair(norte, base, root=root)
            sbc_live.save_pair(sul, {**base, "realms": {"a-beta-link1": {
                "inbound_active": 3, "outbound_active": 0}}}, root=root)
            sbc_live.save_pair(norte, {**base, "source_ok": False,
                                        "timestamp": "2026-09-27T15:00:15-03:00"}, root=root)
            self.assertFalse(json.loads((root / "dados/sbc_live/norte.json").read_text())["source_ok"])
            self.assertTrue(json.loads((root / "dados/sbc_live/sul.json").read_text())["source_ok"])
            self.assertEqual(len((root / "dados/sip_capacity/sip_capacity_2026-09-27.jsonl").read_text().splitlines()), 1)
            self.assertEqual(len((root / "dados/sip_capacity/sul/sip_capacity_2026-09-27.jsonl").read_text().splitlines()), 1)

    def test_oid_index_must_stay_in_its_subtree(self):
        self.assertEqual(sbc_live._index((1, 3, 6, 1, 9), "1.3.6.1"), "9")
        self.assertIsNone(sbc_live._index((1, 3, 6, 2, 9), "1.3.6.1"))


class LiveCollectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_active_node_supplies_realm_values(self):
        class Engine:
            def close_dispatcher(self):
                pass

        async def scalar(_engine, _community, ip):
            return {"ip": ip, "ha": "ACTIVE" if ip.endswith(".132") else "STANDBY",
                    "hostname": ip, "sessions_total": 10}

        with patch.object(sbc_live, "SnmpEngine", return_value=Engine()), \
             patch.object(sbc_live, "_scalars", side_effect=scalar), \
             patch.object(sbc_live, "_agents", new_callable=AsyncMock, return_value={}), \
             patch.object(sbc_live, "_walk", new_callable=AsyncMock, return_value={}), \
             patch.object(sbc_live, "_realms", new_callable=AsyncMock,
                          return_value={"a-alfa-2": {"inbound_active": 4,
                                                    "outbound_active": 0}}) as realms:
            data = await sbc_live.collect_pair("test", AMBIENTES["SBCs NORTE"])
        self.assertTrue(data["source_ok"])
        self.assertEqual(data["sbc_active"]["ip"], "10.90.11.132")
        self.assertEqual(data["realms"]["a-alfa-2"]["inbound_active"], 4)
        self.assertEqual(realms.await_count, 1)


if __name__ == "__main__":
    unittest.main()
