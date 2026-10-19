from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'))
from sip_inventory import parse_inventory, inventory_report
from mgw.portal_sip import open_store


class SipInventoryTests(unittest.TestCase):
    def test_actual_portal_schema_and_unknown_capacity(self):
        result = parse_inventory({'sipServerList': [
            {'CodeIpSipServer': '10.90.10.141', 'QtAgents': 20, 'QtDistinctAgents': 19, 'QtMaxAgents': 400},
            {'CodeIpSipServer': '10.90.10.142', 'QtAgents': None}],
            'dtCacheAndServerSipServerData': '21:43:24/21:43:24'})
        self.assertEqual(result['items'][0]['occupancy'], 5)
        self.assertEqual(result['items'][0]['distinct_agents'], 19)
        self.assertIsNone(result['items'][1]['occupancy'])
        with self.assertRaises(ValueError):
            parse_inventory({'lastCallsList': []})

    def test_instance_independence_persistence_and_failure_preservation(self):
        with tempfile.TemporaryDirectory() as directory:
            with closing(open_store(directory)) as conn:
                payload = json.dumps(parse_inventory({'sipServerList': [
                    {'CodeIpSipServer': 'server-five', 'QtAgents': 20, 'QtMaxAgents': 400}]}))
                with conn:
                    conn.execute('INSERT INTO snapshots(instance,ts,payload,error) VALUES(?,?,?,?)',
                                 ('5', 1000, payload, None))
            now = datetime.fromtimestamp(1010, timezone.utc)
            fresh = inventory_report(directory, '5', now=now)
            self.assertTrue(fresh['fresh'])
            self.assertEqual(fresh['items'][0]['server'], 'server-five')
            self.assertEqual(inventory_report(directory, '4', now=now)['items'], [])
            with closing(open_store(directory)) as conn:
                with conn:
                    conn.execute('INSERT INTO snapshots(instance,ts,error) VALUES(?,?,?)', ('5', 1011, 'Timeout'))
            failed = inventory_report(directory, '5', now=datetime.fromtimestamp(1012, timezone.utc))
            self.assertFalse(failed['fresh'])
            self.assertEqual(failed['items'], fresh['items'])
            self.assertEqual(failed['status'], 'INDISPONÍVEL')

    def test_empty_inventory_is_observed_but_old_source_is_not_fresh(self):
        with tempfile.TemporaryDirectory() as directory:
            with closing(open_store(directory)) as conn:
                with conn:
                    conn.execute('INSERT INTO snapshots(instance,ts,payload) VALUES(?,?,?)',
                                 ('1', 1000, json.dumps(parse_inventory({'sipServerList': []}))))
            result = inventory_report(directory, '1', now=datetime.fromtimestamp(1100, timezone.utc))
            self.assertEqual(result['items'], [])
            self.assertFalse(result['fresh'])
            self.assertEqual(result['status'], 'ATRASADO')
