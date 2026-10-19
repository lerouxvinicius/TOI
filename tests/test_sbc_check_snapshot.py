"""Checks must stay strict and deterministic while live collectors append."""
from datetime import datetime
from contextlib import closing
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'deploy'))
sys.path.insert(0, str(ROOT / 'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'))
from sbc_check_snapshot import snapshot_sbc_sources
from check_sbc_traffic import check as check_traffic
from check_sbc_sessions import check as check_sessions
from sbc_traffic_history import PAIRS, traffic_report
from mgw.sbc_sessions import open_store, save_cycle


class SnapshotTests(unittest.TestCase):
    def test_append_in_live_sources_does_not_change_filter_comparison(self):
        with tempfile.TemporaryDirectory() as directory, closing(open_store(Path(directory))) as conn:
            root = Path(directory)
            now = datetime.fromisoformat('2026-09-27T11:01:00-03:00')
            def write_cycle(second, total):
                observed = now.replace(second=second, minute=0)
                for pair, config in PAIRS.items():
                    realms = config['realms']
                    path = root / 'dados/sip_capacity' / config['folder'] / 'sip_capacity_2026-09-27.jsonl'
                    path.parent.mkdir(parents=True, exist_ok=True)
                    row = {'timestamp': observed.isoformat(), 'source_ok': True,
                           'realms': {realm: {'inbound_active': total, 'outbound_active': 0}
                                      for realm in realms}, 'nodes': []}
                    with path.open('a', encoding='utf-8') as stream:
                        stream.write(json.dumps(row) + '\n')
                    live = root / 'dados/sbc_live' / f'{pair}.json'
                    live.parent.mkdir(parents=True, exist_ok=True)
                    live.write_text(json.dumps(row), encoding='utf-8')
                    stamp = observed.timestamp()
                    save_cycle(conn, pair, [{'ip': pair, 'error': None, 'boot_epoch': 1000,
                        'role_before': 'ACTIVE', 'role_after': 'ACTIVE',
                        'realms': {realm: {'started': stamp, 'finished': stamp,
                            'in_total': total, 'out_total': 0, 'in_active': 1, 'out_active': 0}
                                   for realm in realms}}], stamp=stamp)
            write_cycle(15, 100)
            write_cycle(30, 110)
            with snapshot_sbc_sources(root, '2026-09-27', include_sessions=True) as frozen:
                before = traffic_report(frozen, 'norte', '2026-09-27', 11, now=now)
                # A real WAL commit and JSONL append occur between the compared reads.
                write_cycle(45, 120)
                after = traffic_report(frozen, 'norte', '2026-09-27', 11, 'a-alfa-2', now=now)
                current = traffic_report(root, 'norte', '2026-09-27', 11, now=now)
                self.assertEqual(before['consolidated'], after['consolidated'])
                self.assertEqual(before['new_sessions'], after['new_sessions'])
                self.assertEqual(after['new_sessions']['realms']['a-alfa-2']['inbound'], 10)
                self.assertEqual(current['new_sessions']['realms']['a-alfa-2']['inbound'], 20)
                self.assertEqual(current['consolidated']['samples'], 3)
                self.assertEqual(after['consolidated']['samples'], 2)
                self.assertEqual(set(check_traffic(frozen, now)), {'norte', 'sul'})
                self.assertEqual(set(check_sessions(frozen, now)), {'norte', 'sul'})
                # Freshness gates must still reject delayed sources.
                stale = now.replace(minute=5)
                with self.assertRaisesRegex(AssertionError, 'history delayed'):
                    check_traffic(frozen, stale)
                with self.assertRaisesRegex(AssertionError, 'ACLI source stale'):
                    check_sessions(frozen, stale)
            self.assertFalse(frozen.exists())

    def test_reports_stop_at_search_time(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'dados/sip_capacity/sip_capacity_2026-09-27.jsonl'
            path.parent.mkdir(parents=True)
            path.write_text('\n'.join(json.dumps({'timestamp': stamp,
                'realms': {'a-alfa-2': {'inbound_active': value, 'outbound_active': 0}}})
                for stamp, value in [('2026-09-27T11:00:00', 1),
                                     ('2026-09-27T11:00:30', 99)]) + '\n', encoding='utf-8')
            result = traffic_report(root, 'norte', '2026-09-27', 11,
                                    now=datetime.fromisoformat('2026-09-27T11:00:15-03:00'))
            self.assertEqual(result['consolidated']['samples'], 1)
            self.assertEqual(result['consolidated']['inbound']['peak'], 1)


if __name__ == '__main__':
    unittest.main()
