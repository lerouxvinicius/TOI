import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import patch
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'))
from sbc_session_counts import session_report
from mgw.sbc_sessions import Reader, open_store, parse_realm, parse_role, parse_uptime, run_pair, save_cycle


def stamp(text):
    return datetime.fromisoformat(text).timestamp()


def sample(conn, at, total, *, outbound=0, ip='10.90.13.10', boot=1000, active=20,
           peer_error=False, error=False):
    time = stamp(at)
    nodes = [{'ip': ip, 'role_before': 'ACTIVE', 'role_after': 'ACTIVE',
              'boot_epoch': boot, 'error': 'failed' if error else None,
              'realms': {'a-beta-link1': {'started': time, 'finished': time,
                         'in_total': total, 'out_total': outbound,
                         'in_active': active, 'out_active': 0}}},
             {'ip': 'peer', 'role_before': 'STANDBY', 'role_after': 'STANDBY',
              'error': 'failed' if peer_error else None}]
    save_cycle(conn, 'sul', nodes, stamp=time)


class SessionCountersTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.conn = open_store(self.root)

    def tearDown(self):
        self.conn.close()
        self.temp.cleanup()

    def report(self, hour=11, now='2026-09-27T13:00:00-03:00'):
        return session_report(self.root, 'sul', ('a-beta-link1',), date(2026, 9, 27),
                              hour, now=datetime.fromisoformat(now))

    def test_211_new_sessions_excludes_initial_20_active_and_old_lifetime(self):
        sample(self.conn, '2026-09-27T10:59:45-03:00', 10000)
        sample(self.conn, '2026-09-27T11:00:00-03:00', 10000)
        sample(self.conn, '2026-09-27T11:00:15-03:00', 10029, outbound=2, active=29)
        sample(self.conn, '2026-09-27T11:00:30-03:00', 10056, outbound=3, active=27)
        sample(self.conn, '2026-09-27T11:59:45-03:00', 10211, outbound=9, active=0)
        sample(self.conn, '2026-09-27T12:00:00-03:00', 10211, outbound=9, active=0)
        result = self.report()['realms']['a-beta-link1']
        self.assertEqual(result['inbound'], 211)
        self.assertEqual(result['outbound'], 9)
        self.assertEqual(result['unallocated_inbound'], 0)
        self.assertEqual(result['status'], 'partial')  # Deliberately missing 59 minutes of HA verification.

    def test_daily_excludes_previous_day_and_long_lived_active_calls(self):
        sample(self.conn, '2026-09-26T23:59:45-03:00', 50, active=50)
        sample(self.conn, '2026-09-27T00:00:00-03:00', 50, active=50)
        sample(self.conn, '2026-09-27T00:00:15-03:00', 55, active=55)
        result = self.report(None, '2026-09-27T00:00:20-03:00')['realms']['a-beta-link1']
        self.assertEqual(result['inbound'], 5)
        self.assertEqual(result['status'], 'collecting')

    def test_nonzero_boundary_increment_is_not_guessed_into_an_hour(self):
        sample(self.conn, '2026-09-27T10:59:55-03:00', 100)
        sample(self.conn, '2026-09-27T11:00:10-03:00', 107)
        sample(self.conn, '2026-09-27T11:00:25-03:00', 112)
        result = self.report(now='2026-09-27T11:00:30-03:00')['realms']['a-beta-link1']
        self.assertEqual(result['inbound'], 5)
        self.assertEqual(result['unallocated_inbound'], 7)
        self.assertEqual(result['status'], 'partial')

    def test_reset_and_switchover_never_add_new_initial_lifetime(self):
        sample(self.conn, '2026-09-27T11:00:00-03:00', 500)
        sample(self.conn, '2026-09-27T11:00:15-03:00', 510)
        sample(self.conn, '2026-09-27T11:00:30-03:00', 2, boot=2000)
        sample(self.conn, '2026-09-27T11:00:45-03:00', 5, boot=2000)
        sample(self.conn, '2026-09-27T11:01:00-03:00', 9000, ip='10.90.13.11', boot=3000)
        sample(self.conn, '2026-09-27T11:01:15-03:00', 9004, ip='10.90.13.11', boot=3000)
        result = self.report(now='2026-09-27T11:01:20-03:00')['realms']['a-beta-link1']
        self.assertEqual(result['inbound'], 17)
        self.assertEqual(result['status'], 'partial')

    def test_failed_poll_deltas_recover_and_database_survives_restart(self):
        sample(self.conn, '2026-09-27T11:00:00-03:00', 100)
        sample(self.conn, '2026-09-27T11:00:15-03:00', 0, error=True)
        self.conn.close()
        self.conn = open_store(self.root)
        sample(self.conn, '2026-09-27T11:00:30-03:00', 123)
        result = self.report(now='2026-09-27T11:00:35-03:00')['realms']['a-beta-link1']
        self.assertEqual(result['inbound'], 23)
        self.assertEqual(result['status'], 'partial')

    def test_historical_window_without_source_is_unknown(self):
        sample(self.conn, '2026-09-27T12:00:00-03:00', 100)
        sample(self.conn, '2026-09-27T12:00:15-03:00', 110)
        self.assertIsNone(self.report()['realms']['a-beta-link1']['inbound'])

    def test_query_does_not_include_cycles_written_after_search_time(self):
        sample(self.conn, '2026-09-27T11:00:00-03:00', 100)
        sample(self.conn, '2026-09-27T11:00:15-03:00', 110)
        sample(self.conn, '2026-09-27T11:00:30-03:00', 999)
        result = self.report(now='2026-09-27T11:00:20-03:00')
        self.assertEqual(result['realms']['a-beta-link1']['inbound'], 10)
        self.assertEqual(result['latest_at'], '2026-09-27T11:00:15-03:00')

    def test_save_uses_reading_time_instead_of_delayed_commit_time(self):
        observed = stamp('2026-09-27T11:00:00-03:00')
        node = {'ip': '10.90.13.10', 'role_before': 'ACTIVE', 'role_after': 'ACTIVE',
                'boot_epoch': 1000, 'error': None, 'realms': {'a-beta-link1': {
                    'started': observed, 'finished': observed, 'in_total': 100, 'out_total': 0}}}
        with patch('mgw.sbc_sessions.time.time', return_value=observed + 120):
            save_cycle(self.conn, 'sul', [node])
        self.assertEqual(self.conn.execute('SELECT ts FROM cycles').fetchone()[0], observed)

    def test_complete_closed_hour_with_zero_boundaries(self):
        sample(self.conn, '2026-09-27T10:59:55-03:00', 100)
        for second in range(10, 3600, 15):
            at = datetime.fromtimestamp(stamp('2026-09-27T11:00:00-03:00') + second,
                                        datetime.fromisoformat('2026-09-27T11:00:00-03:00').tzinfo)
            sample(self.conn, at.isoformat(), 100 + min(second // 15, 200))
        sample(self.conn, '2026-09-27T12:00:10-03:00', 300)
        result = self.report()['realms']['a-beta-link1']
        self.assertEqual(result['inbound'], 200)
        self.assertEqual(result['status'], 'complete')


class ParserTests(unittest.TestCase):
    def test_actual_sbc_counter_columns_and_no_activity(self):
        text = '''show sipd realms a-alfa-2
20:28:21-57
Realm a-alfa-2() [In Service]
                             -- Period -- -------- Lifetime --------
                   Active    High   Total      Total  PerMax    High
Inbound Sessions        2       5       4     248989     102     251
Outbound Sessions       1       3       2        124       3       6
SBC112-DC2> '''
        values = parse_realm(text, 'a-alfa-2')
        self.assertEqual(values['in_total'], 248989)
        self.assertEqual(values['out_total'], 124)
        self.assertEqual(values['in_active'], 2)
        no_activity = 'Realm a-thetalink-corpx() [In Service] NO ACTIVITY\nSBC112-DC2> '
        self.assertEqual(parse_realm(no_activity, 'a-thetalink-corpx')['in_total'], 0)
        with self.assertRaises(ValueError):
            parse_realm(text, 'a-alfa-4')

    def test_role_uptime_and_read_only_guard(self):
        self.assertEqual(parse_role('\tState\t\tActive\n'), 'ACTIVE')
        self.assertEqual(parse_uptime('FRI FEB 25 13:02:55 2005 - up 1 days, 3 hours, 42 minutes, 30 seconds'), 99750)
        reader = Reader('10.90.13.10', ('a-beta-link1',))
        with self.assertRaisesRegex(ValueError, 'COMMAND_NOT_ALLOWED'):
            reader.command('configure terminal', 0)


class PairIsolationTests(unittest.TestCase):
    def test_slow_peer_does_not_block_other_pairs_cycles(self):
        slow_entered, release = Event(), Event()
        fast_stop, slow_stop = Event(), Event()
        class FastStop:
            def is_set(self):
                return fast_stop.is_set()
            def wait(self, seconds):
                return fast_stop.is_set()  # Skip the normal interval in this fixture.
        class FakeReader:
            def __init__(self, ip, realms):
                self.ip, self.realms = ip, realms
            def collect(self):
                if self.ip == 'slow-active':
                    slow_entered.set()
                    if not release.wait(5):
                        raise AssertionError('fixture did not release slow peer')
                return {'ip': self.ip, 'error': None, 'boot_epoch': 1000,
                        'role_before': 'ACTIVE' if self.ip.endswith('active') else 'STANDBY',
                        'role_after': 'ACTIVE' if self.ip.endswith('active') else 'STANDBY',
                        'realms': {'test': {'started': 100, 'finished': 100,
                                           'in_total': 100, 'out_total': 0}}}
            def close(self):
                pass
        states = []
        def publish(pair, result):
            states.append(pair)
            if states.count('norte') == 2:
                fast_stop.set()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            open_store(root).close()
            with patch('mgw.sbc_sessions.Reader', FakeReader), ThreadPoolExecutor(max_workers=2) as pool:
                slow = pool.submit(run_pair, 'sul', {'ips': ('slow-active', 'slow-standby'),
                                                      'realms': ('test',)}, publish, slow_stop, root=root)
                try:
                    self.assertTrue(slow_entered.wait(2))
                    fast = pool.submit(run_pair, 'norte', {'ips': ('fast-active', 'fast-standby'),
                                                           'realms': ('test',)}, publish, FastStop(), root=root)
                    fast.result(timeout=2)
                    self.assertEqual(states, ['norte', 'norte'])
                    self.assertFalse(slow.done())
                finally:
                    slow_stop.set()
                    fast_stop.set()
                    release.set()
                slow.result(timeout=2)


if __name__ == '__main__':
    unittest.main()
