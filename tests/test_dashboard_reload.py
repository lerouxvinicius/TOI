import contextlib
import io
import json
from datetime import datetime,timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from deploy import reload_dashboard as reload


def state(web=101, collector=202):
    return {'components': {'dashboard': {'pid': web}, 'collector': {'pid': collector}}}


class DashboardReloadTests(unittest.TestCase):
    def test_real_verifier_checks_budget_of_requested_component(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            snapshot=state()
            snapshot.update(supervisor_pid=111,generated_at=datetime.now(timezone.utc).isoformat())
            for name,filename in (('dashboard','toi_dashboard_server_v1_10_6.py'),('collector','toi_live_collector_v1_10_6.py')):
                entry=snapshot['components'][name]
                entry.update(running=True,restart_budget_remaining=1 if name=='dashboard' else 9)
                proc=root/'proc'/str(entry['pid'])
                proc.mkdir(parents=True)
                (proc/'cmdline').write_bytes(str(reload.APP/filename).encode()+b'\0')
                (proc/'status').write_text('PPid:\t111\n')
            status=root/'status.json'
            status.write_text(json.dumps(snapshot))
            with patch.object(reload,'STATUS',status), \
                 patch.object(reload,'Path',side_effect=lambda value:root/'proc' if value=='/proc' else Path(value)), \
                 patch.object(reload.os,'getuid',return_value=(root/'proc'/'101').stat().st_uid,create=True):
                with self.assertRaisesRegex(RuntimeError,'restart budget insufficient'):
                    reload.verified_state(2,component='dashboard')
                self.assertEqual(reload.verified_state(2,component='collector')['components']['collector']['restart_budget_remaining'],9)

    def test_only_verified_web_child_receives_signal(self):
        response = MagicMock()
        response.__enter__.return_value.status = 200
        with patch.object(reload, 'verified_state', side_effect=[state(), state(303)]), \
             patch.object(reload.os, 'kill') as kill, \
             patch.object(reload.time, 'sleep'), \
             patch.object(reload, 'urlopen', return_value=response), \
             patch.object(reload.sys, 'argv', ['reload']), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            reload.main()
        kill.assert_called_once_with(101, reload.signal.SIGTERM)
        self.assertIn('"collector_preserved": true', output.getvalue())

    def test_invalid_process_or_budget_never_signalled(self):
        with patch.object(reload, 'verified_state', side_effect=RuntimeError('invalid identity')), \
             patch.object(reload.os, 'kill') as kill:
            with self.assertRaises(RuntimeError):
                reload.main()
        kill.assert_not_called()

    def test_collector_change_is_not_reported_as_success(self):
        with patch.object(reload, 'verified_state', side_effect=[state(), state(303, 404)]), \
             patch.object(reload.os, 'kill'), patch.object(reload.time, 'sleep'), \
             patch.object(reload.sys, 'argv', ['reload']):
            with self.assertRaisesRegex(RuntimeError, 'Collector changed'):
                reload.main()

    def test_preflight_never_signals(self):
        with patch.object(reload, 'verified_state', return_value=state()) as verify, \
             patch.object(reload.os, 'kill') as kill, \
             patch.object(reload.sys, 'argv', ['reload', '--check']), \
             contextlib.redirect_stdout(io.StringIO()):
            reload.main()
        verify.assert_called_once_with(2)
        kill.assert_not_called()

    def test_collector_reload_only_signals_verified_collector_and_preserves_dashboard(self):
        response = MagicMock()
        response.__enter__.return_value.status = 200
        with patch.object(reload, 'verified_state', side_effect=[state(), state(101, 404)]) as verify, \
             patch.object(reload.os, 'kill') as kill, patch.object(reload.time, 'sleep'), \
             patch.object(reload, 'urlopen', return_value=response), \
             patch.object(reload.sys, 'argv', ['reload', '--collector']), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            reload.main()
        kill.assert_called_once_with(202, reload.signal.SIGTERM)
        verify.assert_any_call(1, component='collector')
        self.assertIn('"dashboard_preserved": true', output.getvalue())
        self.assertIn('"collector_pid": 404', output.getvalue())

    def test_collector_preflight_checks_its_budget_without_signalling(self):
        with patch.object(reload, 'verified_state', return_value=state()) as verify, \
             patch.object(reload.os, 'kill') as kill, \
             patch.object(reload.sys, 'argv', ['reload', '--collector', '--check']), \
             contextlib.redirect_stdout(io.StringIO()):
            reload.main()
        verify.assert_called_once_with(2, component='collector')
        kill.assert_not_called()
