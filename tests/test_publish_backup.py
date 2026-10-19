import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

DEPLOY = Path(__file__).resolve().parents[1] / 'deploy'
sys.path.insert(0, str(DEPLOY))
spec = importlib.util.spec_from_file_location('publish_backup_test', DEPLOY / 'publish.py')
publish = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publish)


def state(active, invocation, result='success'):
    return {'ActiveState': active, 'InvocationID': invocation, 'Result': result}


class BackupWaitTests(unittest.TestCase):
    def test_waits_for_new_invocation_not_previous_success(self):
        with patch.object(publish, 'backup_state', side_effect=[
            state('inactive', 'old'), state('inactive', 'old'),
            state('activating', 'new'), state('inactive', 'new')]), \
             patch.object(publish, 'run', return_value='Verified') as run, \
             patch.object(publish.time, 'sleep'):
            publish.create_backup(None, 'unused')
        self.assertIn('start --no-block', run.call_args_list[0].args[1])
        self.assertIn('Verified backup', run.call_args_list[1].args[1])

    def test_existing_backup_not_restarted(self):
        with patch.object(publish, 'backup_state', side_effect=[
            state('activating', 'same'), state('inactive', 'same')]), \
             patch.object(publish, 'run', return_value='Verified') as run:
            publish.create_backup(None, 'unused')
        self.assertEqual(run.call_count, 1)
        self.assertNotIn('systemctl start', run.call_args.args[1])

    def test_failure_blocks_release_without_accepting_old_archive(self):
        with patch.object(publish, 'backup_state', side_effect=[
            state('activating', 'same'), state('failed', 'same', 'exit-code')]), \
             patch.object(publish, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'não aprovado'):
                publish.create_backup(None, 'unused')
        run.assert_not_called()
