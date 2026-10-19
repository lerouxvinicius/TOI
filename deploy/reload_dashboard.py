"""Reload one verified child of the TOI supervisor, preserving the other child."""
import json
import os
from pathlib import Path
import signal
import sys
import time
from datetime import datetime
from urllib.request import urlopen

APP = Path('/var/www/toi/TOI_v1.10.6_DUAL_LANE_ML_SHADOW')
STATUS = APP / 'runtime/toi_supervisor_runtime_status_v1.json'


def verified_state(minimum_budget, component='dashboard'):
    state = json.loads(STATUS.read_text())
    age = time.time() - datetime.fromisoformat(state['generated_at']).timestamp()
    if not -5 <= age <= 40:
        raise RuntimeError('Supervisor status is not current')
    for name, filename in (('dashboard', 'toi_dashboard_server_v1_10_6.py'),
                           ('collector', 'toi_live_collector_v1_10_6.py')):
        entry = state['components'][name]
        proc = Path('/proc') / str(int(entry['pid']))
        command = proc.joinpath('cmdline').read_bytes().split(b'\0')
        if not entry['running'] or str(APP / filename).encode() not in command:
            raise RuntimeError('Process identity mismatch: ' + name)
        fields = dict(line.split(':', 1) for line in proc.joinpath('status').read_text().splitlines() if ':' in line)
        if int(fields['PPid']) != state['supervisor_pid'] or proc.stat().st_uid != os.getuid():
            raise RuntimeError('Process ownership mismatch: ' + name)
    if state['components'][component]['restart_budget_remaining'] < minimum_budget:
        raise RuntimeError('Supervisor restart budget insufficient')
    return state


def main():
    check = '--check' in sys.argv
    component = 'collector' if '--collector' in sys.argv else 'dashboard'
    other = 'dashboard' if component == 'collector' else 'collector'
    def verify(budget):
        return verified_state(budget) if component == 'dashboard' else verified_state(budget, component='collector')
    before = verify(2 if check else 1)
    old_pid = before['components'][component]['pid']
    preserved = before['components'][other]['pid']
    if check:
        print(json.dumps({'dashboard_pid': before['components']['dashboard']['pid'],
                          'collector_pid': before['components']['collector']['pid'],
                          'component': component, 'ready': True}))
        return
    os.kill(old_pid, signal.SIGTERM)
    deadline = time.monotonic() + 75
    while time.monotonic() < deadline:
        time.sleep(1)
        try:
            after = verify(0)
            if after['components'][other]['pid'] != preserved:
                raise RuntimeError(other.capitalize() + ' changed while reloading ' + component)
            if after['components'][component]['pid'] == old_pid:
                continue
            with urlopen('http://127.0.0.1:8088/api/health', timeout=3) as response:
                if response.status == 200:
                    print(json.dumps({'dashboard_pid': after['components']['dashboard']['pid'],
                                      'collector_pid': after['components']['collector']['pid'],
                                      'component': component, 'previous_pid': old_pid,
                                      other+'_preserved': True}))
                    return
        except (OSError, ValueError, ProcessLookupError):
            continue
        except RuntimeError as exc:
            if ' changed while reloading ' in str(exc):
                raise
    raise RuntimeError(component.capitalize() + ' reload not confirmed')


if __name__ == '__main__':
    main()
