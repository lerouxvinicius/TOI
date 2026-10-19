"""Read-only ACLI lifetime realm counters, separate from the SNMP live worker.

The private key and the compatibility runtime remain under /var/lib/toi.
Only configured show commands are sent; unknown host keys and admin prompts
are rejected. Counters are persisted, never summed from active gauges.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
from threading import Event, Lock
import time

ROOT = Path(__file__).resolve().parents[1]
ACCESS = Path('/var/lib/toi/sbc-acli')
INTERVAL = 15
PAIRS = {
    'norte': {'ips': ('10.90.11.132', '10.90.11.133'),
               'realms': ('a-alfa-2', 'a-internet-corpx', 'a-thetalink-corpx', 'a-alfa-4', 'a-alfa-4-stir')},
    'sul': {'ips': ('10.90.13.10', '10.90.13.11'),
              'realms': ('a-beta-link1', 'a-beta-link2', 'a-Epsilon', 'a-Prefeitura')},
}
PROMPT = re.compile(r'(?:^|\n)([\w.()/-]{1,100}[>#])\s*$')


def parse_role(text):
    matches = re.findall(r'^\s*State\s+(Active|Standby)\s*$', text, re.I | re.M)
    if len(matches) != 1:
        raise ValueError('HA_STATE_UNKNOWN')
    return matches[0].upper()


def parse_uptime(text):
    match = re.search(r'\bup\s+(\d+)\s+days?,\s*(\d+)\s+hours?,\s*'
                      r'(\d+)\s+minutes?,\s*(\d+)\s+seconds?', text, re.I)
    if not match:
        raise ValueError('UPTIME_UNKNOWN')
    days, hours, minutes, seconds = map(int, match.groups())
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def parse_realm(text, name):
    header = re.search(r'^Realm\s+([^\s(]+)\([^\n]*\)\s+\[([^\]]+)\](.*)$', text, re.M)
    if not header or header.group(1).casefold() != name.casefold():
        raise ValueError('REALM_OUTPUT_MISMATCH')
    clock = re.search(r'^\d{2}:\d{2}:\d{2}(?:-\d+)?\s*$', text, re.M)
    result = {'device_clock': clock.group(0).strip() if clock else None}
    if header.group(3).strip() == 'NO ACTIVITY':
        return {**result, 'in_active': 0, 'out_active': 0, 'in_total': 0, 'out_total': 0}
    if not re.search(r'Active\s+High\s+Total\s+Total\s+PerMax\s+High', text):
        raise ValueError('REALM_COLUMNS_UNKNOWN')
    for direction, label in (('in', 'Inbound'), ('out', 'Outbound')):
        matches = re.findall(r'^' + label + r' Sessions\s+([\d\s]+)$', text, re.M)
        if len(matches) != 1:
            raise ValueError('REALM_COUNTER_MISSING')
        values = matches[0].split()
        if len(values) != 6 or not all(value.isdigit() for value in values):
            raise ValueError('REALM_COUNTER_COLUMNS_UNKNOWN')
        result[direction + '_active'] = int(values[0])
        result[direction + '_total'] = int(values[3])
    return result


class Reader:
    def __init__(self, ip, realms):
        self.ip, self.realms = ip, realms
        self.client = self.channel = None

    def close(self):
        if self.client:
            self.client.close()
        self.client = self.channel = None

    def prompt(self, deadline):
        text = ''
        while time.monotonic() < deadline:
            if self.channel.recv_ready():
                text += self.channel.recv(65536).decode('utf-8', errors='replace').replace('\r', '')
                match = PROMPT.search(text)
                if match:
                    if not match.group(1).endswith('>'):
                        raise RuntimeError('REFUSED_NON_USER_PROMPT')
                    return text
                if len(text) > 200000:
                    raise RuntimeError('OUTPUT_TOO_LARGE')
            if self.channel.closed:
                raise RuntimeError('CHANNEL_CLOSED')
            time.sleep(.02)
        raise TimeoutError('COMMAND_TIMEOUT')

    def connect(self):
        sys.path.insert(0, str(ACCESS / 'runtime-4.0.0'))
        import paramiko
        if paramiko.__version__ != '4.0.0':
            raise RuntimeError('SBC_COMPATIBILITY_RUNTIME_MISSING')
        self.client = paramiko.SSHClient()
        self.client.load_host_keys(str(ACCESS / 'known_hosts'))
        self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        key = paramiko.RSAKey.from_private_key_file(str(ACCESS / 'toi_readonly_rsa'))
        self.client.connect(self.ip, username='toi-monitor', pkey=key,
                            allow_agent=False, look_for_keys=False, timeout=6,
                            auth_timeout=8, banner_timeout=6,
                            disabled_algorithms={'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']})
        self.client.get_transport().set_keepalive(20)
        self.channel = self.client.invoke_shell(width=220, height=500)
        self.prompt(time.monotonic() + 6)

    def command(self, value, deadline):
        allowed = {'show health', 'show uptime'} | {f'show sipd realms {r}' for r in self.realms}
        if value not in allowed:
            raise ValueError('COMMAND_NOT_ALLOWED')
        started = time.time()
        self.channel.sendall(value + '\n')
        text = self.prompt(min(deadline, time.monotonic() + 6))
        return text, started, time.time()

    def collect(self):
        result = {'ip': self.ip, 'realms': {}, 'error': None}
        try:
            if not self.channel or self.channel.closed:
                self.close()
                self.connect()
            deadline = time.monotonic() + 18
            result['role_before'] = parse_role(self.command('show health', deadline)[0])
            text, started, finished = self.command('show uptime', deadline)
            result['boot_epoch'] = (started + finished) / 2 - parse_uptime(text)
            for realm in self.realms:
                text, started, finished = self.command(f'show sipd realms {realm}', deadline)
                try:
                    values = parse_realm(text, realm)
                except ValueError as exc:
                    values = {'error': str(exc)}
                result['realms'][realm] = {**values, 'started': started, 'finished': finished}
            result['role_after'] = parse_role(self.command('show health', deadline)[0])
            if result['role_before'] != result['role_after']:
                result['error'] = 'HA_CHANGED_DURING_READ'
        except Exception as exc:
            result['error'] = f'{type(exc).__name__}: {exc}'
            self.close()
        return result


def open_store(root=ROOT):
    path = Path(root) / 'dados' / 'sbc_sessions.sqlite'
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=FULL')
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS cycles (
            id INTEGER PRIMARY KEY, pair TEXT NOT NULL, ts REAL NOT NULL,
            ip TEXT, boot_epoch REAL, source_ok INTEGER NOT NULL, error TEXT);
        CREATE INDEX IF NOT EXISTS ix_cycles_pair_ts ON cycles(pair, ts);
        CREATE TABLE IF NOT EXISTS readings (
            cycle_id INTEGER NOT NULL REFERENCES cycles(id), realm TEXT NOT NULL,
            started REAL NOT NULL, finished REAL NOT NULL,
            in_total INTEGER, out_total INTEGER, in_active INTEGER, out_active INTEGER,
            device_clock TEXT, error TEXT, PRIMARY KEY(cycle_id, realm));
    ''')
    conn.commit()
    os.chmod(path, 0o600)
    return conn


def save_cycle(conn, pair, nodes, *, stamp=None):
    active = [node for node in nodes if not node.get('error') and
              node.get('role_before') == node.get('role_after') == 'ACTIVE']
    chosen = active[0] if len(active) == 1 else None
    error = None if chosen else 'ACTIVE_NOT_CONFIRMED'
    if chosen and any(node.get('error') for node in nodes):
        error = 'HA_PEER_UNKNOWN'
    if stamp is None:
        # Waiting for a slow standby must not make the ACTIVE's older readings
        # appear to have been taken at the later database commit time.
        finished = [item['finished'] for item in chosen['realms'].values()] if chosen else []
        stamp = max(finished) if finished else time.time()
    with conn:
        cursor = conn.execute('INSERT INTO cycles(pair,ts,ip,boot_epoch,source_ok,error) '
                              'VALUES(?,?,?,?,?,?)', (pair, stamp,
                              chosen['ip'] if chosen else None,
                              chosen.get('boot_epoch') if chosen else None,
                              int(chosen is not None), error))
        if chosen:
            for realm, item in chosen['realms'].items():
                conn.execute('INSERT INTO readings VALUES(?,?,?,?,?,?,?,?,?,?)',
                             (cursor.lastrowid, realm, item['started'], item['finished'],
                              item.get('in_total'), item.get('out_total'),
                              item.get('in_active'), item.get('out_active'),
                              item.get('device_clock'), item.get('error')))
    return {'timestamp': datetime.fromtimestamp(stamp, timezone.utc).isoformat(),
            'active_ip': chosen['ip'] if chosen else None, 'error': error,
            'source_ok': chosen is not None, 'nodes': nodes}


def run_pair(pair, config, publish, stop, *, root=ROOT):
    conn = open_store(root)
    readers = [Reader(ip, config['realms']) for ip in config['ips']]
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            while not stop.is_set():
                futures = [pool.submit(reader.collect) for reader in readers]
                state = save_cycle(conn, pair, [future.result() for future in futures])
                publish(pair, state)
                print(f"SBC sessions {pair}: active={state['active_ip']} "
                      f"error={state['error']}", flush=True)
                # Align reads to 15-second wall-clock slots, including hour/day boundaries.
                stop.wait(max(.1, INTERVAL - time.time() % INTERVAL))
        finally:
            for reader in readers:
                reader.close()
            conn.close()


def main():
    # Initialize the schema before the independent pair writers start.
    open_store().close()
    stop, lock = Event(), Lock()
    state = {}
    def publish(pair, result):
        with lock:
            state[pair] = result
            path = ROOT / 'dados' / 'sbc_sessions_status.json'
            temp = path.with_suffix('.tmp')
            temp.write_text(json.dumps(state, ensure_ascii=False), encoding='utf-8')
            temp.replace(path)
    # A slow or unreachable Sul node cannot postpone Norte's next cycle,
    # and vice versa. Each pair has its own SSH workers and SQLite connection.
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run_pair, pair, config, publish, stop)
                   for pair, config in PAIRS.items()]
        try:
            for future in as_completed(futures):
                future.result()
        finally:
            stop.set()


if __name__ == '__main__':
    main()
