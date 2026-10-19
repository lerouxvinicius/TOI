"""Read-only SBC HA evidence. SSH requires a configured key and known host.

This collector never changes an appliance and never infers recovery from silence.
It runs as a child of monitor_alertas when SBC_ACLI_USER is configured.
"""
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / 'dados' / 'sbc_peer_acli_state.json'
INVENTORY = ROOT / 'sbc_traps_config.json'
PERIOD_SECONDS = 60
ALARM_ID = '131106'


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def parse_alarms(output, expected_peer):
    """Return True/False only for a complete current-alarm listing."""
    if not re.search(r'\bdone\b', output, re.I) or not re.search(
            r'\b(?:\d+\s+alarms?\s+to\s+show|no\s+alarms?)\b', output, re.I):
        return None
    blocks = re.split(r'(?m)^\s*(?=\d{5,}\s+\d+\s+\d+\s+\d{4}-\d{2}-\d{2})', output)
    active = [block for block in blocks if re.match(r'^\s*131106\s+', block)]
    if not active:
        return False
    # A current alarm with an unidentifiable peer cannot be assigned to a pair.
    sysname = expected_peer.get('sysname', '')
    names = [sysname, sysname.split('.')[0] if sysname else '', expected_peer.get('name', '')]
    if any(name and re.search(r'(?<![\w.-])' + re.escape(name) + r'(?![\w.-])', block, re.I)
           for block in active for name in names):
        return True
    return None


def parse_health(output, expected_peer=None):
    """Extract peer stanza; a static 'last received from' line is not freshness."""
    address = re.search(r'(?im)^\s*Active Peer Address\s*[:=]?\s*(\S+)', output)
    peer_name = (expected_peer or {}).get('sysname')
    stanza = re.search(r'(?im)^\s*([^\s:]+):\s*v\d+,\s*(?:Active|Standby)[^\r\n]*\n\s*last received from\s+([^\s]+)', output)
    matched = bool(stanza and peer_name and stanza.group(1).lower() in
                   {peer_name.lower(), peer_name.split('.')[0].lower()})
    message = re.search(r'(?im)^\s*Last (?:Message|Msg) Received(?: from (?:the )?Peer)?\s*[:=]?\s*(.+?)\s*$', output)
    value = message.group(1).strip() if message else (stanza.group(2) if matched else None)
    stamp = re.search(r'\b(20\d\d-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:Z|[+-]\d\d:?\d\d)?)\b', value or '')
    return {'active_peer_address': address.group(1) if address else None,
            'last_message_received': value,
            'heartbeat_timestamp': stamp.group(1) if stamp else None,
            'health_peer_identified': matched}


def read_command(ip, user, command, *, key=None):
    args = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
            '-o', 'ConnectTimeout=6', '-o', 'NumberOfPasswordPrompts=0']
    if key:
        args += ['-i', key]
    args += [f'{user}@{ip}', command]
    result = subprocess.run(args, capture_output=True, text=True, encoding='utf-8',
                            errors='replace', timeout=12, stdin=subprocess.DEVNULL)
    if result.returncode:
        raise RuntimeError(f'{command}: SSH_EXIT_{result.returncode}')
    return result.stdout


def collect_one(ip, member, inventory, user, key=None, *, runner=read_command):
    at = utc_now()
    peer = inventory.get(member.get('peer_ip'), {})
    entry = {'ip': ip, 'sysname': member.get('sysname'), 'pair': member.get('pair'),
             'peer_ip': member.get('peer_ip'), 'peer_sysname': peer.get('sysname'),
             'collected_utc': at, 'source': 'ALARME_ATUAL_ACLI+HEALTH_ACLI',
             'alarm_active': None, 'last_message_received': None,
             'active_peer_address': None, 'error': None}
    if not peer or not entry['sysname'] or not entry['pair']:
        entry['error'] = 'INVENTARIO_INCOMPLETO'
        return entry
    try:
        alarms = runner(ip, user, 'display-alarms', key=key)
        health = runner(ip, user, 'show health', key=key)
        entry['alarm_active'] = parse_alarms(alarms, peer)
        entry.update(parse_health(health, peer))
        if entry['alarm_active'] is None or not entry['health_peer_identified']:
            entry['error'] = 'SAIDA_ACLI_NAO_INTERPRETAVEL'
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        entry['error'] = type(exc).__name__ if not isinstance(exc, RuntimeError) else str(exc)
    return entry


def collect_once(*, output=OUTPUT, inventory_path=INVENTORY, runner=read_command):
    user = os.getenv('SBC_ACLI_USER', '').strip()
    key = os.getenv('SBC_ACLI_KEY', '').strip() or None
    inventory = json.loads(Path(inventory_path).read_text(encoding='utf-8')).get('members', {})
    result = {'collected_utc': utc_now(), 'poll_seconds': PERIOD_SECONDS,
              'source': 'ACLI_READ_ONLY', 'nodes': {}}
    output = Path(output)
    try:
        previous = json.loads(output.read_text(encoding='utf-8')).get('nodes', {})
    except (OSError, ValueError):
        previous = {}
    if user:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {ip: pool.submit(collect_one, ip, member, inventory, user, key,
                                       runner=runner) for ip, member in inventory.items()}
            for ip, future in futures.items():
                result['nodes'][ip] = future.result()
    else:
        result['nodes'] = {ip: {'ip': ip, 'error': 'SBC_ACLI_USER_NAO_CONFIGURADO',
                                'collected_utc': result['collected_utc']}
                           for ip in inventory}
    for ip in inventory:
        current = result['nodes'][ip]
        current['heartbeat_verified'] = False
        try:
            before = datetime.fromisoformat(previous.get(ip, {}).get('heartbeat_timestamp'))
            after = datetime.fromisoformat(current.get('heartbeat_timestamp'))
            collected = datetime.fromisoformat(current['collected_utc'])
            current['heartbeat_verified'] = (current.get('error') is None and
                current.get('health_peer_identified') is True and
                before.tzinfo is not None and after.tzinfo is not None and
                before < after <= collected and (collected - after).total_seconds() <= 120)
        except (TypeError, ValueError, KeyError):
            pass
        current['heartbeat_verified_since'] = (
            previous.get(ip, {}).get('heartbeat_verified_since') or current['collected_utc']) \
            if current['heartbeat_verified'] else None
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix('.tmp')
    temp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    os.chmod(temp, 0o600)
    temp.replace(output)
    return result


if __name__ == '__main__':
    if '--once' in sys.argv:
        collect_once()
    else:
        while True:
            started = time.monotonic()
            collect_once()
            time.sleep(max(1, PERIOD_SECONDS - (time.monotonic() - started)))
