"""Read-only check of new-session collection and both report/filter paths."""
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import sys

from sbc_check_snapshot import snapshot_sbc_sources

ROOT = Path('/var/www/toi')
sys.path.insert(0, str(ROOT / 'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'))
from sbc_traffic_history import PAIRS, traffic_report

def check(root, now):
    conn = sqlite3.connect((root / 'dados' / 'sbc_sessions.sqlite').as_uri() + '?mode=ro', uri=True)
    result = {}
    try:
        for pair, config in PAIRS.items():
            report = traffic_report(root, pair, now.date().isoformat(), now.hour, now=now)
            counts = report['new_sessions']
            assert counts['fresh'], f'{pair}: ACLI source stale or unavailable'
            recent = conn.execute('SELECT id FROM cycles WHERE pair=? AND source_ok=1 AND error IS NULL '
                                  'AND ts>? AND ts<=? ORDER BY ts DESC LIMIT 2',
                                  (pair, now.timestamp() - 60, now.timestamp())).fetchall()
            assert len(recent) == 2, f'{pair}: waiting for two valid cycles'
            for cycle_id, in recent:
                readings = conn.execute('SELECT realm,in_total,out_total,error FROM readings WHERE cycle_id=?',
                                         (cycle_id,)).fetchall()
                valid = {row[0] for row in readings if row[1] is not None and row[2] is not None and row[3] is None}
                assert set(config['realms']) <= valid, f'{pair}: incomplete realm counters'
            # Startup around an hour boundary still needs two samples in a day scope.
            daily = traffic_report(root, pair, now.date().isoformat(), now=now)
            assert all(daily['new_sessions']['realms'][realm]['inbound'] is not None
                       for realm in config['realms']), f'{pair}: no count deltas yet'
            filtered = traffic_report(root, pair, now.date().isoformat(), now.hour, config['realms'][0], now=now)
            assert report['new_sessions'] == filtered['new_sessions'], (
                f'{pair}: count consolidation changed with filter')
            result[pair] = {'age_s': counts['age_s'], 'realms': len(config['realms']),
                            'first_collection_at': counts['first_collection_at'],
                            'states': {realm: daily['new_sessions']['realms'][realm]['status'] for realm in config['realms']},
                            'known_new_sessions_today': {realm: {
                                'inbound': daily['new_sessions']['realms'][realm]['inbound'],
                                'outbound': daily['new_sessions']['realms'][realm]['outbound']}
                                for realm in config['realms']}}
    finally:
        conn.close()
    return result


def main():
    now = datetime.now().astimezone()
    with snapshot_sbc_sources(ROOT, now.date().isoformat(), include_sessions=True) as frozen:
        result = check(frozen, now)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
