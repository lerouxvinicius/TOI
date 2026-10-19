"""Per-instance SIP server inventory from the Portal's dedicated source."""
from datetime import datetime, timezone
from pathlib import Path
import json
import sqlite3


def parse_inventory(doc):
    if not isinstance(doc, dict) or not isinstance(doc.get('sipServerList'), list):
        raise ValueError('SIP_INVENTORY_SCHEMA')
    def count(value):
        if value is None or isinstance(value, bool):
            return None
        try:
            number = int(value)
            return number if number >= 0 else None
        except (ValueError, TypeError):
            return None
    items = []
    for row in doc['sipServerList']:
        if not isinstance(row, dict) or not row.get('CodeIpSipServer'):
            raise ValueError('SIP_SERVER_ID_MISSING')
        agents, capacity = count(row.get('QtAgents')), count(row.get('QtMaxAgents'))
        items.append({'server': str(row['CodeIpSipServer']), 'agents': agents,
                      'distinct_agents': count(row.get('QtDistinctAgents')),
                      'capacity': capacity,
                      'occupancy': round(100 * agents / capacity, 2)
                      if agents is not None and capacity else None})
    return {'items': items, 'source_clock': doc.get('dtCacheAndServerSipServerData')}


def inventory_report(root, instance, *, now=None):
    now = now or datetime.now(timezone.utc)
    result = {'instance': str(instance), 'items': [], 'sample_at': None, 'age_s': None,
              'fresh': False, 'status': 'SEM_DADOS', 'source': 'Portal Orbe · SIP Servers',
              'poll_interval_s': 15}
    path = Path(root) / 'dados/portal_sip.sqlite'
    if not path.is_file():
        return result
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute('BEGIN')
            latest = conn.execute('SELECT * FROM snapshots WHERE instance=? ORDER BY id DESC LIMIT 1',
                                  (str(instance),)).fetchone()
            valid = conn.execute('SELECT * FROM snapshots WHERE instance=? AND error IS NULL '
                                 'ORDER BY id DESC LIMIT 1', (str(instance),)).fetchone()
        finally:
            conn.close()
        if valid:
            age = now.timestamp() - valid['ts']
            result.update(json.loads(valid['payload']))
            result.update(sample_at=datetime.fromtimestamp(valid['ts'], timezone.utc).isoformat(),
                          age_s=round(max(0, age), 1), fresh=-5 <= age <= 45 and not latest['error'])
            result['status'] = 'LIVE' if result['fresh'] else 'ATRASADO'
        if latest and latest['error']:
            result.update(status='INDISPONÍVEL', fresh=False, error='Falha na leitura; última amostra preservada.')
    except (sqlite3.Error, ValueError, OSError):
        result.update(status='INDISPONÍVEL', error='Consulta SIP temporariamente indisponível.')
    return result
