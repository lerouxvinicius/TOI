"""Count new realm sessions from persisted lifetime counters, with coverage.

No active gauge, sliding window total, standby total or initial lifetime value
is added to the result. Increments crossing a selected boundary are retained
as unallocated; resets and node changes never become an invented count.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

TZ = ZoneInfo('America/Sao_Paulo')


def _iso(stamp):
    return datetime.fromtimestamp(stamp, TZ).isoformat(timespec='seconds') if stamp is not None else None


def session_report(root, pair, realms, day: date, hour=None, *, now=None):
    now = (now or datetime.now(TZ)).astimezone(TZ)
    start_dt = datetime.combine(day, datetime.min.time(), TZ)
    if hour is not None:
        start_dt += timedelta(hours=hour)
    nominal_end = start_dt + timedelta(hours=1) if hour is not None else start_dt + timedelta(days=1)
    start, end = start_dt.timestamp(), min(nominal_end.timestamp(), now.timestamp())
    empty = {name: {'inbound': None, 'outbound': None, 'status': 'unavailable',
                    'unallocated_inbound': None, 'unallocated_outbound': None,
                    'first_at': None, 'as_of': None, 'reasons': []} for name in realms}
    report = {'realms': empty, 'first_collection_at': None, 'latest_at': None,
              'fresh': False, 'age_s': None, 'interval_s': 15, 'source': 'ACLI lifetime counters',
              'window_start': start_dt.isoformat(), 'window_end': _iso(end),
              'open_interval': nominal_end > now,
              'note': 'Novas sessões por realm e sentido; sessões já existentes no início '
                      'não são somadas. Uma chamada pode aparecer em realms diferentes.'}
    path = Path(root) / 'dados' / 'sbc_sessions.sqlite'
    if not path.is_file() or end <= start:
        return report
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)
        conn.row_factory = sqlite3.Row
        try:
            # All metadata and realm deltas must refer to one committed state,
            # even if a collector appends a new cycle during this request.
            conn.execute('BEGIN')
            first = conn.execute('SELECT MIN(ts) FROM cycles WHERE pair=? AND source_ok=1 AND ts<=?',
                                 (pair, now.timestamp())).fetchone()[0]
            latest = conn.execute('SELECT ts, source_ok FROM cycles WHERE pair=? AND ts<=? ORDER BY ts DESC LIMIT 1',
                                  (pair, now.timestamp())).fetchone()
            previous = []
            for realm in realms:
                value = conn.execute('SELECT c.ts FROM cycles c JOIN readings r ON r.cycle_id=c.id '
                    'WHERE c.pair=? AND c.ts<? AND c.source_ok=1 AND r.realm=? AND r.error IS NULL '
                    'AND r.in_total IS NOT NULL AND r.out_total IS NOT NULL '
                    'ORDER BY c.ts DESC LIMIT 1', (pair, start, realm)).fetchone()
                if value:
                    previous.append(value[0])
            lower = min(previous + [start])
            following = conn.execute('SELECT ts FROM cycles WHERE pair=? AND ts>? AND ts<=? ORDER BY ts LIMIT 1',
                                     (pair, end, now.timestamp())).fetchone()
            upper = following[0] if following else end
            rows = conn.execute('SELECT c.id,c.ts,c.ip,c.boot_epoch,c.source_ok,c.error AS cycle_error,'
                'r.realm,r.started,r.finished,r.in_total,r.out_total,r.error AS reading_error '
                'FROM cycles c LEFT JOIN readings r ON r.cycle_id=c.id '
                'WHERE c.pair=? AND c.ts>=? AND c.ts<=? ORDER BY c.ts,c.id', (pair, lower, upper)).fetchall()
        finally:
            conn.close()
    except (sqlite3.Error, OSError):
        report['error'] = 'Histórico de novas sessões temporariamente indisponível.'
        return report
    report['first_collection_at'] = _iso(first)
    if latest:
        age = now.timestamp() - latest['ts']
        report.update(latest_at=_iso(latest['ts']), age_s=round(max(0, age), 1),
                      fresh=bool(latest['source_ok'] and -5 <= age <= 45))
    cycles = {}
    for row in rows:
        cycle = cycles.setdefault(row['id'], {'sample': row, 'realms': {}})
        if row['realm']:
            cycle['realms'][row['realm']] = row
    for realm in realms:
        before = None
        pending_gap = False
        count_in = count_out = uncertain_in = uncertain_out = 0
        available = start_covered = end_covered = False
        reasons = set()
        first_at = as_of = None
        for cycle in cycles.values():
            meta, current = cycle['sample'], cycle['realms'].get(realm)
            valid = (meta['source_ok'] and current and current['reading_error'] is None and
                     current['in_total'] is not None and current['out_total'] is not None and
                     meta['boot_epoch'] is not None)
            if not valid:
                if meta['ts'] >= start and meta['ts'] <= end:
                    reasons.add('Falha de leitura no período')
                    pending_gap = True
                continue
            if current['finished'] >= start and current['finished'] <= end:
                first_at = first_at if first_at is not None else current['finished']
                as_of = current['finished']
            if before is not None:
                overlapping = current['finished'] > start and before['started'] < end
                same_source = (before['ip'] == current['ip'] and
                               abs(before['boot_epoch'] - current['boot_epoch']) <= 3)
                in_delta = current['in_total'] - before['in_total']
                out_delta = current['out_total'] - before['out_total']
                if overlapping and (not same_source or in_delta < 0 or out_delta < 0):
                    reasons.add('Troca de SBC ou reinício de contador')
                elif overlapping:
                    available = True
                    if pending_gap or current['finished'] - before['finished'] > 45 or meta['cycle_error']:
                        reasons.add('Lacuna entre leituras; total pode estar incompleto')
                    contained = before['started'] >= start and current['finished'] <= end
                    if contained:
                        count_in += in_delta
                        count_out += out_delta
                    else:
                        uncertain_in += in_delta
                        uncertain_out += out_delta
                    # A zero increment spanning a boundary proves no new sessions
                    # crossed it; nonzero increments cannot be assigned to either hour.
                    if before['finished'] <= start <= current['started']:
                        start_covered = in_delta == out_delta == 0 or before['started'] == before['finished'] == start
                    if before['finished'] <= end <= current['started']:
                        end_covered = in_delta == out_delta == 0 or current['started'] == current['finished'] == end
            before = current
            pending_gap = False
        if not available:
            continue
        if not start_covered:
            reasons.add('Início do período sem contagem completa')
        if uncertain_in or uncertain_out:
            reasons.add('Novas sessões próximas ao limite do período não atribuídas à hora')
        if not report['open_interval'] and not end_covered:
            reasons.add('Fim do período sem contagem completa')
        if report['open_interval'] and not report['fresh']:
            reasons.add('Aguardando nova leitura')
        status = 'partial' if reasons else 'collecting' if report['open_interval'] else 'complete'
        report['realms'][realm] = {
            'inbound': count_in, 'outbound': count_out, 'status': status,
            'unallocated_inbound': uncertain_in, 'unallocated_outbound': uncertain_out,
            'first_at': _iso(first_at), 'as_of': _iso(as_of), 'reasons': sorted(reasons),
        }
    return report
