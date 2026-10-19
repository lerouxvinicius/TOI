"""Confirma mudanças contínuas por entidade antes de notificá-las.

Banco próprio: nunca escreve no SQLite LIVE do TOI. Python 3.10+.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

WINDOW_SECONDS = 300
MAX_GAP_SECONDS = 120  # maior que dois ciclos SBC de 60 s exige nova confirmação


def _utc(when):
    when = when or datetime.now(timezone.utc)
    if when.tzinfo is None:
        raise ValueError("timestamps devem incluir fuso horário")
    return when.astimezone(timezone.utc)


class Confirmador:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS entity_state (
                    entity TEXT PRIMARY KEY, stable TEXT NOT NULL,
                    candidate TEXT, candidate_at TEXT, last_seen TEXT);
                CREATE TABLE IF NOT EXISTS event (
                    id TEXT PRIMARY KEY, entity TEXT NOT NULL,
                    before_value TEXT NOT NULL, after_value TEXT NOT NULL,
                    started_at TEXT NOT NULL, confirmed_at TEXT NOT NULL,
                    payload TEXT NOT NULL, delivered_at TEXT);
                CREATE INDEX IF NOT EXISTS idx_event_entity ON event(entity,confirmed_at);
                CREATE TABLE IF NOT EXISTS sbc_snapshot (
                    ip TEXT PRIMARY KEY, pair_name TEXT NOT NULL, observed_at TEXT NOT NULL,
                    snmp_ok INTEGER NOT NULL, role TEXT, hostname TEXT, error TEXT);
            """)
            columns = {row[1] for row in db.execute('PRAGMA table_info(sbc_snapshot)')}
            if 'metrics' not in columns:
                db.execute("ALTER TABLE sbc_snapshot ADD COLUMN metrics TEXT")

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=8)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=8000")
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def observe(self, entity, value, *, at=None, baseline=None, payload=None):
        """value=None = amostra desconhecida: interrompe confirmação pendente.

        Retorna um evento apenas na primeira amostra após os 300 segundos.
        O chamador deve repetir observe em *todos* os ciclos, não apenas nas mudanças.
        """
        stamp = _utc(at)
        now = stamp.isoformat()
        entity = str(entity)
        if not entity:
            raise ValueError("entity não pode ser vazia")
        value = None if value is None else str(value)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM entity_state WHERE entity=?", (entity,)).fetchone()
            if row is None:
                stable = str(baseline) if baseline is not None else value
                if stable is None:
                    return None
                db.execute("INSERT INTO entity_state(entity,stable,last_seen) VALUES(?,?,?)",
                           (entity, stable, now if value is not None else None))
                row = db.execute("SELECT * FROM entity_state WHERE entity=?", (entity,)).fetchone()
            if value is None:
                db.execute("UPDATE entity_state SET candidate=NULL,candidate_at=NULL WHERE entity=?", (entity,))
                return None
            previous = row["last_seen"]
            if previous and stamp < datetime.fromisoformat(previous):
                return None  # amostra atrasada não retrocede o estado
            if (previous and row['candidate'] is not None and
                    (stamp - datetime.fromisoformat(previous)).total_seconds() > MAX_GAP_SECONDS):
                db.execute("UPDATE entity_state SET candidate=?,candidate_at=?,last_seen=? WHERE entity=?",
                           (value if value != row['stable'] else None,
                            now if value != row['stable'] else None, now, entity))
                return None
            if value == row["stable"]:
                db.execute("UPDATE entity_state SET candidate=NULL,candidate_at=NULL,last_seen=? WHERE entity=?",
                           (now, entity))
                return None
            if row["candidate"] != value or not row["candidate_at"]:
                db.execute("UPDATE entity_state SET candidate=?,candidate_at=?,last_seen=? WHERE entity=?",
                           (value, now, now, entity))
                return None
            elapsed = (stamp - datetime.fromisoformat(row["candidate_at"])).total_seconds()
            if elapsed < WINDOW_SECONDS:
                db.execute("UPDATE entity_state SET last_seen=? WHERE entity=?", (now, entity))
                return None
            event_id = hashlib.sha256((entity + "|" + row["candidate_at"] + "|" + value).encode()).hexdigest()[:32]
            event = dict(id=event_id, entity=entity, before=row["stable"], after=value,
                         started_at=row["candidate_at"], confirmed_at=now,
                         payload=payload or {})
            db.execute("""INSERT OR IGNORE INTO event
                (id,entity,before_value,after_value,started_at,confirmed_at,payload)
                VALUES(?,?,?,?,?,?,?)""",
                (event_id, entity, row["stable"], value, event["started_at"], now,
                 json.dumps(event["payload"], ensure_ascii=False)))
            db.execute("UPDATE entity_state SET stable=?,candidate=NULL,candidate_at=NULL,last_seen=? WHERE entity=?",
                       (value, now, entity))
            return event

    def pending_deliveries(self):
        with self._db() as db:
            rows = db.execute("SELECT * FROM event WHERE delivered_at IS NULL ORDER BY confirmed_at,id").fetchall()
            return [dict(id=r["id"], entity=r["entity"], before=r["before_value"],
                         after=r["after_value"], started_at=r["started_at"],
                         confirmed_at=r["confirmed_at"], payload=json.loads(r["payload"])) for r in rows]

    def mark_delivered(self, event_id, *, at=None):
        with self._db() as db:
            db.execute("UPDATE event SET delivered_at=? WHERE id=? AND delivered_at IS NULL",
                       (_utc(at).isoformat(), event_id))

    def states(self, prefix=None):
        with self._db() as db:
            rows = db.execute("SELECT * FROM entity_state WHERE entity LIKE ? ORDER BY entity",
                              ((prefix or "") + "%",)).fetchall()
            return [dict(r) for r in rows]

    def recent_events(self, prefix=None, limit=50):
        with self._db() as db:
            rows = db.execute("SELECT * FROM event WHERE entity LIKE ? ORDER BY confirmed_at DESC LIMIT ?",
                              ((prefix or "") + "%", limit)).fetchall()
            return [dict(r) for r in rows]

    def record_sbc(self, ip, pair_name, data, *, at=None):
        stamp = _utc(at).isoformat()
        good = isinstance(data, dict) and 'erro' not in data
        with self._db() as db:
            db.execute('''INSERT INTO sbc_snapshot
                (ip,pair_name,observed_at,snmp_ok,role,hostname,error,metrics) VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(ip) DO UPDATE SET pair_name=excluded.pair_name,
                observed_at=excluded.observed_at,snmp_ok=excluded.snmp_ok,
                role=excluded.role,hostname=excluded.hostname,error=excluded.error,
                metrics=excluded.metrics''',
                (ip, pair_name, stamp, int(good), data.get('ha') if good else None,
                 data.get('hostname') if good else None,
                 None if good else (data.get('erro', 'Sem resultado') if isinstance(data, dict) else 'Sem resultado'),
                 json.dumps({key: data.get(key) for key in ('health', 'cpu', 'memoria', 'sip_sessions_ativas')},
                            ensure_ascii=False) if good else None))

    def sbc_feed(self, *, limit=30):
        """Dados para GET /api/sbcs, sem expor a base LIVE."""
        with self._db() as db:
            nodes = [dict(row) for row in db.execute('SELECT * FROM sbc_snapshot ORDER BY pair_name,ip')]
            events = [dict(row) for row in db.execute(
                "SELECT * FROM event WHERE entity LIKE 'sbc:%' ORDER BY confirmed_at DESC LIMIT ?",
                (limit,))]
        for event in events:
            event['payload'] = json.loads(event['payload'])
        return {'nodes': nodes, 'incidents': events, 'confirmation_seconds': WINDOW_SECONDS,
                'generated_at': datetime.now(timezone.utc).isoformat()}
