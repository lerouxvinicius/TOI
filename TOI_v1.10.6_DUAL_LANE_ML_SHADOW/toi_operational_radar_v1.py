"""Global operational Radar and persistent gateway load investigations.

Reads small snapshots, an incremental journal and optional bounded LIVE evidence
supplied by the dashboard. Writes only its own incident/learning store. Sampling
runs independently of browser requests; no collector or routing changes.
"""
import hashlib
import html
import json
import math
import os
import re
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager, closing
from datetime import datetime, timezone, timedelta
from pathlib import Path

RELEASE = "R1.2.14 INCIDENT OCCURRENCES"
LOCAL_ZONE = timezone(timedelta(hours=-3))
CONFIRM_SECONDS = 600
GATEWAY_FRESH_SECONDS = 45
PORTAL_FRESH_SECONDS = 180
MAX_JSON_BYTES = 4 * 1024 * 1024
JOURNAL_TICK_BYTES = 256 * 1024


class GatewayDeliveryError(Exception):
    """Only safe error codes cross the transport boundary; URLs contain secrets."""
    def __init__(self, code, retry_after=None, ambiguous=False):
        super().__init__(code)
        self.retry_after = retry_after
        self.ambiguous = ambiguous


class GatewayTelegram:
    def __init__(self, token, chat_id):
        self.token, self.chat_id = token, chat_id

    def send(self, text):
        import requests
        try:
            response = requests.post('https://api.telegram.org/bot'+self.token+'/sendMessage',
                json={'chat_id': self.chat_id, 'text': text, 'parse_mode': 'HTML',
                      'disable_web_page_preview': True}, timeout=(5, 15))
        except requests.ConnectTimeout:
            raise GatewayDeliveryError('CONNECT_TIMEOUT', retry_after=30) from None
        except requests.RequestException:
            # A lost response can follow a successful send. Do not send twice.
            raise GatewayDeliveryError('RESPONSE_UNKNOWN', ambiguous=True) from None
        try:
            doc = response.json()
            if not isinstance(doc, dict):
                raise ValueError()
        except (ValueError, TypeError):
            raise GatewayDeliveryError('INVALID_RESPONSE', ambiguous=True) from None
        if doc.get('ok') is True:
            mid = doc.get('result', {}).get('message_id')
            if isinstance(mid, int) and not isinstance(mid, bool) and mid > 0:
                return {'message_ids': [mid]}
            raise GatewayDeliveryError('RECEIPT_MISSING', ambiguous=True)
        code = doc.get('error_code', response.status_code)
        if code == 429:
            delay = finite(doc.get('parameters', {}).get('retry_after')) or 60
            raise GatewayDeliveryError('RATE_LIMITED', retry_after=max(1, min(delay, 86400)))
        if isinstance(code, int) and code >= 500:
            raise GatewayDeliveryError('TELEGRAM_UNAVAILABLE', retry_after=60)
        raise GatewayDeliveryError('TELEGRAM_REJECTED_'+str(code) if isinstance(code, int) else 'TELEGRAM_REJECTED')


def configured_gateway_telegram():
    token, chat_id = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
    return GatewayTelegram(token, chat_id) if token and chat_id else None


def epoch(value):
    if isinstance(value, (int, float)):
        return float(value)
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LOCAL_ZONE)
        return dt.timestamp()
    except (ValueError, TypeError, OverflowError):
        return None


def iso(value):
    value = epoch(value)
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds") if value is not None else None


def identifier(source, key, started):
    # Portal journal hashes the original timestamp with -03:00; normalize here.
    dt = datetime.fromtimestamp(epoch(started), LOCAL_ZONE).isoformat(timespec="seconds")
    prefix = "pl_" if source == "PORTAL_ORBE" else "gw_"
    return prefix + hashlib.sha256((str(key) + "|" + dt).encode()).hexdigest()[:24]


def finite(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def read_small_json(path):
    with Path(path).open("rb") as src:
        raw = src.read(MAX_JSON_BYTES + 1)
    if len(raw) > MAX_JSON_BYTES:
        raise ValueError("SNAPSHOT_TOO_LARGE")
    doc = json.loads(raw.decode("utf-8-sig"))
    if not isinstance(doc, dict):
        raise ValueError("SNAPSHOT_CONTRACT")
    return doc


class RadarService:
    def __init__(self, app_dir, clock=time.time, balance_provider=None, telegram_sender=None):
        self.app_dir = Path(app_dir)
        self.root = self.app_dir.parent
        self.portal_path = self.root / "dados" / "status_portal_orbe.json"
        self.gateway_path = self.app_dir / "static" / "toi_gateway_health_latest.json"
        self.agiroute_path = self.app_dir / "static" / "toi_agiroute_latest.json"
        self.journal_dir = self.root / "dados" / "portal_orbe_radar"
        self.confirm_db = self.root / "dados" / "monitor_confirmacao_5min.sqlite"
        self.cleanup_log = self.root / "logs" / "canais_travados_autolimpeza.log"
        self.db_path = self.root / "dados_toi" / "operational" / "toi_radar_v1.sqlite"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.balance_provider = balance_provider
        self.telegram_sender = telegram_sender
        self.telegram_thread = None
        self.balance_poll_at = None
        self.stop_event = threading.Event()
        self.thread = None
        self.lock = threading.RLock()
        self.status = {"portal": "STARTING", "gateway": "STARTING", "journal": "STARTING"}
        self.last_tick = None
        self.tick_ms = None
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS incidents(
                    id TEXT PRIMARY KEY, source TEXT NOT NULL, entity_key TEXT NOT NULL,
                    instancia TEXT NOT NULL, codigo TEXT NOT NULL, active INTEGER NOT NULL,
                    started REAL NOT NULL, ack_at TEXT, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS incident_active ON incidents(active, started DESC);
                CREATE INDEX IF NOT EXISTS incident_pending ON incidents(ack_at, started DESC);
                CREATE INDEX IF NOT EXISTS incident_entity ON incidents(source,entity_key,active);
                CREATE TABLE IF NOT EXISTS events(
                    id TEXT PRIMARY KEY, incident_id TEXT NOT NULL, ts REAL NOT NULL,
                    instancia TEXT NOT NULL, codigo TEXT NOT NULL, kind TEXT NOT NULL,
                    payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS event_time ON events(ts DESC,id DESC);
                CREATE INDEX IF NOT EXISTS event_scope ON events(instancia,ts DESC,id DESC);
                CREATE INDEX IF NOT EXISTS event_incident_kind ON events(incident_id,kind,ts,id);
                CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS gateway_telegram_outbox(
                    incident_id TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
                    created REAL NOT NULL, due REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    claim TEXT, lease_until REAL, receipt TEXT,
                    PRIMARY KEY(incident_id,kind));
                CREATE INDEX IF NOT EXISTS gateway_telegram_due ON gateway_telegram_outbox(state,due);
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.db_path, timeout=2)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()  # sqlite context managers alone do NOT close on Windows

    def start(self):
        if not self.thread:
            self.thread = threading.Thread(target=self._run, name="TOI-Radar", daemon=True)
            self.thread.start()
        if self.telegram_sender is not None and not self.telegram_thread:
            self.telegram_thread = threading.Thread(target=self._telegram_run, name='TOI-Gateway-Telegram', daemon=True)
            self.telegram_thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)
        if self.telegram_thread:
            self.telegram_thread.join(timeout=2)

    def _run(self):
        while not self.stop_event.is_set():
            try:
                self.tick()
            except Exception as exc:
                with self.lock:
                    self.status["store"] = type(exc).__name__
            self.stop_event.wait(5)

    @staticmethod
    def _get(db, iid):
        row = db.execute("SELECT payload FROM incidents WHERE id=?", (iid,)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def _active(db, source, key):
        row = db.execute("SELECT payload FROM incidents WHERE source=? AND entity_key=? AND active=1 ORDER BY started DESC LIMIT 1", (source,key)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def _put(db, rec):
        body = json.dumps(rec, ensure_ascii=False, sort_keys=True, allow_nan=False)
        db.execute("""INSERT INTO incidents VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET active=excluded.active,ack_at=excluded.ack_at,
            payload=excluded.payload WHERE incidents.payload<>excluded.payload""",
            (rec["id"],rec["source"],rec["entity_key"],rec["instancia"],rec.get("codigo", ""),
             int(rec["active"]),epoch(rec["started_at"]),rec.get("ack_at"),body))

    @staticmethod
    def _event(db, rec, kind, when, eid=None, delivery=None):
        event = {"id": eid or (rec["id"] + ":" + kind + ":" + str(when)),
                 "incident_id":rec["id"], "source":rec["source"], "instancia":rec["instancia"],
                 "codigo":rec.get("codigo", ""), "entity":rec["entity"],
                 "projeto":rec.get("projeto", ""), "kind":kind, "occurred_at":iso(when),
                 "started_at":rec["started_at"], "delivery":delivery or {"status":"NOT_RECORDED"}}
        for field in ('channels_stuck', 'channels_released', 'condition', 'traffic_returned_at',
                      'normalization_evidence_started_at', 'valid_busy', 'valid_total', 'occupancy_pct'):
            if field in rec:
                event[field] = rec[field]
        if rec.get("kind") == "LOAD_IMBALANCE":
            event["diagnostic_kind"] = "LOAD_IMBALANCE"
            event["investigation"] = rec.get("investigation", {})
        db.execute("""INSERT INTO events VALUES(?,?,?,?,?,?,?) ON CONFLICT(id)
            DO UPDATE SET payload=excluded.payload WHERE events.payload<>excluded.payload""",
            (event["id"], rec["id"], epoch(when), rec["instancia"],rec.get("codigo",""), kind,
             json.dumps(event, ensure_ascii=False, sort_keys=True)))

    @staticmethod
    def _kv(db, key, default=None):
        row = db.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    @staticmethod
    def _setkv(db, key, value):
        db.execute("INSERT INTO kv VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE kv.value<>excluded.value",
                   (key,json.dumps(value, sort_keys=True)))

    def tick(self):
        t0 = time.perf_counter()
        with self.lock, self.connect() as db:
            if not self._kv(db,"history_started_at"):
                self._setkv(db,"history_started_at",iso(self.clock()))
            # Journal first: exact lifecycle/send evidence takes precedence.
            for label, function in (("journal",self._journal),("portal",self._portal),("gateway",self._gateways),
                                    ("agiroute",self._agiroute),
                                    ("monitor",self._monitor_alerts),("cleanup",self._cleanup),
                                    ("gateway_balance",self._gateway_balance)):
                try:
                    self.status[label] = function(db) or "OK"
                except Exception as exc:
                    self.status[label] = type(exc).__name__
            self.last_tick = iso(self.clock())
            self.tick_ms = round((time.perf_counter()-t0)*1000, 2)
            self.status.pop("store", None)
            self._queue_gateway_telegram(db)

    @staticmethod
    def gateway_telegram_text(rec, kind):
        def escaped(value, limit=160):
            parts, length = [], 0
            for char in str('' if value is None else value)[:limit]:
                encoded = html.escape(char, quote=False)
                if length+len(encoded) > limit:
                    break
                parts.append(encoded)
                length += len(encoded)
            return ''.join(parts)
        if rec.get('source') == 'AGIROUTE':
            state = 'normalizado' if kind == 'NORMALIZED' else 'indisponível'
            return ('<b>TOI · Agiroute Instância 6</b>\n'
                    + escaped(rec.get('entity'), 100) + ' · ' + state + '\n'
                    + 'Condição: ' + escaped(rec.get('condition'), 100) + '\n'
                    + 'Última verificação: ' + escaped(rec.get('last_sample_at'), 64) + '\n'
                    + '<a href="http://10.90.11.20/toi/">Abrir TOI</a>')
        title = {'ICMP': 'Gateway sem resposta ICMP', 'NO_TRAFFIC': 'Gateway sem ocupação observada',
                 'LOAD_IMBALANCE': 'Gateways com distribuição de tráfego desequilibrada'}.get(rec.get('kind'), 'Alerta de gateway')
        recovered_title = ('Gateway voltou a receber canais válidos ocupados' if rec.get('kind') == 'NO_TRAFFIC'
                           else 'Normalização confirmada')
        lines = ['<b>TOI · '+(recovered_title if kind == 'NORMALIZED' else title)+'</b>',
                 'Instância '+escaped(rec['instancia'])+' · '+escaped(rec['entity']),
                 'Início: '+escaped(datetime.fromtimestamp(epoch(rec['started_at']), LOCAL_ZONE).strftime('%d/%m/%Y %H:%M:%S'))]
        if kind == 'NORMALIZED':
            if rec.get('kind') == 'NO_TRAFFIC':
                returned = epoch(rec.get('traffic_returned_at') or rec.get('normalization_evidence_started_at') or rec.get('resolved_at'))
                lines.append('Retorno observado: '+escaped(datetime.fromtimestamp(returned, LOCAL_ZONE).strftime('%d/%m/%Y %H:%M:%S')))
                lines.append('Canais válidos ocupados agora: '+(escaped(rec.get('valid_busy'), 16) or 'N/C')+
                             ' / '+(escaped(rec.get('valid_total'), 16) or 'N/C')+' · ocupação '+(escaped(rec.get('occupancy_pct'), 16) or 'N/C')+'%.')
            confirmed = epoch(rec.get('resolved_at'))
            if confirmed is not None:
                lines.append('Normalização confirmada em: '+datetime.fromtimestamp(confirmed, LOCAL_ZONE).strftime('%d/%m/%Y %H:%M:%S'))
            lines.append('A condição foi normalizada após '+str(rec.get('normalization_minutes', 10))+' min de confirmação.')
        elif rec.get('kind') == 'LOAD_IMBALANCE':
            report = rec.get('investigation', {})
            for row in report.get('low_gateways', [])[:12]:
                lines.append(escaped(row['entity'], 80)+': '+escaped(row['occupancy_pct'], 16)+'% de ocupação')
            lines.append('Mediana dos pares: '+escaped(report.get('peer_median_pct'), 16)+'%.')
            queues = report.get('congested_queues', [])
            lines.append(str(len(queues))+' fila(s) com P135 ≥ 20%. A relação causal está em investigação.')
            for step in report.get('investigation_steps', [])[:3]:
                lines.append('• '+escaped(step, 220))
        elif rec.get('kind') == 'NO_TRAFFIC':
            lines.append('Sem ocupação com expectativa de tráfego, confirmada por 10 min.')
            lines.append('Investigar balanceamento, rotas e disponibilidade do gateway.')
        else:
            lines.append('Sem resposta ICMP, confirmado por 10 min. Verificar gateway e caminho de rede.')
        lines.extend(['Última amostra: '+escaped(rec.get('last_sample_at')),
                      'Incidente: '+escaped(rec['id']), '<a href="http://10.90.11.20/toi/">Abrir TOI</a>'])
        return '\n'.join(lines)  # Bounded to one Telegram message; no partial multi-message send.

    def _gateway_sendable(self, rec, kind):
        stamp = epoch(rec.get('last_sample_at'))
        max_age = 150 if rec.get('source') == 'AGIROUTE' else GATEWAY_FRESH_SECONDS
        fresh = stamp is not None and -5 <= self.clock()-stamp <= max_age
        if rec.get('source') not in ('GATEWAY', 'AGIROUTE') or rec.get('evidence_available') is not True:
            return False
        if kind == 'OPENED':
            return fresh and rec.get('active') is True and rec.get('status') == 'ACTIVE'
        resolved = epoch(rec.get('resolved_at'))
        return not rec.get('active') and rec.get('status') == 'NORMALIZED' and resolved is not None and -5 <= self.clock()-resolved <= 86400

    def _gateway_delivery(self, db, iid, kind, delivery):
        rec = self._get(db, iid)
        if rec:
            rec['delivery'] = delivery
            self._put(db, rec)
        for row in db.execute('SELECT id,payload FROM events WHERE incident_id=? AND kind=?', (iid, kind)).fetchall():
            event = json.loads(row['payload'])
            event['delivery'] = delivery
            db.execute('UPDATE events SET payload=? WHERE id=?', (json.dumps(event, ensure_ascii=False, sort_keys=True), row['id']))

    def _queue_gateway_telegram(self, db):
        now = self.clock()
        rows = db.execute("SELECT payload FROM incidents WHERE source IN ('GATEWAY','AGIROUTE') AND (active=1 OR id IN "
                          "(SELECT incident_id FROM events WHERE kind='NORMALIZED' AND ts>=?)) LIMIT 1000",
                          (now-GATEWAY_FRESH_SECONDS,)).fetchall()
        for row in rows:
            rec = json.loads(row[0])
            kind = 'OPENED' if rec['active'] else 'NORMALIZED'
            if not self._gateway_sendable(rec, kind):
                continue
            if kind == 'NORMALIZED' and not db.execute("SELECT 1 FROM gateway_telegram_outbox WHERE incident_id=? AND kind='OPENED' AND state='SENT'", (rec['id'],)).fetchone():
                continue  # No historical replay of resolved incidents after enabling the sender.
            inserted = db.execute("INSERT OR IGNORE INTO gateway_telegram_outbox(incident_id,kind,state,created,due) VALUES(?,?,'PENDING',?,?)",
                                  (rec['id'], kind, now, now)).rowcount
            if inserted:
                self._gateway_delivery(db, rec['id'], kind, {'status': 'QUEUED', 'kind': kind, 'queued_at': iso(now)})
        self.status['gateway_telegram'] = self._telegram_health(db)

    def _telegram_health(self, db):
        if self.telegram_sender is None:
            return 'NOT_CONFIGURED'
        states = {row[0] for row in db.execute("SELECT DISTINCT state FROM gateway_telegram_outbox WHERE state IN ('FAILED','UNCONFIRMED','RETRY')")}
        return next((value for value in ('UNCONFIRMED', 'FAILED', 'RETRY') if value in states), 'OK')

    def _telegram_run(self):
        while not self.stop_event.is_set():
            try:
                with self.lock, self.connect() as db:
                    self._setkv(db, 'gateway:telegram:worker', {'pid': os.getpid(), 'heartbeat_at': iso(self.clock()),
                        'configured': self.telegram_sender is not None, 'release': RELEASE})
                self.deliver_gateway_telegram()
            except Exception:
                with self.lock:
                    self.status['gateway_telegram'] = 'STORE_UNAVAILABLE'
            self.stop_event.wait(2)

    def deliver_gateway_telegram(self):
        if self.telegram_sender is None:
            return False
        now, claim = self.clock(), uuid.uuid4().hex
        with self.lock, self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            for old in db.execute("SELECT incident_id,kind FROM gateway_telegram_outbox WHERE state='SENDING' AND lease_until<?", (now,)).fetchall():
                delivery = {'status': 'UNCONFIRMED', 'kind': old['kind'], 'error_code': 'INTERRUPTED_SEND'}
                db.execute("UPDATE gateway_telegram_outbox SET state='UNCONFIRMED',receipt=? WHERE incident_id=? AND kind=?",
                           (json.dumps(delivery), old['incident_id'], old['kind']))
                self._gateway_delivery(db, old['incident_id'], old['kind'], delivery)
            job = db.execute("SELECT * FROM gateway_telegram_outbox WHERE state IN ('PENDING','RETRY') AND due<=? ORDER BY created LIMIT 1", (now,)).fetchone()
            if job is None:
                return False
            rec = self._get(db, job['incident_id'])
            if not rec or not self._gateway_sendable(rec, job['kind']):
                if rec and rec.get('active') and job['kind'] == 'OPENED':
                    db.execute('UPDATE gateway_telegram_outbox SET due=? WHERE incident_id=? AND kind=?', (now+15, job['incident_id'], job['kind']))
                    return False  # Wait for fresh confirmation; stale evidence must not cause a send.
                delivery = {'status': 'CANCELLED', 'kind': job['kind'], 'error_code': 'CONDITION_ENDED_BEFORE_SEND'}
                db.execute("UPDATE gateway_telegram_outbox SET state='CANCELLED',receipt=? WHERE incident_id=? AND kind=?", (json.dumps(delivery), job['incident_id'], job['kind']))
                self._gateway_delivery(db, job['incident_id'], job['kind'], delivery)
                return False
            db.execute("UPDATE gateway_telegram_outbox SET state='SENDING',claim=?,lease_until=?,attempts=attempts+1 WHERE incident_id=? AND kind=?",
                       (claim, now+60, job['incident_id'], job['kind']))
            self._gateway_delivery(db, job['incident_id'], job['kind'], {'status': 'ATTEMPTED', 'kind': job['kind'], 'attempted_at': iso(now)})
        # No incident transaction or UI lock is held during network I/O.
        try:
            receipt = self.telegram_sender.send(self.gateway_telegram_text(rec, job['kind']))
            ids = receipt.get('message_ids') if isinstance(receipt, dict) else None
            if not ids or not all(isinstance(x, int) and not isinstance(x, bool) and x > 0 for x in ids):
                raise GatewayDeliveryError('RECEIPT_MISSING', ambiguous=True)
            state, due = 'SENT', now
            delivery = {'status': state, 'kind': job['kind'], 'sent_at': iso(self.clock()), 'message_ids': ids}
        except GatewayDeliveryError as exc:
            state = 'UNCONFIRMED' if exc.ambiguous else 'RETRY' if exc.retry_after is not None else 'FAILED'
            due = self.clock()+(exc.retry_after or 0)
            delivery = {'status': state, 'kind': job['kind'], 'error_code': str(exc), 'attempted_at': iso(now)}
            if state == 'RETRY':
                delivery['retry_at'] = iso(due)
        except Exception:
            state, due = 'UNCONFIRMED', now
            delivery = {'status': state, 'kind': job['kind'], 'error_code': 'RESPONSE_UNKNOWN', 'attempted_at': iso(now)}
        with self.lock, self.connect() as db:
            updated = db.execute("UPDATE gateway_telegram_outbox SET state=?,due=?,receipt=?,lease_until=NULL WHERE incident_id=? AND kind=? AND claim=? AND state='SENDING'",
                                (state, due, json.dumps(delivery), job['incident_id'], job['kind'], claim)).rowcount
            if updated:
                self._gateway_delivery(db, job['incident_id'], job['kind'], delivery)
            self.status['gateway_telegram'] = self._telegram_health(db)
        return state == 'SENT'

    def _agiroute(self, db):
        if not self.agiroute_path.is_file():
            return 'NOT_STARTED'
        doc = read_small_json(self.agiroute_path)
        stamp = epoch(doc.get('generated_at'))
        if doc.get('schema') != 'toi_agiroute_health_v1' or doc.get('instancia') != '6':
            raise ValueError('AGIROUTE_CONTRACT')
        if stamp is None or not -30 <= self.clock()-stamp <= 150:
            return 'STALE'
        if doc.get('configured') is not True:
            return 'NOT_CONFIGURED'
        hosts = doc.get('hosts')
        if not isinstance(hosts, list) or {row.get('ip') for row in hosts if isinstance(row, dict)} != {'10.90.10.165', '10.90.10.124'}:
            raise ValueError('AGIROUTE_HOSTS')
        observations = []
        for host in hosts:
            ip, host_state = host['ip'], host.get('status')
            if host_state not in ('OK', 'UNREACHABLE', 'UNKNOWN'):
                raise ValueError('AGIROUTE_STATUS')
            observations.append((f'{ip}:ssh', ip,
                                 'HOST_UNREACHABLE' if host_state == 'UNREACHABLE' else 'CHECK_FAILED',
                                 host_state != 'OK', host_state))
            if host_state == 'OK':
                services = host.get('services') or {}
                if set(services) != {'agiroutey', 'proxysql'}:
                    raise ValueError('AGIROUTE_SERVICES')
                for unit in ('agiroutey', 'proxysql'):
                    state = services[unit]
                    if state.get('status') not in ('OK', 'DOWN'):
                        raise ValueError('AGIROUTE_UNIT_STATE')
                    observations.append((f'{ip}:{unit}', f'{ip} · {unit}', 'SERVICE_DOWN',
                                         state['status'] == 'DOWN', state.get('active', 'unknown')))
                    if state['status'] == 'OK' and state.get('boot') in ('disabled', 'masked'):
                        observations.append((f'{ip}:{unit}:boot', f'{ip} · {unit} · início automático',
                                             'BOOT_DISABLED', True, state['boot']))
                    elif state['status'] == 'OK':
                        observations.append((f'{ip}:{unit}:boot', f'{ip} · {unit} · início automático',
                                             'BOOT_DISABLED', False, state.get('boot', 'unknown')))
        for key, entity, kind, bad, condition in observations:
            previous = self._kv(db, 'agiroute_observation:' + key, {})
            marker = 'bad' if bad else 'good'
            if previous.get('sample_at') == doc['generated_at']:
                count = previous.get('count', 0)
            else:
                count = min(2, previous.get('count', 0) + 1) if previous.get('state') == marker else 1
                self._setkv(db, 'agiroute_observation:' + key,
                            {'sample_at': doc['generated_at'], 'state': marker, 'count': count})
            prior = self._active(db, 'AGIROUTE', key)
            if prior and bad:
                prior.update(last_sample_at=doc['generated_at'], condition=condition)
                self._put(db, prior)
            if count < 2:
                continue
            if bad and not prior:
                started = epoch(previous.get('sample_at')) if previous.get('state') == marker else stamp
                iid = 'agi_' + hashlib.sha256((key + '|' + str(started)).encode()).hexdigest()[:24]
                rec = {'id': iid, 'source': 'AGIROUTE', 'entity_key': key,
                       'instancia': '6', 'codigo': '', 'entity': entity, 'projeto': 'Agiroute',
                       'kind': kind, 'started_at': iso(started), 'confirmed_at': iso(stamp),
                       'detected_at': iso(self.clock()), 'last_sample_at': doc['generated_at'],
                       'ack_at': None, 'active': True, 'status': 'ACTIVE',
                       'evidence_available': True, 'condition': condition,
                       'delivery': {'status': 'NOT_RECORDED'}}
                self._put(db, rec)
                self._event(db, rec, 'OPENED', stamp)
            elif not bad and prior:
                prior.update(active=False, status='NORMALIZED', resolved_at=iso(stamp),
                             last_sample_at=doc['generated_at'], condition='active')
                self._put(db, prior)
                self._event(db, prior, 'NORMALIZED', stamp)
        return 'OK'

    def _monitor_alerts(self, db):
        if not self.confirm_db.is_file():
            return "NOT_STARTED"
        with closing(sqlite3.connect(self.confirm_db.as_uri() + '?mode=ro', uri=True, timeout=2)) as source:
            source.row_factory = sqlite3.Row
            rows = source.execute('SELECT * FROM event ORDER BY confirmed_at DESC LIMIT 500').fetchall()
        for row in reversed(rows):
            if db.execute('SELECT 1 FROM events WHERE id=?', ('monitor_' + row['id'],)).fetchone():
                continue
            entity = row['entity']
            ts = epoch(row['confirmed_at'])
            if ts is None or ts > self.clock() + 30:
                continue
            payload = json.loads(row['payload'])
            after = row['after_value']
            recovery = ((entity.startswith(('mgw:route:', 'mgw:server:', 'vpn:connectivity', 'sbc:snmp:')) and after == 'True') or
                        (entity.startswith('sbc:integrity:') and after == 'NORMAL') or
                        (entity.startswith('sbc:agent:') and after == '3') or
                        (entity.startswith('sbc:sip:') and after == 'NORMAL') or
                        (entity.startswith('sbc:peer_timeout:') and after == 'CLEAR'))
            one_shot = entity.startswith(('sbc:active:', 'sbc:new_agent:', 'vpn:ip'))
            key = entity
            prior = self._active(db, 'MONITOR_ALERTAS', key)
            kind = 'OBSERVED' if one_shot else 'NORMALIZED' if recovery else 'PERSISTENT' if prior else 'OPENED'
            iid = prior['id'] if prior and not one_shot else 'mgw_' + row['id']
            rec = self._get(db, iid) if prior and not one_shot else None
            if rec is None:
                rec = {'id': iid, 'source': 'MONITOR_ALERTAS', 'entity_key': key,
                       'instancia': str(payload.get('instancia') or ('SBC' if entity.startswith('sbc:') else 'MGW')),
                       'codigo': '', 'entity': str(payload.get('peer') or payload.get('agent') or payload.get('ip') or entity),
                       'projeto': 'SBC' if entity.startswith('sbc:') else 'MGW',
                       'started_at': iso(row['started_at']), 'confirmed_at': iso(ts),
                       'detected_at': iso(self.clock()), 'ack_at': None,
                       'active': not (recovery or one_shot), 'status': 'OBSERVED' if one_shot else 'NORMALIZED' if recovery else 'ACTIVE',
                       'delivery': {'status': 'NOT_RECORDED'}, 'condition': after}
            rec.update(last_sample_at=iso(ts), condition=after)
            if recovery:
                rec.update(active=False, status='NORMALIZED', resolved_at=iso(ts))
            self._put(db, rec)
            self._event(db, rec, kind, ts, eid='monitor_' + row['id'])
        return 'OK'

    def _cleanup(self, db):
        if not self.cleanup_log.is_file():
            return 'NOT_STARTED'
        # Auditoria já contém a contagem antes/depois e a instância. Apenas
        # resultados encerrados entram no histórico; INICIADA não é sucesso.
        with self.cleanup_log.open(encoding='utf-8', errors='replace') as source:
            lines = source.readlines()[-500:]
        for line in lines:
            m = re.match(r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) \| Instância (\S+) \| (SUCESSO|INCOMPLETA|FALHA) \| (.*)', line)
            if not m:
                continue
            stamp = datetime.strptime(m[1], '%d/%m/%Y %H:%M:%S').replace(tzinfo=LOCAL_ZONE).timestamp()
            if stamp > self.clock() + 30:
                continue
            fields = dict(re.findall(r'(antes|depois|liberados)=(\d+)', m[4]))
            key = hashlib.sha256(line.encode()).hexdigest()[:24]
            iid = 'cleanup_' + key
            if self._get(db, iid):
                continue
            rec = {'id': iid, 'source': 'AUTO_LIMPEZA', 'entity_key': iid,
                   'instancia': m[2], 'codigo': '', 'entity': 'Limpeza automática de canais',
                   'projeto': 'Portal Orbe', 'started_at': iso(stamp), 'confirmed_at': iso(stamp),
                   'detected_at': iso(self.clock()), 'last_sample_at': iso(stamp),
                   'active': False, 'status': m[3], 'ack_at': None,
                   'channels_stuck': int(fields['antes']) if 'antes' in fields else None,
                   'channels_released': int(fields['liberados']) if 'liberados' in fields else None,
                   'delivery': {'status': 'NOT_RECORDED'}}
            self._put(db, rec)
            self._event(db, rec, 'AUTO_CLEANUP', stamp, eid=iid)
        return 'OK'

    def _portal(self, db):
        doc = read_small_json(self.portal_path)
        rows = doc.get("filas")
        if not isinstance(rows, dict):
            raise ValueError("PORTAL_CONTRACT")
        config = doc.get("metadados", {}).get("assinatura_config", {})
        now = self.clock()
        for key, row in rows.items():
            if not isinstance(row, dict):
                continue
            sample = epoch(row.get("ultima_amostra"))
            if sample is None or sample > now + 30:
                continue
            prior = self._active(db, "PORTAL_ORBE",key)
            started = epoch(row.get("inicio_incidente") or row.get("observacao_inicio"))
            if row.get("incidente_confirmado") is True and started is not None:
                iid = identifier("PORTAL_ORBE",key,started)
                rec = self._get(db,iid)
                if rec and sample < epoch(rec.get("last_sample_at", started)):
                    continue
                if rec and not rec["active"]:
                    continue  # never resurrect a normalized lifecycle with a stale snapshot
                if prior and prior["id"] != iid:
                    prior.update(active=False, status="SUPERSEDED", resolved_at=None)
                    self._put(db,prior)
                    self._event(db,prior,"SUPERSEDED",sample)
                if rec is None:
                    rec={"id":iid,"source":"PORTAL_ORBE","entity_key":key,
                         "started_at":iso(started),"confirmed_at":None,"detected_at":iso(now),
                         "instancia":str(row.get("instancia","")),"codigo":str(row.get("codigo","")),
                         "entity":str(row.get("fila") or row.get("codigo") or key),
                         "projeto":str(row.get("projeto","")),"ack_at":None,"active":True,
                         "delivery":{"status":"NOT_RECORDED"}}
                    self._event(db,rec,"IMPORTED_ACTIVE",now)
                rec.update(last_sample_at=iso(sample), p34=finite(row.get("status34_percentual")),
                           p135=finite(row.get("status135_percentual")),
                           normalization_started_at=iso(row.get("normalizacao_inicio")),
                           status="NORMALIZATION_PENDING" if row.get("normalizacao_inicio") else "ACTIVE",
                           threshold_p34=finite(config.get("limite_p34")),
                           threshold_p135=finite(config.get("limite_p135")),
                           confirmation_minutes=config.get("confirmacao_minutos"),
                           normalization_minutes=config.get("confirmacao_normalizacao_minutos"))
                # This is a notification attempt clock, never a Telegram receipt.
                rec["legacy_notification_at"] = iso(row.get("ultima_notificacao"))
                self._put(db,rec)
            elif prior and row.get("incidente_confirmado") is False:
                if sample > (epoch(prior.get("last_sample_at")) or epoch(prior["started_at"])):
                    normalization_start=epoch(prior.get("normalization_started_at"))
                    required_minutes=finite(prior.get("normalization_minutes")) or 10
                    observed_seconds=sample-normalization_start if normalization_start is not None else None
                    p34=finite(row.get("status34_percentual"))
                    p135=finite(row.get("status135_percentual"))
                    below_limits=(p34 is not None and p135 is not None and
                                  p34<(finite(prior.get("threshold_p34")) or 20) and
                                  p135<(finite(prior.get("threshold_p135")) or 20))
                    confirmed=bool(observed_seconds is not None and
                                   observed_seconds>=required_minutes*60-5 and below_limits)
                    prior.update(active=False,status="NORMALIZED",resolved_at=iso(sample),
                                 last_sample_at=iso(sample),normalization_started_at=None,
                                 normalization_evidence_started_at=iso(normalization_start),
                                 normalization_observed_seconds=round(observed_seconds,1) if observed_seconds is not None else None,
                                 normalization_confirmed=confirmed,p34=p34,p135=p135)
                    self._put(db,prior)
                    self._event(db,prior,"NORMALIZED" if confirmed else "NORMALIZATION_OBSERVED",sample)
        return "OK"

    def _journal(self, db):
        if not self.journal_dir.exists():
            return "NOT_STARTED"
        budget=JOURNAL_TICK_BYTES
        remaining=False
        bad=False
        # Sequential, durable offsets: bounded bootstrap/replay and normal tails.
        for path in sorted(self.journal_dir.glob("events_????-??-??.jsonl")):
            key="journal:"+path.name
            offset=self._kv(db,key,0)
            size=path.stat().st_size
            if size<offset:
                offset=0
            if size==offset:
                continue
            if budget<=0:
                remaining=True
                break
            with path.open("rb") as stream:
                stream.seek(offset)
                data=stream.read(min(budget,size-offset))
            budget-=len(data)
            end=data.rfind(b"\n")
            if end<0:
                if len(data)>=JOURNAL_TICK_BYTES:
                    raise ValueError("JOURNAL_LINE_TOO_LARGE")
                remaining=True
                continue
            for line in data[:end].split(b"\n"):
                if not line.strip():
                    continue
                try:
                    self._ingest_event(db,json.loads(line))
                except (KeyError,ValueError,TypeError):
                    bad=True
            offset+=end+1
            self._setkv(db,key,offset)
            remaining=remaining or offset<size
        if bad:
            self._setkv(db,"journal_contract_warning",iso(self.clock()))
        if self._kv(db,"journal_contract_warning"):
            return "CONTRACT_WARNING"
        return "CATCHING_UP" if remaining else "OK"

    def _ingest_event(self, db, event):
        if event.get("schema")!="portal_radar_v1" or event.get("kind") not in ("OPENED","PERSISTENT","NORMALIZED"):
            raise ValueError("EVENT_CONTRACT")
        started, when = epoch(event["started_at"]), epoch(event["occurred_at"])
        if started is None or when is None or when < started or when > self.clock()+30:
            raise ValueError("EVENT_TIME")
        iid=identifier("PORTAL_ORBE",event["entity_key"],started)
        rec=self._get(db,iid)
        kind=event["kind"]
        if rec is None:
            rec={"id":iid,"source":"PORTAL_ORBE","entity_key":event["entity_key"],
                 "started_at":iso(started),"confirmed_at":None,"detected_at":iso(self.clock()),
                 "instancia":str(event["instancia"]),"codigo":str(event["codigo"]),
                 "entity":str(event["entity"]),"projeto":str(event.get("projeto","")),
                 "ack_at":None,"active":kind!="NORMALIZED","status":"ACTIVE"}
        if kind=="OPENED":
            rec["confirmed_at"]=iso(when)
        last_event=epoch(rec.get("last_event_at")) or 0
        delivery=event.get("delivery",{"status":"UNCONFIRMED"})
        # Defense in depth: arbitrary truthy return values never become SENT.
        if not isinstance(delivery,dict):
            delivery={"status":"UNCONFIRMED"}
        if delivery.get("status")=="SENT" and (not epoch(delivery.get("sent_at")) or not delivery.get("message_ids")):
            delivery={"status":"UNCONFIRMED"}
        if when>=last_event:
            rec.update(last_event_at=iso(when),last_event_kind=kind,delivery=delivery)
            if kind=="NORMALIZED":
                rec.update(active=False,status="NORMALIZED",resolved_at=iso(when))
        if when >= (epoch(rec.get("last_sample_at")) or 0):
            rec.update(last_sample_at=iso(when),p34=finite(event.get("p34")),p135=finite(event.get("p135")))
        self._put(db,rec)
        self._event(db,rec,kind,when,eid=str(event["event_id"]),delivery=delivery)

    @staticmethod
    def gateway_conditions(g):
        if g.get("path_suppressed") or str(g.get("path_state"))!="OK" or g.get("inventory_fresh") is not True:
            return {"ICMP":None,"NO_TRAFFIC":None}
        reach=str(g.get("reachability_state", ""))
        icmp=True if reach=="DOWN" else False if reach=="OK" else None
        excluded=str(g.get("semantic_status", "")) in {
            "STANDBY_EXPECTED","PLANNED_INACTIVE","PARTIAL_VISIBILITY","OPERATIONAL_PARTIAL_VISIBILITY"}
        raw=str(g.get("raw_status", ""))
        occ=finite(g.get("occupancy_pct"))
        traffic=None
        if g.get("operational_window") is True and not excluded and reach=="OK":
            if occ is not None and occ>0:
                traffic=False
            elif raw in ("NO_TRAFFIC_OBSERVE","NO_TRAFFIC_WARN","NO_TRAFFIC_CRITICAL"):
                traffic=True
        return {"ICMP":icmp,"NO_TRAFFIC":traffic}

    def _gateways(self, db):
        doc=read_small_json(self.gateway_path)
        ts=epoch(doc.get("generated_at"))
        if ts is None or not isinstance(doc.get("gateways"),list):
            raise ValueError("GATEWAY_CONTRACT")
        if not -5<=self.clock()-ts<=GATEWAY_FRESH_SECONDS:
            return "STALE"
        try:
            context = read_small_json(self.app_dir / 'static' / 'gateway_semantic_context_v1.json')
        except (OSError, ValueError):
            context = {}
        confirmed_balance = bool(context.get('balanced_traffic_policy'))
        roles = {(str(x.get('instancia')), str(x.get('entity', '')).upper()): x
                 for x in context.get('gateways', [])}
        for g in doc["gateways"]:
            if not isinstance(g,dict) or not g.get("entity") or not g.get("instancia"):
                continue
            if confirmed_balance:
                # The latest operator reference replaces obsolete human roles
                # from the Shadow snapshot, while machine telemetry is preserved.
                role = roles.get((str(g['instancia']), str(g['entity']).upper()), {})
                g = {**g, 'semantic_status': 'STANDBY_EXPECTED' if
                     role.get('lifecycle_state') == 'IN_SERVICE_STANDBY' else 'CONTEXT_INFO'}
            for kind,condition in self.gateway_conditions(g).items():
                key=str(g["instancia"])+"|"+str(g["entity"])+"|"+kind
                state=self._kv(db,"gw:"+key,{})
                last=state.get("last",0)
                if ts<=last:
                    continue
                if ts-last>GATEWAY_FRESH_SECONDS or condition is None:
                    state.pop("candidate",None)
                    state.pop("recovery",None)
                rec=self._active(db,"GATEWAY",key)
                if rec:
                    rec.update(last_sample_at=iso(ts),evidence_available=condition is not None,
                               occupancy_pct=finite(g.get("occupancy_pct")),reachability=g.get("reachability_state"),
                               valid_busy=finite(g.get('busy')),valid_total=finite(g.get('total')))
                    if condition is False:
                        state.setdefault("recovery",ts)
                        rec.update(status="NORMALIZATION_PENDING",normalization_started_at=iso(state["recovery"]))
                        if kind == 'NO_TRAFFIC':
                            rec['traffic_returned_at'] = iso(state['recovery'])
                        if ts-state["recovery"]>=CONFIRM_SECONDS:
                            rec.update(active=False,status="NORMALIZED",resolved_at=iso(ts),normalization_started_at=None,
                                       normalization_evidence_started_at=iso(state['recovery']),
                                       normalization_confirmed=True,last_event_kind='NORMALIZED')
                            self._event(db,rec,"NORMALIZED",ts)
                            state.pop("recovery",None)
                    else:
                        state.pop("recovery",None)
                        rec.update(status="ACTIVE",normalization_started_at=None)
                        if kind == 'NO_TRAFFIC':
                            rec['traffic_returned_at'] = None
                    self._put(db,rec)
                elif condition is True:
                    state.setdefault("candidate",ts)
                    if ts-state["candidate"]>=CONFIRM_SECONDS:
                        started=state["candidate"]
                        rec={"id":identifier("GATEWAY",key,started),"source":"GATEWAY","entity_key":key,
                             "instancia":str(g["instancia"]),"codigo":"","entity":str(g["entity"]),
                             "projeto":"", "kind":kind,"active":True,"status":"ACTIVE","ack_at":None,
                             "started_at":iso(started),"confirmed_at":iso(ts),"detected_at":iso(self.clock()),
                             "last_sample_at":iso(ts),"evidence_available":True,"confirmation_minutes":10,
                             "normalization_minutes":10,"delivery":{"status":"NOT_SENT"},
                             "occupancy_pct":finite(g.get('occupancy_pct')),
                             "valid_busy":finite(g.get('busy')),"valid_total":finite(g.get('total'))}
                        self._put(db,rec)
                        self._event(db,rec,"OPENED",ts)
                        state.pop("candidate",None)
                else:
                    state.pop("candidate",None)
                state["last"]=ts
                self._setkv(db,"gw:"+key,state)
        return "OK"

    @staticmethod
    def balance_analysis(snapshot, semantics, profiles, queues, routes, operational=True, now=None):
        """Explainable LIVE comparison; correlation is never a proven root cause."""
        now = time.time() if now is None else now
        stamp = epoch(snapshot.get("sample_at"))
        result = {"instancia": str(snapshot.get("instancia", "")), "sample_at": iso(stamp),
                  "status": "SEM_DADOS", "detected": False, "valid": False,
                  "low_gateways": [], "loaded_gateways": [], "excluded_gateways": [],
                  "congested_queues": [], "causal_status": "NAO_COMPROVADA",
                  "policy": {"low_max_pct": 25, "loaded_min_pct": 80,
                             "gap_min_pp": 40, "low_peer_ratio_max": .35,
                             "confirm_seconds": 180, "recovery_seconds": 300},
                  "investigation_steps": [
                      "Confirmar papel operacional, manutenção e ordem das rotas dos gateways com baixa ocupação.",
                      "Verificar pesos, limites de canais, bloqueios e habilitação dos grupos de troncos e rotas.",
                      "Consultar respostas SIP, tentativas e completamentos nas filas relacionadas; Ping isolado não comprova serviço.",
                      "Comparar a evolução de P135 com a recuperação da distribuição após uma intervenção autorizada."],
                  "limitations": ["Last Calls comprova uso observado; não revela todas as rotas configuradas nem comprova ausência de tráfego.",
                                  "Canais livres não garantem capacidade utilizável pela fila; demanda, papel e roteamento precisam ser confirmados."]}
        result["findings"] = []
        reconciliation = snapshot.get('source_reconciliation', {})
        if reconciliation:
            result['limitations'].append('A comparação de carga usa canais válidos do Portal. '
                'Chamadas por troncos do Monitor têm outra definição e horário de coleta desconhecido.')
            for comparison in reconciliation.get('rows', []):
                if 'MONITOR_COM_TRAFEGO_PORTAL_ZERO' in comparison.get('flags', []):
                    result['findings'].append(f"{comparison['entity']}: Portal informa zero canais válidos, "
                        f"mas o Monitor informa {comparison['monitor_calls']:g} chamadas por troncos. "
                        'Não concluir ausência total de tráfego; verificar definição e horário das fontes.')
            if reconciliation.get('unmatched_monitor'):
                result['findings'].append('Existem nomes do Monitor sem correspondência exata no Portal; '
                    'validar o cadastro antes de associar contadores ou atribuir uma falha ao gateway.')
        maximum_age = finite(snapshot.get('max_age_s')) or GATEWAY_FRESH_SECONDS
        if stamp is None or not -5 <= now-stamp <= maximum_age:
            result["status"] = "DADOS_ATRASADOS"
            return result
        if not operational:
            result["status"] = "FORA_DA_JANELA"
            return result
        reference = {str(x.get("entity", "")).upper(): x for x in semantics}
        rows, seen = [], set()
        for raw in snapshot.get("gateways", []):
            name = str(raw.get("entity") or "").strip().upper()
            occ, total, busy = finite(raw.get("occupancy")), finite(raw.get("total")), finite(raw.get("busy"))
            if not name or name in seen:
                continue
            seen.add(name)
            context = reference.get(name, {})
            excluded = (str(context.get("canonical_status")) in {
                "STANDBY_EXPECTED", "PLANNED_INACTIVE", "PARTIAL_VISIBILITY", "OPERATIONAL_PARTIAL_VISIBILITY"}
                or context.get("traffic_expectation") in {
                    "NO_TRAFFIC_PLANNED", "SPORADIC_LAST_ROUTE", "RECEPTIVE_LOW_OR_VARIABLE"})
            if excluded:
                result["excluded_gateways"].append({"entity": name, "reason": context.get("note") or "Papel especial confirmado"})
            elif occ is not None and 0 <= occ <= 100 and total is not None and total > 0 and busy is not None and 0 <= busy <= total:
                rows.append({"entity": name, "occupancy_pct": round(occ, 2), "total": total,
                             "busy": busy, "free_channels": max(0, total-busy),
                             "invalid_channels": finite(raw.get("invalid")),
                             "role_confirmed": bool(context.get("evidence_source"))})
        result["inventory_count"] = len(seen)
        result["comparable_count"] = len(rows)
        required = max(3, math.ceil((len(seen)-len(result["excluded_gateways"]))*.8))
        if len(rows) < required:
            result["status"] = "COMPARACAO_INSUFICIENTE"
            return result
        ordered = sorted(x["occupancy_pct"] for x in rows)
        middle = len(ordered)//2
        median = ordered[middle] if len(ordered)%2 else (ordered[middle-1]+ordered[middle])/2
        loaded = [x for x in rows if x["occupancy_pct"] >= 80]
        low = []
        for row in rows:
            if median <= 0 or median-row["occupancy_pct"] < 40:
                continue
            profile = profiles.get(row["entity"], {})
            ratio = row["occupancy_pct"]/median
            mature = profile.get("samples", 0) >= 30 and len(profile.get("days", [])) >= 3
            learned_drop = mature and ratio < (finite(profile.get("ratio_mean")) or 0)*.6 and ratio <= .55
            if row["occupancy_pct"] <= 25 and ratio <= .35 or learned_drop:
                low.append(row)
        detected = bool(low and median >= 80 and len(loaded) >= max(2, math.ceil(len(rows)/2)))
        for row in low:
            profile = profiles.get(row["entity"], {})
            mean = finite(profile.get("ratio_mean"))
            mature = profile.get("samples", 0) >= 30 and len(profile.get("days", [])) >= 3
            ratio = row["occupancy_pct"]/median
            row.update(gap_pp=round(median-row["occupancy_pct"], 2),
                       baseline_samples=profile.get("samples", 0), baseline_days=len(profile.get("days", [])),
                       baseline_mean_pct=round(profile["occupancy_mean"], 2) if mature else None,
                       baseline_ratio=round(mean, 3) if mature and mean is not None else None,
                       learning_status="APRENDIDO" if mature else "EM_APRENDIZADO",
                       behavior="QUEDA_VS_HABITUAL" if mature and mean is not None and ratio < mean*.6
                       else "BAIXA_HABITUAL" if mature and mean is not None and mean <= .35
                       else "DESVIO_DE_BALANCEAMENTO" if row["role_confirmed"] else "PAPEL_A_CONFIRMAR")
        low_names = {x["entity"] for x in low}
        loaded_names = {x["entity"] for x in loaded}
        for queue in queues:
            qstamp = epoch(queue.get("ts"))
            p135 = finite(queue.get("p135_pct"))
            if (qstamp is None or not -5 <= now-qstamp <= queue.get("max_age_s", 330)
                    or p135 is None or p135 < 20 or p135 > 100):
                continue
            code = str(queue.get("codigo", ""))
            route = routes.get(code, {})
            rstamp = epoch(route.get("sample_at"))
            used = set(route.get("gateways", [])) if rstamp is not None and -5 <= now-rstamp <= 600 else set()
            result["congested_queues"].append({"codigo": code, "nome": queue.get("nome", code),
                "p135_pct": p135, "sample_at": iso(qstamp), "max_age_s": queue.get("max_age_s", 330),
                "observed_low_gateways": sorted(used & low_names),
                "observed_loaded_gateways": sorted(used & loaded_names),
                "mapping_status": "USO_OBSERVADO" if used else "SEM_RELACAO_COMPROVADA",
                "mapping_sample_at": iso(rstamp) if used else None,
                "gateway_signals": {name: signal for name, signal in route.get("signals", {}).items()
                                    if name in used & (low_names | loaded_names)}})
        result.update(valid=True, detected=detected, status="DESEQUILIBRIO_OBSERVADO" if detected else "SEM_DESEQUILIBRIO",
                      peer_median_pct=round(median, 2), low_gateways=low if detected else [],
                      loaded_gateways=loaded, spare_channels_observed=sum(x["free_channels"] for x in low) if detected else 0,
                      compared_gateways=rows,
                      association="COINCIDENCIA_TEMPORAL" if detected and result["congested_queues"] else "SEM_CONGESTIONAMENTO_ATUAL_COMPROVADO")
        if any(x["observed_low_gateways"] or x["observed_loaded_gateways"] for x in result["congested_queues"]) and detected:
            result["association"] = "COINCIDENCIA_E_USO_OBSERVADO"
        if detected:
            if all(x["role_confirmed"] for x in low):
                result["investigation_steps"][0] = "Conferir se todos os gateways previstos no balanceamento estão habilitados nas rotas, sem manutenção ou bloqueios."
            if any(not x["role_confirmed"] for x in low):
                result["findings"].append("O papel operacional dos gateways com baixa ocupação ainda não foi confirmado; verificar reserva, receptivo e ordem das rotas.")
            invalid = [x for x in low if (x["invalid_channels"] or 0) > 0]
            if invalid:
                result["findings"].append("Canais inválidos reportados pela coleta LIVE: "+", ".join(
                    x["entity"]+": "+str(round(x["invalid_channels"])) for x in invalid)+". Verificar limitações de serviço e de canais.")
            for queue in result["congested_queues"]:
                for name, signal in queue["gateway_signals"].items():
                    errors = sum(count for code, count in signal.get("sip_responses", {}).items() if str(code).startswith("5"))
                    if errors:
                        result["findings"].append(f"Fila {queue['codigo']} / {name}: {errors} respostas SIP 5xx observadas em Last Calls; investigar serviço e destino. A amostra não é um CDR nem uma taxa global de falhas.")
            if not result["findings"]:
                result["findings"].append("A fonte atual comprova distribuição desigual; ainda faltam evidências suficientes para identificar a restrição de rota ou serviço.")
        return result

    def _gateway_balance(self, db):
        if self.balance_provider is None:
            return "NOT_STARTED"
        now = self.clock()
        if self.balance_poll_at is not None and 0 <= now-self.balance_poll_at < 15:
            return "OK"
        document = self.balance_provider()
        self.balance_poll_at = now
        self._observe_gateway_activity(db, document)
        status = "OK"
        for snapshot in document.get("snapshots", []):
            inst = str(snapshot.get("instancia", ""))
            stamp = epoch(snapshot.get("sample_at"))
            if not inst:
                continue
            hour = datetime.fromtimestamp(stamp or now, LOCAL_ZONE).hour
            profiles = {str(x.get("entity", "")).upper(): self._kv(db, f"balance:profile:{inst}:{str(x.get('entity','')).upper()}:{hour}", {})
                        for x in snapshot.get("gateways", [])}
            report = self.balance_analysis(snapshot, document.get("semantics", {}).get(inst, []), profiles,
                document.get("queues", {}).get(inst, []), document.get("routes", {}).get(inst, {}),
                document.get("operational", {}).get(inst, True), now)
            key, state_key = inst+"|LOAD_IMBALANCE", "balance:state:"+inst
            state = self._kv(db, state_key, {})
            rec = self._active(db, "GATEWAY", key)
            report.update(generated_at=iso(now), confirmation_minutes=3, recovery_minutes=5,
                          recurrence_count=db.execute("SELECT COUNT(*) FROM incidents WHERE source='GATEWAY' AND entity_key=?", (key,)).fetchone()[0])
            if not report["valid"]:
                state.pop("candidate", None)
                state.pop("recovery", None)
                state["samples"] = 0
                if rec:
                    rec.update(evidence_available=False, status="EVIDENCE_PENDING", investigation=report)
                    self._put(db, rec)
                report["active_incident_id"] = rec["id"] if rec else None
                self._setkv(db, "balance:report:"+inst, report)
                self._setkv(db, state_key, state)
                if report["status"] != "FORA_DA_JANELA":
                    status = "PARTIAL"
                continue
            previous = state.get("last", 0)
            if stamp <= previous:
                continue  # Repeated browser reads never train or extend persistence.
            if stamp-previous > GATEWAY_FRESH_SECONDS:
                state.pop("candidate", None)
                state.pop("recovery", None)
                state["samples"] = 0
            if report["detected"]:
                names = sorted(x["entity"] for x in report["low_gateways"])
                if state.get("candidate_names") != names:
                    state.pop("candidate", None)
                    state["samples"] = 0
                state["candidate_names"] = names
                state.setdefault("candidate", stamp)
                state["samples"] = state.get("samples", 0)+1
                state.pop("recovery", None)
                duration = max(0, stamp-state["candidate"])
                report.update(status="EM_OBSERVACAO", observed_seconds=round(duration), observation_samples=state["samples"])
                if not rec and duration >= 180 and state["samples"] >= 6:
                    started = state["candidate"]
                    rec = {"id": identifier("GATEWAY", key, started), "source": "GATEWAY", "entity_key": key,
                           "instancia": inst, "codigo": "", "entity": "Distribuição de tráfego dos gateways",
                           "kind": "LOAD_IMBALANCE", "active": True, "status": "ACTIVE", "ack_at": None,
                           "started_at": iso(started), "confirmed_at": iso(stamp), "detected_at": iso(now),
                           "confirmation_minutes": 3, "normalization_minutes": 5, "delivery": {"status": "NOT_SENT"},
                           "initial_queues": report["congested_queues"]}
                    report["recurrence_count"] += 1
                    report["status"] = "CONFIRMADO"
                    rec.update(last_sample_at=iso(stamp), evidence_available=True, investigation=report)
                    self._put(db, rec)
                    self._event(db, rec, "OPENED", stamp)
                    state["last_event"] = stamp
                if rec:
                    report.update(status="CONFIRMADO", active_incident_id=rec["id"],
                                  observed_seconds=round(stamp-epoch(rec["started_at"])))
                    rec.update(last_sample_at=iso(stamp), evidence_available=True, status="ACTIVE",
                               normalization_started_at=None, investigation=report)
                    self._put(db, rec)
                    if stamp-state.get("last_event", stamp) >= 1800:
                        self._event(db, rec, "PERSISTENT", stamp)
                        state["last_event"] = stamp
            else:
                state.pop("candidate", None)
                state["samples"] = 0
                if rec:
                    state.setdefault("recovery", stamp)
                    rec.update(last_sample_at=iso(stamp), evidence_available=True, status="NORMALIZATION_PENDING",
                               normalization_started_at=iso(state["recovery"]))
                    report.update(status="NORMALIZACAO_EM_OBSERVACAO", active_incident_id=rec["id"])
                    if stamp-state["recovery"] >= 300:
                        after = {q["codigo"]: q for q in document.get("queues", {}).get(inst, [])
                                 if epoch(q.get("ts")) is not None and -5 <= now-epoch(q["ts"]) <= q.get("max_age_s", 330)}
                        report["outcome"] = [{"codigo": q["codigo"], "p135_before": q["p135_pct"],
                                              "p135_after": finite(after.get(q["codigo"], {}).get("p135_pct"))}
                                             for q in rec.get("initial_queues", [])]
                        report["outcome_note"] = "Antes/depois observado; a normalização não comprova causalidade nem intervenção."
                        rec.update(active=False, status="NORMALIZED", resolved_at=iso(stamp), investigation=report,
                                   normalization_started_at=None, last_event_kind="NORMALIZED")
                        self._event(db, rec, "NORMALIZED", stamp)
                        report["status"] = "NORMALIZADO"
                        state.pop("recovery", None)
                    self._put(db, rec)
            # Learn an hourly baseline only from usable, distinct, non-anomalous samples.
            low_names = {x["entity"] for x in report["low_gateways"]}
            median = report["peer_median_pct"]
            for row in report.get("compared_gateways", []):
                profile = profiles.get(row["entity"], {})
                if median <= 0 or row["entity"] in low_names or stamp-profile.get("last", 0) < 60:
                    continue
                count = profile.get("samples", 0)+1
                ratio = row["occupancy_pct"]/median
                old_mean = profile.get("ratio_mean", ratio)
                mean = old_mean+(ratio-old_mean)/count
                profile.update(samples=count, ratio_mean=mean,
                    ratio_m2=profile.get("ratio_m2", 0)+(ratio-old_mean)*(ratio-mean),
                    occupancy_mean=profile.get("occupancy_mean", row["occupancy_pct"])+
                        (row["occupancy_pct"]-profile.get("occupancy_mean", row["occupancy_pct"]))/count,
                    last=stamp, days=sorted(set(profile.get("days", [])+[datetime.fromtimestamp(stamp, LOCAL_ZONE).date().isoformat()]))[-28:])
                self._setkv(db, f"balance:profile:{inst}:{row['entity']}:{hour}", profile)
            state["last"] = stamp
            self._setkv(db, state_key, state)
            self._setkv(db, "balance:report:"+inst, report)
        return status

    def _observe_gateway_activity(self, db, document):
        """Persist separate counter transitions; Monitor timestamps denote consultations."""
        now = self.clock()
        for snapshot in document.get('snapshots', []):
            inst = str(snapshot.get('instancia', ''))
            if not inst:
                continue
            reconciliation = snapshot.get('source_reconciliation', {})
            monitor = reconciliation.get('monitor', {})
            monitor_rows = {str(row.get('entity', '')).upper(): row for row in reconciliation.get('rows', [])}
            for gateway in snapshot.get('gateways', []):
                entity = str(gateway.get('entity', '')).upper()
                if not entity:
                    continue
                comparison = monitor_rows.get(entity, {})
                for counter, value, stamp, usable, limit, basis in (
                    ('PORTAL_VALID_BUSY', finite(gateway.get('busy')), epoch(snapshot.get('sample_at')),
                     snapshot.get('status') == 'LIVE', snapshot.get('max_age_s', 30), 'PORTAL_SAMPLE'),
                    ('MONITOR_CALLS', finite(comparison.get('monitor_calls')), epoch(monitor.get('consulted_at')),
                     monitor.get('status') == 'CONSULTADO', 5, 'MONITOR_CONSULTATION')):
                    if not usable or stamp is None or value is None or value < 0 or not -5 <= now-stamp <= limit:
                        continue
                    key = 'gateway:activity:'+inst+':'+entity+':'+counter
                    state = self._kv(db, key, {})
                    previous = finite(state.get('last_observed_epoch'))
                    if previous is None:
                        previous = epoch(state.get('last_observed_at'))
                    if previous is not None and stamp <= previous:
                        continue
                    gap = previous is not None and stamp-previous > 45
                    if value == 0:
                        if not state or state.get('value') != 0 or gap:
                            state.update(zero_since=iso(stamp), zero_start_basis='FIRST_ZERO_OBSERVATION',
                                         zero_left_censored=not state or gap, previous_positive_at=state.get('last_positive_at'))
                    else:
                        if state.get('value') == 0:
                            state.update(last_zero_since=state.get('zero_since'), returned_at=iso(stamp),
                                         return_after_gap=gap)
                        state.update(zero_since=None, last_positive_at=iso(stamp))
                    state.update(value=value, last_observed_at=iso(stamp), last_observed_epoch=stamp, time_basis=basis)
                    self._setkv(db, key, state)

    def gateway_activity_snapshot(self, instancia):
        """Current event clocks are independent of the selected historical period."""
        inst, result = str(instancia), {}
        prefix = 'gateway:activity:'+inst+':'
        with self.lock, self.connect() as db:
            for row in db.execute('SELECT key,value FROM kv WHERE key>=? AND key<?', (prefix, prefix+'\uffff')):
                entity, counter = row['key'][len(prefix):].rsplit(':', 1)
                result.setdefault(entity, {})['portal_valid' if counter == 'PORTAL_VALID_BUSY' else 'monitor_calls'] = json.loads(row['value'])
            seen = set()
            for row in db.execute("SELECT payload FROM incidents WHERE source='GATEWAY' AND instancia=? ORDER BY started DESC LIMIT 1000", (inst,)):
                rec = json.loads(row[0])
                entity = str(rec.get('entity', '')).upper()
                if rec.get('kind') != 'NO_TRAFFIC' or entity in seen:
                    continue
                seen.add(entity)
                entry = result.setdefault(entity, {})
                portal = entry.setdefault('portal_valid', {})
                entry['incident'] = {key:rec.get(key) for key in ('id','active','status','started_at','confirmed_at','resolved_at',
                    'traffic_returned_at','last_sample_at','delivery','normalization_confirmed')}
                if rec.get('active'):
                    portal.update(incident_zero_since=rec['started_at'], zero_start_basis='RADAR_INCIDENT')
                if rec.get('traffic_returned_at'):
                    portal['incident_returned_at'] = rec['traffic_returned_at']
        return result

    def gateway_balance_snapshot(self, instancia):
        with self.lock, self.connect() as db:
            result = self._kv(db, "balance:report:"+str(instancia), {})
        stamp = epoch(result.get("sample_at"))
        if not result:
            return {"status": "EM_INICIALIZACAO", "detected": False, "low_gateways": []}
        if stamp is None or not -5 <= self.clock()-stamp <= GATEWAY_FRESH_SECONDS:
            result.update(status="DADOS_ATRASADOS", valid=False, detected=False)
        elif result.get("detected") and result.get("valid"):
            # Gateway and queue cadences differ. Do not keep an obsolete P135
            # association alive just because a gateway sample is still fresh.
            current = []
            for queue in result.get("congested_queues", []):
                age = self.clock()-(epoch(queue.get("sample_at")) or 0)
                if -5 <= age <= queue.get("max_age_s", 330):
                    current.append(queue)
            result["congested_queues"] = current
            if not current:
                result["association"] = "SEM_CONGESTIONAMENTO_ATUAL_COMPROVADO"
                result["findings"] = [x for x in result.get("findings", []) if not x.startswith("Fila ")]
        result.pop("compared_gateways", None)
        return result

    def _decorate(self, rec):
        rec=dict(rec)
        sample=epoch(rec.get("last_sample_at"))
        age=self.clock()-sample if sample is not None else None
        limit=(PORTAL_FRESH_SECONDS if rec["source"]=="PORTAL_ORBE" else
               GATEWAY_FRESH_SECONDS if rec["source"]=="GATEWAY" else
               150 if rec["source"]=="AGIROUTE" else 86400)
        rec["sample_age_seconds"]=round(max(0,age),1) if age is not None else None
        rec["stale"]=age is None or age>limit or age<-30
        rec["ack_pending"]=not bool(rec.get("ack_at"))
        rec["normalization_ack_pending"]=(rec.get("status")=="NORMALIZED" and
                                          (rec.get("last_event_kind")=="NORMALIZED" or rec.get("normalization_confirmed") is True) and
                                          bool(rec.get("resolved_at")) and
                                          not bool(rec.get("normalization_ack_at")))
        return rec

    def snapshot(self):
        with self.lock, self.connect() as db:
            # O painel Agora recomeça na meia-noite local; o histórico persiste.
            cutoff = datetime.fromtimestamp(self.clock(), LOCAL_ZONE).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            rows=db.execute("""SELECT payload FROM incidents
                WHERE id IN
                    (SELECT incident_id FROM events WHERE ts>=?)
                ORDER BY started DESC LIMIT 1001""",(cutoff,)).fetchall()
            def current(rec):
                # Um incidente ainda confirmado por amostras recentes pode durar >24h.
                if rec.get("active"):
                    return (epoch(rec.get("last_sample_at")) or epoch(rec.get("started_at")) or 0) >= cutoff
                return (epoch(rec.get("resolved_at")) or epoch(rec.get("last_sample_at")) or epoch(rec.get("started_at")) or 0) >= cutoff
            visible=[]
            for row in rows:
                rec=json.loads(row[0])
                if current(rec):
                    visible.append(rec)
            items=[self._decorate(rec) for rec in visible[:1000]]
            counts={"PORTAL_ORBE":0,"GATEWAY":0}
            for rec in visible:
                if rec.get("active"):
                    counts[rec.get("source")]=counts.get(rec.get("source"),0)+1
            pending=sum(not bool(rec.get("ack_at")) or
                        (rec.get("status")=="NORMALIZED" and
                         (rec.get("last_event_kind")=="NORMALIZED" or rec.get("normalization_confirmed") is True) and
                         bool(rec.get("resolved_at")) and
                         not bool(rec.get("normalization_ack_at"))) for rec in visible)
            events=[json.loads(r[0]) for r in db.execute("SELECT payload FROM events WHERE ts>=? ORDER BY ts DESC,id DESC LIMIT 60",(cutoff,))]
            return {"release":RELEASE,"generated_at":iso(self.clock()),"last_tick":self.last_tick,
                    "tick_ms":self.tick_ms,"sources":dict(self.status),"items":items,"events":events,
                    "counts":{"portal_active":counts.get("PORTAL_ORBE",0),"gateway_active":counts.get("GATEWAY",0),
                              "agiroute_active":counts.get("AGIROUTE",0),
                              "monitor_active":counts.get("MONITOR_ALERTAS",0),
                              "ack_pending":pending},"truncated":len(visible)>1000,
                    "history_started_at":self._kv(db,"history_started_at"),
                    "gateway_telegram":{"configured":self.telegram_sender is not None,
                        "states":{row[0]:row[1] for row in db.execute('SELECT state,COUNT(*) FROM gateway_telegram_outbox GROUP BY state')},
                        "worker":self._kv(db,'gateway:telegram:worker',{})}}

    def history(self, start, end, instancia="", codigo="", before=None, *,
                exact_scope=False, source="", allowed_instances=None, occurrences=False):
        a,b=epoch(start),epoch(end)
        if a is None or b is None or b<a:
            raise ValueError("Periodo invalido")
        sql="SELECT payload,ts,id FROM events WHERE ts>=? AND ts<=?"
        args=[a,b]
        if occurrences:
            # Pick canonical transitions over the complete lifecycle BEFORE date
            # filtering/pagination. Science and persistence remain in the audit.
            sql+=""" AND (kind IN ('AUTO_CLEANUP','OBSERVED') OR
                id=(SELECT e.id FROM events e WHERE e.incident_id=events.incident_id
                    AND e.kind IN ('OPENED','IMPORTED_ACTIVE','PERSISTENT')
                    ORDER BY CASE e.kind WHEN 'OPENED' THEN 0 WHEN 'IMPORTED_ACTIVE' THEN 1 ELSE 2 END,
                             e.ts,e.id LIMIT 1) OR
                id=(SELECT e.id FROM events e WHERE e.incident_id=events.incident_id
                    AND e.kind IN ('NORMALIZED','NORMALIZATION_OBSERVED','SUPERSEDED','HISTORY_RECONCILED')
                    ORDER BY CASE e.kind WHEN 'NORMALIZED' THEN 0 WHEN 'NORMALIZATION_OBSERVED' THEN 1
                                        WHEN 'SUPERSEDED' THEN 2 ELSE 3 END,
                             e.ts,e.id LIMIT 1))"""
        if instancia:
            sql+=(" AND instancia=?" if exact_scope else
                  " AND (instancia=? OR incident_id IN (SELECT id FROM incidents WHERE source='MONITOR_ALERTAS'))")
            args.append(str(instancia))
        if codigo:
            sql+=" AND codigo=?" if exact_scope else " AND (codigo=? OR codigo='')"
            args.append(str(codigo))
        if source:
            sql+=" AND incident_id IN (SELECT id FROM incidents WHERE source=?)"
            args.append(str(source))
        visibility=""
        visibility_args=[]
        if allowed_instances is not None:
            instances=sorted(str(x) for x in allowed_instances)
            visibility="source='MONITOR_ALERTAS'"
            if instances:
                visibility+=" OR instancia IN ("+",".join("?" for _ in instances)+")"
                visibility_args=instances
            sql+=" AND incident_id IN (SELECT id FROM incidents WHERE "+visibility+")"
            args.extend(visibility_args)
        if before:
            cursor=json.loads(before)
            if not isinstance(cursor,list) or len(cursor)!=2:
                raise ValueError("Cursor invalido")
            sql+=" AND (ts<? OR (ts=? AND id<?))"
            args.extend([float(cursor[0]),float(cursor[0]),str(cursor[1])])
        with self.lock, self.connect() as db:
            rows=db.execute(sql+" ORDER BY ts DESC,id DESC LIMIT 201",args).fetchall()
            result=[json.loads(r[0]) for r in rows[:200]]
            if occurrences:
                ids=sorted({item['incident_id'] for item in result})
                records={}
                if ids:
                    records={r['id']:json.loads(r['payload']) for r in db.execute(
                        'SELECT id,payload FROM incidents WHERE id IN ('+','.join('?' for _ in ids)+')',ids)}
                for item in result:
                    rec=records.get(item['incident_id'],{})
                    kind=item['kind']
                    if kind in ('AUTO_CLEANUP','OBSERVED'):
                        item['occurrence']='event'
                        item['state']='Limpeza automática' if kind=='AUTO_CLEANUP' else 'Observado'
                        if kind=='AUTO_CLEANUP':
                            item['channels_released']=rec.get('channels_released',item.get('channels_released'))
                    else:
                        item['occurrence']='activation' if kind in ('OPENED','IMPORTED_ACTIVE','PERSISTENT') else 'resolution'
                        item['active']=bool(rec.get('active'))
                        item['ack_at']=rec.get('ack_at')
                        item['state']=('Ativo e Ciente' if rec.get('ack_at') else 'Ativo e pendência de Ciência') if item['active'] and item['occurrence']=='activation' else 'Resolvido'
                        item['resolution_time_basis']=rec.get('resolution_time_basis')
            next_cursor=json.dumps([rows[199][1],rows[199][2]]) if len(rows)>200 else None
            payload={"events":result,"next_cursor":next_cursor,"history_started_at":self._kv(db,"history_started_at")}
            if exact_scope and not before:
                # Include scopes from older incidents, even when today's result is empty.
                scopes=db.execute("SELECT DISTINCT instancia,codigo FROM incidents"+
                                  (" WHERE "+visibility if visibility else "")+
                                  " ORDER BY instancia,codigo",visibility_args).fetchall()
                payload["scopes"]=[dict(row) for row in scopes]
            return payload

    def reconcile_historical_incidents(self, before, *, audit_key):
        """One explicit operator correction, never a midnight auto-resolution rule.

        Existing observed resolution clocks win. Unknown normalization clocks
        remain unknown; the new occurrence records the correction's own time.
        """
        cutoff=epoch(before)
        if cutoff is None:
            raise ValueError('Data de corte invalida')
        with self.lock, self.connect() as db:
            previous=self._kv(db,audit_key)
            if previous:
                return previous
            now=self.clock()
            changed=[]
            rows=db.execute('SELECT payload FROM incidents WHERE active=1 AND started<?',(cutoff,)).fetchall()
            for row in rows:
                rec=json.loads(row[0])
                evidence=db.execute("SELECT payload,ts FROM events WHERE incident_id=? AND kind IN ('NORMALIZED','NORMALIZATION_OBSERVED') ORDER BY ts LIMIT 1",(rec['id'],)).fetchone()
                rec.update(active=False,status='NORMALIZED',normalization_started_at=None,
                           historical_reconciled_at=iso(now),historical_reconciliation_reason='Operador confirmou resolução dos alertas anteriores a 29/09/2026')
                if evidence:
                    rec.update(resolved_at=iso(evidence['ts']),resolution_time_basis='OBSERVED_EVENT')
                else:
                    rec.update(resolved_at=None,resolution_recorded_at=iso(now),resolution_time_basis='OPERATOR_RECONCILIATION')
                self._put(db,rec)
                if not evidence:
                    self._event(db,rec,'HISTORY_RECONCILED',now,eid=rec['id']+':history-reconciled-20260929')
                changed.append(rec['id'])
            report={'before':iso(cutoff),'recorded_at':iso(now),'corrected_count':len(changed),'incident_ids':changed}
            self._setkv(db,audit_key,report)
            return report

    def acknowledge(self, iid):
        if not isinstance(iid,str) or len(iid)>100:
            raise ValueError("Incidente invalido")
        with self.lock, self.connect() as db:
            rec=self._get(db,iid)
            if rec is None:
                raise KeyError("Incidente nao encontrado")
            if not rec.get("ack_at"):
                rec.update(ack_at=iso(self.clock()),ack_operator="operador_local")
                self._put(db,rec)
                self._event(db,rec,"ACKNOWLEDGED",rec["ack_at"])
            return {"ok":True,"incident":self._decorate(rec)}

    def acknowledge_normalization(self, iid):
        if not isinstance(iid,str) or len(iid)>100:
            raise ValueError("Incidente invalido")
        with self.lock, self.connect() as db:
            rec=self._get(db,iid)
            if rec is None:
                raise KeyError("Incidente nao encontrado")
            if rec.get("status")!="NORMALIZED" or not (rec.get("last_event_kind")=="NORMALIZED" or rec.get("normalization_confirmed") is True) or not rec.get("resolved_at"):
                raise ValueError("Normalizacao ainda nao confirmada")
            if not rec.get("normalization_ack_at"):
                rec["normalization_ack_at"]=iso(self.clock())
                rec["normalization_ack_operator"]="operador_local"
                if not rec.get("ack_at"):
                    rec["ack_at"]=rec["normalization_ack_at"]
                    rec["ack_operator"]="operador_local"
                    self._event(db,rec,"ACKNOWLEDGED",rec["ack_at"])
                self._put(db,rec)
                self._event(db,rec,"NORMALIZATION_ACKNOWLEDGED",rec["normalization_ack_at"])
            return {"ok":True,"incident":self._decorate(rec)}
