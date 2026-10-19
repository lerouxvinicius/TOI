"""Independent, read-only SIP inventory collection; never waits for a queue."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json
import os
import sqlite3
import sys
from threading import Event
import time

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'
sys.path.insert(0, str(APP))
from sip_inventory import parse_inventory


def open_store(root=ROOT):
    path = Path(root) / 'dados/portal_sip.sqlite'
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=FULL')
    conn.executescript('''CREATE TABLE IF NOT EXISTS snapshots(
        id INTEGER PRIMARY KEY, instance TEXT NOT NULL, ts REAL NOT NULL,
        payload TEXT, error TEXT);
        CREATE INDEX IF NOT EXISTS ix_sip_instance_id ON snapshots(instance,id);''')
    os.chmod(path, 0o600)
    return conn


def collect_instance(instance, stop):
    from toi_collector_core_v3 import PortalSession, DEFAULT_BASE_URL
    ps = PortalSession(instance, os.environ['ORBE_LOGIN'], os.environ['ORBE_PASSWORD'],
                       os.getenv('ORBE_BASE_URL') or DEFAULT_BASE_URL, timeout=8)
    conn = open_store()
    try:
        while not stop.is_set():
            started = time.monotonic()
            try:
                if ps.cookies is None:
                    ps.auth()
                doc, _, _ = ps.get_json('metrics/dashboardtrunk/getsipserverdata', timeout=8, retries=0)
                payload, error = json.dumps(parse_inventory(doc), ensure_ascii=False), None
            except Exception as exc:
                payload, error = None, type(exc).__name__
            with conn:
                conn.execute('INSERT INTO snapshots(instance,ts,payload,error) VALUES(?,?,?,?)',
                             (str(instance), time.time(), payload, error))
            print(f'SIP Servers inst={instance} status={error or "OK"}', flush=True)
            stop.wait(max(1, 15 - (time.monotonic() - started)))
    finally:
        conn.close()


def main():
    catalog = json.loads((APP / 'portal_queue_catalog.json').read_text(encoding='utf-8'))
    instances = sorted({str(row['instancia']) for row in catalog['queues']})
    open_store().close()
    stop = Event()
    with ThreadPoolExecutor(max_workers=len(instances)) as pool:
        futures = [pool.submit(collect_instance, instance, stop) for instance in instances]
        try:
            for future in as_completed(futures):
                future.result()
        finally:
            stop.set()


if __name__ == '__main__':
    main()
