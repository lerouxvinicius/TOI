"""Auditoria semanal do ML Shadow; somente leitura do LIVE e nenhum alerta."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import threading

APP = Path(__file__).resolve().parent
LATEST = APP / 'static' / 'ml_model_audit_latest.json'
ARCHIVE = APP / 'homologacao'


def due(now=None):
    now = now or datetime.now(timezone.utc)
    try:
        last = datetime.fromisoformat(json.loads(LATEST.read_text(encoding='utf-8'))['generated_at'])
        return now - last >= timedelta(days=7)
    except (OSError, ValueError, KeyError, TypeError):
        return True


def run_once(db=None):
    if not due():
        return False
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    name = datetime.now(timezone.utc).strftime('ml_review_%Y%m%dT%H%M%SZ.json')
    cmd = [sys.executable, str(APP / 'toi_ml_homologation_audit_v1.py'),
           '--days', '14', '--output', str(ARCHIVE / name)]
    if db:
        cmd += ['--db', str(Path(db).resolve())]
    result = subprocess.run(cmd, cwd=APP, capture_output=True, text=True, timeout=1800)
    if result.returncode:
        raise RuntimeError('Auditoria ML falhou: ' + result.stderr[-1000:])
    return True


def start_weekly(stop_event, db=None):
    """Audita o mesmo banco que o painel está servindo."""
    def worker():
        while not stop_event.is_set():
            try:
                run_once(db)
            except Exception as exc:
                print('[ML WEEKLY] ' + str(exc), flush=True)
            stop_event.wait(3600)
    thread = threading.Thread(target=worker, name='TOI-ML-Weekly', daemon=True)
    thread.start()
    return thread
