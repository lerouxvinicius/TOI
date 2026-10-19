#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TOI Dashboard v1.10.6
Backend + API + servidor estático sem dependências externas.

Uso:
    python toi_dashboard_server_v1.py

Opcional:
    python toi_dashboard_server_v1.py --db dados_toi/demo_v3\20260828_173142\toi_demo.sqlite
    python toi_dashboard_server_v1.py --host 0.0.0.0 --port 8088

O servidor procura automaticamente o SQLite mais recente em:
    dados_toi/demo_v3/*/toi_demo.sqlite
    dados_toi/demo/*/toi_demo.sqlite

Depois abra:
    http://127.0.0.1:8088
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import ipaddress
from http.cookies import SimpleCookie
import json
import math
import mimetypes
import os
import re
import secrets
import sqlite3
import statistics
import sys
import threading
import uuid
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

from toi_ipbx_live_trace_v1 import build_current_report, log_trace
from toi_ipbx_blocked_calls_v1 import (build_blocked_calls_report, QUEUES as BLOCKED_QUEUES,
                                      IPBX_STATUSES, DEFAULT_STATUS, validate_ipbx_status)
import toi_shared_admin_v1 as shared_admin
from sbc_card_bridge import card_data
from sbc_traffic_history import live_report, traffic_report

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
SBC_CONFIRM_DB = APP_DIR.parent / "dados" / "monitor_confirmacao_5min.sqlite"
SBC_MONITOR_ROOT = APP_DIR.parent
SBC_HA_HISTORY = APP_DIR.parent / "dados_toi" / "history" / "sbc_ha_observed.jsonl"
_SBC_HA_HISTORY_LOCK = threading.Lock()
_SBC_HA_LAST = {}


def _record_sbc_ha_history(feed):
    """Retém no back-end as leituras HA ocultadas do card."""
    snapshot_at = feed.get("snapshot_at")
    if not snapshot_at:
        return
    rows = []
    with _SBC_HA_HISTORY_LOCK:
        for pair in feed.get("pairs", []):
            name = pair.get("name")
            state = {key: pair.get(key) for key in
                     ("observed_ha", "source_ha", "peer_heartbeat", "peer_identity", "peer_last_reading_utc")}
            if not name or _SBC_HA_LAST.get(name) == (snapshot_at, state):
                continue
            _SBC_HA_LAST[name] = (snapshot_at, state)
            rows.append({"snapshot_at": snapshot_at, "pair": name, **state})
        if rows:
            SBC_HA_HISTORY.parent.mkdir(parents=True, exist_ok=True)
            with SBC_HA_HISTORY.open("a", encoding="utf-8") as stream:
                for row in rows:
                    stream.write(json.dumps(row, ensure_ascii=False) + "\n")


def sbc_monitor_feed():
    """Snapshot que alimenta /sbc + eventos confirmados; sem SNMP na API."""
    feed = card_data(SBC_MONITOR_ROOT)
    _record_sbc_ha_history(feed)
    events = feed["incidents"]
    resolved_by_entity = {}
    for item in events:  # já ordenados do mais recente para o mais antigo
        entity = item["entity"]
        value = item["after_value"]
        healthy = ((entity.startswith(("mgw:route:", "mgw:server:", "vpn:connectivity", "sbc:snmp:")) and value == "True") or
                   (entity.startswith("sbc:integrity:") and value == "NORMAL") or
                   (entity.startswith("sbc:agent:") and value == "3") or
                   (entity.startswith("sbc:sip:") and value == "NORMAL") or
                   (entity.startswith("sbc:peer_timeout:") and value == "CLEAR"))
        one_shot = entity.startswith(("sbc:active:", "sbc:new_agent:", "vpn:ip"))
        item["type"] = ("PEER_TIMED_OUT" if entity.startswith("sbc:peer_timeout:") else
                        "SESSION_AGENT" if entity.startswith(("sbc:agent:", "sbc:new_agent:")) else
                        "SWITCHOVER" if entity.startswith("sbc:active:") else
                        "SBC_" + entity.split(":", 2)[1].upper() if entity.startswith("sbc:") else
                        "MGW_" + entity.split(":", 2)[1].upper() if entity.startswith("mgw:") else "VPN")
        item["resolved_at"] = resolved_by_entity.get(entity)
        item["status"] = "Evento" if one_shot else "Resolvido" if healthy or item["resolved_at"] else "Ativo"
        if healthy:
            resolved_by_entity[entity] = item["confirmed_at"]
    feed["incidentes"] = events
    feed["sbc_incidentes"] = [item for item in events if item["scope"] == "SBC"]
    # Campos legados para outras telas existentes; a fonte do card é feed.pairs.
    feed["pares"] = [{"name": pair["name"], "status": pair["observed_ha"],
                      "nodes": [{"ip": node["ip"], "role": node["role"], "snmp": node["snmp"]}
                                for node in pair["nodes"]]} for pair in feed["pairs"]]
    feed["heartbeat_peer_verificado"] = False
    return feed


GATEWAY_SHADOW_DB = APP_DIR.parent / "dados_toi" / "shadow" / "toi_gateway_health_shadow.sqlite"
OPERATIONAL_KNOWLEDGE_FILE = STATIC_DIR / "operational_knowledge_cases_v1.json"
OPERATIONAL_FEEDBACK_FILE = APP_DIR.parent / "dados_toi" / "knowledge" / "operational_feedback_v1.jsonl"
_FEEDBACK_LOCK = threading.Lock()
DEMO_SCOPE_FILE = APP_DIR / "toi_demo_scope_v1.json"
QUEUE_DISCOVERY_TAIL_ROWS = 20000
CURRENT_LOOKUP_TAIL_ROWS = 20000
QUEUE_ROWID_RANGE_SAFETY_ROWS = 256
ANALYTICS_LONG_CACHE_BUCKET_MINUTES = 5
# R1.2.4 H2-C1 H1 — cache analítico leve e persistente, separado do SQLite LIVE.
# O LIVE permanece estritamente READ ONLY; somente este sidecar derivado pode receber escrita.
ANALYTICS_SIDECAR_DB = APP_DIR.parent / "dados_toi" / "cache" / "toi_analytics_light_v1.sqlite"
ANALYTICS_SIDECAR_RETENTION_DAYS = 32
ANALYTICS_SIDECAR_LONG_MINUTES = 24 * 60
ANALYTICS_SIDECAR_SYNC_THROTTLE_S = 5.0
ANALYTICS_SIDECAR_CHUNK_ROWS = 5000
_ANALYTICS_SIDECAR_LOCK = threading.Lock()
_TRACE_REPORT_LOCK = threading.Lock()
_TOI_AI_QUERY_LOCK = threading.Lock()
_BLOCKED_JOBS_LOCK = threading.Lock()
_BLOCKED_JOBS = {}
_BLOCKED_SUMMARY_DIR = APP_DIR.parent / "dados_toi" / "history" / "blocked_calls_latest"


def _blocked_summary_path(login):
    name = hashlib.sha256(login.encode("utf-8")).hexdigest()[:24] + ".json"
    return _BLOCKED_SUMMARY_DIR / name


def _save_blocked_summary(login, report):
    """Guarda apenas o último resumo, sem os telefones consultados."""
    _BLOCKED_SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    destination = _blocked_summary_path(login)
    temporary = destination.with_suffix(".tmp")
    payload = {"login": login, **{key: report[key] for key in
                                  ("queue", "rows", "period", "carriers")}}
    payload.update(ipbx_status=report.get("ipbx_status", DEFAULT_STATUS),
                   ipbx_status_label=report.get("ipbx_status_label", IPBX_STATUSES[DEFAULT_STATUS]))
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, destination)


def _load_blocked_summary(login):
    try:
        payload = json.loads(_blocked_summary_path(login).read_text(encoding="utf-8"))
        if payload.get("login") != login:
            return None
        result={key: payload[key] for key in ("queue", "rows", "period", "carriers")}
        status=validate_ipbx_status(payload.get("ipbx_status", DEFAULT_STATUS))
        result.update(ipbx_status=status,ipbx_status_label=IPBX_STATUSES[status])
        return result
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _run_blocked_calls_job(job_id, queue, login, start_at, end_at, ipbx_status=DEFAULT_STATUS):
    def set_stage(stage):
        with _BLOCKED_JOBS_LOCK:
            _BLOCKED_JOBS[job_id]["stage"] = stage

    try:
        report = build_blocked_calls_report(queue, start_at=start_at, end_at=end_at,
                                            job_id=job_id, progress=set_stage, ipbx_status=ipbx_status)
        output = Path(report["output"]).read_bytes()
        try:
            _save_blocked_summary(login, report)
        except OSError:
            log_trace("summary_persist_failed", queue, job_id)
        shared_admin.audit(login, "ipbx_blocked_calls_generated",
                           f"fila={queue};status_ipbx={ipbx_status};linhas={report['rows']};periodo={report['period']}")
        with _BLOCKED_JOBS_LOCK:
            _BLOCKED_JOBS[job_id].update(state="done", stage="Concluído",
                                         rows=report["rows"], date=report["date"],
                                         period=report["period"],
                                         carriers=report["carriers"],
                                         ipbx_status=ipbx_status,ipbx_status_label=IPBX_STATUSES[ipbx_status],
                                         filename=f"telefones_fila_{queue}_Rel{ipbx_status}.xlsx",
                                         portal_limit_possible=report["portal_limit_possible"],
                                         output=output)
    except Exception as exc:
        log_trace("job_failed", queue, job_id, error=type(exc).__name__)
        with _BLOCKED_JOBS_LOCK:
            _BLOCKED_JOBS[job_id].update(state="failed", stage="Falha",
                                         error=str(exc) if isinstance(exc, (ValueError, RuntimeError))
                                         else "Consulta indisponível: " + type(exc).__name__)
    finally:
        _TRACE_REPORT_LOCK.release()

# R1.2.4 H2-C2 — Query Plan + Performance Gate Hardening.
# Regra: diagnosticar causa raiz antes de instalar; cache nunca mascara consulta ruim.
ANALYTICS_RELEASE = "R1.2.6 READ STORM HARDENING D3 - ALL CURRENT LOOKUPS ROWID-BOUNDED"
ANALYTICS_DIAGNOSTIC_READONLY = False

PERIOD_MAP = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "60m": 60,      # alias aceito por ferramentas/validadores
    "2h": 120,
    "6h": 360,
    "1d": 1440,
    "24h": 1440,    # alias aceito por ferramentas/validadores
    "7d": 10080,
    "14d": 20160,
    "30d": 43200,
}

# v1.10.6.3.7.5 R1.2.4 H2-C1 - Operational Window + Gateway Story + Decision UX Hardening.
# H2-C permanece preservado como base: Event Intelligence + Operational Pressure + Semantic KPI Hardening.
# Camada analítica somente leitura; feedback humano permanece append-only em arquivo separado.
# Janelas >=7 dias usam PERFIL HORÁRIO 00:00..23:59 na apresentação,
# preservando o intervalo cronológico real como evidência.
ANALYTICS_PROFILE_PERIODS = {"7d", "14d", "30d"}
ANALYTICS_MAX_CUSTOM_MINUTES = 60 * 24 * 31
ANALYTICS_BUCKET_MINUTES = 15
# R1.2.4 — cache/rollup somente em memória do Dashboard. Não escreve no SQLite.
ANALYTICS_CACHE_TTL_SHORT_S = 20
ANALYTICS_CACHE_TTL_LONG_S = 180
ANALYTICS_CACHE_MAX_ENTRIES = 48
# Read Storm Hardening: o cockpit LIVE atualiza a cada 10 s, mas consumidores
# simultaneos da mesma selecao compartilham o mesmo calculo. O ganho principal
# vem das leituras por faixa de ROWID; este cache curto evita duplicacao entre
# abas/requests concorrentes sem envelhecer a telemetria.
OVERVIEW_CACHE_TTL_S = 4.0
GATEWAY_LONG_WINDOW_MINUTES = 24 * 60
GATEWAY_NOMINAL_CADENCE_S = 10.0
GATEWAY_GAP_CAP_S = 30.0
OPERATIONAL_CONTEXT_FILE = STATIC_DIR / "toi_operational_context_v1.json"
GATEWAY_OPERATIONAL_DEFAULT_START = "08:00"
GATEWAY_OPERATIONAL_DEFAULT_END = "21:00"

def _hhmm_minutes(value, fallback=None):
    m=re.match(r"^(\d{1,2}):(\d{2})$", str(value or "").strip())
    if not m:
        return fallback
    h=int(m.group(1)); mi=int(m.group(2))
    if h<0 or h>23 or mi<0 or mi>59:
        return fallback
    return h*60+mi

def load_operational_window(instancia=None, codigo=None):
    """Resolve a janela operacional usada nas leituras de negócio.

    A configuração é apenas lida do frontend/config existente. Se não estiver
    disponível, usa 08:00–21:00 como fallback explícito.
    """
    cfg={"enabled":True,"start_time":GATEWAY_OPERATIONAL_DEFAULT_START,"end_time":GATEWAY_OPERATIONAL_DEFAULT_END,"source":"FALLBACK_DEFAULT"}
    try:
        if OPERATIONAL_CONTEXT_FILE.exists():
            raw=json.loads(OPERATIONAL_CONTEXT_FILE.read_text(encoding="utf-8"))
            base=dict(raw.get("default") or {})
            qmap=raw.get("queues") or {}
            key=f"{instancia}:{codigo}" if instancia is not None and codigo else None
            specific=(qmap.get(key) if key else None) or (qmap.get(str(codigo)) if codigo else None) or {}
            merged={**base,**specific}
            if merged:
                cfg.update(merged); cfg["source"]="toi_operational_context_v1.json"
    except Exception:
        pass
    start=_hhmm_minutes(cfg.get("start_time"),8*60)
    end=_hhmm_minutes(cfg.get("end_time"),21*60)
    cfg["start_minute"]=start;cfg["end_minute"]=end
    cfg["start_time"]=f"{start//60:02d}:{start%60:02d}";cfg["end_time"]=f"{end//60:02d}:{end%60:02d}"
    cfg["label"]=f"{cfg['start_time']}–{cfg['end_time']}"
    return cfg

def _inside_operational_minutes(minute_of_day,start_minute,end_minute,enabled=True):
    if not enabled or start_minute==end_minute:
        return True
    if start_minute<end_minute:
        return start_minute<=minute_of_day<end_minute
    return minute_of_day>=start_minute or minute_of_day<end_minute

def _inside_operational_dt(dt,window):
    if dt is None:return False
    return _inside_operational_minutes(dt.hour*60+dt.minute,int(window.get("start_minute",480)),int(window.get("end_minute",1260)),bool(window.get("enabled",True)))

def _operational_intervals(start_dt,end_dt,window):
    """Intervalos efetivos da janela operacional dentro do range solicitado."""
    if end_dt<=start_dt:return []
    if not window.get("enabled",True) or int(window.get("start_minute",0))==int(window.get("end_minute",0)):
        return [(start_dt,end_dt)]
    sm=int(window.get("start_minute",480));em=int(window.get("end_minute",1260))
    tz=start_dt.tzinfo
    day=start_dt.date()-timedelta(days=1 if sm>em else 0)
    last=end_dt.date()+timedelta(days=1)
    out=[]
    while day<=last:
        a=datetime.combine(day,datetime.min.time(),tzinfo=tz)+timedelta(minutes=sm)
        if sm<em:
            b=datetime.combine(day,datetime.min.time(),tzinfo=tz)+timedelta(minutes=em)
        else:
            b=datetime.combine(day+timedelta(days=1),datetime.min.time(),tzinfo=tz)+timedelta(minutes=em)
        aa=max(a,start_dt);bb=min(b,end_dt)
        if bb>aa:out.append((aa,bb))
        day+=timedelta(days=1)
    return out

def _operational_requested_seconds(start_dt,end_dt,window):
    return sum((b-a).total_seconds() for a,b in _operational_intervals(start_dt,end_dt,window))

def _operational_sql_clause(window,column="ts"):
    if not window.get("enabled",True):return "1=1",[]
    sm=int(window.get("start_minute",480));em=int(window.get("end_minute",1260))
    if sm==em:return "1=1",[]
    st=f"{sm//60:02d}:{sm%60:02d}";et=f"{em//60:02d}:{em%60:02d}"
    expr=f"substr({column},12,5)"
    if sm<em:return f"({expr}>=? AND {expr}<?)",[st,et]
    return f"({expr}>=? OR {expr}<?)",[st,et]


TAG_RE = re.compile(r"<[^>]+>")
NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")

def strip_html_text(v):
    if v is None:
        return ""
    text = str(v).replace("&nbsp;", " ").replace("\xa0", " ")
    text = TAG_RE.sub(" ", text)
    return " ".join(text.split())

def parse_portal_number(v, default=0.0):
    """Extrai o número de HTML/texto do Portal preservando decimal pt-BR/en-US."""
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return float(v)
    text = strip_html_text(v).replace("%", "")
    matches = NUM_RE.findall(text)
    if not matches:
        return default
    token = matches[-1]
    if "," in token and "." in token:
        # assume o último separador como decimal
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").replace(",", ".")
        else:
            token = token.replace(",", "")
    elif "," in token:
        token = token.replace(",", ".")
    try:
        return float(token)
    except Exception:
        return default


def _semantic_canonical(item):
    """Normaliza Ground Truth operacional sem alterar qualquer estado de máquina."""
    lifecycle = str((item or {}).get("lifecycle_state") or "").upper()
    role = str((item or {}).get("operational_role") or "").upper()
    traffic = str((item or {}).get("traffic_expectation") or "").upper()
    coverage = str((item or {}).get("coverage_level") or "").upper()
    if lifecycle == "FAULT_CONFIRMED":
        return "FAULT_CONFIRMED_CONTEXT"
    if lifecycle in {"RECOVERED_CONFIRMED", "OPERATIONAL_RESTORED"} or traffic == "TRAFFIC_RESTORED":
        return "RECOVERED_CONFIRMED"
    if lifecycle == "VENDOR_CONFIGURATION" or traffic == "NO_TRAFFIC_PLANNED":
        return "PLANNED_INACTIVE"
    if lifecycle == "IN_SERVICE_STANDBY" or role == "SBC_LAST_ROUTE" or traffic == "SPORADIC_LAST_ROUTE":
        return "STANDBY_EXPECTED"
    if coverage == "PARTIAL_QUEUE_SCOPE":
        return "OPERATIONAL_PARTIAL_VISIBILITY"
    return "CONTEXT_INFO"


def _semantic_capacity_treatment(canonical):
    return {
        "RECOVERED_CONFIRMED": {
            "capacity_treatment": "OBSERVATIONAL_RECOVERED",
            "capacity_label": "Recuperado; capacidade atual depende da telemetria observada",
            "resilience_impact": "NONE",
            "confidence_impact": "NONE",
        },
        "FAULT_CONFIRMED_CONTEXT": {
            "capacity_treatment": "EXCLUDE_FROM_RELIABLE_HEADROOM_UNTIL_REVALIDATED",
            "capacity_label": "Não contar como redundância saudável",
            "resilience_impact": "REDUCED",
            "confidence_impact": "NONE",
        },
        "PLANNED_INACTIVE": {
            "capacity_treatment": "EXCLUDE_FROM_IMMEDIATE_CAPACITY",
            "capacity_label": "Fora da capacidade imediatamente utilizável",
            "resilience_impact": "PLANNED",
            "confidence_impact": "NONE",
        },
        "STANDBY_EXPECTED": {
            "capacity_treatment": "CONTINGENCY_CANDIDATE_ONLY",
            "capacity_label": "Standby; não interpretar baixa ocupação como folga ativa",
            "resilience_impact": "STANDBY",
            "confidence_impact": "NONE",
        },
        "OPERATIONAL_PARTIAL_VISIBILITY": {
            "capacity_treatment": "OBSERVATIONAL_REDUCE_CONFIDENCE",
            "capacity_label": "Não penaliza capacidade; reduz confiança",
            "resilience_impact": "NONE",
            "confidence_impact": "REDUCED",
        },
    }.get(canonical, {
        "capacity_treatment": "OBSERVATIONAL_ONLY",
        "capacity_label": "Contexto observacional",
        "resilience_impact": "NONE",
        "confidence_impact": "NONE",
    })


def load_operational_knowledge_cases(instancia=None, entity=None):
    """Lê casos operacionais estruturados; somente leitura / Shadow."""
    try:
        raw = json.loads(OPERATIONAL_KNOWLEDGE_FILE.read_text(encoding="utf-8")) if OPERATIONAL_KNOWLEDGE_FILE.exists() else {}
    except Exception:
        raw = {}
    rows=[]
    ent_key=str(entity or "").strip().upper()
    for item in (raw.get("cases") or []):
        if instancia is not None and str(item.get("instancia")) != str(instancia):
            continue
        if ent_key and str(item.get("entity") or "").strip().upper() != ent_key:
            continue
        rows.append(dict(item))
    return {
        "version":raw.get("version") or "toi_operational_knowledge_cases_v1",
        "mode":raw.get("mode") or "SHADOW_CASE_BASED_REASONING",
        "governance":raw.get("governance") or {},
        "cases":rows,
        "source":str(OPERATIONAL_KNOWLEDGE_FILE),
        "available":bool(raw),
    }


def _feedback_text(v, max_len=4000):
    text = str(v or "").strip().replace("\x00", "")
    return text[:max_len]


def _feedback_dt(v):
    if not v:
        return None
    try:
        dt=datetime.fromisoformat(str(v))
        return dt.astimezone() if dt.tzinfo is None else dt
    except Exception:
        return None


def load_operational_feedback(instancia=None, entity=None, start=None, end=None, limit=500):
    """Lê feedback operacional append-only separado dos bancos LIVE/Shadow."""
    rows=[]
    if not OPERATIONAL_FEEDBACK_FILE.exists():
        return {"version":"toi_operational_feedback_v1","mode":"APPEND_ONLY_HUMAN_FEEDBACK","records":[],"source":str(OPERATIONAL_FEEDBACK_FILE),"available":True}
    inst_key=str(instancia or "").strip()
    ent_key=str(entity or "").strip().upper()
    start_dt=_feedback_dt(start); end_dt=_feedback_dt(end)
    try:
        for line in OPERATIONAL_FEEDBACK_FILE.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try: item=json.loads(line)
            except Exception: continue
            if inst_key and str(item.get("instancia") or "") != inst_key: continue
            if ent_key and str(item.get("entity") or "").strip().upper() != ent_key: continue
            evt=_feedback_dt(item.get("event_time")) or _feedback_dt(item.get("recorded_at"))
            if start_dt and evt and evt < start_dt: continue
            if end_dt and evt and evt > end_dt: continue
            rows.append(item)
    except Exception:
        rows=[]
    rows=rows[-max(1,min(int(limit or 500),2000)):]
    return {"version":"toi_operational_feedback_v1","mode":"APPEND_ONLY_HUMAN_FEEDBACK","records":rows,"source":str(OPERATIONAL_FEEDBACK_FILE),"available":True}


def append_operational_feedback(payload):
    """Persiste somente conhecimento humano; nunca escreve nos bancos analíticos."""
    allowed_types={"OBSERVATION","CAUSE","INTERVENTION","RECOVERY","FALSE_POSITIVE"}
    allowed_precision={"EXACT","APPROXIMATE","UNKNOWN"}
    allowed_causality={"CONFIRMED","PROBABLE","HYPOTHESIS","NOT_APPLICABLE","UNKNOWN"}
    event_type=str(payload.get("event_type") or "OBSERVATION").upper()
    if event_type not in allowed_types: raise ValueError("event_type inválido")
    precision=str(payload.get("time_precision") or "UNKNOWN").upper()
    if precision not in allowed_precision: raise ValueError("time_precision inválido")
    causality=str(payload.get("causality") or "UNKNOWN").upper()
    if causality not in allowed_causality: raise ValueError("causality inválida")
    instancia=_feedback_text(payload.get("instancia"),32)
    entity=_feedback_text(payload.get("entity"),160).upper()
    description=_feedback_text(payload.get("description"),4000)
    if not instancia or not entity or not description:
        raise ValueError("instancia, entity e description são obrigatórios")
    event_time=_feedback_text(payload.get("event_time"),64) or None
    if event_time and not _feedback_dt(event_time):
        raise ValueError("event_time deve estar em formato ISO local, ex.: 2026-09-06T18:30")
    now=datetime.now().astimezone().isoformat(timespec="seconds")
    rec={
        "record_id":"OPF-"+uuid.uuid4().hex[:16].upper(),
        "recorded_at":now,
        "instancia":instancia,
        "entity":entity,
        "event_type":event_type,
        "event_time":event_time,
        "time_precision":precision,
        "description":description,
        "result":_feedback_text(payload.get("result"),1000) or None,
        "causality":causality,
        "source":_feedback_text(payload.get("source"),300) or "OPERADOR",
        "operator":_feedback_text(payload.get("operator"),200) or "TOI Admin",
        "notes":_feedback_text(payload.get("notes"),2000) or None,
        "governance":{
            "changes_diagnosis":False,"generates_alerts":False,"executes_actions":False,
            "purpose":"Conhecimento operacional humano versionado e separado da telemetria/ML."
        }
    }
    OPERATIONAL_FEEDBACK_FILE.parent.mkdir(parents=True,exist_ok=True)
    with _FEEDBACK_LOCK:
        with OPERATIONAL_FEEDBACK_FILE.open("a",encoding="utf-8",newline="\n") as fh:
            fh.write(json.dumps(rec,ensure_ascii=False,separators=(",",":"))+"\n")
            fh.flush(); os.fsync(fh.fileno())
    return rec


def load_gateway_semantic_reference(instancia=None, entities=None):
    """Lê referência versionada empacotada com o Dashboard; somente leitura."""
    path = STATIC_DIR / "gateway_semantic_context_v1.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        raw = {}
    rows = []
    for item in (raw.get("gateways") or []):
        if instancia is not None and str(item.get("instancia")) != str(instancia):
            continue
        out = dict(item)
        canonical = _semantic_canonical(out)
        out["canonical_status"] = canonical
        out.update(_semantic_capacity_treatment(canonical))
        rows.append(out)
    defaults = raw.get("balanced_traffic_policy", {})
    if defaults and instancia is not None and entities is not None:
        known = {str(x.get("entity", "")).upper() for x in rows}
        for name in sorted({str(x).strip().upper() for x in entities if str(x).strip()}-known):
            out = {**defaults, "instancia": str(instancia), "entity": name}
            out["canonical_status"] = _semantic_canonical(out)
            out.update(_semantic_capacity_treatment(out["canonical_status"]))
            rows.append(out)
    return {
        "version": raw.get("version") or "gateway_semantic_context_v1",
        "generated_at": raw.get("generated_at"),
        "mode": raw.get("mode") or "SHADOW_HUMAN_GROUND_TRUTH",
        "governance": raw.get("governance") or {},
        "gateways": rows,
        "source": str(path),
        "available": bool(raw),
        "balanced_traffic_policy": defaults,
    }


def normalize_queue_raw(row):
    """Corrige campos HTML do getqueuedetaildata sem perder o snapshot original.

    R1.2.4 aceita também aliases extraídos por json_extract(). Isso evita trazer o
    blob raw_queue inteiro em janelas históricas longas, preservando a mesma regra
    de normalização dos campos operacionais relevantes.
    """
    d = dict(row) if row else {}
    alias_map = {
        "PercHitRateLast5": d.pop("_raw_hit_rate5", None),
        "PercHitRateLast15": d.pop("_raw_hit_rate15", None),
        "QtBuffer": d.pop("_raw_buffer", None),
        "QtMailingOnScreen": d.pop("_raw_mailing_on_screen", None),
        "QtGetBufferTelephone": d.pop("_raw_buffer_config", None),
        "DtServer": d.pop("_raw_source_server_time", None),
        "dtCacheAndServerQueueDetailData": d.pop("_raw_source_cache_time", None),
        "VlAwtLast5": d.pop("_raw_awt5", None),
        "VlAwtLast15": d.pop("_raw_awt15", None),
        "VlMwt": d.pop("_raw_mwt", None),
    }
    raw = {k: v for k, v in alias_map.items() if v is not None}
    if not raw:
        try:
            raw = json.loads(d.get("raw_queue") or "{}")
        except Exception:
            raw = {}
    if raw:
        d["hit_rate5"] = parse_portal_number(raw.get("PercHitRateLast5"), d.get("hit_rate5") or 0)
        d["hit_rate15"] = parse_portal_number(raw.get("PercHitRateLast15"), d.get("hit_rate15") or 0)
        d["buffer"] = parse_portal_number(raw.get("QtBuffer"), d.get("buffer") or 0)
        d["mailing_on_screen"] = parse_portal_number(raw.get("QtMailingOnScreen"), 0)
        d["buffer_config"] = parse_portal_number(raw.get("QtGetBufferTelephone"), 0)
        d["source_server_time"] = raw.get("DtServer")
        d["source_cache_time"] = raw.get("dtCacheAndServerQueueDetailData")
        d["awt5"] = strip_html_text(raw.get("VlAwtLast5"))
        d["awt15"] = strip_html_text(raw.get("VlAwtLast15"))
        d["mwt"] = strip_html_text(raw.get("VlMwt"))
    d.pop("raw_queue", None)
    return d


def find_latest_db():
    candidates = []
    roots = [Path.cwd(), APP_DIR, APP_DIR.parent]
    # também sobe alguns níveis para funcionar quando servidor está numa subpasta
    p = APP_DIR
    for _ in range(4):
        roots.append(p)
        p = p.parent
    seen = set()
    for root in roots:
        try:
            root = root.resolve()
        except Exception:
            continue
        if root in seen:
            continue
        seen.add(root)
        for rel in ("dados_toi/live/toi_live.sqlite", "dados_toi/demo_v3/*/toi_demo.sqlite", "dados_toi/demo/*/toi_demo.sqlite"):
            for db in root.glob(rel):
                if db.is_file():
                    candidates.append(db)
    if not candidates:
        return None
    return max(set(candidates), key=lambda p: p.stat().st_mtime)


def gateway_source_name(value):
    return re.sub(r'^GW\s*[-:]\s*', '', str(value or '').strip(), flags=re.I).upper()


GATEWAY_MONITOR_ALIASES = {'GWS826-DC1Y': 'GWS826-DC1'}
GATEWAY_MONITOR_INSTANCE_ALIASES = {('6', 'GWS803-DC1'): 'GWS803-DC1'}


def gateway_monitor_name(value, instancia=None):
    name = gateway_source_name(value)
    # Only identities explicitly confirmed by the operator may cross names.
    name = GATEWAY_MONITOR_INSTANCE_ALIASES.get((str(instancia), name), name)
    return GATEWAY_MONITOR_ALIASES.get(name, name)


class GatewayMonitorSource:
    """Read the colocated Monitor API in the background, never on dashboard requests.

    The API has no collection timestamp. A successful GET is a consultation,
    not proof that the upstream collector has produced a recent measurement.
    Only explicit equipment names within the same instance are matched.
    """
    POLL_INTERVAL_S = 1
    MAX_CONSULTATION_AGE_S = 5

    def __init__(self, url='http://127.0.0.1:8000/api/metrics', clock=time.time):
        self.url, self.clock = url, clock
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None
        self.rows, self.observed_at, self.error = [], None, None

    def refresh(self):
        try:
            with urlopen(self.url, timeout=2) as response:
                raw = response.read(1024*1024+1)
            if len(raw) > 1024*1024:
                raise ValueError('Response too large')
            doc = json.loads(raw)
            if not isinstance(doc, list) or len(doc) > 200:
                raise ValueError('Unexpected metrics structure')
            rows = []
            for item in doc:
                if not isinstance(item, dict):
                    continue
                inst = re.fullmatch(r'(?:Inst[aâ]ncia\s*)?([1-6])', str(item.get('instancia', '')).strip(), re.I)
                name = gateway_monitor_name(item.get('hostname'), inst.group(1) if inst else None)
                calls = parse_portal_number(item.get('calls'), None)
                if not inst or not name or len(name) > 120:
                    continue
                if isinstance(item.get('calls'), bool) or calls is not None and (not 0 <= calls <= 1000000):
                    calls = None
                idle = parse_portal_number(item.get('idle_cycles'), None)
                if isinstance(item.get('idle_cycles'), bool) or idle is not None and not 0 <= idle <= 1000000:
                    idle = None
                rows.append({'instancia': inst.group(1), 'entity': name, 'calls': calls, 'idle_cycles': idle})
            with self.lock:
                self.rows, self.observed_at, self.error = rows, self.clock(), None
        except Exception as error:
            with self.lock:
                self.error = type(error).__name__

    def snapshot(self, instancia):
        with self.lock:
            age = max(0, self.clock()-self.observed_at) if self.observed_at is not None else None
            status = ('INDISPONIVEL' if self.error else 'EM_INICIALIZACAO' if age is None else
                      'CONSULTADO' if age <= self.MAX_CONSULTATION_AGE_S else 'ATRASADO')
            # Failed/stale observations must not keep displaying old counters as current.
            rows = [{**row, 'calls': row['calls'] if status == 'CONSULTADO' else None,
                     'idle_cycles': row.get('idle_cycles') if status == 'CONSULTADO' else None}
                    for row in self.rows if row['instancia'] == str(instancia)]
            return {'source': 'MONITOR_ORBE_TRUNK_CALLS', 'status': status, 'gateways': rows,
                    'consulted_at': datetime.fromtimestamp(self.observed_at, timezone.utc).isoformat()
                        if self.observed_at is not None else None,
                    'consultation_age_s': round(age, 2) if age is not None else None,
                    'sample_at': None, 'sample_age_s': None, 'poll_interval_s': self.POLL_INTERVAL_S}

    def start(self):
        def poll():
            while not self.stop_event.is_set():
                self.refresh()
                self.stop_event.wait(self.POLL_INTERVAL_S)
        self.thread = threading.Thread(target=poll, name='gateway-monitor-source', daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=3)


class DataService:
    def __init__(self, db_path: Path, policy_path: Path | None = None):
        self.db_path = Path(db_path)
        self.policy_path = Path(policy_path) if policy_path else None
        self.policy = {}
        self._analytics_cache = {}
        self._gateway_analytics_cache = {}
        self._gateway_balance_reader = None
        self._gateway_activity_reader = None
        self._gateway_monitor = None
        self._overview_cache = {}
        self._overview_lock = threading.Lock()
        self._live_series_cache = {}
        self._live_series_lock = threading.Lock()
        self._queue_tail_catalog = {}
        self._queue_tail_catalog_rows = []
        self._queue_tail_catalog_at = 0.0
        self._queue_tail_catalog_lock = threading.Lock()
        self._sidecar_last_sync_monotonic = 0.0
        self._sidecar_status = {"available": False, "reason": "not_synced"}
        self._curated_queues = set()
        try:
            scope = json.loads(DEMO_SCOPE_FILE.read_text(encoding="utf-8"))
            self._curated_queues = {
                (str(q["instancia"]), str(q["codigo"]))
                for q in scope.get("queues", [])
            }
        except (OSError, ValueError, KeyError, TypeError):
            pass
        if self.policy_path and self.policy_path.exists():
            try:
                self.policy = json.loads(self.policy_path.read_text(encoding="utf-8"))
            except Exception:
                self.policy = {}

    @staticmethod
    def _cache_get(store, key, ttl_s):
        item = store.get(key)
        if not item:
            return None
        created, value = item
        if time.monotonic() - created > ttl_s:
            store.pop(key, None)
            return None
        return value

    @staticmethod
    def _cache_put(store, key, value):
        store[key] = (time.monotonic(), value)
        if len(store) > ANALYTICS_CACHE_MAX_ENTRIES:
            oldest = sorted(store.items(), key=lambda kv: kv[1][0])[: max(1, len(store)-ANALYTICS_CACHE_MAX_ENTRIES)]
            for old_key, _ in oldest:
                store.pop(old_key, None)
        return value

    def instance_policy(self, instancia):
        policy=dict(((self.policy or {}).get("instancias") or {}).get(str(instancia), {}))
        infra=dict((self.policy or {}).get('infra_defaults') or {})
        infra.update(policy.get('infra') or {})
        if infra:policy['infra']=infra
        return policy

    def queue_policy(self, instancia, codigo):
        configured = (self.instance_policy(instancia).get("queues") or {}).get(str(codigo))
        if configured is not None:
            return configured
        if (str(instancia), str(codigo)) not in self._curated_queues:
            # O coletor agenda filas do catálogo a cada 300 s. Aplicar o
            # limite de 35 s das filas prioritárias gera STALE falso.
            return {"target_s": 300, "max_age_s": 900}
        return {"target_s": 10, "max_age_s": 35}

    def connect(self):
        conn = sqlite3.connect(
            f"file:{self.db_path.as_posix()}?mode=ro",
            uri=True,
            timeout=3,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=3000")
        return conn

    def table_exists(self, conn, name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()
        return bool(row)

    @staticmethod
    def _current_tail_floor(conn, table, tail_rows=CURRENT_LOOKUP_TAIL_ROWS):
        allowed = {
            "queue_snapshot", "infra_snapshot", "stuck_channels_snapshot",
            "dialer_snapshot", "lastcalls_snapshot", "collector_event",
            "ml_shadow_prediction",
        }
        if table not in allowed:
            raise ValueError(f"Tabela nao autorizada para tail lookup: {table}")
        hi = conn.execute(
            f"SELECT rowid FROM {table} ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        if not hi:
            return None
        return max(1, int(hi["rowid"]) - int(tail_rows) + 1)

    @classmethod
    def _current_tail_latest(cls, conn, table, where_sql="", params=(), columns="*"):
        """Lookup atual limitado ao tail global; nunca confirma ausencia por full scan."""
        floor = cls._current_tail_floor(conn, table)
        if floor is None:
            return None
        suffix = f" AND ({where_sql})" if where_sql else ""
        return conn.execute(
            f"SELECT {columns} FROM {table} WHERE rowid>=?{suffix} "
            "ORDER BY rowid DESC LIMIT 1",
            (floor, *tuple(params)),
        ).fetchone()

    def _refresh_queue_tail_catalog(self, force=False):
        """Mantem um catalogo bounded das filas presentes no tail do LIVE.

        A ausencia de uma chave neste tail e no sidecar derivado e uma resposta
        negativa valida. Nunca confirmamos ausencia com
        ``WHERE instancia/codigo ORDER BY rowid DESC`` porque, sem indice, esse
        plano percorre o arquivo inteiro para filas homologadas sem amostra.
        """
        with self._queue_tail_catalog_lock:
            if (not force and self._queue_tail_catalog_rows and
                    time.monotonic() - self._queue_tail_catalog_at < 2.0):
                return list(self._queue_tail_catalog_rows)
            conn = self.connect()
            try:
                rows = conn.execute(
                    """SELECT rowid,ts,instancia,codigo,nome,projeto_id,motivo
                       FROM queue_snapshot ORDER BY rowid DESC LIMIT ?""",
                    (QUEUE_DISCOVERY_TAIL_ROWS,),
                ).fetchall()
            finally:
                conn.close()
            catalog = {}
            materialized = [dict(r) for r in rows]
            for d in materialized:
                key = (str(d.get("instancia") or "").strip(),
                       str(d.get("codigo") or "").strip())
                if key[0] and key[1] and key not in catalog:
                    catalog[key] = {"rowid": int(d["rowid"]), "ts": d.get("ts")}
            self._queue_tail_catalog = catalog
            self._queue_tail_catalog_rows = materialized
            self._queue_tail_catalog_at = time.monotonic()
            return list(materialized)

    def _queue_anchor_row_fast(self, instancia, codigo):
        """Resolve a ultima amostra sem scan filtrado reverso no LIVE.

        Primeiro consulta o tail bounded em memoria. Para filas inativas usa o
        sidecar leve, indexado por instancia/codigo/ts, e valida o source_rowid
        por lookup exato na B-tree nativa do LIVE. Se ambos nao conhecem a fila,
        retorna None imediatamente.
        """
        key = (str(instancia), str(codigo))
        self._refresh_queue_tail_catalog()
        item = self._queue_tail_catalog.get(key)
        if item:
            return dict(item)

        if ANALYTICS_SIDECAR_DB.exists():
            try:
                uri = f"file:{ANALYTICS_SIDECAR_DB.as_posix()}?mode=ro"
                con = sqlite3.connect(uri, uri=True, timeout=1)
                try:
                    con.row_factory = sqlite3.Row
                    con.execute("PRAGMA query_only=ON")
                    con.execute("PRAGMA busy_timeout=500")
                    side = con.execute(
                        """SELECT source_rowid AS rowid,ts FROM queue_light
                           WHERE instancia=? AND codigo=?
                           ORDER BY ts DESC LIMIT 1""",
                        key,
                    ).fetchone()
                finally:
                    con.close()
                if side:
                    live = self.connect()
                    try:
                        exact = live.execute(
                            """SELECT rowid,ts FROM queue_snapshot
                               WHERE rowid=? AND instancia=? AND codigo=?""",
                            (int(side["rowid"]), *key),
                        ).fetchone()
                    finally:
                        live.close()
                    if exact:
                        return {"rowid": int(exact["rowid"]), "ts": exact["ts"]}
            except sqlite3.Error:
                pass
        return None

    def queues(self):
        """Descobre as filas sem varrer todo o histórico de queue_snapshot.

        O LIVE DB pode ter vários GiB e queue_snapshot é append-only sem índice
        dedicado para GROUP BY de catálogo. A versão anterior agrupava a tabela
        inteira, o que fazia /api/queues degradar conforme o histórico crescia.

        R1.2.4 H1 usa somente o tail recente por ROWID (sem raw_queue) e mescla
        o escopo homologado em toi_demo_scope_v1.json para não perder filas que
        estejam temporariamente sem amostra recente. Tudo permanece READ ONLY.
        """
        recent = {}
        try:
            rows = self._refresh_queue_tail_catalog(force=True)
            for row in rows:
                d = dict(row)
                inst = str(d.get("instancia") or "").strip()
                code = str(d.get("codigo") or "").strip()
                if not inst or not code:
                    continue
                key = (inst, code)
                cur = recent.get(key)
                if cur is None:
                    recent[key] = {
                        "instancia": inst,
                        "codigo": code,
                        "nome": str(d.get("nome") or "").strip(),
                        "projeto_id": str(d.get("projeto_id") or "").strip(),
                        "motivo": d.get("motivo"),
                        "last_ts": d.get("ts"),
                        "samples": 1,
                    }
                else:
                    cur["samples"] = int(cur.get("samples") or 0) + 1
        except sqlite3.Error:
            recent = {}

        # O arquivo é somente referência de escopo; nenhuma aquisição é alterada.
        try:
            scope = json.loads(DEMO_SCOPE_FILE.read_text(encoding="utf-8")) if DEMO_SCOPE_FILE.exists() else {}
            scope_queues = scope.get("queues") if isinstance(scope, dict) else None
            if isinstance(scope_queues, list):
                for q in scope_queues:
                    if not isinstance(q, dict):
                        continue
                    inst = str(q.get("instancia") or "").strip()
                    code = str(q.get("codigo") or "").strip()
                    if not inst or not code:
                        continue
                    key = (inst, code)
                    cur = recent.get(key)
                    if cur is None:
                        recent[key] = {
                            "instancia": inst,
                            "codigo": code,
                            "nome": str(q.get("nome") or "").strip(),
                            "projeto_id": str(q.get("projeto_id") or "").strip(),
                            "motivo": q.get("motivo_prioridade"),
                            "last_ts": None,
                            "samples": 0,
                        }
                    else:
                        # Escopo homologado corrige apenas metadados ausentes;
                        # freshness continua vindo exclusivamente do LIVE DB.
                        if not cur.get("nome"):
                            cur["nome"] = str(q.get("nome") or "").strip()
                        if not cur.get("projeto_id"):
                            cur["projeto_id"] = str(q.get("projeto_id") or "").strip()
                        if cur.get("motivo") in (None, ""):
                            cur["motivo"] = q.get("motivo_prioridade")
        except Exception:
            pass

        collected_keys = set(recent)
        try:
            catalog = json.loads((APP_DIR / "portal_queue_catalog.json").read_text(encoding="utf-8"))
            for q in catalog.get("queues", []):
                if not isinstance(q, dict):
                    continue
                inst, code = str(q.get("instancia") or ""), str(q.get("codigo") or "")
                if not inst or not code:
                    continue
                key = (inst, code)
                if key not in recent:
                    recent[key] = {"instancia": inst, "codigo": code,
                                   "nome": str(q.get("nome") or ""),
                                   "projeto_id": str(q.get("projeto_id") or ""),
                                   "motivo": "portal_catalog", "last_ts": None, "samples": 0}
        except (OSError, ValueError, TypeError):
            pass
        out = list(recent.values())
        for q in out:
            q["collected"] = (q["instancia"], q["codigo"]) in collected_keys
        out.sort(key=lambda x: (int(x.get("instancia") or 999), str(x.get("nome") or ""), str(x.get("codigo") or "")))
        return out

    @staticmethod
    def portal_queue_status(instancia, codigo):
        """Leitura pontual do monitor existente; não afirma coleta detalhada no LIVE DB."""
        source = APP_DIR.parent / "dados" / "status_portal_orbe.json"
        doc = json.loads(source.read_text(encoding="utf-8"))
        rows = doc.get("filas") or {}
        match = next((row for row in rows.values()
                      if isinstance(row, dict) and str(row.get("instancia")) == str(instancia)
                      and str(row.get("codigo")) == str(codigo)), None)
        if match is None:
            return {"available": False, "reason": "Fila sem estado no monitor do Portal Orbe"}
        stamp = str(match.get("ultima_amostra") or "")
        try:
            age = max(0, (datetime.now() - datetime.fromisoformat(stamp)).total_seconds())
        except (ValueError, TypeError):
            age = None
        return {"available": age is not None and age <= 180,
                "sample_at": stamp, "age_seconds": round(age, 1) if age is not None else None,
                "fila": str(match.get("fila") or ""), "codigo": str(codigo),
                "instancia": str(instancia),
                "p34": match.get("status34_percentual"),
                "p135": match.get("status135_percentual"),
                "active": bool(match.get("incidente_confirmado")),
                "source": "Portal Orbe · monitor operacional"}

    def latest_queue(self, instancia, codigo):
        anchor = self._queue_anchor_row_fast(instancia, codigo)
        if not anchor:
            return None
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT * FROM queue_snapshot WHERE rowid=? AND instancia=? AND codigo=?",
                (int(anchor["rowid"]), str(instancia), str(codigo)),
            ).fetchone()
            return normalize_queue_raw(row) if row else None
        finally:
            conn.close()

    def timeseries(self, instancia, codigo, minutes):
        anchor_row = self._queue_anchor_row_fast(instancia, codigo)
        if not anchor_row:
            return []
        conn = self.connect()
        try:
            # O LIVE e append-only. Localiza a janela pela B-tree nativa de
            # ROWID e aplica instancia/codigo somente dentro dessa faixa curta.
            anchor = datetime.fromisoformat(anchor_row["ts"])
            start = anchor - timedelta(minutes=minutes)
            bounds = self._queue_rowid_bounds(conn, start, anchor)
            if not bounds:
                return []
            lo, hi = bounds
            rows = conn.execute("""
                SELECT ts,p34_pct,p135_pct,hit_rate5,hit_rate15,
                       calls_dialing,calls_in_attendance,buffer,dialing_rate,
                       agents_available,agents_talking,agents_acw,
                       agents_paused,agents_other,agents_dialing,
                       latency_queue,latency_stats,raw_queue
                FROM queue_snapshot
                WHERE rowid>=? AND rowid<=?
                  AND instancia=? AND codigo=? AND ts>=? AND ts<=?
                ORDER BY ts
            """, (lo, hi, str(instancia), str(codigo), start.isoformat(), anchor.isoformat())).fetchall()
            return [normalize_queue_raw(r) for r in rows]
        finally:
            conn.close()

    def live_timeseries(self, instancia, codigo, minutes=60):
        """Bootstrap bounded uma vez; depois consome somente ROWIDs novos."""
        key=(str(instancia),str(codigo))
        with self._live_series_lock:
            state=self._live_series_cache.get(key)
            if state is None:
                rows=self.timeseries(instancia,codigo,minutes)
                conn = self.connect()
                try:
                    tail=conn.execute("SELECT rowid FROM queue_snapshot ORDER BY rowid DESC LIMIT 1").fetchone()
                finally:
                    conn.close()
                # Depois do bootstrap, o cursor sempre avanca ate o tail global.
                # Assim uma fila ausente nao revisita linhas de outras filas.
                state={"last_rowid":int(tail["rowid"]) if tail else 0,"rows":rows}
                self._live_series_cache[key]=state
            else:
                conn = self.connect()
                try:
                    global_tail=conn.execute("SELECT rowid FROM queue_snapshot ORDER BY rowid DESC LIMIT 1").fetchone()
                    global_rowid=int(global_tail["rowid"]) if global_tail else 0
                    if int(state["last_rowid"])>global_rowid:
                        rows=self.timeseries(instancia,codigo,minutes)
                        state={"last_rowid":global_rowid,"rows":rows}
                        self._live_series_cache[key]=state
                    else:
                        fresh=conn.execute("""SELECT rowid,ts,p34_pct,p135_pct,hit_rate5,hit_rate15,
                               calls_dialing,calls_in_attendance,buffer,dialing_rate,
                               agents_available,agents_talking,agents_acw,agents_paused,agents_other,agents_dialing,
                               latency_queue,latency_stats,raw_queue
                            FROM queue_snapshot WHERE rowid>? AND instancia=? AND codigo=? ORDER BY rowid""",
                            (int(state["last_rowid"]),*key)).fetchall()
                        for raw in fresh:
                            d=normalize_queue_raw(raw);rid=int(d.pop("rowid"));state["last_rowid"]=max(int(state["last_rowid"]),rid)
                            state["rows"].append(d)
                        # Mesmo sem linha da fila selecionada, avanca ate o tail global:
                        # evita revisitar as linhas das outras filas no proximo poll.
                        state["last_rowid"]=max(int(state["last_rowid"]),global_rowid)
                finally:
                    conn.close()
            rows=state["rows"]
            if rows:
                end=datetime.fromisoformat(str(rows[-1]["ts"]));cutoff=end-timedelta(minutes=minutes)
                state["rows"]=[r for r in rows if datetime.fromisoformat(str(r["ts"]))>=cutoff]
            return list(state["rows"]),int(state["last_rowid"])

    def infra(self, instancia, minutes):
        """
        Fotografia ATUAL da infraestrutura da Instância.

        Regra v1.10.6.3.7:
        - usa o raw_json do snapshot MAIS RECENTE de cada source;
        - reconstrói gatewayList/trunkGrouplist como fotografia coerente;
        - inclusão no Portal aparece automaticamente após a próxima coleta;
        - remoção no Portal também desaparece automaticamente;
        - não faz união histórica de entidades antigas.

        A aquisição permanece intocada: este método apenas lê infra_snapshot.
        """
        conn = self.connect()
        try:
            if not self.table_exists(conn, "infra_snapshot"):
                return {"gateways": [], "trunks": [], "meta": {}}

            def latest_source(source):
                rawrow = self._current_tail_latest(
                    conn, "infra_snapshot", "instancia=? AND source=?",
                    (str(instancia), str(source)),
                )

                if not rawrow:
                    return [], None

                ts = rawrow["ts"]
                latency = rawrow["latency"]
                rows_out = []

                try:
                    doc = json.loads(rawrow["raw_json"] or "{}")
                except Exception:
                    doc = {}

                if source == "gateway":
                    arr = (
                        doc.get("gatewayList")
                        or doc.get("gatewayDataList")
                        or doc.get("GatewayDataList")
                        or []
                    )
                    if isinstance(arr, list):
                        for x in arr:
                            entity = str(x.get("NmGateway") or "").strip()
                            if not entity:
                                continue
                            rows_out.append({
                                "ts": ts,
                                "instancia": str(instancia),
                                "source": "gateway",
                                "entity": entity,
                                "total": parse_portal_number(x.get("QtValidsTotal"), None),
                                "busy": parse_portal_number(x.get("QtValidsBusy"), None),
                                "idle": parse_portal_number(x.get("QtValidsIdle"), None),
                                "occupancy": parse_portal_number(x.get("VlPercValidsBusy"), None),
                                "invalid": parse_portal_number(x.get("QtInvalidsTotal"), None),
                                "latency": latency,
                            })
                else:
                    arr = (
                        doc.get("trunkGrouplist")
                        or doc.get("trunkGroupList")
                        or doc.get("trunkDataList")
                        or doc.get("TrunkDataList")
                        or []
                    )
                    if isinstance(arr, list):
                        for x in arr:
                            entity = str(
                                x.get("DeNmTrunkGroup")
                                or x.get("NmTrunkGroup")
                                or ""
                            ).strip()
                            if not entity:
                                continue
                            rows_out.append({
                                "ts": ts,
                                "instancia": str(instancia),
                                "source": "trunk",
                                "entity": entity,
                                "total": parse_portal_number(x.get("QtValidsTotal"), None),
                                "busy": parse_portal_number(x.get("QtValidsBusy"), None),
                                "idle": parse_portal_number(x.get("QtValidsIdle"), None),
                                "occupancy": parse_portal_number(x.get("VlPercValidsBusy"), None),
                                "invalid": parse_portal_number(x.get("QtInvalidsTotal"), None),
                                "latency": latency,
                            })

                # Fallback somente para o MESMO timestamp.
                # Nunca reutiliza uma entidade de fotografia histórica anterior.
                if not rows_out:
                    floor = self._current_tail_floor(conn, "infra_snapshot")
                    rows = conn.execute("""
                        SELECT *
                        FROM infra_snapshot
                        WHERE rowid>=? AND instancia=? AND source=? AND ts=? AND entity!='__RAW__'
                        ORDER BY occupancy DESC, entity
                    """, (floor, str(instancia), str(source), ts)).fetchall()
                    rows_out = [dict(r) for r in rows]

                rows_out.sort(
                    key=lambda x: (
                        -float(x.get("occupancy") or 0),
                        str(x.get("entity") or "").upper()
                    )
                )
                return rows_out, ts

            gateways, gateway_ts = latest_source("gateway")
            trunks, trunk_ts = latest_source("trunk")

            return {
                "gateways": gateways,
                "trunks": trunks,
                "meta": {
                    "gateway_ts": gateway_ts,
                    "trunk_ts": trunk_ts,
                    "gateway_count": len(gateways),
                    "trunk_count": len(trunks),
                    "inventory_mode": "LATEST_COHERENT_RAW_SNAPSHOT",
                    "lookup_mode": "ROWID_BOUNDED_TAIL",
                },
            }
        finally:
            conn.close()

    def stuck(self, instancia):
        conn = self.connect()
        try:
            if not self.table_exists(conn, "stuck_channels_snapshot"):
                return None
            row = self._current_tail_latest(
                conn, "stuck_channels_snapshot", "instancia=?", (str(instancia),)
            )
            return dict(row) if row else None
        finally:
            conn.close()

    def dialer(self, instancia, codigo):
        conn = self.connect()
        try:
            if not self.table_exists(conn, "dialer_snapshot"):
                return None
            row = self._current_tail_latest(
                conn, "dialer_snapshot", "instancia=? AND codigo=?",
                (str(instancia), str(codigo)),
            )
            if not row:
                return None
            d = dict(row)
            try:
                d["raw"] = json.loads(d.pop("raw_json"))
            except Exception:
                d["raw"] = None
            return d
        finally:
            conn.close()

    def lastcalls(self, instancia, codigo, limit=100):
        conn = self.connect()
        try:
            if not self.table_exists(conn, "lastcalls_snapshot"):
                return None
            row = self._current_tail_latest(
                conn, "lastcalls_snapshot", "instancia=? AND codigo=?",
                (str(instancia), str(codigo)),
            )
            if not row:
                return None
            d = dict(row)
            try:
                raw = json.loads(d.pop("raw_json"))
            except Exception:
                raw = None
            d["raw"] = raw
            return d
        finally:
            conn.close()

    def source_health(self, instancia, minutes=15, start_dt=None, end_dt=None):
        with closing(self.connect()) as conn:
            if not self.table_exists(conn, "collector_event"):
                return {}
            # collector_event nao possui garantia de indice temporal. Ler um
            # tail limitado por ROWID evita varrer o LIVE inteiro a cada poll.
            event_rows=[]
            if start_dt is not None and end_dt is not None:
                ebounds=self._table_rowid_bounds(conn,"collector_event",start_dt,end_dt,safety_rows=64)
                if ebounds:
                    elo,ehi=ebounds
                    event_rows=conn.execute("""
                        SELECT ts,level,entity FROM collector_event
                        WHERE rowid>=? AND rowid<=? AND ts>=? AND ts<=?
                        ORDER BY rowid
                    """,(elo,ehi,start_dt.isoformat(),end_dt.isoformat())).fetchall()
            else:
                event_rows=conn.execute("""
                    SELECT ts,level,entity FROM collector_event
                    ORDER BY rowid DESC LIMIT 5000
                """).fetchall()
            if end_dt is None:
                for item in event_rows:
                    try:
                        end_dt = datetime.fromisoformat(str(item["ts"])); break
                    except Exception:
                        continue
            if end_dt is not None and start_dt is None:
                start_dt = end_dt - timedelta(minutes=minutes)
            start = start_dt.isoformat() if start_dt is not None else None
            errors = sum(
                1 for item in event_rows
                if (start is None or str(item["ts"] or "") >= start)
                and str(item["level"] or "").upper() == "ERROR"
                and (str(item["entity"] or "") == f"inst{instancia}"
                     or str(item["entity"] or "").startswith(f"inst{instancia}:"))
            )

            latencies=[]
            if start_dt is not None and end_dt is not None:
                bounds=self._queue_rowid_bounds(conn,start_dt,end_dt)
                if bounds:
                    lo,hi=bounds
                    qrows=conn.execute("""
                        SELECT latency_queue,latency_stats FROM queue_snapshot
                        WHERE rowid>=? AND rowid<=? AND instancia=? AND ts>=? AND ts<=?
                    """,(lo,hi,str(instancia),start_dt.isoformat(),end_dt.isoformat())).fetchall()
                    for q in qrows:
                        try:latencies.append(float(q["latency_queue"] or 0)+float(q["latency_stats"] or 0))
                        except Exception:pass

            return {
                "errors_window": errors,
                "avg_latency": round(sum(latencies)/len(latencies), 2) if latencies else 0,
                "max_latency": round(max(latencies), 2) if latencies else 0,
                "samples": len(latencies),
            }

    @staticmethod
    def parse_dialer_summary(dialer):
        """Normaliza apenas campos realmente expostos por getdialerstatisticsdata.

        Não inventa equivalências: campos que não existem no endpoint permanecem
        ausentes e a UI os mostra como "—". O resumo agrega as campanhas da fila,
        preservando também o timestamp/cache de origem para auditoria.
        """
        out = {
            "campaigns": 0,
            "calls5": 0,
            "calls15": 0,
            "buffer": 0,
            "mailing_total": 0,
            "mailing_available": 0,
            "mailing_free": 0,
            "available_next_hour": 0,
            "finalized_total": 0,
            "finalized_crm": 0,
            "finalized_attempts": 0,
            "finalized_reject": 0,
            "blocked": 0,
            "blocked_filter": 0,
            "blocked_schedule": 0,
            "blocked_ddd": 0,
            "blocked_sequence": 0,
            "blocked_telephone_score": 0,
            "blocked_telephone_line": 0,
            "blocked_internal_status": 0,
            "blocked_queue_state": 0,
            "blocked_queue_state_30m": 0,
            "blocked_attempts_daily": 0,
            "hit15_values": [],
            "data_state": "SEM_DADOS",
            "source_ts": (dialer or {}).get("ts") if isinstance(dialer, dict) else None,
            "source_cache": None,
        }
        if not dialer or not isinstance(dialer.get("raw"), dict):
            return out

        raw = dialer["raw"]
        out["source_cache"] = raw.get("dtCacheAndServerDialerStatisticsData")
        arr = raw.get("dialerStatisticsList")
        if arr is None:
            # Compatibilidade defensiva com respostas futuras sem escolher uma lista
            # arbitrária se a chave oficial estiver presente com outro tipo.
            arr = next(
                (v for v in raw.values()
                 if isinstance(v, list) and (not v or isinstance(v[0], dict))),
                [],
            )
        if not isinstance(arr, list) or not arr:
            known_lists = [k for k, v in raw.items() if isinstance(v, list)]
            out["data_state"] = "SEM_CAMPANHAS" if known_lists else "SEM_DADOS"
            return out

        out["data_state"] = "OK"
        out["campaigns"] = len(arr)

        numeric_map = {
            "calls5": "QtCallsLast5",
            "calls15": "QtCallsLast15",
            "buffer": "QtBuffer",
            "mailing_total": "QtMailingsTotal",
            "mailing_available": "QtMailingsAvailable",
            "mailing_free": "QtMailingsFree",
            "available_next_hour": "QtAvailableWithinNextHour",
            "finalized_total": "QtFinalizedTotal",
            "finalized_crm": "QtFinalizedByCrm",
            "finalized_attempts": "QtFinalizeByAttempts",
            "finalized_reject": "QtFinalizedByReject",
            "blocked": "QtBlockedTotal",
            "blocked_filter": "QtBlockedByFilter",
            "blocked_schedule": "QtBlockedBySchedule",
            "blocked_ddd": "QtBlockedByDDD",
            "blocked_sequence": "QtBlockedBySequence",
            "blocked_telephone_score": "QtBlockedByTelephoneScore",
            "blocked_telephone_line": "QtBlockedByTelephoneLine",
            "blocked_internal_status": "QtBlockedByInternalStatus",
            "blocked_queue_state": "QtBlockedByQueueState",
            "blocked_queue_state_30m": "QtBlockedByQueueState30m",
            "blocked_attempts_daily": "QtBlockedByAttemptsDaily",
        }

        for row in arr:
            if not isinstance(row, dict):
                continue
            for out_key, portal_key in numeric_map.items():
                out[out_key] += int(parse_portal_number(row.get(portal_key), 0))
            hr = parse_portal_number(row.get("VlHitRateLast15"), 0)
            if hr is not None:
                out["hit15_values"].append(float(hr))

        out["hit15_avg"] = round(
            sum(out["hit15_values"]) / len(out["hit15_values"]), 2
        ) if out["hit15_values"] else 0.0
        return out

    @staticmethod
    def diagnose(latest, series, infra, stuck, dialer_summary, source_health, quality_gate=None):
        if not latest:
            return {}

        qg = quality_gate or {}
        qs = (qg.get("sources") or {})
        queue_ok = (qs.get("queue") or {}).get("usable", True)
        infra_ok = (qs.get("infra") or {}).get("usable", True)
        stuck_ok = (qs.get("stuck_channels") or {}).get("usable", True)
        dialer_ok = (qs.get("dialer") or {}).get("usable", True)

        p135 = float(latest.get("p135_pct") or 0) if queue_ok else 0.0
        p34 = float(latest.get("p34_pct") or 0) if queue_ok else 0.0
        hr15 = float(latest.get("hit_rate15") or 0) if queue_ok else 0.0
        rate = float(latest.get("dialing_rate") or 0) if queue_ok else 0.0
        agents_avail = int(latest.get("agents_available") or 0) if queue_ok else 0
        stuck_qty = ((stuck or {}).get("quantity") or 0) if stuck_ok else None

        trunk_vals = [float(x.get("occupancy")) for x in infra.get("trunks", []) if x.get("occupancy") is not None] if infra_ok else []
        gateway_vals = [float(x.get("occupancy")) for x in infra.get("gateways", []) if x.get("occupancy") is not None] if infra_ok else []
        max_trunk = max(trunk_vals) if trunk_vals else None
        max_gw = max(gateway_vals) if gateway_vals else None

        p135_vals = [float(x.get("p135_pct") or 0) for x in series]
        p135_avg = round(sum(p135_vals)/len(p135_vals),2) if p135_vals else p135

        factors = []
        scores = {"discagem": 0, "capacidade": 0, "infra": 0, "agentes": 0}

        if p135 >= 20:
            factors.append({"impact": -12, "text": "P135 elevado no momento"})
        elif p135_avg >= 15:
            factors.append({"impact": -8, "text": "P135 médio elevado na janela"})

        if hr15 and hr15 < 2:
            scores["discagem"] += 3
            factors.append({"impact": -8, "text": "Hit Rate 15' baixo"})
        elif hr15 and hr15 < 3:
            scores["discagem"] += 2
            factors.append({"impact": -5, "text": "Hit Rate 15' abaixo de 3%"})

        if rate >= 5:
            scores["discagem"] += 2
            factors.append({"impact": -6, "text": "Taxa de discagem elevada"})

        if dialer_ok and dialer_summary.get("calls15", 0) > 10000:
            scores["discagem"] += 2
            factors.append({"impact": -5, "text": "Alto volume de tentativas do discador"})

        if max_trunk is not None and max_trunk >= 90:
            scores["capacidade"] += 4
            factors.append({"impact": -10, "text": f"Grupo de troncos em {max_trunk:.1f}%"})
        elif max_trunk is not None and max_trunk >= 80:
            scores["capacidade"] += 2
            factors.append({"impact": -6, "text": f"Pressão de troncos ({max_trunk:.1f}%)"})
        elif max_trunk is not None and max_trunk < 70 and max_trunk > 0:
            factors.append({"impact": +5, "text": "Grupos de troncos com folga"})

        if max_gw is not None and max_gw >= 90:
            scores["infra"] += 3
            factors.append({"impact": -8, "text": f"Gateway em alta ocupação ({max_gw:.1f}%)"})
        elif max_gw is not None and max_gw < 75 and max_gw > 0:
            factors.append({"impact": +4, "text": "Gateways com folga operacional"})

        if agents_avail == 0:
            scores["agentes"] += 2
            factors.append({"impact": -5, "text": "Nenhum agente disponível"})
        elif agents_avail >= 3:
            factors.append({"impact": +4, "text": "Há agentes disponíveis"})

        if stuck_qty is not None and stuck_qty >= 20:
            scores["infra"] += 2
            factors.append({"impact": -5, "text": f"{stuck_qty} canais travados na instância"})
        elif stuck_qty is not None:
            factors.append({"impact": +3, "text": "Canais travados sem sinal crítico"})

        quality_notes=[]
        labels_q={"queue":"Fila","infra":"Infraestrutura","stuck_channels":"IPBX/Canais Travados","dialer":"Discador"}
        for src in ("queue","infra","stuck_channels","dialer"):
            info=qs.get(src) or {}
            if info.get("status") in ("STALE","SEM_DADOS","PAUSADO"):
                txt=f"{labels_q[src]} não considerado — fonte {info.get('status','indisponível')}"
                factors.append({"impact":0,"text":txt,"kind":"quality"})
                quality_notes.append(txt)
            elif info.get("status")=="ATRASO":
                txt=f"{labels_q[src]} em atraso — confiança reduzida"
                factors.append({"impact":0,"text":txt,"kind":"quality"})
                quality_notes.append(txt)

        if source_health.get("errors_window", 0) >= 3 or source_health.get("avg_latency", 0) >= 5:
            factors.append({"impact": -4, "text": "Fonte/instância com degradação de latência"})

        # hipótese principal
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top, score = ranked[0]
        labels = {
            "discagem": "Pressão operacional / estratégia de discagem",
            "capacidade": "Pressão de capacidade / headroom reduzido",
            "infra": "Possível degradação de infraestrutura",
            "agentes": "Pressão de disponibilidade operacional",
        }

        if p135 < 10 and p34 < 10:
            title = "Operação sem congestionamento crítico no momento"
            severity = "NORMAL"
            confidence = 82
        elif score == 0:
            title = "Congestionamento detectado sem causa dominante"
            severity = "ATENÇÃO"
            confidence = 55
        else:
            title = labels[top]
            severity = "CRÍTICO" if max(p135, p34) >= 20 else "ATENÇÃO"
            confidence = min(92, 62 + score * 6)

        technical_confidence = confidence
        quality_score = int(qg.get("quality_score", 100))
        confidence = max(0, min(99, round(technical_confidence * quality_score / 100.0)))

        health = 100
        health -= min(35, p135 * 0.8)
        health -= min(20, p34 * 0.5)
        health -= 10 if hr15 and hr15 < 2 else 0
        health -= 10 if max_trunk is not None and max_trunk >= 90 else (5 if max_trunk is not None and max_trunk >= 80 else 0)
        health -= 5 if source_health.get("errors_window", 0) >= 3 else 0
        health = max(0, min(100, round(health)))

        return {
            "title": title,
            "severity": severity,
            "confidence": confidence,
            "technical_confidence": technical_confidence,
            "quality_score": quality_score,
            "quality_notes": quality_notes,
            "health_score": health,
            "p135_avg": p135_avg,
            "max_trunk": round(max_trunk, 2) if max_trunk is not None else None,
            "max_gateway": round(max_gw, 2) if max_gw is not None else None,
            "factors": sorted(factors, key=lambda x: x["impact"]),
            "scores": scores,
        }

    def queue_infra_context(self, instancia, codigo, infra, start_dt=None, end_dt=None):
        """Relaciona fila -> grupos/gateways pela evidência de Last Calls da janela.

        H2-C2 P2: a etapa de mapeamento NÃO espera a sincronização global do sidecar.
        Para janelas longas, lê ``lastcalls_light`` diretamente em mode=ro e completa
        somente a cauda ainda ausente pelo SQLite LIVE, com intervalo temporal/ROWID
        bounded. Se o sidecar estiver indisponível, usa o mesmo fallback bounded.

        Isto separa a latência do request da manutenção assíncrona de queue + Last Calls
        + infraestrutura, preservando o LIVE estritamente READ ONLY e sem descartar
        evidência histórica já existente.
        """
        usage={"trunks":{},"gateways":{},"sample_ts":None,"calls":0,"snapshots_total":0,"snapshots_used":0,"sampling_stride":1,"mapping_complete":True,
               "window_start":start_dt.isoformat() if start_dt else None,"window_end":end_dt.isoformat() if end_dt else None,
               "source":"LIVE_RECENT"}

        def _live_compact_rows(live_start, live_end, max_snapshots=2400):
            """Converte apenas a faixa solicitada de lastcalls_snapshot em linhas compactas.

            A consulta permanece READ ONLY e limitada por ROWID+tempo. Retorna também o
            total de snapshots elegíveis e o stride efetivamente usado.
            """
            conn=self.connect()
            try:
                if not self.table_exists(conn,"lastcalls_snapshot"):
                    return [],0,1
                bounds=self._table_rowid_bounds(conn,"lastcalls_snapshot",live_start,live_end,safety_rows=32)
                if not bounds:
                    return [],0,1
                lo,hi=bounds
                where="""rowid>=? AND rowid<=? AND instancia=? AND codigo=? AND ts>=? AND ts<=?
                         AND raw_json LIKE '%\"lastCallsList\": [{%'"""
                params=(lo,hi,str(instancia),str(codigo),live_start.isoformat(),live_end.isoformat())
                total=int(conn.execute(f"SELECT COUNT(*) FROM lastcalls_snapshot WHERE {where}",params).fetchone()[0] or 0)
                stride=max(1,int((total+max_snapshots-1)//max_snapshots))
                if total:
                    if stride==1:
                        rows=conn.execute(f"SELECT rowid,ts,raw_json FROM lastcalls_snapshot WHERE {where} ORDER BY rowid",params).fetchall()
                    else:
                        rows=conn.execute(f"SELECT rowid,ts,raw_json FROM lastcalls_snapshot WHERE {where} AND (rowid % ?)=0 ORDER BY rowid",(*params,stride)).fetchall()
                        last=conn.execute(f"SELECT rowid,ts,raw_json FROM lastcalls_snapshot WHERE {where} ORDER BY rowid DESC LIMIT 1",params).fetchone()
                        if last is not None and (not rows or rows[-1]["rowid"]!=last["rowid"]):
                            rows=list(rows)+[last]
                else:
                    rows=[]
            finally:
                conn.close()

            out=[]
            for row in rows:
                try: raw=json.loads(row["raw_json"] or "{}")
                except Exception: raw={}
                arr=raw.get("lastCallsList") or []
                trunks={};gws={}
                for x in arr if isinstance(arr,list) else []:
                    code=str(x.get("CodeTrunkGroup") or "").strip()
                    gw=str(x.get("NmGateway") or "").strip().upper()
                    if code: trunks[code]=trunks.get(code,0)+1
                    if gw: gws[gw]=gws.get(gw,0)+1
                out.append({"ts":row["ts"],"trunk_codes":json.dumps(trunks,separators=(',',':')),
                            "gateway_names":json.dumps(gws,separators=(',',':')),
                            "calls":len(arr) if isinstance(arr,list) else 0})
            return out,total,stride

        compact_rows=None
        compact_total=0
        compact_stride=1
        long_window=(start_dt is not None and end_dt is not None and
                     (end_dt-start_dt).total_seconds()/60.0>=ANALYTICS_SIDECAR_LONG_MINUTES)

        if long_window and ANALYTICS_SIDECAR_DB.exists():
            # P2: não chama _sync_analytics_sidecar() aqui. O refresh iniciado por
            # queue_range pode continuar em paralelo; esta leitura nunca aguarda o lock global.
            con=None
            try:
                uri=f"file:{ANALYTICS_SIDECAR_DB.as_posix()}?mode=ro"
                con=sqlite3.connect(uri,uri=True,timeout=0.25)
                con.row_factory=sqlite3.Row
                con.execute("PRAGMA query_only=ON")
                con.execute("PRAGMA busy_timeout=250")
                rr=con.execute("""SELECT ts,trunk_codes,gateway_names,calls FROM lastcalls_light
                      WHERE instancia=? AND codigo=? AND ts>=? AND ts<=? ORDER BY ts""",
                      (str(instancia),str(codigo),start_dt.isoformat(),end_dt.isoformat())).fetchall()
                compact_rows=[dict(x) for x in rr]
                compact_total=len(compact_rows)
                usage["source"]="SIDECAR_READ_ONLY"
            except Exception:
                compact_rows=None
            finally:
                if con is not None:
                    con.close()

            # Read-through somente da cauda que ainda não chegou ao sidecar. Isso mantém
            # o mapa atual sem serializar o request atrás do refresh global.
            if compact_rows is not None:
                tail_start=start_dt
                if compact_rows:
                    try: last_dt=datetime.fromisoformat(str(compact_rows[-1].get("ts")))
                    except Exception: last_dt=None
                    if last_dt is not None:
                        tail_start=max(start_dt,last_dt+timedelta(microseconds=1))
                if tail_start<=end_dt:
                    tail,tail_total,tail_stride=_live_compact_rows(tail_start,end_dt)
                    if tail:
                        compact_rows=list(compact_rows)+tail
                        usage["source"]="SIDECAR_PLUS_LIVE_TAIL"
                    compact_total+=tail_total
                    compact_stride=max(compact_stride,tail_stride)

        if compact_rows is None:
            if start_dt is not None and end_dt is not None:
                compact_rows,compact_total,compact_stride=_live_compact_rows(start_dt,end_dt)
                usage["source"]="LIVE_BOUNDED_FALLBACK"
            else:
                # Caminho curto/atual: mantém a semântica anterior e lê poucas amostras.
                conn=self.connect()
                try:
                    if not self.table_exists(conn,"lastcalls_snapshot"):
                        rows=[]
                    else:
                        rows=conn.execute("""SELECT rowid,ts,raw_json FROM lastcalls_snapshot WHERE instancia=? AND codigo=?
                               AND raw_json LIKE '%\"lastCallsList\": [{%' ORDER BY rowid DESC LIMIT 20""",(str(instancia),str(codigo))).fetchall()
                finally:
                    conn.close()
                compact_rows=[]
                for row in rows:
                    try: raw=json.loads(row["raw_json"] or "{}")
                    except Exception: raw={}
                    arr=raw.get("lastCallsList") or []
                    trunks={};gws={}
                    for x in arr if isinstance(arr,list) else []:
                        code=str(x.get("CodeTrunkGroup") or "").strip();gw=str(x.get("NmGateway") or "").strip().upper()
                        if code: trunks[code]=trunks.get(code,0)+1
                        if gw: gws[gw]=gws.get(gw,0)+1
                    compact_rows.append({"ts":row["ts"],"trunk_codes":json.dumps(trunks,separators=(',',':')),
                                         "gateway_names":json.dumps(gws,separators=(',',':')),
                                         "calls":len(arr) if isinstance(arr,list) else 0})
                compact_total=len(compact_rows);compact_stride=1;usage["source"]="LIVE_RECENT"

        usage["snapshots_total"]=int(compact_total or len(compact_rows or []))
        # O sidecar já é compacto; somente o fallback LIVE pode vir amostrado por stride.
        max_snapshots=2400
        if compact_rows is not None and len(compact_rows)>max_snapshots:
            stride=max(1,(len(compact_rows)+max_snapshots-1)//max_snapshots)
            rows_used=compact_rows[::stride]
            if compact_rows and rows_used and rows_used[-1].get("ts")!=compact_rows[-1].get("ts"):
                rows_used=list(rows_used)+[compact_rows[-1]]
            usage["sampling_stride"]=max(compact_stride,stride)
            usage["mapping_complete"]=False
        else:
            rows_used=compact_rows or []
            usage["sampling_stride"]=max(1,compact_stride)
            usage["mapping_complete"]=usage["sampling_stride"]==1
        usage["snapshots_used"]=len(rows_used)

        for row in rows_used:
            if usage["sample_ts"] is None or str(row.get("ts"))>str(usage["sample_ts"]):
                usage["sample_ts"]=row.get("ts")
            usage["calls"]+=int(row.get("calls") or 0)
            try: trunks=json.loads(row.get("trunk_codes") or "{}")
            except Exception: trunks={}
            try: gws=json.loads(row.get("gateway_names") or "{}")
            except Exception: gws={}
            for code,n in (trunks.items() if isinstance(trunks,dict) else []):
                item=usage["trunks"].setdefault(str(code),{"code":str(code),"name":"","calls":0});item["calls"]+=int(n or 0)
            for gw,n in (gws.items() if isinstance(gws,dict) else []):
                usage["gateways"][str(gw).upper()]=usage["gateways"].get(str(gw).upper(),0)+int(n or 0)

        codes=set(usage["trunks"].keys());gateway_names=set(usage["gateways"].keys())
        def trunk_code(entity):
            mm=re.search(r"\((\d+)\)\s*$",str(entity or ""));return mm.group(1) if mm else None
        all_trunks=list((infra or {}).get("trunks",[]) or []);all_gateways=list((infra or {}).get("gateways",[]) or [])
        relevant_trunks=[x for x in all_trunks if trunk_code(x.get("entity")) in codes] if codes else []
        relevant_gateways=[x for x in all_gateways if str(x.get("entity") or "").strip().upper() in gateway_names] if gateway_names else []
        inferred=bool(relevant_trunks or relevant_gateways)
        return {"usage":usage,"inferred_from_lastcalls":inferred,"trunks":relevant_trunks if relevant_trunks else all_trunks,
                "gateways":relevant_gateways if relevant_gateways else all_gateways,"instance_trunks":all_trunks,"instance_gateways":all_gateways}

    def infra_history(self, instancia, codigo, minutes, infra_context):
        """Histórico de infraestrutura com envelope e séries individuais por entidade."""
        conn = self.connect()
        try:
            anchor_row = self._current_tail_latest(
                conn, "infra_snapshot", "instancia=?", (str(instancia),), "rowid,ts"
            )
            if not anchor_row:
                return {"trunk": [], "gateway": [], "trunk_entities": [], "gateway_entities": [],
                        "trunk_all_entities": [], "gateway_all_entities": []}
            anchor = datetime.fromisoformat(anchor_row["ts"])
            start_dt = anchor - timedelta(minutes=minutes)
            start = start_dt.isoformat()
            bounds=self._table_rowid_bounds(conn,"infra_snapshot",start_dt,anchor,safety_rows=256)
            if not bounds:
                return {"trunk": [], "gateway": [], "trunk_entities": [], "gateway_entities": [],
                        "trunk_all_entities": [], "gateway_all_entities": []}
            lo,hi=bounds

            codes = set((infra_context.get("usage") or {}).get("trunks", {}).keys())
            gw_names = {
                str(x).strip().upper()
                for x in (infra_context.get("usage") or {}).get("gateways", {}).keys()
                if str(x).strip()
            }

            trunk_rows = conn.execute("""
                SELECT ts,raw_json FROM infra_snapshot
                WHERE rowid>=? AND rowid<=? AND instancia=? AND source='trunk' AND ts>=? AND ts<=?
                ORDER BY ts
            """, (lo,hi,str(instancia),start,anchor.isoformat())).fetchall()

            trunk_all_entities = []
            seen_ts = set()
            for row in trunk_rows:
                ts = row["ts"]
                if ts in seen_ts:
                    continue
                seen_ts.add(ts)
                try:
                    doc = json.loads(row["raw_json"] or "{}")
                except Exception:
                    continue
                arr = doc.get("trunkGrouplist") or doc.get("trunkGroupList") or doc.get("trunkDataList") or doc.get("TrunkDataList") or []
                for x in arr if isinstance(arr, list) else []:
                    ent = str(x.get("DeNmTrunkGroup") or x.get("NmTrunkGroup") or "").strip()
                    if not ent:
                        continue
                    occ = parse_portal_number(x.get("VlPercValidsBusy"), None)
                    if occ is None:
                        continue
                    trunk_all_entities.append({"ts": ts, "entity": ent, "occupancy": round(occ, 3)})

            def trunk_code(entity):
                m = re.search(r"\((\d+)\)\s*$", str(entity or ""))
                return m.group(1) if m else None

            trunk_entities = [
                r for r in trunk_all_entities
                if not codes or trunk_code(r.get("entity")) in codes
            ]

            gw_rows = conn.execute("""
                SELECT ts,entity,occupancy FROM infra_snapshot
                WHERE rowid>=? AND rowid<=? AND instancia=? AND source='gateway'
                  AND ts>=? AND ts<=? AND entity!='__RAW__'
                ORDER BY ts
            """, (lo,hi,str(instancia),start,anchor.isoformat())).fetchall()
            gateway_all_entities = [
                {"ts": r["ts"], "entity": str(r["entity"] or "").strip().upper(), "occupancy": round(float(r["occupancy"]), 3)}
                for r in gw_rows if r["occupancy"] is not None and str(r["entity"] or "").strip()
            ]
            gateway_entities = [
                r for r in gateway_all_entities
                if not gw_names or str(r.get("entity") or "").upper() in gw_names
            ]
        finally:
            conn.close()

        def envelope(rows):
            grouped = {}
            for r in rows:
                if r.get("occupancy") is not None:
                    grouped.setdefault(r.get("ts"), []).append(float(r["occupancy"]))
            return [
                {"ts": ts, "occupancy": round(max(vals), 3)}
                for ts, vals in sorted(grouped.items()) if vals
            ]

        return {
            "trunk": envelope(trunk_entities),
            "gateway": envelope(gateway_entities),
            "trunk_entities": trunk_entities,
            "gateway_entities": gateway_entities,
            "trunk_all_entities": trunk_all_entities,
            "gateway_all_entities": gateway_all_entities,
        }

    @staticmethod
    def _metric_stats(values):
        vals = [float(v) for v in values if v is not None]
        if not vals:
            return {
                "current": None, "mean": None, "min": None,
                "max": None, "p95": None, "delta": None,
            }
        ordered = sorted(vals)
        idx95 = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * 0.95)))
        return {
            "current": round(vals[-1], 3),
            "mean": round(sum(vals) / len(vals), 3),
            "min": round(min(vals), 3),
            "max": round(max(vals), 3),
            "p95": round(ordered[idx95], 3),
            "delta": round(vals[-1] - vals[0], 3) if len(vals) > 1 else 0.0,
        }

    @staticmethod
    def _coverage_metadata(rows, requested_minutes):
        if not rows:
            return {
                "requested_minutes": requested_minutes,
                "available_minutes": 0.0,
                "coverage_pct": 0.0,
                "complete": False,
                "samples": 0,
                "first_ts": None,
                "last_ts": None,
            }
        first_ts = rows[0].get("ts")
        last_ts = rows[-1].get("ts")
        try:
            available = max(
                0.0,
                (datetime.fromisoformat(last_ts) - datetime.fromisoformat(first_ts)).total_seconds() / 60.0,
            )
        except Exception:
            available = 0.0
        # Uma janela pode ter a primeira amostra alguns segundos depois do limite.
        complete = available >= max(0.0, requested_minutes - 1.0)
        return {
            "requested_minutes": requested_minutes,
            "available_minutes": round(available, 2),
            "coverage_pct": round(min(100.0, (available / requested_minutes * 100.0) if requested_minutes else 100.0), 2),
            "complete": complete,
            "samples": len(rows),
            "first_ts": first_ts,
            "last_ts": last_ts,
        }

    def period_summary(self, instancia, codigo, minutes, series=None):
        """Resumo EXCLUSIVO da janela escolhida pelo usuário."""
        rows = series if series is not None else self.timeseries(instancia, codigo, minutes)
        coverage = self._coverage_metadata(rows, minutes)
        if rows:
            try:
                end_dt = datetime.fromisoformat(str(rows[-1]["ts"]))
                start_dt = end_dt - timedelta(minutes=minutes)
                times = [datetime.fromisoformat(str(row["ts"])) for row in rows]
                gap_coverage = self._coverage_from_times(times, start_dt, end_dt)
                data_pct = float(gap_coverage["data_coverage_pct"])
                coverage.update({
                    "available_minutes": round(minutes * data_pct / 100.0, 2),
                    "coverage_pct": data_pct,
                    "data_coverage_pct": data_pct,
                    "span_coverage_pct": gap_coverage["span_coverage_pct"],
                    "gap_count": gap_coverage["gap_count"],
                    "largest_gap_min": gap_coverage["largest_gap_min"],
                    "median_cadence_s": gap_coverage["median_cadence_s"],
                    "complete": data_pct >= 95.0 and gap_coverage["gap_count"] == 0,
                })
            except (KeyError, TypeError, ValueError):
                coverage["complete"] = False

        keys = (
            "p34_pct", "p135_pct", "hit_rate5", "hit_rate15",
            "calls_dialing", "calls_in_attendance", "buffer", "dialing_rate",
            "agents_available", "agents_talking", "agents_acw",
            "agents_paused", "agents_other", "agents_dialing",
        )
        out = {"coverage": coverage}
        for key in keys:
            out[key] = self._metric_stats([r.get(key) for r in rows])

        p135 = [float(r.get("p135_pct") or 0) for r in rows]
        p34 = [float(r.get("p34_pct") or 0) for r in rows]
        out["minutes_p135_over_20_est"] = round(
            coverage["available_minutes"] * sum(1 for v in p135 if v >= 20) / max(len(p135), 1), 2
        )
        out["minutes_p34_over_20_est"] = round(
            coverage["available_minutes"] * sum(1 for v in p34 if v >= 20) / max(len(p34), 1), 2
        )
        return out

    def stuck_history(self, instancia, minutes):
        """Histórico real de canais travados na janela selecionada."""
        conn = self.connect()
        try:
            if not self.table_exists(conn, "stuck_channels_snapshot"):
                return []
            anchor_row = self._current_tail_latest(
                conn, "stuck_channels_snapshot", "instancia=?",
                (str(instancia),), "rowid,ts"
            )
            if not anchor_row:
                return []
            anchor = datetime.fromisoformat(anchor_row["ts"])
            start_dt = anchor - timedelta(minutes=minutes)
            bounds=self._table_rowid_bounds(conn,"stuck_channels_snapshot",start_dt,anchor,safety_rows=64)
            if not bounds:return []
            lo,hi=bounds
            rows = conn.execute("""
                SELECT ts,quantity,status
                FROM stuck_channels_snapshot
                WHERE rowid>=? AND rowid<=? AND instancia=? AND ts>=? AND ts<=?
                ORDER BY ts
            """, (lo,hi,str(instancia),start_dt.isoformat(),anchor.isoformat())).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def stuck_period_summary(self, instancia, minutes, rows=None):
        rows = rows if rows is not None else self.stuck_history(instancia, minutes)
        coverage = self._coverage_metadata(rows, minutes)
        stats = self._metric_stats([r.get("quantity") for r in rows])
        return {
            **stats,
            "coverage": coverage,
        }


    # ======================================================================
    # v1.10.6.3.7.5 - TEMPORAL ANALYTICS / MANAGERIAL DRILLDOWNS
    # Somente leitura. Não altera aquisição, SQLite, diagnóstico ou alertas.
    # ======================================================================

    @staticmethod
    def _parse_iso_local(value, anchor=None):
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        # datetime-local chega sem timezone; herda o timezone da âncora do banco.
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None and anchor is not None and anchor.tzinfo is not None:
            dt = dt.replace(tzinfo=anchor.tzinfo)
        return dt

    def _queue_anchor(self, instancia, codigo):
        """Âncora sem scan reverso para combinações instancia/fila ausentes."""
        row = self._queue_anchor_row_fast(instancia, codigo)
        return datetime.fromisoformat(row["ts"]) if row and row["ts"] else None

    @staticmethod
    def _queue_rowid_lower_bound(conn, target_dt):
        """Localiza por busca binária o primeiro ROWID cujo ts global é >= target_dt.

        queue_snapshot é append-only no runtime homologado; ROWID acompanha a ordem de coleta.
        O método usa a B-tree nativa de ROWID e nunca faz GROUP BY/scan completo para localizar a janela.
        """
        lo_row = conn.execute("SELECT rowid,ts FROM queue_snapshot ORDER BY rowid ASC LIMIT 1").fetchone()
        hi_row = conn.execute("SELECT rowid,ts FROM queue_snapshot ORDER BY rowid DESC LIMIT 1").fetchone()
        if not lo_row or not hi_row:
            return None
        lo, hi = int(lo_row["rowid"]), int(hi_row["rowid"]) + 1
        target = target_dt
        for _ in range(64):
            if lo >= hi:
                break
            mid = (lo + hi) // 2
            probe = conn.execute(
                "SELECT rowid,ts FROM queue_snapshot WHERE rowid>=? ORDER BY rowid LIMIT 1",
                (mid,),
            ).fetchone()
            if probe is None:
                hi = mid
                continue
            rid = int(probe["rowid"])
            try:
                ts = datetime.fromisoformat(probe["ts"])
            except Exception:
                lo = rid + 1
                continue
            if ts < target:
                lo = rid + 1
            else:
                hi = mid
        row = conn.execute(
            "SELECT rowid FROM queue_snapshot WHERE rowid>=? ORDER BY rowid LIMIT 1",
            (lo,),
        ).fetchone()
        return int(row["rowid"]) if row else int(hi_row["rowid"]) + 1

    @classmethod
    def _queue_rowid_bounds(cls, conn, start_dt, end_dt):
        first = cls._queue_rowid_lower_bound(conn, start_dt)
        after = cls._queue_rowid_lower_bound(conn, end_dt + timedelta(microseconds=1))
        if first is None or after is None:
            return None
        lo = max(1, int(first) - QUEUE_ROWID_RANGE_SAFETY_ROWS)
        hi = max(lo, int(after) - 1 + QUEUE_ROWID_RANGE_SAFETY_ROWS)
        max_row = conn.execute("SELECT rowid FROM queue_snapshot ORDER BY rowid DESC LIMIT 1").fetchone()
        if max_row:
            hi = min(hi, int(max_row["rowid"]))
        return lo, hi

    @staticmethod
    def _table_rowid_lower_bound(conn, table, target_dt):
        """Busca binária por ROWID em tabelas append-only do LIVE sem criar índice."""
        allowed={"queue_snapshot","lastcalls_snapshot","infra_snapshot","stuck_channels_snapshot","collector_event"}
        if table not in allowed:
            raise ValueError(f"Tabela não autorizada para busca ROWID: {table}")
        lo_row=conn.execute(f"SELECT rowid,ts FROM {table} ORDER BY rowid ASC LIMIT 1").fetchone()
        hi_row=conn.execute(f"SELECT rowid,ts FROM {table} ORDER BY rowid DESC LIMIT 1").fetchone()
        if not lo_row or not hi_row:return None
        lo,hi=int(lo_row["rowid"]),int(hi_row["rowid"])+1
        for _ in range(64):
            if lo>=hi:break
            mid=(lo+hi)//2
            probe=conn.execute(f"SELECT rowid,ts FROM {table} WHERE rowid>=? ORDER BY rowid LIMIT 1",(mid,)).fetchone()
            if probe is None:hi=mid;continue
            rid=int(probe["rowid"])
            try:ts=datetime.fromisoformat(str(probe["ts"]))
            except Exception:lo=rid+1;continue
            if ts<target_dt:lo=rid+1
            else:hi=mid
        row=conn.execute(f"SELECT rowid FROM {table} WHERE rowid>=? ORDER BY rowid LIMIT 1",(lo,)).fetchone()
        return int(row["rowid"]) if row else int(hi_row["rowid"])+1

    @classmethod
    def _table_rowid_bounds(cls, conn, table, start_dt, end_dt, safety_rows=64):
        first=cls._table_rowid_lower_bound(conn,table,start_dt)
        after=cls._table_rowid_lower_bound(conn,table,end_dt+timedelta(microseconds=1))
        if first is None or after is None:return None
        lo=max(1,int(first)-int(safety_rows));hi=max(lo,int(after)-1+int(safety_rows))
        mx=conn.execute(f"SELECT rowid FROM {table} ORDER BY rowid DESC LIMIT 1").fetchone()
        if mx:hi=min(hi,int(mx["rowid"]))
        return lo,hi

    def _sidecar_connect(self):
        # H2-C2: o modo diagnóstico abre o sidecar estritamente READ ONLY.
        # Em runtime normal, mantém o comportamento derivado/WAL do H1.
        if ANALYTICS_DIAGNOSTIC_READONLY:
            con=sqlite3.connect(f"file:{ANALYTICS_SIDECAR_DB.as_posix()}?mode=ro",uri=True,timeout=8)
            con.row_factory=sqlite3.Row
            con.execute("PRAGMA query_only=ON")
            con.execute("PRAGMA busy_timeout=8000")
            return con
        ANALYTICS_SIDECAR_DB.parent.mkdir(parents=True,exist_ok=True)
        con=sqlite3.connect(ANALYTICS_SIDECAR_DB,timeout=8)
        con.row_factory=sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA busy_timeout=8000")
        return con

    @staticmethod
    def _sidecar_meta_get(con,key,default=None):
        row=con.execute("SELECT value FROM meta WHERE key=?",(str(key),)).fetchone()
        return row["value"] if row else default

    @staticmethod
    def _sidecar_meta_set(con,key,value):
        con.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(key),str(value)))

    def _sidecar_init(self,con):
        con.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE IF NOT EXISTS queue_light(
          source_rowid INTEGER PRIMARY KEY,ts TEXT NOT NULL,instancia TEXT NOT NULL,codigo TEXT NOT NULL,
          p34_pct REAL,p135_pct REAL,hit_rate5 REAL,hit_rate15 REAL,calls_dialing REAL,calls_in_attendance REAL,calls_last5 REAL,
          buffer REAL,dialing_rate REAL,agents_available REAL,agents_talking REAL,agents_acw REAL,agents_paused REAL,agents_other REAL,agents_dialing REAL
        );
        CREATE INDEX IF NOT EXISTS ix_queue_light_inst_code_ts ON queue_light(instancia,codigo,ts);
        CREATE TABLE IF NOT EXISTS lastcalls_light(
          source_rowid INTEGER PRIMARY KEY,ts TEXT NOT NULL,instancia TEXT NOT NULL,codigo TEXT NOT NULL,
          trunk_codes TEXT NOT NULL,gateway_names TEXT NOT NULL,calls INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS ix_lastcalls_light_inst_code_ts ON lastcalls_light(instancia,codigo,ts);
        CREATE TABLE IF NOT EXISTS infra_light(
          source_rowid INTEGER NOT NULL,ts TEXT NOT NULL,instancia TEXT NOT NULL,source TEXT NOT NULL,entity TEXT NOT NULL,
          total REAL,busy REAL,idle REAL,occupancy REAL,invalid REAL,latency REAL,
          PRIMARY KEY(instancia,source,ts,entity)
        );
        CREATE INDEX IF NOT EXISTS ix_infra_light_inst_source_ts ON infra_light(instancia,source,ts);
        CREATE INDEX IF NOT EXISTS ix_infra_light_inst_source_entity_ts ON infra_light(instancia,source,entity,ts);
        CREATE TABLE IF NOT EXISTS stuck_light(
          source_rowid INTEGER PRIMARY KEY,ts TEXT NOT NULL,instancia TEXT NOT NULL,quantity REAL,status TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_stuck_light_inst_ts ON stuck_light(instancia,ts);
        """)
        self._sidecar_meta_set(con,"schema","2")

    def _sidecar_reset_if_source_restarted(self,src,dst,table,sidecar_table,meta_key):
        mx=src.execute(f"SELECT rowid FROM {table} ORDER BY rowid DESC LIMIT 1").fetchone()
        latest=int(mx["rowid"]) if mx else 0
        last=int(self._sidecar_meta_get(dst,meta_key,"0") or 0)
        if last>latest:
            dst.execute(f"DELETE FROM {sidecar_table}")
            last=0;self._sidecar_meta_set(dst,meta_key,"0")
        return last,latest

    def _sidecar_initial_rowid(self,src,table,anchor_dt):
        cutoff=anchor_dt-timedelta(days=ANALYTICS_SIDECAR_RETENTION_DAYS)
        first=self._table_rowid_lower_bound(src,table,cutoff)
        return max(0,int(first or 1)-1)

    def _sync_analytics_sidecar(self,force=False):
        """Sincroniza cache leve derivado. LIVE é aberto apenas mode=ro.

        O custo grande ocorre uma única vez no prewarm/primeira criação. Depois a sincronização
        lê somente ROWIDs novos. O sidecar contém apenas campos analíticos normalizados, nunca
        substitui o Ground Truth nem é fonte de aquisição.
        """
        now_mono=time.monotonic()
        if ANALYTICS_DIAGNOSTIC_READONLY:
            available=ANALYTICS_SIDECAR_DB.exists()
            status={"available":available,"reason":"DIAGNOSTIC_READ_ONLY" if available else "SIDECAR_MISSING","path":str(ANALYTICS_SIDECAR_DB),"elapsed_s":0.0,"queue_rows":0,"lastcalls_rows":0,"infra_rows":0}
            self._sidecar_status=status;self._sidecar_last_sync_monotonic=now_mono
            return status
        if not force and self._sidecar_status.get("available") and now_mono-self._sidecar_last_sync_monotonic<ANALYTICS_SIDECAR_SYNC_THROTTLE_S:
            return self._sidecar_status
        with _ANALYTICS_SIDECAR_LOCK:
            now_mono=time.monotonic()
            if not force and self._sidecar_status.get("available") and now_mono-self._sidecar_last_sync_monotonic<ANALYTICS_SIDECAR_SYNC_THROTTLE_S:
                return self._sidecar_status
            t0=time.perf_counter();stats={"queue_rows":0,"lastcalls_rows":0,"infra_rows":0,"stuck_rows":0}
            try:
                with self.connect() as src, self._sidecar_connect() as dst:
                    self._sidecar_init(dst)
                    anchor_row=src.execute("SELECT rowid,ts FROM queue_snapshot ORDER BY rowid DESC LIMIT 1").fetchone()
                    if not anchor_row:
                        raise RuntimeError("queue_snapshot sem dados para sincronizar sidecar")
                    anchor_dt=datetime.fromisoformat(str(anchor_row["ts"]))

                    # queue_snapshot -> queue_light
                    last,latest=self._sidecar_reset_if_source_restarted(src,dst,"queue_snapshot","queue_light","queue_last_rowid")
                    if last==0:last=self._sidecar_initial_rowid(src,"queue_snapshot",anchor_dt)
                    qsql="""SELECT rowid AS source_rowid,ts,instancia,codigo,p34_pct,p135_pct,hit_rate5,hit_rate15,
                                   calls_dialing,calls_in_attendance,calls_last5,buffer,dialing_rate,
                                   agents_available,agents_talking,agents_acw,agents_paused,agents_other,agents_dialing,
                                   json_extract(raw_queue,'$.PercHitRateLast5') AS _raw_hit_rate5,
                                   json_extract(raw_queue,'$.PercHitRateLast15') AS _raw_hit_rate15,
                                   json_extract(raw_queue,'$.QtBuffer') AS _raw_buffer
                            FROM queue_snapshot WHERE rowid>? ORDER BY rowid LIMIT ?"""
                    while last<latest:
                        rows=src.execute(qsql,(last,ANALYTICS_SIDECAR_CHUNK_ROWS)).fetchall()
                        if not rows:break
                        vals=[]
                        for rr in rows:
                            d=normalize_queue_raw(rr)
                            vals.append((int(rr["source_rowid"]),d.get("ts"),str(d.get("instancia")),str(d.get("codigo")),
                              d.get("p34_pct"),d.get("p135_pct"),d.get("hit_rate5"),d.get("hit_rate15"),d.get("calls_dialing"),d.get("calls_in_attendance"),d.get("calls_last5"),
                              d.get("buffer"),d.get("dialing_rate"),d.get("agents_available"),d.get("agents_talking"),d.get("agents_acw"),d.get("agents_paused"),d.get("agents_other"),d.get("agents_dialing")))
                        dst.executemany("""INSERT OR REPLACE INTO queue_light VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",vals)
                        last=int(rows[-1]["source_rowid"]);stats["queue_rows"]+=len(rows);self._sidecar_meta_set(dst,"queue_last_rowid",last);dst.commit()
                    cutoff=(anchor_dt-timedelta(days=ANALYTICS_SIDECAR_RETENTION_DAYS)).isoformat()
                    dst.execute("DELETE FROM queue_light WHERE ts<?",(cutoff,));dst.commit()

                    # lastcalls_snapshot -> mapa compacto por snapshot
                    if self.table_exists(src,"lastcalls_snapshot"):
                        last,latest=self._sidecar_reset_if_source_restarted(src,dst,"lastcalls_snapshot","lastcalls_light","lastcalls_last_rowid")
                        if last==0:last=self._sidecar_initial_rowid(src,"lastcalls_snapshot",anchor_dt)
                        while last<latest:
                            rows=src.execute("SELECT rowid AS source_rowid,ts,instancia,codigo,raw_json FROM lastcalls_snapshot WHERE rowid>? ORDER BY rowid LIMIT ?",(last,ANALYTICS_SIDECAR_CHUNK_ROWS)).fetchall()
                            if not rows:break
                            vals=[]
                            for rr in rows:
                                try:doc=json.loads(rr["raw_json"] or "{}")
                                except Exception:doc={}
                                arr=doc.get("lastCallsList") or []
                                trunks={};gws={}
                                for x in arr if isinstance(arr,list) else []:
                                    code=str(x.get("CodeTrunkGroup") or "").strip();gw=str(x.get("NmGateway") or "").strip().upper()
                                    if code:trunks[code]=trunks.get(code,0)+1
                                    if gw:gws[gw]=gws.get(gw,0)+1
                                vals.append((int(rr["source_rowid"]),rr["ts"],str(rr["instancia"]),str(rr["codigo"]),json.dumps(trunks,separators=(',',':')),json.dumps(gws,separators=(',',':')),len(arr) if isinstance(arr,list) else 0))
                            dst.executemany("INSERT OR REPLACE INTO lastcalls_light VALUES(?,?,?,?,?,?,?)",vals)
                            last=int(rows[-1]["source_rowid"]);stats["lastcalls_rows"]+=len(rows);self._sidecar_meta_set(dst,"lastcalls_last_rowid",last);dst.commit()
                        dst.execute("DELETE FROM lastcalls_light WHERE ts<?",(cutoff,));dst.commit()

                    # infra_snapshot -> entidades leves; evita reabrir raw_json em janelas longas.
                    if self.table_exists(src,"infra_snapshot"):
                        last,latest=self._sidecar_reset_if_source_restarted(src,dst,"infra_snapshot","infra_light","infra_last_rowid")
                        if last==0:last=self._sidecar_initial_rowid(src,"infra_snapshot",anchor_dt)
                        while last<latest:
                            rows=src.execute("SELECT rowid AS source_rowid,ts,instancia,source,entity,total,busy,idle,occupancy,invalid,latency,raw_json FROM infra_snapshot WHERE rowid>? ORDER BY rowid LIMIT ?",(last,ANALYTICS_SIDECAR_CHUNK_ROWS)).fetchall()
                            if not rows:break
                            vals=[]
                            for rr in rows:
                                srcname=str(rr["source"] or "");inst=str(rr["instancia"]);ts=rr["ts"];rid=int(rr["source_rowid"]);entity=str(rr["entity"] or "").strip()
                                if entity and entity!="__RAW__" and rr["occupancy"] is not None:
                                    vals.append((rid,ts,inst,srcname,entity.upper() if srcname=="gateway" else entity,rr["total"],rr["busy"],rr["idle"],rr["occupancy"],rr["invalid"],rr["latency"]))
                                raw=rr["raw_json"]
                                if raw and srcname in ("trunk","gateway"):
                                    try:doc=json.loads(raw or "{}")
                                    except Exception:doc={}
                                    if srcname=="trunk":arr=doc.get("trunkGrouplist") or doc.get("trunkGroupList") or doc.get("trunkDataList") or doc.get("TrunkDataList") or []
                                    else:arr=doc.get("gatewayList") or doc.get("gatewayDataList") or doc.get("GatewayDataList") or []
                                    for x in arr if isinstance(arr,list) else []:
                                        if srcname=="trunk":ent=str(x.get("DeNmTrunkGroup") or x.get("NmTrunkGroup") or "").strip()
                                        else:ent=str(x.get("NmGateway") or "").strip().upper()
                                        if not ent:continue
                                        occ=parse_portal_number(x.get("VlPercValidsBusy"),None)
                                        if occ is None:continue
                                        vals.append((rid,ts,inst,srcname,ent,parse_portal_number(x.get("QtValidsTotal"),None),parse_portal_number(x.get("QtValidsBusy"),None),parse_portal_number(x.get("QtValidsIdle"),None),occ,parse_portal_number(x.get("QtInvalidsTotal"),None),rr["latency"]))
                            if vals:dst.executemany("""INSERT OR REPLACE INTO infra_light(source_rowid,ts,instancia,source,entity,total,busy,idle,occupancy,invalid,latency) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",vals)
                            last=int(rows[-1]["source_rowid"]);stats["infra_rows"]+=len(rows);self._sidecar_meta_set(dst,"infra_last_rowid",last);dst.commit()
                        dst.execute("DELETE FROM infra_light WHERE ts<?",(cutoff,));dst.commit()

                    # stuck_channels_snapshot -> stuck_light. H2-C2 Closure performance fix:
                    # tira o histórico de canais travados do arquivo LIVE de vários GiB no cold path.
                    if self.table_exists(src,"stuck_channels_snapshot"):
                        last,latest=self._sidecar_reset_if_source_restarted(src,dst,"stuck_channels_snapshot","stuck_light","stuck_last_rowid")
                        if last==0:last=self._sidecar_initial_rowid(src,"stuck_channels_snapshot",anchor_dt)
                        while last<latest:
                            rows=src.execute("SELECT rowid AS source_rowid,ts,instancia,quantity,status FROM stuck_channels_snapshot WHERE rowid>? ORDER BY rowid LIMIT ?",(last,ANALYTICS_SIDECAR_CHUNK_ROWS)).fetchall()
                            if not rows:break
                            vals=[(int(rr["source_rowid"]),rr["ts"],str(rr["instancia"]),rr["quantity"],rr["status"]) for rr in rows]
                            if vals:dst.executemany("INSERT OR REPLACE INTO stuck_light(source_rowid,ts,instancia,quantity,status) VALUES(?,?,?,?,?)",vals)
                            last=int(rows[-1]["source_rowid"]);stats["stuck_rows"]+=len(rows);self._sidecar_meta_set(dst,"stuck_last_rowid",last);dst.commit()
                        dst.execute("DELETE FROM stuck_light WHERE ts<?",(cutoff,));dst.commit()

                    self._sidecar_meta_set(dst,"last_sync_ts",datetime.now().astimezone().isoformat(timespec="seconds"));dst.commit()
                status={"available":True,"path":str(ANALYTICS_SIDECAR_DB),"sync_s":round(time.perf_counter()-t0,3),"imported":stats,"mode":"PERSISTENT_DERIVED_CACHE_LIVE_READ_ONLY"}
            except Exception as exc:
                status={"available":False,"path":str(ANALYTICS_SIDECAR_DB),"sync_s":round(time.perf_counter()-t0,3),"imported":stats,"reason":f"{type(exc).__name__}: {exc}"}
            self._sidecar_status=status;self._sidecar_last_sync_monotonic=time.monotonic();return status

    def prewarm_analytics_sidecar(self):
        return self._sync_analytics_sidecar(force=True)

    def _analytics_bounds(self, instancia, codigo, period, start=None, end=None):
        period = str(period or "15m").strip().lower()
        anchor = self._queue_anchor(instancia, codigo)
        if anchor is None:
            raise ValueError("Fila sem amostras para definir âncora temporal.")

        if period == "custom":
            start_dt = self._parse_iso_local(start, anchor)
            end_dt = self._parse_iso_local(end, anchor)
            if start_dt is None or end_dt is None:
                raise ValueError("Período personalizado exige start e end em ISO local.")
            if end_dt > anchor:
                # Não inventa futuro; análise termina na última evidência disponível.
                end_dt = anchor
            if end_dt <= start_dt:
                raise ValueError("Período personalizado inválido: end deve ser maior que start.")
            minutes = (end_dt - start_dt).total_seconds() / 60.0
            if minutes > ANALYTICS_MAX_CUSTOM_MINUTES:
                raise ValueError("Período personalizado limitado a 31 dias nesta versão.")
            mode = "PROFILE_24H" if minutes >= 7 * 24 * 60 else "CHRONOLOGY"
            label = "Personalizado"
        else:
            minutes = PERIOD_MAP.get(period)
            if minutes is None:
                raise ValueError(f"Período não suportado: {period}")
            end_dt = anchor
            start_dt = end_dt - timedelta(minutes=minutes)
            mode = "PROFILE_24H" if period in ANALYTICS_PROFILE_PERIODS else "CHRONOLOGY"
            labels = {
                "5m":"5 min","15m":"15 min","30m":"30 min","1h":"60 min","60m":"60 min",
                "2h":"2H","6h":"6H","1d":"24H","24h":"24H","7d":"7 dias",
                "14d":"14 dias","30d":"30 dias",
            }
            label = labels.get(period, period)

        return {
            "period": period,
            "label": label,
            "mode": mode,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "requested_minutes": round(float(minutes), 3),
        }


    def _queue_range(self, instancia, codigo, start_dt, end_dt):
        """Lê a janela da fila sem bloquear o cold path na manutenção do sidecar.

        R1.2.4 H2-C2 P1 — Queue Range Cold Path Hardening
        --------------------------------------------------
        A causa observada no runtime não era a consulta indexada de ``queue_light``;
        era a sincronização COMPLETA do sidecar (queue + Last Calls + infra) executada
        de forma síncrona antes da leitura da fila. Isso fazia ``queue_range`` herdar
        custo que não pertencia à leitura da fila.

        Para janelas longas e sidecar já existente, o refresh é iniciado em paralelo,
        enquanto a fila é lida por conexão estritamente READ ONLY. Se o sidecar ainda
        não contém o tail até ``end_dt``, somente esse tail é completado pelo LIVE usando
        ROWID_BOUNDED_RANGE. A sincronização completa continua obrigatória para as etapas
        seguintes: ``queue_infra_context`` / ``_infra_range`` aguardam o mesmo lock caso o
        refresh ainda esteja em curso, preservando Last Calls e infraestrutura atuais.

        Nenhum intervalo ausente é convertido em zero e o SQLite LIVE permanece mode=ro.
        """
        requested_minutes=max(0.0,(end_dt-start_dt).total_seconds()/60.0)

        def _live_rows(lo_dt, hi_dt):
            if hi_dt < lo_dt:
                return []
            base = """SELECT ts,p34_pct,p135_pct,hit_rate5,hit_rate15,
                             calls_dialing,calls_in_attendance,calls_last5,
                             buffer,dialing_rate,
                             agents_available,agents_talking,agents_acw,
                             agents_paused,agents_other,agents_dialing"""
            conn = self.connect()
            try:
                bounds = self._queue_rowid_bounds(conn, lo_dt, hi_dt)
                if not bounds:
                    return []
                rowid_lo, rowid_hi = bounds
                params = (rowid_lo, rowid_hi, str(instancia), str(codigo), lo_dt.isoformat(), hi_dt.isoformat())
                where = """ FROM queue_snapshot
                            WHERE rowid>=? AND rowid<=?
                              AND instancia=? AND codigo=? AND ts>=? AND ts<=?
                            ORDER BY rowid"""
                try:
                    rows = conn.execute(base + """,
                             json_extract(raw_queue,'$.PercHitRateLast5') AS _raw_hit_rate5,
                             json_extract(raw_queue,'$.PercHitRateLast15') AS _raw_hit_rate15,
                             json_extract(raw_queue,'$.QtBuffer') AS _raw_buffer,
                             json_extract(raw_queue,'$.QtMailingOnScreen') AS _raw_mailing_on_screen,
                             json_extract(raw_queue,'$.QtGetBufferTelephone') AS _raw_buffer_config,
                             json_extract(raw_queue,'$.DtServer') AS _raw_source_server_time,
                             json_extract(raw_queue,'$.dtCacheAndServerQueueDetailData') AS _raw_source_cache_time,
                             json_extract(raw_queue,'$.VlAwtLast5') AS _raw_awt5,
                             json_extract(raw_queue,'$.VlAwtLast15') AS _raw_awt15,
                             json_extract(raw_queue,'$.VlMwt') AS _raw_mwt""" + where, params).fetchall()
                except sqlite3.OperationalError:
                    rows = conn.execute(base + ", raw_queue" + where, params).fetchall()
            finally:
                # sqlite3.Connection.__exit__ faz commit/rollback, mas não fecha o handle.
                # Em Windows isso mantinha live.sqlite bloqueado no teste temporário.
                conn.close()
            return [normalize_queue_raw(r) for r in rows]

        if requested_minutes>=ANALYTICS_SIDECAR_LONG_MINUTES and ANALYTICS_SIDECAR_DB.exists():
            # Não serializa o request atrás da manutenção global do sidecar. O refresh
            # completo roda em paralelo; etapas dependentes de Last Calls/infra continuam
            # sincronizadas pelo lock existente em _sync_analytics_sidecar().
            if not getattr(self, "_queue_range_refresh_inflight", False):
                self._queue_range_refresh_inflight = True
                def _refresh_sidecar_for_queue_range():
                    try:
                        # H2-C2 Closure: let the current request finish its sidecar read
                        # before derived refresh starts, reducing I/O contention.
                        time.sleep(0.25)
                        self._sync_analytics_sidecar(force=True)
                    finally:
                        self._queue_range_refresh_inflight = False
                threading.Thread(
                    target=_refresh_sidecar_for_queue_range,
                    name="toi-analytics-sidecar-refresh",
                    daemon=True,
                ).start()

            try:
                uri=f"file:{ANALYTICS_SIDECAR_DB.as_posix()}?mode=ro"
                con=sqlite3.connect(uri, uri=True, timeout=2)
                try:
                    con.row_factory=sqlite3.Row
                    con.execute("PRAGMA query_only=ON")
                    con.execute("PRAGMA busy_timeout=1500")
                    rows=con.execute("""SELECT ts,p34_pct,p135_pct,hit_rate5,hit_rate15,
                               calls_dialing,calls_in_attendance,calls_last5,buffer,dialing_rate,
                               agents_available,agents_talking,agents_acw,agents_paused,agents_other,agents_dialing
                        FROM queue_light WHERE instancia=? AND codigo=? AND ts>=? AND ts<=? ORDER BY ts""",
                        (str(instancia),str(codigo),start_dt.isoformat(),end_dt.isoformat())).fetchall()
                finally:
                    con.close()
                side_rows=[dict(r) for r in rows]

                # Read-through do tail: se a coleta avançou após o último ponto do sidecar,
                # complementa SOMENTE a cauda pelo LIVE bounded. Assim a fila permanece atual
                # sem esperar a importação completa de Last Calls/infra.
                tail_rows=[]
                if side_rows:
                    try:
                        last_dt=datetime.fromisoformat(str(side_rows[-1].get("ts")))
                    except Exception:
                        last_dt=None
                    if last_dt is not None and last_dt < end_dt:
                        tail_rows=_live_rows(max(start_dt,last_dt+timedelta(microseconds=1)),end_dt)
                else:
                    tail_rows=_live_rows(start_dt,end_dt)

                if not tail_rows:
                    return side_rows
                merged={str(r.get("ts")):r for r in side_rows if r.get("ts")}
                for r in tail_rows:
                    if r.get("ts"):
                        merged[str(r.get("ts"))]=r
                return [merged[k] for k in sorted(merged)]
            except Exception:
                # Sidecar ausente/ocupado/corrompido: preserva o fallback READ ONLY e bounded.
                pass

        return _live_rows(start_dt,end_dt)

    def _analytics_infra_snapshot(self, instancia, end_dt):
        """Fotografia de infraestrutura para analytics longos sem varrer o LIVE.

        H2-C2 Consistency Closure performance correction. A fotografia é lida do
        sidecar derivado, indexado por (instancia, source, ts), no último timestamp
        observado até o fim da janela analítica. O LIVE continua estritamente READ ONLY
        e permanece fallback apenas quando o cache derivado não possui evidência.
        """
        if ANALYTICS_SIDECAR_DB.exists():
            con=None
            try:
                uri=f"file:{ANALYTICS_SIDECAR_DB.as_posix()}?mode=ro"
                con=sqlite3.connect(uri,uri=True,timeout=0.5)
                con.row_factory=sqlite3.Row
                con.execute("PRAGMA query_only=ON")
                con.execute("PRAGMA busy_timeout=500")
                out={"gateways":[],"trunks":[],"meta":{"inventory_mode":"ANALYTICS_SIDECAR_LATEST_ASOF"}}
                for source,key in (("gateway","gateways"),("trunk","trunks")):
                    rr=con.execute("SELECT MAX(ts) AS ts FROM infra_light WHERE instancia=? AND source=? AND ts<=?",
                                   (str(instancia),source,end_dt.isoformat())).fetchone()
                    ts=rr["ts"] if rr else None
                    if not ts: continue
                    rows=con.execute("""SELECT ts,instancia,source,entity,total,busy,idle,occupancy,invalid,latency
                          FROM infra_light WHERE instancia=? AND source=? AND ts=? ORDER BY occupancy DESC,entity""",
                          (str(instancia),source,ts)).fetchall()
                    out[key]=[dict(r) for r in rows]
                    out["meta"][source+"_ts"]=ts
                    out["meta"][source+"_count"]=len(rows)
                if out["gateways"] or out["trunks"]:
                    return out
            except Exception:
                pass
            finally:
                if con is not None: con.close()
        return self.infra(instancia,15)

    def _stuck_range(self, instancia, start_dt, end_dt):
        """Histórico de canais travados; janelas longas usam sidecar derivado."""
        requested_minutes=max(0.0,(end_dt-start_dt).total_seconds()/60.0)
        if requested_minutes>=ANALYTICS_SIDECAR_LONG_MINUTES and ANALYTICS_SIDECAR_DB.exists():
            con=None
            try:
                uri=f"file:{ANALYTICS_SIDECAR_DB.as_posix()}?mode=ro"
                con=sqlite3.connect(uri,uri=True,timeout=0.5)
                con.row_factory=sqlite3.Row;con.execute("PRAGMA query_only=ON");con.execute("PRAGMA busy_timeout=500")
                rows=con.execute("""SELECT ts,quantity,status FROM stuck_light
                      WHERE instancia=? AND ts>=? AND ts<=? ORDER BY ts""",
                      (str(instancia),start_dt.isoformat(),end_dt.isoformat())).fetchall()
                return [dict(r) for r in rows]
            except Exception:
                pass
            finally:
                if con is not None:con.close()

        conn=self.connect()
        try:
            if not self.table_exists(conn, "stuck_channels_snapshot"):
                return []
            bounds=self._table_rowid_bounds(conn,"stuck_channels_snapshot",start_dt,end_dt,safety_rows=16)
            if not bounds:return []
            lo,hi=bounds
            rows = conn.execute(
                """SELECT ts,quantity,status
                   FROM stuck_channels_snapshot
                   WHERE rowid>=? AND rowid<=? AND instancia=? AND ts>=? AND ts<=?
                     AND quantity IS NOT NULL
                   ORDER BY rowid""",
                (lo,hi,str(instancia), start_dt.isoformat(), end_dt.isoformat()),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    @staticmethod
    def _capacity_profile_sidecar_fast(entity, rows, current_row, window_start_dt, window_end_dt):
        """Perfil H3 equivalente para uma única entidade já ordenada por ``ts``.

        H2-C2 Performance Closure P3. Esta variante existe apenas no cold path do sidecar:
        evita remontar ``dict`` por amostra e evita os agrupamentos genéricos de
        ``_capacity_group_profiles`` quando a consulta SQL já entrega uma única entidade em
        ordem cronológica. Mantém os mesmos campos, limiares, gaps, lifecycle de ocupação 0%
        e cálculo de cobertura.
        """
        def num(value):
            if value is None or value == "": return None
            try: n=float(value)
            except (TypeError,ValueError): return None
            if n != n or n in (float("inf"),float("-inf")): return None
            return n

        ent=str(entity or "").strip()
        cur=current_row or {}
        current=num(cur.get("occupancy")) if hasattr(cur,"get") else None
        samples=[]
        vals=[]
        peak=None; peak_ts=None; total=0.0
        for r in rows or []:
            try:
                raw_ts=r["ts"] if not hasattr(r,"get") else r.get("ts")
                raw_v=r["occupancy"] if not hasattr(r,"get") else r.get("occupancy")
                ts=datetime.fromisoformat(str(raw_ts or ""))
            except Exception:
                continue
            v=num(raw_v)
            samples.append((ts,v))
            if v is not None:
                vals.append(v); total+=v
                if peak is None or v>peak:
                    peak=v; peak_ts=ts.isoformat()
        if current is None and vals:
            current=vals[-1]
        if not samples:
            return {
                "entity":ent,"current_pct":round(current,2) if current is not None else None,
                "mean_pct":None,"p95_pct":None,"peak_pct":None,"peak_ts":None,"samples":0,
                "headroom_p95_pct":None,"minutes_over_75":0.0,"minutes_over_85":0.0,"minutes_over_90":0.0,
                "episodes_over_85":0,"episodes_over_90":0,"longest_episode_over_85_min":0.0,
                "active_samples_pct":None,"zero_samples_pct":None,
                "current_zero_since":None,"current_zero_duration_min":None,"current_zero_left_censored":False,"current_zero_samples":0,
                "last_zero_episode_since":None,"last_zero_episode_end":None,"last_zero_episode_duration_min":None,
                "last_zero_episode_left_censored":False,"last_zero_recovery_at":None,"last_zero_recovery_pct":None,
                "zero_episodes_completed":0,"trend_15m_vs_prev45m":"N/C","trend_delta_pct_points":None,
                "quality":{},"busy":cur.get("busy") if hasattr(cur,"get") else None,"total":cur.get("total") if hasattr(cur,"get") else None,
            }

        # SQL ORDER BY ts garante ordem. Ainda ignoramos deltas não positivos exatamente
        # como a implementação geral.
        deltas=[]
        for i in range(1,len(samples)):
            d=(samples[i][0]-samples[i-1][0]).total_seconds()
            if d>0: deltas.append(d)
        valid_deltas=[d for d in deltas if d<=600]
        cadence=float(statistics.median(valid_deltas)) if valid_deltas else 60.0
        cap_s=min(600.0,max(60.0,cadence*3.0))

        if vals:
            ordered_vals=sorted(vals)
            pidx=min(len(ordered_vals)-1,max(0,round((len(ordered_vals)-1)*0.95)))
            pv=ordered_vals[pidx]
            mean=total/len(vals)
        else:
            pv=mean=None

        mins75=mins85=mins90=0.0
        ep85_start=ep90_start=None
        ep85_count=ep90_count=0
        ep85_longest=ep90_longest=0.0
        recent_sum=prev_sum=0.0; recent_n=prev_n=0
        recent_start=(window_end_dt-timedelta(minutes=15)) if window_end_dt else None
        prev_start=(window_end_dt-timedelta(minutes=60)) if window_end_dt else None
        active_count=0
        zero_run_start=None
        zero_run_samples=0
        zero_completed=0
        last_zero_material=None

        def close85(end_dt):
            nonlocal ep85_start,ep85_count,ep85_longest
            if ep85_start is not None:
                ep85_count+=1
                ep85_longest=max(ep85_longest,max(0.0,(end_dt-ep85_start).total_seconds())/60.0)
                ep85_start=None
        def close90(end_dt):
            nonlocal ep90_start,ep90_count,ep90_longest
            if ep90_start is not None:
                ep90_count+=1
                ep90_longest=max(ep90_longest,max(0.0,(end_dt-ep90_start).total_seconds())/60.0)
                ep90_start=None

        prev_ts=None; prev_v=None
        for i,(ts,v) in enumerate(samples):
            gap=None
            if prev_ts is not None:
                gap=(ts-prev_ts).total_seconds()
                if gap>0 and prev_v is not None:
                    eff=min(gap,cap_s)
                    if prev_v>=75: mins75+=eff
                    if prev_v>=85: mins85+=eff
                    if prev_v>=90: mins90+=eff
                if gap>cap_s:
                    close85(prev_ts+timedelta(seconds=cadence)); close90(prev_ts+timedelta(seconds=cadence))
                    zero_run_start=None; zero_run_samples=0
                elif gap<=0:
                    zero_run_start=None; zero_run_samples=0

            a85=(v is not None and v>=85); a90=(v is not None and v>=90)
            if a85 and ep85_start is None: ep85_start=ts
            elif not a85 and ep85_start is not None: close85(ts)
            if a90 and ep90_start is None: ep90_start=ts
            elif not a90 and ep90_start is not None: close90(ts)

            if v is not None:
                if v>0: active_count+=1
                if window_end_dt:
                    if ts>=recent_start: recent_sum+=v; recent_n+=1
                    elif prev_start<=ts<recent_start: prev_sum+=v; prev_n+=1

                if v<=0:
                    if zero_run_start is None:
                        zero_run_start=(i,ts)
                        zero_run_samples=1
                    else:
                        zero_run_samples+=1
                elif zero_run_start is not None:
                    zi,zts=zero_run_start
                    duration_min=max(0.0,(ts-zts).total_seconds())/60.0
                    if duration_min>=1.0:
                        left=False
                        if zi==0 and window_start_dt is not None:
                            left=(zts-window_start_dt).total_seconds()<=cap_s
                        zero_completed+=1
                        last_zero_material=(zts,ts,duration_min,left,v)
                    zero_run_start=None; zero_run_samples=0
            prev_ts=ts; prev_v=v

        final_dt=samples[-1][0]+timedelta(seconds=cadence)
        close85(final_dt); close90(final_dt)

        active_samples_pct=(active_count*100.0/len(vals)) if vals else None
        zero_samples_pct=(100.0-active_samples_pct) if active_samples_pct is not None else None
        current_zero_since=None; current_zero_duration_min=None; current_zero_left_censored=False; current_zero_samples=0
        if current is not None and current<=0 and vals and samples[-1][1] is not None and samples[-1][1]<=0 and zero_run_start is not None:
            zi,zts=zero_run_start; zend=samples[-1][0]
            try:
                cur_ts=datetime.fromisoformat(str(cur.get("ts") or ""))
                if cur_ts>=zend and (cur_ts-zend).total_seconds()<=cap_s: zend=cur_ts
            except Exception:
                pass
            if window_end_dt and window_end_dt>=zend and (window_end_dt-zend).total_seconds()<=cap_s: zend=window_end_dt
            current_zero_since=zts.isoformat(); current_zero_duration_min=round(max(0.0,(zend-zts).total_seconds())/60.0,2)
            current_zero_samples=int(zero_run_samples)
            if zi==0 and window_start_dt is not None:
                current_zero_left_censored=(zts-window_start_dt).total_seconds()<=cap_s

        last_zero_episode_since=last_zero_episode_end=last_zero_recovery_at=None
        last_zero_episode_duration_min=last_zero_recovery_pct=None; last_zero_episode_left_censored=False
        if last_zero_material is not None:
            zst,zen,zdur,zleft,zpct=last_zero_material
            last_zero_episode_since=zst.isoformat(); last_zero_episode_end=zen.isoformat(); last_zero_episode_duration_min=round(zdur,2)
            last_zero_episode_left_censored=bool(zleft); last_zero_recovery_at=zen.isoformat(); last_zero_recovery_pct=round(float(zpct),2)

        # Coverage exatamente equivalente a _coverage_from_sorted_times, reaproveitando
        # os timestamps/deltas já calculados acima.
        q={}
        if window_start_dt and window_end_dt:
            total_s=max(1.0,(window_end_dt-window_start_dt).total_seconds())
            cov_cadence=float(statistics.median(valid_deltas)) if valid_deltas else None
            gap_threshold=min(600.0,max(60.0,(cov_cadence or 60.0)*3.0))
            gaps=[d for d in deltas if d>gap_threshold]
            covered_s=sum(min(d,gap_threshold) for d in deltas)+min(gap_threshold,cov_cadence or gap_threshold)
            span_s=max(0.0,(samples[-1][0]-samples[0][0]).total_seconds())
            span_pct=min(100.0,span_s/total_s*100.0); data_pct=min(100.0,covered_s/total_s*100.0)
            q={
                "requested_minutes":round(total_s/60.0,2),"span_minutes":round(span_s/60.0,2),
                "span_coverage_pct":round(span_pct,2),"data_coverage_pct":round(data_pct,2),"coverage_pct":round(data_pct,2),
                "complete":data_pct>=95.0,"samples":len(samples),"first_ts":samples[0][0].isoformat(),"last_ts":samples[-1][0].isoformat(),
                "median_cadence_s":round(cov_cadence,2) if cov_cadence is not None else None,
                "gap_threshold_s":round(gap_threshold,2),"gap_count":len(gaps),
                "largest_gap_min":round((max(gaps) if gaps else 0.0)/60.0,2),
                "days_observed":len({x[0].date() for x in samples}),
            }

        trend_state="N/C"; trend_delta=None
        if recent_n and prev_n:
            trend_delta=(recent_sum/recent_n)-(prev_sum/prev_n)
            trend_state="SUBINDO" if trend_delta>=5 else "ALIVIANDO" if trend_delta<=-5 else "ESTÁVEL"

        return {
            "entity":ent,"current_pct":round(current,2) if current is not None else None,
            "mean_pct":round(mean,2) if mean is not None else None,"p95_pct":round(pv,2) if pv is not None else None,
            "peak_pct":round(peak,2) if peak is not None else None,"peak_ts":peak_ts,"samples":len(vals),
            "headroom_p95_pct":round(max(0.0,100.0-pv),2) if pv is not None else None,
            "minutes_over_75":round(mins75/60.0,2),"minutes_over_85":round(mins85/60.0,2),"minutes_over_90":round(mins90/60.0,2),
            "episodes_over_85":ep85_count,"episodes_over_90":ep90_count,"longest_episode_over_85_min":round(ep85_longest,2),
            "active_samples_pct":round(active_samples_pct,2) if active_samples_pct is not None else None,
            "zero_samples_pct":round(zero_samples_pct,2) if zero_samples_pct is not None else None,
            "current_zero_since":current_zero_since,"current_zero_duration_min":current_zero_duration_min,
            "current_zero_left_censored":bool(current_zero_left_censored),"current_zero_samples":int(current_zero_samples),
            "last_zero_episode_since":last_zero_episode_since,"last_zero_episode_end":last_zero_episode_end,
            "last_zero_episode_duration_min":last_zero_episode_duration_min,"last_zero_episode_left_censored":bool(last_zero_episode_left_censored),
            "last_zero_recovery_at":last_zero_recovery_at,"last_zero_recovery_pct":last_zero_recovery_pct,
            "zero_episodes_completed":int(zero_completed),"trend_15m_vs_prev45m":trend_state,
            "trend_delta_pct_points":round(trend_delta,2) if trend_delta is not None else None,"quality":q or {},
            "busy":cur.get("busy") if hasattr(cur,"get") else None,"total":cur.get("total") if hasattr(cur,"get") else None,
        }

    def _capacity_profiles_from_sidecar(self, instancia, source, current_rows, start_dt, end_dt):
        """Perfis por entidade usando o índice entidade+tempo do sidecar derivado.

        Em vez de transportar toda a Instância em uma única matriz de dezenas de milhares
        de dicts, cada entidade é lida por faixa indexada e processada uma única vez. O
        resultado preserva a semântica de _capacity_group_profiles e retorna também o
        número de amostras lidas para diagnóstico.
        """
        if not ANALYTICS_SIDECAR_DB.exists():
            return [],0
        con=None
        try:
            uri=f"file:{ANALYTICS_SIDECAR_DB.as_posix()}?mode=ro"
            con=sqlite3.connect(uri,uri=True,timeout=1.0);con.row_factory=sqlite3.Row
            con.execute("PRAGMA query_only=ON");con.execute("PRAGMA busy_timeout=1000")
            rr=con.execute("""SELECT DISTINCT entity FROM infra_light
                  WHERE instancia=? AND source=? AND ts>=? AND ts<=? AND occupancy IS NOT NULL""",
                  (str(instancia),str(source),start_dt.isoformat(),end_dt.isoformat())).fetchall()
            current_map={str(x.get("entity") or "").strip().upper() if source=="gateway" else str(x.get("entity") or "").strip():dict(x)
                         for x in (current_rows or []) if str(x.get("entity") or "").strip()}
            names=list(dict.fromkeys(list(current_map.keys())+[(str(x["entity"] or "").strip().upper() if source=="gateway" else str(x["entity"] or "").strip()) for x in rr if str(x["entity"] or "").strip()]))
            out=[];loaded=0
            for ent in names:
                rows=con.execute("""SELECT ts,occupancy FROM infra_light
                      WHERE instancia=? AND source=? AND entity=? AND ts>=? AND ts<=? AND occupancy IS NOT NULL
                      ORDER BY ts""",
                      (str(instancia),str(source),ent,start_dt.isoformat(),end_dt.isoformat())).fetchall()
                loaded+=len(rows)
                cur=current_map.get(ent)
                prof=self._capacity_profile_sidecar_fast(ent,rows,cur,start_dt,end_dt)
                if prof:out.append(prof)
            return out,loaded
        finally:
            if con is not None:con.close()

    def _infra_range(self, instancia, start_dt, end_dt, infra_context):
        """Histórico de infraestrutura no intervalo exato.

        H2-C2 Closure performance correction: em janelas longas, o request lê do sidecar
        somente envelopes temporais e perfis por entidade indexados. Não transporta mais
        toda a matriz trunk_all_entities/gateway_all_entities para Python e não espera o
        refresh global do sidecar no cold path. O refresh permanece assíncrono e o LIVE é
        fallback READ ONLY.
        """
        codes=set((infra_context.get("usage") or {}).get("trunks",{}).keys())
        gw_names={str(x).strip().upper() for x in (infra_context.get("usage") or {}).get("gateways",{}).keys() if str(x).strip()}
        requested_minutes=max(0.0,(end_dt-start_dt).total_seconds()/60.0)
        instance_trunks=list(infra_context.get("instance_trunks") or [])
        instance_gateways=list(infra_context.get("instance_gateways") or [])

        def trunk_code(entity):
            mm=re.search(r"\((\d+)\)\s*$",str(entity or ""));return mm.group(1) if mm else None
        mapped_trunk_names=[str(x.get("entity") or "").strip() for x in instance_trunks if str(x.get("entity") or "").strip() and (not codes or trunk_code(x.get("entity")) in codes)]
        mapped_gateway_names=[str(x.get("entity") or "").strip().upper() for x in instance_gateways if str(x.get("entity") or "").strip() and (not gw_names or str(x.get("entity") or "").strip().upper() in gw_names)]

        if requested_minutes>=ANALYTICS_SIDECAR_LONG_MINUTES and ANALYTICS_SIDECAR_DB.exists():
            con=None;sidecar_ok=False
            try:
                uri=f"file:{ANALYTICS_SIDECAR_DB.as_posix()}?mode=ro"
                con=sqlite3.connect(uri,uri=True,timeout=1.0);con.row_factory=sqlite3.Row
                con.execute("PRAGMA query_only=ON");con.execute("PRAGMA busy_timeout=1000")
                def envelope(source,names):
                    params=[str(instancia),source,start_dt.isoformat(),end_dt.isoformat()]
                    filt=""
                    if names:
                        filt=" AND entity IN ("+",".join("?" for _ in names)+")"
                        params.extend(names)
                    rows=con.execute("""SELECT ts,MAX(occupancy) AS occupancy FROM infra_light
                          WHERE instancia=? AND source=? AND ts>=? AND ts<=? AND occupancy IS NOT NULL"""+filt+" GROUP BY ts ORDER BY ts",params).fetchall()
                    return [{"ts":r["ts"],"occupancy":round(float(r["occupancy"]),3)} for r in rows if r["occupancy"] is not None]
                hist_trunk_names=[str(r["entity"] or "").strip() for r in con.execute("""SELECT DISTINCT entity FROM infra_light
                      WHERE instancia=? AND source='trunk' AND ts>=? AND ts<=? AND entity<>''""",
                      (str(instancia),start_dt.isoformat(),end_dt.isoformat())).fetchall()]
                hist_gateway_names=[str(r["entity"] or "").strip().upper() for r in con.execute("""SELECT DISTINCT entity FROM infra_light
                      WHERE instancia=? AND source='gateway' AND ts>=? AND ts<=? AND entity<>''""",
                      (str(instancia),start_dt.isoformat(),end_dt.isoformat())).fetchall()]
                # O mapeamento temporal deve incluir entidades vistas NO PERÍODO, mesmo que
                # tenham saído da fotografia atual. Assim não perdemos um tronco/Gateway
                # histórico citado por Last Calls dentro da própria janela selecionada.
                mapped_trunk_names=[ent for ent in hist_trunk_names if not codes or trunk_code(ent) in codes]
                mapped_gateway_names=[ent for ent in hist_gateway_names if not gw_names or ent in gw_names]
                trunk=envelope("trunk",mapped_trunk_names)
                gateway=envelope("gateway",mapped_gateway_names)
                all_trunk_times=[datetime.fromisoformat(str(r["ts"])) for r in con.execute("""SELECT ts FROM infra_light WHERE instancia=? AND source='trunk' AND ts>=? AND ts<=? GROUP BY ts ORDER BY ts""",(str(instancia),start_dt.isoformat(),end_dt.isoformat())).fetchall()]
                all_gateway_times=[datetime.fromisoformat(str(r["ts"])) for r in con.execute("""SELECT ts FROM infra_light WHERE instancia=? AND source='gateway' AND ts>=? AND ts<=? GROUP BY ts ORDER BY ts""",(str(instancia),start_dt.isoformat(),end_dt.isoformat())).fetchall()]
                sidecar_ok=True
            except Exception:
                sidecar_ok=False
            finally:
                if con is not None:con.close()
            if sidecar_ok:
                try:
                    all_tp,tp_loaded=self._capacity_profiles_from_sidecar(instancia,"trunk",instance_trunks,start_dt,end_dt)
                    all_gp,gp_loaded=self._capacity_profiles_from_sidecar(instancia,"gateway",instance_gateways,start_dt,end_dt)
                    mt=set(mapped_trunk_names);mg=set(mapped_gateway_names)
                    queue_tp=[x for x in all_tp if str(x.get("entity") or "").strip() in mt] if mt else list(all_tp)
                    queue_gp=[x for x in all_gp if str(x.get("entity") or "").strip().upper() in mg] if mg else list(all_gp)
                    return {
                        "trunk":trunk,"gateway":gateway,
                        "trunk_entities":[],"gateway_entities":[],"trunk_all_entities":[],"gateway_all_entities":[],
                        "precomputed_profiles":{"trunk":all_tp,"gateway":all_gp,"queue_trunk":queue_tp,"queue_gateway":queue_gp},
                        "instance_coverage":{
                            "trunk":self._coverage_from_sorted_times(all_trunk_times,start_dt,end_dt,sample_count=len(all_trunk_times)),
                            "gateway":self._coverage_from_sorted_times(all_gateway_times,start_dt,end_dt,sample_count=len(all_gateway_times)),
                        },
                        "profile_rows_loaded":{"trunk":tp_loaded,"gateway":gp_loaded},
                        "transport":"SIDECAR_ENVELOPE_PLUS_ENTITY_INDEXED_PROFILES",
                    }
                except Exception:
                    pass

        # Caminho curto/fallback: preserva implementação histórica READ ONLY.
        trunk_all_entities=[];gateway_all_entities=[]
        conn=self.connect()
        try:
            if not self.table_exists(conn,"infra_snapshot"):
                return {"trunk":[],"gateway":[],"trunk_entities":[],"gateway_entities":[],"trunk_all_entities":[],"gateway_all_entities":[]}
            bounds=self._table_rowid_bounds(conn,"infra_snapshot",start_dt,end_dt,safety_rows=32)
            if not bounds:
                return {"trunk":[],"gateway":[],"trunk_entities":[],"gateway_entities":[],"trunk_all_entities":[],"gateway_all_entities":[]}
            lo,hi=bounds
            trunk_rows=conn.execute("""SELECT ts,raw_json FROM infra_snapshot
                  WHERE rowid>=? AND rowid<=? AND instancia=? AND source='trunk' AND ts>=? AND ts<=? ORDER BY rowid""",
                  (lo,hi,str(instancia),start_dt.isoformat(),end_dt.isoformat())).fetchall()
            seen_ts=set()
            for row in trunk_rows:
                ts=row["ts"]
                if ts in seen_ts:continue
                seen_ts.add(ts)
                try:doc=json.loads(row["raw_json"] or "{}")
                except Exception:continue
                arr=doc.get("trunkGrouplist") or doc.get("trunkGroupList") or doc.get("trunkDataList") or doc.get("TrunkDataList") or []
                for x in arr if isinstance(arr,list) else []:
                    ent=str(x.get("DeNmTrunkGroup") or x.get("NmTrunkGroup") or "").strip();occ=parse_portal_number(x.get("VlPercValidsBusy"),None)
                    if ent and occ is not None:trunk_all_entities.append({"ts":ts,"entity":ent,"occupancy":round(occ,3)})
            gateway_rows=conn.execute("""SELECT ts,entity,occupancy FROM infra_snapshot
                  WHERE rowid>=? AND rowid<=? AND instancia=? AND source='gateway' AND ts>=? AND ts<=? AND entity!='__RAW__' ORDER BY rowid""",
                  (lo,hi,str(instancia),start_dt.isoformat(),end_dt.isoformat())).fetchall()
            gateway_all_entities=[{"ts":r["ts"],"entity":str(r["entity"] or "").strip().upper(),"occupancy":round(float(r["occupancy"]),3)} for r in gateway_rows if r["occupancy"] is not None and str(r["entity"] or "").strip()]
        finally:
            conn.close()
        trunk_entities=[r for r in trunk_all_entities if not codes or trunk_code(r.get("entity")) in codes]
        gateway_entities=[r for r in gateway_all_entities if not gw_names or str(r.get("entity") or "").upper() in gw_names]
        def py_envelope(rows):
            grouped={}
            for r in rows:
                if r.get("occupancy") is not None:grouped.setdefault(r.get("ts"),[]).append(float(r["occupancy"]))
            return [{"ts":ts,"occupancy":round(max(vals),3)} for ts,vals in sorted(grouped.items()) if vals]
        return {"trunk":py_envelope(trunk_entities),"gateway":py_envelope(gateway_entities),"trunk_entities":trunk_entities,"gateway_entities":gateway_entities,"trunk_all_entities":trunk_all_entities,"gateway_all_entities":gateway_all_entities}

    @staticmethod
    def _gateway_pct_count(v,total):
        if not total:return None
        return round(float(v or 0)*100.0/float(total),2)

    @classmethod
    def _gateway_quality_for_rows(cls, rows, start_dt, end_dt, operational_window=None):
        """Qualidade operacional conservadora.

        Separa duas perguntas que antes eram misturadas:
        - completude nominal: quantas amostras chegaram frente à cadência nominal de 10 s;
        - continuidade temporal: quanto do intervalo ficou conectado sem gaps acima do tolerado.

        coverage_pct é o menor dos dois valores para não declarar 99% quando a densidade
        nominal estiver parcial. Gaps de borda (início/fim do período) também contam.
        """
        operational_window=operational_window or load_operational_window()
        intervals=_operational_intervals(start_dt,end_dt,operational_window)
        requested_s=max(0.0,sum((b-a).total_seconds() for a,b in intervals))
        parsed=[]
        for rr in rows or []:
            try:ts=datetime.fromisoformat(str(rr.get("ts") or ""))
            except Exception:continue
            if _inside_operational_dt(ts,operational_window):parsed.append(ts)
        parsed.sort()
        deltas=[]
        for ia,ib in intervals:
            times=[t for t in parsed if ia<=t<=ib]
            deltas.extend([(y-x).total_seconds() for x,y in zip(times,times[1:]) if y>x and (y-x).total_seconds()<=120])
        cadence=float(statistics.median(deltas)) if deltas else GATEWAY_NOMINAL_CADENCE_S
        threshold=max(GATEWAY_GAP_CAP_S,cadence*3.0)
        continuity_covered=0.0;gaps=[];leading_gaps=[];trailing_gaps=[]
        for ia,ib in intervals:
            times=[t for t in parsed if ia<=t<=ib]
            interval_s=max(0.0,(ib-ia).total_seconds())
            if not times:
                if interval_s>0:
                    gaps.append(interval_s);leading_gaps.append(interval_s);trailing_gaps.append(interval_s)
                continue
            leading=max(0.0,(times[0]-ia).total_seconds())
            trailing=max(0.0,(ib-times[-1]).total_seconds())
            leading_gaps.append(leading);trailing_gaps.append(trailing)
            ds=[(y-x).total_seconds() for x,y in zip(times,times[1:]) if y>x]
            gaps.extend([d for d in ds if d>threshold])
            if leading>threshold:gaps.append(leading)
            if trailing>threshold:gaps.append(trailing)
            internal_uncovered=sum(max(0.0,d-threshold) for d in ds)
            observed_span=max(0.0,(times[-1]-times[0]).total_seconds()+min(cadence,threshold))
            continuity_covered+=max(0.0,min(interval_s,observed_span-internal_uncovered))
        expected=requested_s/GATEWAY_NOMINAL_CADENCE_S if requested_s>0 else 0.0
        nominal_completeness=min(100.0,(len(parsed)/expected*100.0)) if expected>0 else 0.0
        continuity=min(100.0,(continuity_covered/requested_s*100.0)) if requested_s>0 else 0.0
        conservative=min(nominal_completeness,continuity)
        return {
            "coverage_pct":round(conservative,2),
            "nominal_completeness_pct":round(nominal_completeness,2),
            "continuity_coverage_pct":round(continuity,2),
            "observed_samples":len(parsed),
            "expected_nominal_samples":round(expected,1),
            "median_cadence_s":round(cadence,2) if cadence is not None else None,
            "gap_count":sum(1 for g in gaps if g>threshold),
            "largest_gap_s":round(max(gaps),2) if gaps else 0.0,
            "leading_gap_s":round(max(leading_gaps),2) if leading_gaps else 0.0,
            "trailing_gap_s":round(max(trailing_gaps),2) if trailing_gaps else 0.0,
            "requested_operational_minutes":round(requested_s/60.0,2),
            "method":"OPERATIONAL_WINDOW_CONSERVATIVE_NOMINAL_PLUS_CONTINUITY",
        }

    @classmethod
    def _gateway_technical_quality(cls, rows, start_dt, end_dt):
        total_s=max(1.0,(end_dt-start_dt).total_seconds())
        times=[]
        for rr in rows or []:
            try:times.append(datetime.fromisoformat(str(rr.get("ts") or "")))
            except Exception:pass
        times.sort()
        ds=[(b-a).total_seconds() for a,b in zip(times,times[1:]) if b>a]
        valid=[d for d in ds if 0<d<=120]
        cadence=float(statistics.median(valid)) if valid else GATEWAY_NOMINAL_CADENCE_S
        threshold=max(GATEWAY_GAP_CAP_S,cadence*3.0)
        leading=max(0.0,(times[0]-start_dt).total_seconds()) if times else total_s
        trailing=max(0.0,(end_dt-times[-1]).total_seconds()) if times else total_s
        gap_values=[d for d in ds if d>threshold]
        if leading>threshold:gap_values.append(leading)
        if trailing>threshold:gap_values.append(trailing)
        internal_uncovered=sum(max(0.0,d-threshold) for d in ds)
        observed_span=max(0.0,(times[-1]-times[0]).total_seconds()+min(cadence,threshold)) if times else 0.0
        continuity_covered=max(0.0,min(total_s,observed_span-internal_uncovered))
        continuity=min(100.0,continuity_covered/total_s*100.0)
        expected=total_s/GATEWAY_NOMINAL_CADENCE_S
        nominal_completeness=min(100.0,len(times)/expected*100.0) if expected>0 else 0.0
        conservative=min(nominal_completeness,continuity)
        return {
            "coverage_pct":round(conservative,2),
            "nominal_completeness_pct":round(nominal_completeness,2),
            "continuity_coverage_pct":round(continuity,2),
            "observed_samples":len(times),
            "expected_nominal_samples":round(expected,1),
            "median_cadence_s":round(cadence,2),
            "gap_count":len(gap_values),
            "largest_gap_s":round(max(gap_values),2) if gap_values else 0.0,
            "leading_gap_s":round(leading,2),
            "trailing_gap_s":round(trailing,2),
            "method":"TECHNICAL_24X7_CONSERVATIVE_NOMINAL_PLUS_CONTINUITY",
        }

    @staticmethod
    def _gateway_silence_episodes(rows, operational_window=None):
        operational_window=operational_window or load_operational_window()
        critical={"SILENT_WARN","SILENT_CRITICAL","SILENT_SUSPECT"}
        out=[];cur=None;prev_ts=None
        for rr in rows or []:
            try:ts=datetime.fromisoformat(str(rr.get("ts") or ""))
            except Exception:continue
            if not _inside_operational_dt(ts,operational_window):
                if cur is not None:out.append(cur);cur=None
                prev_ts=None;continue
            st=str(rr.get("service_state") or "")
            split=prev_ts is not None and (ts-prev_ts).total_seconds()>GATEWAY_GAP_CAP_S
            if split and cur is not None:out.append(cur);cur=None
            if st in critical:
                if cur is None:cur={"start":ts,"end":ts,"states":{st},"recovered":False,"recovery_ts":None}
                else:cur["end"]=ts;cur["states"].add(st)
            elif cur is not None:
                cur["recovered"]=True;cur["recovery_ts"]=ts;out.append(cur);cur=None
            prev_ts=ts
        if cur is not None:out.append(cur)
        result=[]
        for x in out:
            dur=max(GATEWAY_NOMINAL_CADENCE_S,(x["end"]-x["start"]).total_seconds()+GATEWAY_NOMINAL_CADENCE_S)/60.0
            result.append({"start":x["start"].isoformat(),"end":x["end"].isoformat(),"duration_min":round(dur,2),"states":sorted(x["states"]),"recovered":bool(x.get("recovered")),"recovery_ts":x.get("recovery_ts").isoformat() if x.get("recovery_ts") else None,"scope":"OPERATIONAL_WINDOW"})
        return result

    @staticmethod
    def _gateway_zero_occupancy_episodes(rows, operational_window=None, start_dt=None, end_dt=None):
        """Episódios de ocupação exatamente 0% observada no raw v5.

        Mantém separado do service_state SILENT_* (que possui histerese/baseline).
        Retorno é o primeiro sample positivo observado. Se houver gap acima da tolerância
        antes do primeiro positivo, o TOI registra "primeiro positivo após gap" e não
        inventa o instante exato da transição.
        """
        operational_window=operational_window or load_operational_window()
        parsed=[]
        for rr in rows or []:
            try:ts=datetime.fromisoformat(str(rr.get("ts") or ""))
            except Exception:continue
            if not _inside_operational_dt(ts,operational_window):continue
            occ=rr.get("occupancy_pct")
            if occ is None:continue
            parsed.append((ts,float(occ),rr))
        parsed.sort(key=lambda x:x[0])
        ds=[(b[0]-a[0]).total_seconds() for a,b in zip(parsed,parsed[1:]) if b[0]>a[0]]
        valid=[d for d in ds if 0<d<=120]
        cadence=float(statistics.median(valid)) if valid else GATEWAY_NOMINAL_CADENCE_S
        threshold=max(GATEWAY_GAP_CAP_S,cadence*3.0)
        out=[];cur=None;prev=None
        def close(reason, positive=None, gap_s=None):
            nonlocal cur
            if cur is None:return
            cur["closure_reason"]=reason
            cur["recovered"]=False
            cur["recovery_ts"]=None
            cur["recovery_pct"]=None
            cur["recovery_observed_after_gap"]=False
            cur["recovery_gap_s"]=round(gap_s,2) if gap_s is not None else None
            if positive is not None:
                pts,pocc=positive
                if reason=="POSITIVE_CONTIGUOUS":
                    cur["recovered"]=True;cur["recovery_ts"]=pts;cur["recovery_pct"]=pocc
                elif reason=="FIRST_POSITIVE_AFTER_GAP":
                    cur["recovery_observed_after_gap"]=True;cur["first_positive_after_gap_ts"]=pts;cur["first_positive_after_gap_pct"]=pocc
            out.append(cur);cur=None
        for ts,occ,rr in parsed:
            gap=(ts-prev[0]).total_seconds() if prev else None
            if cur is not None and gap is not None and gap>threshold:
                if occ>0:close("FIRST_POSITIVE_AFTER_GAP",(ts,occ),gap)
                else:close("GAP",None,gap)
            if occ<=0:
                if cur is None:
                    cur={"start":ts,"end":ts,"zero_samples":1,"first_zero_pct":occ,
                         "left_censored":bool(start_dt and (ts-start_dt).total_seconds()<=threshold)}
                else:
                    cur["end"]=ts;cur["zero_samples"]+=1
            elif cur is not None:
                close("POSITIVE_CONTIGUOUS",(ts,occ),gap)
            prev=(ts,occ,rr)
        if cur is not None:close("OPEN_WINDOW_END")
        result=[]
        for x in out:
            observed=max(GATEWAY_NOMINAL_CADENCE_S,(x["end"]-x["start"]).total_seconds()+min(cadence,threshold))/60.0
            rec_ts=x.get("recovery_ts")
            first_after=x.get("first_positive_after_gap_ts")
            result.append({
                "start":x["start"].isoformat(),
                "last_zero_ts":x["end"].isoformat(),
                "duration_min":round(observed,2),
                "zero_samples":int(x.get("zero_samples") or 0),
                "left_censored":bool(x.get("left_censored")),
                "right_censored":bool(x.get("closure_reason")=="OPEN_WINDOW_END"),
                "closure_reason":x.get("closure_reason"),
                "recovered":bool(x.get("recovered")),
                "recovery_ts":rec_ts.isoformat() if rec_ts else None,
                "recovery_pct":round(float(x["recovery_pct"]),2) if x.get("recovery_pct") is not None else None,
                "recovery_observed_after_gap":bool(x.get("recovery_observed_after_gap")),
                "recovery_gap_s":x.get("recovery_gap_s"),
                "first_positive_after_gap_ts":first_after.isoformat() if first_after else None,
                "first_positive_after_gap_pct":round(float(x["first_positive_after_gap_pct"]),2) if x.get("first_positive_after_gap_pct") is not None else None,
                "scope":"OPERATIONAL_WINDOW_RAW_OCCUPANCY_ZERO",
            })
        return result

    @staticmethod
    def _gateway_zero_occupancy_clusters(episodes, max_join_gap_s=300.0):
        """Agrupa episódios raw de 0% próximos em uma história operacional única.

        Preserva todos os episódios individuais. O cluster existe apenas para narrativa:
        pequenas retomadas/gaps entre episódios não apagam o fato de recorrência.
        Não declara continuidade física durante gaps.
        """
        eps=[]
        for e in episodes or []:
            try:
                st=datetime.fromisoformat(str(e.get("start") or ""))
                lz=datetime.fromisoformat(str(e.get("last_zero_ts") or e.get("end") or ""))
            except Exception:
                continue
            x=dict(e);x["_start_dt"]=st;x["_last_zero_dt"]=lz;eps.append(x)
        eps.sort(key=lambda x:x["_start_dt"])
        clusters=[];cur=None
        def final_positive(e):
            ts=e.get("recovery_ts") or e.get("first_positive_after_gap_ts")
            pct=e.get("recovery_pct") if e.get("recovery_ts") else e.get("first_positive_after_gap_pct")
            mode="CONTIGUOUS" if e.get("recovery_ts") else ("AFTER_GAP" if e.get("first_positive_after_gap_ts") else None)
            return ts,pct,mode,e.get("recovery_gap_s")
        def close_cluster():
            nonlocal cur
            if not cur:return
            last=cur["episodes"][-1]
            rec_ts,rec_pct,rec_mode,rec_gap=final_positive(last)
            start=cur["episodes"][0]["_start_dt"];last_zero=last["_last_zero_dt"]
            inter=[]
            for e in cur["episodes"][:-1]:
                ts,pct,mode,gap=final_positive(e)
                if ts:inter.append({"ts":ts,"pct":pct,"mode":mode,"gap_s":gap})
            total_zero=sum(float(e.get("duration_min") or 0) for e in cur["episodes"])
            cluster={
                "start":start.isoformat(),
                "last_zero_ts":last_zero.isoformat(),
                "span_min":round(max(0.0,(last_zero-start).total_seconds())/60.0,2),
                "zero_duration_min":round(total_zero,2),
                "episode_count":len(cur["episodes"]),
                "contains_gap":any(str(e.get("closure_reason") or "").upper() in {"GAP","FIRST_POSITIVE_AFTER_GAP"} for e in cur["episodes"]),
                "intermediate_positive_observations":inter,
                "final_recovery_ts":rec_ts,
                "final_recovery_pct":rec_pct,
                "final_recovery_mode":rec_mode,
                "final_recovery_gap_s":rec_gap,
                "episodes":[{k:v for k,v in e.items() if not k.startswith("_")} for e in cur["episodes"]],
                "scope":"OPERATIONAL_WINDOW_RAW_ZERO_CLUSTER",
            }
            clusters.append(cluster);cur=None
        for e in eps:
            if cur is None:
                cur={"episodes":[e]};continue
            prev=cur["episodes"][-1]
            gap=(e["_start_dt"]-prev["_last_zero_dt"]).total_seconds()
            if 0 <= gap <= float(max_join_gap_s):
                cur["episodes"].append(e)
            else:
                close_cluster();cur={"episodes":[e]}
        close_cluster()
        return clusters

    def gateway_shadow_summary(self, instancia, start_dt, end_dt, entities=None, operational_window=None):
        """Resumo rápido em uma única passagem do Shadow.

        H2-C1 H1 remove os três scans agregados anteriores. A janela operacional é
        aplicada em memória somente para métricas de negócio; rede/ICMP continua 24x7.
        """
        operational_window=operational_window or load_operational_window(instancia)
        result={"available":False,"source":str(GATEWAY_SHADOW_DB),"mode":"READ_ONLY_GATEWAY_SHADOW_SUMMARY_ONE_PASS_H1","start":start_dt.isoformat(),"end":end_dt.isoformat(),"entities":[],"reason":None,"operational_window":operational_window}
        if not GATEWAY_SHADOW_DB.exists():result["reason"]="Gateway Shadow DB não encontrado";return result
        try:
            uri=f"file:{GATEWAY_SHADOW_DB.as_posix()}?mode=ro";con=sqlite3.connect(uri,uri=True,timeout=3);con.row_factory=sqlite3.Row;con.execute("PRAGMA query_only=ON");con.execute("PRAGMA busy_timeout=3000")
        except Exception as exc:result["reason"]=f"Falha ao abrir Gateway Shadow em READ ONLY: {exc}";return result
        try:
            names=sorted({str(x or '').strip().upper() for x in (entities or []) if str(x or '').strip()})
            where="instancia=? AND ts>=? AND ts<=?";params=[str(instancia),start_dt.isoformat(),end_dt.isoformat()]
            if names:
                where+=" AND entity IN ("+",".join("?" for _ in names)+")";params.extend(names)
            rows=con.execute(f"""SELECT ts,entity,occupancy_pct,peer_active_ratio,peer_median_occupancy,
                       reachability_state,service_state,path_state,path_suppressed,loss_pct,rtt_p50_ms,rtt_p95_ms,
                       stable_status,display_status,baseline_class,baseline_ready,baseline_active_ratio,
                       traffic_share_pct,call_share_pct,rtt_jitter_ms,silence_warn_min,silence_critical_min,last_call_evidence_ts,reason
                    FROM gateway_health_sample_v5 WHERE {where}""",params).fetchall()
            op_seconds=max(1.0,_operational_requested_seconds(start_dt,end_dt,operational_window))
            agg={}
            def aget(ent):
                return agg.setdefault(ent,{"op_n":0,"tech_n":0,"first_ts":None,"last_ts":None,
                    "occ_sum":0.0,"occ_n":0,"occ_peak":None,"peer_active_sum":0.0,"peer_active_n":0,"peer_med_sum":0.0,"peer_med_n":0,
                    "peer_div_sum":0.0,"peer_div_n":0,"peer_div_peak":None,"svc":{},"reach":{},"path":{},
                    "loss_sum":0.0,"loss_n":0,"rtt50_sum":0.0,"rtt50_n":0,"rtt95_max":None,"latest":None})
            for rr in rows:
                ent=str(rr["entity"] or "").strip().upper()
                if not ent:continue
                a=aget(ent);a["tech_n"]+=1
                for key,bucket in (("reachability_state","reach"),("path_state","path")):
                    st=str(rr[key] or "N/C");a[bucket][st]=a[bucket].get(st,0)+1
                if rr["loss_pct"] is not None:a["loss_sum"]+=float(rr["loss_pct"]);a["loss_n"]+=1
                if rr["rtt_p50_ms"] is not None:a["rtt50_sum"]+=float(rr["rtt_p50_ms"]);a["rtt50_n"]+=1
                if rr["rtt_p95_ms"] is not None:a["rtt95_max"]=max(float(rr["rtt_p95_ms"]),a["rtt95_max"] if a["rtt95_max"] is not None else float(rr["rtt_p95_ms"]))
                if a["latest"] is None or str(rr["ts"])>str(a["latest"]["ts"]):a["latest"]=dict(rr)
                try:dt=datetime.fromisoformat(str(rr["ts"]))
                except Exception:continue
                if not _inside_operational_dt(dt,operational_window):continue
                a["op_n"]+=1;a["first_ts"]=min(str(rr["ts"]),a["first_ts"]) if a["first_ts"] else str(rr["ts"]);a["last_ts"]=max(str(rr["ts"]),a["last_ts"]) if a["last_ts"] else str(rr["ts"])
                svc=str(rr["service_state"] or "N/C");a["svc"][svc]=a["svc"].get(svc,0)+1
                occ=rr["occupancy_pct"];pa=rr["peer_active_ratio"];pm=rr["peer_median_occupancy"]
                if occ is not None:a["occ_sum"]+=float(occ);a["occ_n"]+=1;a["occ_peak"]=max(float(occ),a["occ_peak"] if a["occ_peak"] is not None else float(occ))
                if pa is not None:a["peer_active_sum"]+=float(pa);a["peer_active_n"]+=1
                if pm is not None:a["peer_med_sum"]+=float(pm);a["peer_med_n"]+=1
                if occ is not None and pm is not None:
                    dv=float(pm)-float(occ);a["peer_div_sum"]+=dv;a["peer_div_n"]+=1;a["peer_div_peak"]=max(dv,a["peer_div_peak"] if a["peer_div_peak"] is not None else dv)
            for n in names:aget(n)
            for ent in sorted(agg):
                a=agg[ent];n=a["op_n"];tn=a["tech_n"];l=a["latest"] or {};coverage=min(100.0,n*GATEWAY_NOMINAL_CADENCE_S/op_seconds*100.0) if op_seconds>0 else 0.0
                result["entities"].append({
                  "entity":ent,"samples":n,"first_ts":a["first_ts"],"last_ts":a["last_ts"],
                  "occupancy_mean_pct":round(a["occ_sum"]/a["occ_n"],2) if a["occ_n"] else None,"occupancy_peak_pct":round(a["occ_peak"],2) if a["occ_peak"] is not None else None,"occupancy_p95_pct":None,
                  "peer_active_ratio_mean_pct":round(a["peer_active_sum"]/a["peer_active_n"],2) if a["peer_active_n"] else None,
                  "peer_median_occupancy_mean_pct":round(a["peer_med_sum"]/a["peer_med_n"],2) if a["peer_med_n"] else None,
                  "peer_divergence_mean_pct_points":round(a["peer_div_sum"]/a["peer_div_n"],2) if a["peer_div_n"] else None,"peer_divergence_peak_pct_points":round(a["peer_div_peak"],2) if a["peer_div_peak"] is not None else None,
                  "service_state_pct":{"ACTIVE":self._gateway_pct_count(a["svc"].get("ACTIVE"),n),"SILENT_WARN":self._gateway_pct_count(a["svc"].get("SILENT_WARN"),n),"SILENT_CRITICAL":self._gateway_pct_count(a["svc"].get("SILENT_CRITICAL"),n),"SILENT_SUSPECT":self._gateway_pct_count(a["svc"].get("SILENT_SUSPECT"),n)},
                  "reachability_pct":{"OK":self._gateway_pct_count(a["reach"].get("OK"),tn),"INTERMITTENT":self._gateway_pct_count(a["reach"].get("INTERMITTENT"),tn),"DOWN":self._gateway_pct_count(a["reach"].get("DOWN"),tn),"PATH_UNCERTAIN":self._gateway_pct_count(a["reach"].get("PATH_UNCERTAIN"),tn)},
                  "path_pct":{"OK":self._gateway_pct_count(a["path"].get("OK"),tn),"RECOVERY":self._gateway_pct_count(a["path"].get("RECOVERY"),tn),"DEGRADED":self._gateway_pct_count(a["path"].get("DEGRADED"),tn)},
                  "loss_mean_pct":round(a["loss_sum"]/a["loss_n"],2) if a["loss_n"] else None,"rtt_p50_mean_ms":round(a["rtt50_sum"]/a["rtt50_n"],2) if a["rtt50_n"] else None,"rtt_p95_max_ms":round(a["rtt95_max"],2) if a["rtt95_max"] is not None else None,
                  "quality":{"coverage_pct":round(coverage,2),"requested_operational_minutes":round(op_seconds/60.0,2),"method":"SAMPLE_DENSITY_SUMMARY"},"technical_samples":tn,
                  "latest":{k:l.get(k) for k in ("ts","stable_status","display_status","reachability_state","service_state","path_state","path_suppressed","baseline_class","baseline_ready","baseline_active_ratio","traffic_share_pct","call_share_pct","occupancy_pct","loss_pct","rtt_p50_ms","rtt_p95_ms","rtt_jitter_ms","peer_active_ratio","peer_median_occupancy","silence_warn_min","silence_critical_min","last_call_evidence_ts","reason")},
                })
            result["available"]=True;result["gateway_count"]=len(result["entities"]);result["performance"]={"scan_passes":1,"mode":"ONE_PASS_PYTHON_AGGREGATION_H1"};return result
        except Exception as exc:result["reason"]=f"Falha ao consultar Gateway Shadow: {exc}";return result
        finally:con.close()

    def gateway_shadow_analytics(self, instancia, start_dt, end_dt, entities=None, operational_window=None):
        """Detalhe por Gateway: negócio na janela operacional; rede/ICMP 24x7."""
        operational_window=operational_window or load_operational_window(instancia)
        result={"available":False,"source":str(GATEWAY_SHADOW_DB),"mode":"READ_ONLY_GATEWAY_SHADOW_DETAIL","start":start_dt.isoformat(),"end":end_dt.isoformat(),"entities":[],"reason":None,"operational_window":operational_window}
        if not GATEWAY_SHADOW_DB.exists():result["reason"]="Gateway Shadow DB não encontrado";return result
        try:
            uri=f"file:{GATEWAY_SHADOW_DB.as_posix()}?mode=ro";con=sqlite3.connect(uri,uri=True,timeout=3);con.row_factory=sqlite3.Row
        except Exception as exc:result["reason"]=f"Falha ao abrir Gateway Shadow em READ ONLY: {exc}";return result
        try:
            names=sorted({str(x or '').strip().upper() for x in (entities or []) if str(x or '').strip()})
            where="instancia=? AND ts>=? AND ts<=?";params=[str(instancia),start_dt.isoformat(),end_dt.isoformat()]
            if names:where+=" AND UPPER(entity) IN ("+",".join("?" for _ in names)+")";params.extend(names)
            rows=con.execute(f"""SELECT ts,UPPER(entity) entity,occupancy_pct,loss_pct,rtt_p50_ms,rtt_p95_ms,rtt_jitter_ms,
                       peer_active_ratio,peer_median_occupancy,reachability_state,service_state,path_state,path_suppressed,
                       stable_status,display_status,baseline_class,baseline_ready,baseline_active_ratio,traffic_share_pct,call_share_pct,
                       silence_warn_min,silence_critical_min,last_call_evidence_ts,reason
                    FROM gateway_health_sample_v5 WHERE {where} ORDER BY UPPER(entity),ts""",params).fetchall()
            by={}
            for rr in rows:by.setdefault(rr["entity"],[]).append(dict(rr))
            if names:
                for n in names:by.setdefault(n,[])
            def stat(vals,kind="mean"):
                a=[float(v) for v in vals if v is not None]
                if not a:return None
                if kind=="mean":return round(sum(a)/len(a),2)
                if kind=="max":return round(max(a),2)
                if kind=="p95":
                    a.sort();idx=max(0,min(len(a)-1,int((len(a)-1)*0.95)));return round(a[idx],2)
            for ent,all_rows in sorted(by.items()):
                op_rows=[]
                for rr in all_rows:
                    try:dt=datetime.fromisoformat(str(rr.get("ts") or ""))
                    except Exception:continue
                    if _inside_operational_dt(dt,operational_window):op_rows.append(rr)
                n=len(op_rows);tn=len(all_rows);latest=all_rows[-1] if all_rows else {}
                svc={k:sum(1 for r in op_rows if str(r.get("service_state") or "")==k) for k in ("ACTIVE","SILENT_WARN","SILENT_CRITICAL","SILENT_SUSPECT","IDLE_RECENT")}
                reach={k:sum(1 for r in all_rows if str(r.get("reachability_state") or "")==k) for k in ("OK","INTERMITTENT","DOWN","PATH_UNCERTAIN","NC")}
                path={k:sum(1 for r in all_rows if str(r.get("path_state") or "")==k) for k in ("OK","RECOVERY","DEGRADED")};path["SUPPRESSED"]=sum(1 for r in all_rows if int(r.get("path_suppressed") or 0)==1)
                div=[float(r.get("peer_median_occupancy"))-float(r.get("occupancy_pct")) for r in op_rows if r.get("peer_median_occupancy") is not None and r.get("occupancy_pct") is not None]
                eps=self._gateway_silence_episodes(all_rows,operational_window)
                zero_eps=self._gateway_zero_occupancy_episodes(all_rows,operational_window,start_dt,end_dt)
                zero_clusters=self._gateway_zero_occupancy_clusters(zero_eps)
                transitions=0;prev=None
                for rr in all_rows:
                    cur=str(rr.get("stable_status") or "")
                    if prev is not None and cur!=prev:transitions+=1
                    prev=cur
                reason=str(latest.get("reason") or "");m=re.search(r"Silêncio funcional\s+([0-9]+(?:[.,][0-9]+)?)\s+min",reason,re.I)
                result["entities"].append({
                  "entity":ent,"samples":n,"technical_samples":tn,"first_ts":op_rows[0]["ts"] if op_rows else None,"last_ts":op_rows[-1]["ts"] if op_rows else None,
                  "occupancy_mean_pct":stat([r.get("occupancy_pct") for r in op_rows]),"occupancy_p95_pct":stat([r.get("occupancy_pct") for r in op_rows],"p95"),"occupancy_peak_pct":stat([r.get("occupancy_pct") for r in op_rows],"max"),
                  "loss_mean_pct":stat([r.get("loss_pct") for r in all_rows]),"rtt_p50_mean_ms":stat([r.get("rtt_p50_ms") for r in all_rows]),"rtt_p95_max_ms":stat([r.get("rtt_p95_ms") for r in all_rows],"max"),
                  "peer_active_ratio_mean_pct":stat([r.get("peer_active_ratio") for r in op_rows]),"peer_median_occupancy_mean_pct":stat([r.get("peer_median_occupancy") for r in op_rows]),"peer_divergence_mean_pct_points":round(sum(div)/len(div),2) if div else None,"peer_divergence_peak_pct_points":round(max(div),2) if div else None,
                  "localized_silence_samples_pct":self._gateway_pct_count(sum(1 for r in op_rows if float(r.get("occupancy_pct") or 0)<=0 and float(r.get("peer_active_ratio") or 0)>=50 and str(r.get("reachability_state") or '')=='OK'),n),
                  "reachability_pct":{k:self._gateway_pct_count(v,tn) for k,v in reach.items()},"service_active_pct":self._gateway_pct_count(svc.get("ACTIVE"),n),"service_state_pct":{k:self._gateway_pct_count(v,n) for k,v in svc.items()},"path_pct":{k:self._gateway_pct_count(v,tn) for k,v in path.items()},
                  "stable_status_distribution":{},"transitions":transitions,"last_transition_ts":None,
                  "latest":{k:latest.get(k) for k in ("ts","stable_status","display_status","reachability_state","service_state","path_state","baseline_class","baseline_ready","baseline_active_ratio","traffic_share_pct","call_share_pct","occupancy_pct","loss_pct","rtt_p50_ms","rtt_p95_ms","rtt_jitter_ms","peer_active_ratio","peer_median_occupancy","silence_warn_min","silence_critical_min","last_call_evidence_ts","reason")},
                  "quality":self._gateway_quality_for_rows(all_rows,start_dt,end_dt,operational_window),"technical_quality":self._gateway_technical_quality(all_rows,start_dt,end_dt),
                  "silence_episodes_count":len(eps),"longest_silence_episode_min":round(max([e.get("duration_min") or 0 for e in eps] or [0]),2),"silence_episodes":eps[-25:],
                  "zero_occupancy_episodes_count":len(zero_eps),"longest_zero_occupancy_episode_min":round(max([e.get("duration_min") or 0 for e in zero_eps] or [0]),2),"zero_occupancy_episodes":zero_eps[-50:],
                  "zero_occupancy_incident_clusters_count":len(zero_clusters),"zero_occupancy_incident_clusters":zero_clusters[-25:],
                  "current_functional_silence_min":float(m.group(1).replace(',','.')) if m else None,
                  "operational_window":operational_window,
                })
            result["available"]=True;result["gateway_count"]=len(result["entities"]);return result
        except Exception as exc:result["reason"]=f"Falha ao consultar Gateway Shadow: {exc}";return result
        finally:con.close()

    @staticmethod
    def _period_event_dt(value, end_of_day=False):
        if not value:
            return None
        raw=str(value).strip()
        try:
            dt=datetime.fromisoformat(raw + ("T23:59:59" if end_of_day else "T00:00:00")) if len(raw)==10 else datetime.fromisoformat(raw)
            return dt.astimezone() if dt.tzinfo is None else dt
        except Exception:
            return None

    def gateway_story_evidence(self, instancia, selected_start, selected_end, selected_entities, semantic_reference, knowledge, feedback, operational_window=None):
        """Recortes operacionais compactos antes/durante/depois de eventos conhecidos.

        O objetivo é sustentar narrativa operacional sem transformar ausência de dados
        em zero. Somente SELECT no Gateway Shadow; eventos humanos ficam fora do DB.
        """
        out={}
        operational_window=operational_window or load_operational_window(instancia)
        op_clause,op_params=_operational_sql_clause(operational_window,"ts")
        if not GATEWAY_SHADOW_DB.exists(): return out
        selected_names={str((x or {}).get("entity") or "").strip().upper() for x in (selected_entities or [])}
        selected_map={str((x or {}).get("entity") or "").strip().upper():(x or {}) for x in (selected_entities or [])}
        sem_map={str(x.get("entity") or "").strip().upper():x for x in (semantic_reference.get("gateways") or [])}
        case_map={}
        for x in (knowledge.get("cases") or []): case_map.setdefault(str(x.get("entity") or "").strip().upper(),[]).append(x)
        fb_map={}
        for x in (feedback.get("records") or []): fb_map.setdefault(str(x.get("entity") or "").strip().upper(),[]).append(x)
        names=sorted(selected_names | set(sem_map) | set(case_map) | set(fb_map))
        try:
            uri=f"file:{GATEWAY_SHADOW_DB.as_posix()}?mode=ro"
            con=sqlite3.connect(uri,uri=True,timeout=3); con.row_factory=sqlite3.Row
        except Exception:
            return out
        def basic(ent,a,b):
            if not a or not b or b<=a: return None
            rows=[dict(x) for x in con.execute(f"""
                SELECT ts,occupancy_pct,peer_median_occupancy,peer_active_ratio,reachability_state,
                       service_state,path_state,loss_pct,rtt_p50_ms,rtt_p95_ms
                FROM gateway_health_sample_v5
                WHERE instancia=? AND UPPER(entity)=? AND ts>=? AND ts<=? AND {op_clause}
                ORDER BY ts
            """,(str(instancia),ent,a.isoformat(),b.isoformat(),*op_params)).fetchall()]
            n=len(rows)
            if not n:return {"samples":0,"start":a.isoformat(),"end":b.isoformat(),
                             "quality":self._gateway_quality_for_rows([],a,b,operational_window)}
            occ=[float(r["occupancy_pct"]) for r in rows if r.get("occupancy_pct") is not None]
            peer=[float(r["peer_median_occupancy"]) for r in rows if r.get("peer_median_occupancy") is not None]
            div=[float(r["peer_median_occupancy"])-float(r["occupancy_pct"]) for r in rows if r.get("peer_median_occupancy") is not None and r.get("occupancy_pct") is not None]
            def p95(vals):
                if not vals:return None
                z=sorted(vals);idx=max(0,min(len(z)-1,int((len(z)-1)*0.95)));return round(z[idx],2)
            quality=self._gateway_quality_for_rows(rows,a,b,operational_window)
            return {
                "samples":n,"start":a.isoformat(),"end":b.isoformat(),"first_ts":rows[0]["ts"],"last_ts":rows[-1]["ts"],
                "occupancy_mean_pct":round(sum(occ)/len(occ),2) if occ else None,
                "occupancy_p95_pct":p95(occ),
                "occupancy_peak_pct":round(max(occ),2) if occ else None,
                "zero_occupancy_pct":round(sum(1 for v in occ if v<=0)*100.0/len(occ),2) if occ else None,
                "peer_mean_pct":round(sum(peer)/len(peer),2) if peer else None,
                "peer_gap_mean_pct_points":round(sum(div)/len(div),2) if div else None,
                "peer_active_mean_pct":round(sum(float(r["peer_active_ratio"]) for r in rows if r.get("peer_active_ratio") is not None)/max(1,sum(1 for r in rows if r.get("peer_active_ratio") is not None)),2) if any(r.get("peer_active_ratio") is not None for r in rows) else None,
                "ping_ok_pct":round(sum(1 for r in rows if str(r.get("reachability_state") or "")=="OK")*100.0/n,2),
                "path_ok_pct":round(sum(1 for r in rows if str(r.get("path_state") or "")=="OK")*100.0/n,2),
                "service_active_pct":round(sum(1 for r in rows if str(r.get("service_state") or "")=="ACTIVE")*100.0/n,2),
                "service_silent_critical_pct":round(sum(1 for r in rows if str(r.get("service_state") or "")=="SILENT_CRITICAL")*100.0/n,2),
                "quality":quality,
            }
        try:
            for ent in names:
                sem=sem_map.get(ent) or {}; cases=case_map.get(ent) or []; fbs=fb_map.get(ent) or []
                fault=None
                for h in (sem.get("lifecycle_history") or []):
                    st=(str(h.get("canonical_status") or "")+" "+str(h.get("lifecycle_state") or "")).upper()
                    if "FAULT" not in st: continue
                    hs=self._period_event_dt(h.get("valid_from")); he=self._period_event_dt(h.get("valid_to"),True) or selected_end
                    if hs and he and he>=selected_start and hs<=selected_end: fault=(h,hs,he)
                recovery_fb=next((x for x in reversed(fbs) if str(x.get("event_type") or "").upper()=="RECOVERY" and x.get("event_time")),None)
                intervention_fb=next((x for x in reversed(fbs) if str(x.get("event_type") or "").upper()=="INTERVENTION" and x.get("event_time")),None)
                cause_fb=next((x for x in reversed(fbs) if str(x.get("event_type") or "").upper()=="CAUSE"),None)
                human_recovery_dt=self._period_event_dt((recovery_fb or {}).get("event_time"))
                if human_recovery_dt is None and str(sem.get("lifecycle_state") or "").upper()=="RECOVERED_CONFIRMED":
                    human_recovery_dt=self._period_event_dt(sem.get("effective_from"))
                event_start=fault[1] if fault else None
                event_end=(human_recovery_dt or (fault[2] if fault else None))
                event_source="LIFECYCLE" if fault else None
                telemetry_recovery_dt=None;telemetry_recovery_pct=None;telemetry_recovery_mode=None;telemetry_recovery_gap_s=None

                # Para ausência de ocupação, a cronologia primária vem do raw occupancy_pct.
                # Service-state SILENT_* é evidência complementar, não substitui o 0% observado.
                auto_eps=[e for e in ((selected_map.get(ent) or {}).get("zero_occupancy_episodes") or []) if float(e.get("duration_min") or 0)>=1.0]
                auto_clusters=(selected_map.get(ent) or {}).get("zero_occupancy_incident_clusters") or self._gateway_zero_occupancy_clusters(auto_eps)
                primary_cluster=max(auto_clusters,key=lambda c:(float(c.get("zero_duration_min") or 0),str(c.get("start") or ""))) if auto_clusters else None
                primary_auto=max(auto_eps,key=lambda e:(float(e.get("duration_min") or 0),str(e.get("start") or ""))) if auto_eps else None
                if not event_start and (primary_cluster or primary_auto):
                    source=primary_cluster or primary_auto
                    event_start=self._period_event_dt(source.get("start"))
                    if primary_cluster:
                        telemetry_recovery_dt=self._period_event_dt(primary_cluster.get("final_recovery_ts"))
                        telemetry_recovery_pct=primary_cluster.get("final_recovery_pct")
                        telemetry_recovery_mode=primary_cluster.get("final_recovery_mode") or "OPEN"
                        telemetry_recovery_gap_s=primary_cluster.get("final_recovery_gap_s")
                        event_end=telemetry_recovery_dt or self._period_event_dt(primary_cluster.get("last_zero_ts"))
                    else:
                        telemetry_recovery_dt=self._period_event_dt(primary_auto.get("recovery_ts") or primary_auto.get("first_positive_after_gap_ts"))
                        telemetry_recovery_pct=primary_auto.get("recovery_pct") if primary_auto.get("recovery_ts") else primary_auto.get("first_positive_after_gap_pct")
                        telemetry_recovery_mode="CONTIGUOUS" if primary_auto.get("recovery_ts") else ("AFTER_GAP" if primary_auto.get("first_positive_after_gap_ts") else "OPEN")
                        telemetry_recovery_gap_s=primary_auto.get("recovery_gap_s")
                        event_end=telemetry_recovery_dt or self._period_event_dt(primary_auto.get("last_zero_ts"))
                    event_source="RAW_ZERO_OCCUPANCY_CLUSTER" if primary_cluster else "RAW_ZERO_OCCUPANCY"

                # Caso operacional pode fornecer horários aproximados mesmo sem lifecycle/raw suficiente.
                primary_case=cases[-1] if cases else None
                if not event_start and primary_case:
                    d=str(primary_case.get("observed_date") or "")
                    t=str((primary_case.get("observations") or {}).get("observed_start_approx") or "")
                    if d and t: event_start=self._period_event_dt(d+"T"+t);event_source="KNOWLEDGE_CASE"
                if not human_recovery_dt and primary_case:
                    d=str(primary_case.get("observed_date") or "")
                    t=str((primary_case.get("observations") or {}).get("recovery_observed_approx") or "")
                    if d and t: human_recovery_dt=self._period_event_dt(d+"T"+t)
                if not event_start and not human_recovery_dt and not fbs and not cases and not (sem.get("lifecycle_history") or []):
                    continue
                rec={"event_start":event_start.isoformat() if event_start else None,"event_end":event_end.isoformat() if event_end else None,
                     "event_source":event_source,
                     "recovery_boundary":human_recovery_dt.isoformat() if human_recovery_dt else None,
                     "telemetry_recovery_at":telemetry_recovery_dt.isoformat() if telemetry_recovery_dt else None,
                     "telemetry_recovery_pct":round(float(telemetry_recovery_pct),2) if telemetry_recovery_pct is not None else None,
                     "telemetry_recovery_mode":telemetry_recovery_mode,
                     "telemetry_recovery_gap_s":telemetry_recovery_gap_s,
                     "zero_incident_cluster":primary_cluster if 'primary_cluster' in locals() else None,
                     "fault_precision":(fault[0].get("precision") if fault else None),
                     "cause_feedback":cause_fb,"intervention_feedback":intervention_fb,"recovery_feedback":recovery_fb,
                     "before":None,"before_first_hour":None,"before_last_hour":None,"during":None,"after":None,
                     "first_activity_after_recovery":None,"first_anomaly_observed":None}
                if event_start:
                    # A história pré-evento é sempre 2H reais antes do evento, mesmo que
                    # anteceda a janela selecionada. Isso é contexto histórico READ ONLY.
                    rec["before"]=basic(ent,event_start-timedelta(hours=2),event_start)
                    rec["before_first_hour"]=basic(ent,event_start-timedelta(hours=2),event_start-timedelta(hours=1))
                    rec["before_last_hour"]=basic(ent,event_start-timedelta(hours=1),event_start)
                    rec["during"]=basic(ent,event_start,event_end or min(datetime.now().astimezone(),event_start+timedelta(hours=2)))
                    try:
                        rr=con.execute(f"""SELECT ts,occupancy_pct,peer_median_occupancy,reachability_state,service_state FROM gateway_health_sample_v5
                           WHERE instancia=? AND UPPER(entity)=? AND ts>=? AND ts<=? AND {op_clause} AND
                           (COALESCE(occupancy_pct,0)<=0 OR service_state IN ('SILENT_WARN','SILENT_CRITICAL','SILENT_SUSPECT'))
                           ORDER BY ts LIMIT 1""",(str(instancia),ent,event_start.isoformat(),(event_end or selected_end).isoformat(),*op_params)).fetchone()
                        if rr: rec["first_anomaly_observed"]=dict(rr)
                    except Exception: pass
                recovery_for_after=telemetry_recovery_dt or human_recovery_dt
                if recovery_for_after:
                    rec["after"]=basic(ent,recovery_for_after,recovery_for_after+timedelta(hours=2))
                    try:
                        rr=con.execute(f"""SELECT ts,occupancy_pct,peer_median_occupancy,reachability_state,service_state FROM gateway_health_sample_v5
                           WHERE instancia=? AND UPPER(entity)=? AND ts>=? AND ts<=? AND {op_clause} AND occupancy_pct>0 ORDER BY ts LIMIT 1""",
                           (str(instancia),ent,recovery_for_after.isoformat(),(recovery_for_after+timedelta(hours=2)).isoformat(),*op_params)).fetchone()
                        if rr: rec["first_activity_after_recovery"]=dict(rr)
                    except Exception: pass
                out[ent]=rec
            return out
        finally:
            con.close()

    def gateway_balance_report(self, instancia):
        return (self._gateway_balance_reader(instancia) if self._gateway_balance_reader else
                {"status": "EM_INICIALIZACAO", "detected": False, "low_gateways": []})

    def gateway_activity_report(self, instancia):
        return self._gateway_activity_reader(instancia) if self._gateway_activity_reader else {}

    def gateway_balance_inputs(self):
        """Bounded read-only evidence for background learning, independent of browsers.

        No scan of the multi-GiB history and no dependency on the Gateway Shadow.
        Last Calls is a partial observation of routing, never the configured route.
        """
        from toi_operational_radar_v1 import epoch, LOCAL_ZONE
        now = time.time()
        latest = {}
        with closing(self.connect()) as conn:
            if self.table_exists(conn, "queue_snapshot"):
                for row in conn.execute("""SELECT ts,instancia,codigo,nome,p135_pct
                        FROM queue_snapshot ORDER BY rowid DESC LIMIT ?""", (QUEUE_DISCOVERY_TAIL_ROWS,)):
                    key = (str(row["instancia"]), str(row["codigo"]))
                    if key not in latest:
                        latest[key] = dict(row)
            instances = set(str(x) for x in (self.policy.get("instancias") or {})) | {str(x) for x in range(1, 7)}
            instances.update(key[0] for key in latest)
            if self.table_exists(conn, "infra_snapshot"):
                instances.update(str(row[0]) for row in conn.execute(
                    "SELECT instancia FROM infra_snapshot ORDER BY rowid DESC LIMIT 1024"))
        document = {"snapshots": [], "queues": {}, "routes": {}, "semantics": {}, "operational": {}}
        local_now = datetime.fromtimestamp(now, LOCAL_ZONE)
        for inst in sorted(instances)[:32]:
            snapshot = self.gateway_current_snapshot(inst)
            if not snapshot.get("gateways") and not snapshot.get("sample_at"):
                continue
            document["snapshots"].append(snapshot)
            document["semantics"][inst] = load_gateway_semantic_reference(inst, [g.get("entity", "") for g in snapshot["gateways"]]).get("gateways", [])
            document["operational"][inst] = _inside_operational_dt(local_now, load_operational_window(inst))
        for (inst, code), queue in latest.items():
            target = float(self.queue_policy(inst, code).get("target_s", 300))
            queue["max_age_s"] = max(45, target+30)
            document["queues"].setdefault(inst, []).append(queue)
        # Only investigate routing when occupancy has a potentially relevant contrast.
        suspect = {x["instancia"] for x in document["snapshots"] if
                   any(parse_portal_number(g.get("occupancy"), 101) <= 25 for g in x["gateways"]) and
                   sum(parse_portal_number(g.get("occupancy"), -1) >= 80 for g in x["gateways"]) >= 2}
        if suspect:
            with closing(self.connect()) as conn:
                if self.table_exists(conn, "lastcalls_snapshot"):
                    byte_budget = 4*1024*1024
                    rows = conn.execute("""SELECT ts,instancia,codigo,
                        CASE WHEN length(raw_json)<=524288 THEN raw_json ELSE NULL END AS raw_json
                        FROM lastcalls_snapshot ORDER BY rowid DESC LIMIT 120""")
                    for row in rows:
                        inst, code = str(row["instancia"]), str(row["codigo"])
                        stamp = epoch(row["ts"])
                        raw = row["raw_json"] or ""
                        if inst not in suspect or stamp is None or not -5 <= now-stamp <= 600:
                            continue
                        byte_budget -= len(raw)
                        if byte_budget < 0:
                            break
                        if code in document["routes"].get(inst, {}):
                            continue
                        try:
                            calls = json.loads(raw).get("lastCallsList", [])
                            names = sorted({str(call.get("NmGateway") or "").strip().upper()
                                            for call in calls[:1000] if isinstance(call, dict) and call.get("NmGateway")})
                        except (ValueError, AttributeError, TypeError):
                            continue
                        signals = {}
                        seen_calls = set()
                        for call in calls[:1000]:
                            if not isinstance(call, dict):
                                continue
                            name = str(call.get("NmGateway") or "").strip().upper()
                            identity = str(call.get("IdCallIPBX") or "")
                            if not name or identity and identity in seen_calls:
                                continue
                            if identity:
                                seen_calls.add(identity)
                            signal = signals.setdefault(name, {"calls_observed": 0, "sip_responses": {}, "trunk_codes": []})
                            signal["calls_observed"] += 1
                            sip = re.search(r"\b[1-6][0-9]{2}\b", str(call.get("NmStatusSip") or ""))
                            if sip:
                                code_sip = sip.group()
                                signal["sip_responses"][code_sip] = signal["sip_responses"].get(code_sip, 0)+1
                            trunk = str(call.get("CodeTrunkGroup") or "").strip()
                            if trunk and trunk not in signal["trunk_codes"]:
                                signal["trunk_codes"].append(trunk)
                        document["routes"].setdefault(inst, {})[code] = {
                            "gateways": names, "signals": signals, "sample_at": row["ts"],
                            "source": "LAST_CALLS_BOUNDED", "complete": False}
        return document

    def gateway_freshness(self, instancia, stamp):
        infra = self.instance_policy(instancia).get('infra') or {}
        target = parse_portal_number(infra.get('gateway_target_s'), 15)
        maximum = parse_portal_number(infra.get('gateway_max_age_s'), max(45, target*2))
        age = self._age_seconds(stamp)
        return {'sample_at': stamp, 'age_s': age, 'target_s': target, 'max_age_s': maximum,
                'status': 'SEM_DADOS' if age is None else 'LIVE' if age <= maximum else 'ATRASADO'}

    def gateway_current_snapshot(self, instancia):
        """Same coherent LIVE inventory as the overview card, independent of Shadow."""
        try:
            inventory = self.infra(instancia, 15)
            stamp = inventory.get("meta", {}).get("gateway_ts")
            snapshot = {"instancia": str(instancia), "source": "LIVE_INFRA_SNAPSHOT",
                    **self.gateway_freshness(instancia, stamp),
                    "gateways": inventory.get("gateways", [])}
        except sqlite3.Error:
            snapshot = {"instancia": str(instancia), "source": "LIVE_INFRA_SNAPSHOT",
                    "sample_at": None, "age_s": None, "status": "INDISPONIVEL", "gateways": []}
        snapshot['source_reconciliation'] = self.gateway_source_reconciliation(instancia, snapshot)
        snapshot['gateway_activity'] = self.gateway_activity_report(instancia)
        return snapshot

    def gateway_source_reconciliation(self, instancia, portal):
        monitor = (self._gateway_monitor.snapshot(instancia) if self._gateway_monitor else
                   {'status': 'EM_INICIALIZACAO', 'gateways': [], 'sample_at': None,
                    'sample_age_s': None, 'consulted_at': None, 'consultation_age_s': None})
        matches = {}
        for row in monitor['gateways']:
            matches.setdefault(gateway_monitor_name(row['entity'], instancia), []).append(row)
        rows, names = [], set()
        for gateway in portal.get('gateways', []):
            name = gateway_source_name(gateway.get('entity'))
            names.add(name)
            peers = matches.get(name, [])
            calls = peers[0]['calls'] if len(peers) == 1 else None
            idle = peers[0].get('idle_cycles') if len(peers) == 1 else None
            valid = parse_portal_number(gateway.get('busy'), None)
            flags = []
            if portal.get('status') != 'LIVE':
                flags.append('COLETA_PORTAL_ATRASADA')
            if len(peers) != 1 and monitor['status'] == 'CONSULTADO':
                flags.append('NOME_SEM_CORRESPONDENCIA' if not peers else 'NOME_AMBIGUO')
            if monitor['status'] == 'CONSULTADO' and calls is not None and valid == 0 and calls > 0:
                flags.append('MONITOR_COM_TRAFEGO_PORTAL_ZERO')
            if str(instancia) == '4' and name == 'GWS843-DC1' and calls is not None and calls > 0:
                flags.append('TRAFEGO_NO_STANDBY')
            rows.append({'entity': gateway.get('entity'), 'portal_valid_busy': valid,
                         'monitor_calls': calls, 'monitor_idle_cycles': idle, 'flags': flags})
        unmatched = [row for row in monitor['gateways'] if gateway_monitor_name(row['entity'], instancia) not in names]
        return {'monitor': {key: value for key, value in monitor.items() if key != 'gateways'},
                'rows': rows, 'unmatched_monitor': unmatched,
                'portal_sample_at': portal.get('sample_at'), 'portal_age_s': portal.get('age_s'),
                'semantics': 'Canais válidos ocupados do Portal e chamadas por troncos do Monitor são contadores distintos. '
                             'Zero canais válidos não comprova ausência de qualquer tráfego. '
                             'A hora da coleta do Monitor não é informada pela fonte. '
                             'Nomes diferentes não são associados automaticamente.'}

    def gateway_analytics_bundle(self, instancia, period="15m", start=None, end=None, codigo=None, detail="full", entity=None, cache_bypass=False):
        """Gateway analytics H2-C1.

        summary: abre o modal rapidamente, sem montar história completa por equipamento.
        entity: carrega a história detalhada de um único Gateway sob demanda.
        full: compatibilidade com consumidores anteriores.
        """
        latest_ts=None
        if GATEWAY_SHADOW_DB.exists():
            try:
                uri=f"file:{GATEWAY_SHADOW_DB.as_posix()}?mode=ro";con=sqlite3.connect(uri,uri=True,timeout=3)
                try:
                    row=con.execute("SELECT MAX(last_seen_ts) FROM gateway_state_v5 WHERE instancia=?",(str(instancia),)).fetchone()
                    latest_ts=row[0] if row else None
                except Exception:
                    row=con.execute("SELECT MAX(ts) FROM gateway_health_sample_v5 WHERE instancia=?",(str(instancia),)).fetchone();latest_ts=row[0] if row else None
                con.close()
            except Exception:latest_ts=None
        current_snapshot = self.gateway_current_snapshot(instancia)
        # A stale Shadow must not silently move a relative period back by hours.
        anchor=datetime.now().astimezone()
        age_s=self._age_seconds(latest_ts)
        p=str(period or "15m").lower()
        if p=="custom":
            if not start or not end:raise ValueError("Período personalizado exige start e end.")
            selected_start=datetime.fromisoformat(start);selected_end=datetime.fromisoformat(end)
            if selected_end<=selected_start:raise ValueError("Fim do período personalizado deve ser posterior ao início.")
            selected_label="Personalizado"
        else:
            minutes=PERIOD_MAP.get(p,15);selected_end=anchor;selected_start=anchor-timedelta(minutes=minutes)
            selected_label={"5m":"5 min","15m":"15 min","30m":"30 min","1h":"60 min","2h":"2H","6h":"6H","1d":"24H","24h":"24H","7d":"7 dias","14d":"14 dias","30d":"30 dias"}.get(p,p)
        detail=str(detail or "full").lower();entity_name=str(entity or "").strip().upper() or None
        op_window=load_operational_window(instancia,codigo)
        requested_minutes=max(1.0,(selected_end-selected_start).total_seconds()/60.0)
        requested_operational_minutes=round(_operational_requested_seconds(selected_start,selected_end,op_window)/60.0,2)
        cache_slot=(selected_end if p=="custom" else selected_end.replace(second=0,microsecond=0)).isoformat()
        cache_key=("gw",str(instancia),str(codigo or ""),p,str(start or ""),str(end or ""),cache_slot,detail,entity_name or "")
        cache_ttl=ANALYTICS_CACHE_TTL_LONG_S if requested_minutes>GATEWAY_LONG_WINDOW_MINUTES else ANALYTICS_CACHE_TTL_SHORT_S
        cached=None if cache_bypass else self._cache_get(self._gateway_analytics_cache,cache_key,cache_ttl)
        if cached is not None:
            return {**cached,"current_snapshot":current_snapshot,"shadow_age_s":age_s,
                    "gateway_investigation":self.gateway_balance_report(instancia)}

        if detail=="summary":
            selected=self.gateway_shadow_summary(instancia,selected_start,selected_end,operational_window=op_window)
            quick={};story_evidence={}
        elif detail=="entity":
            if not entity_name:raise ValueError("detail=entity exige entity.")
            selected=self.gateway_shadow_analytics(instancia,selected_start,selected_end,entities=[entity_name],operational_window=op_window)
            quick={}
            for key,minutes in (("5m",5),("15m",15),("1h",60),("2h",120)):
                quick[key]=self.gateway_shadow_analytics(instancia,selected_end-timedelta(minutes=minutes),selected_end,entities=[entity_name],operational_window=op_window)
        else:
            selected=self.gateway_shadow_analytics(instancia,selected_start,selected_end,operational_window=op_window)
            quick={}
            for key,minutes in (("5m",5),("15m",15),("1h",60),("2h",120)):
                quick[key]=self.gateway_shadow_analytics(instancia,selected_end-timedelta(minutes=minutes),selected_end,operational_window=op_window)

        entities_selected=(selected.get("entities") or []) if isinstance(selected,dict) else []
        cov_values=[float((x.get("quality") or {}).get("coverage_pct")) for x in entities_selected if (x.get("quality") or {}).get("coverage_pct") is not None]
        first_values=[x.get("first_ts") for x in entities_selected if x.get("first_ts")];last_values=[x.get("last_ts") for x in entities_selected if x.get("last_ts")]
        observed_start=min(first_values) if first_values else None;observed_end=max(last_values) if last_values else None
        observed_span_min=0.0
        try:observed_span_min=max(0.0,(datetime.fromisoformat(observed_end)-datetime.fromisoformat(observed_start)).total_seconds()/60.0) if observed_start and observed_end else 0.0
        except Exception:pass
        cov_median=round(float(statistics.median(cov_values)),2) if cov_values else 0.0
        coverage_summary={
            "requested_minutes":round(requested_minutes,2),"requested_calendar_minutes":round(requested_minutes,2),"requested_operational_minutes":requested_operational_minutes,
            "observed_span_minutes":round(observed_span_min,2),"observed_start":observed_start,"observed_end":observed_end,
            "entity_coverage_min_pct":round(min(cov_values),2) if cov_values else 0.0,"entity_coverage_median_pct":cov_median,"entity_coverage_max_pct":round(max(cov_values),2) if cov_values else 0.0,
            "complete_entities":sum(1 for v in cov_values if v>=95.0),"partial_entities":sum(1 for v in cov_values if v<95.0),"entity_count":len(entities_selected),
            "complete":bool(cov_values) and all(v>=95.0 for v in cov_values),"status":"COMPLETE" if cov_values and all(v>=95.0 for v in cov_values) else ("PARTIAL" if cov_values else "NO_DATA"),
            "scope":"OPERATIONAL_WINDOW","operational_window":op_window,
        }
        knowledge=load_operational_knowledge_cases(instancia,entity_name if detail=="entity" else None)
        feedback=load_operational_feedback(instancia=instancia,entity=entity_name if detail=="entity" else None,start=selected_start.isoformat(),end=selected_end.isoformat())
        semantic_reference=load_gateway_semantic_reference(instancia,[g.get("entity", "") for g in current_snapshot["gateways"]])
        story_evidence={}
        if detail in ("entity","full") and selected.get("available") and entities_selected:
            story_evidence=self.gateway_story_evidence(instancia,selected_start,selected_end,entities_selected,semantic_reference,knowledge,feedback,operational_window=op_window)
        payload={
            "schema":"toi_gateway_temporal_analytics_v1_3_h2c1","generated_at":datetime.now().astimezone().isoformat(timespec="seconds"),"instancia":str(instancia),"codigo":str(codigo or ""),"detail":detail,
            "mode":"READ_ONLY_GATEWAY_SHADOW_ON_DEMAND","source":str(GATEWAY_SHADOW_DB),"anchor_ts":selected_end.isoformat(),"shadow_latest_ts":latest_ts,"shadow_age_s":round(age_s,2) if age_s is not None else None,
            "operational_window":op_window,"current_snapshot":current_snapshot,
            "gateway_investigation":self.gateway_balance_report(instancia),
            "selected_period":{"period":p,"label":selected_label,"start":selected_start.isoformat(),"end":selected_end.isoformat(),"requested_minutes":round(requested_minutes,2),"requested_operational_minutes":requested_operational_minutes,"coverage_summary":coverage_summary,"analytics":selected},
            "quick_windows":quick,"operational_knowledge":knowledge,"operational_feedback":feedback,"semantic_reference":semantic_reference,"operational_story_evidence":story_evidence,
            "semantics":{
                "operational_window":"Ocupação, atividade, ausência funcional e comparação entre Gateways são avaliadas somente dentro da janela operacional configurada. Fora dela, esses períodos não contam como zero nem como falha.",
                "technical_24x7":"Ping, perda, latência e caminho de rede permanecem evidência técnica 24x7 e são apresentados separadamente da leitura operacional.",
                "machine_vs_context":"Estado técnico e contexto operacional/Ground Truth são dimensões separadas.",
                "historical_completeness":"Cobertura do período é independente da freshness LIVE. Janela parcial representa somente a evidência observada.",
                "lazy_detail":"O resumo abre primeiro; a história completa e os comparativos de cada Gateway são carregados somente quando o usuário expande o equipamento.",
                "ml_status":"Features temporais preparadas para ML Shadow; nenhuma predição desta API altera diagnóstico, alerta, chamado ou ação.",
            },
        }
        return self._cache_put(self._gateway_analytics_cache,cache_key,payload)

    @staticmethod
    def _envelope_series(rows):
        grouped = {}
        for row in rows or []:
            if row.get("occupancy") is None:
                continue
            grouped.setdefault(row.get("ts"), []).append(float(row.get("occupancy")))
        return [
            {"ts": ts, "occupancy": round(max(vals), 3)}
            for ts, vals in sorted(grouped.items()) if vals
        ]

    @staticmethod
    def _series_cadence_seconds(rows):
        dts = []
        for a, b in zip(rows, rows[1:]):
            try:
                dt = (
                    datetime.fromisoformat(b["ts"])
                    - datetime.fromisoformat(a["ts"])
                ).total_seconds()
            except Exception:
                continue
            if 0 < dt <= 600:
                dts.append(dt)
        if not dts:
            return None
        try:
            return float(statistics.median(dts))
        except Exception:
            return None

    @staticmethod
    def _coverage_from_times(times, start_dt, end_dt, sample_count=None):
        """Gap-aware coverage from already parsed/sorted datetimes. H2-C2 compute hardening."""
        total_s = max(1.0, (end_dt - start_dt).total_seconds())
        if not times:
            return {
                "requested_minutes": round(total_s / 60.0, 2),
                "span_minutes": 0.0, "span_coverage_pct": 0.0, "data_coverage_pct": 0.0,
                "complete": False, "samples": 0, "first_ts": None, "last_ts": None,
                "median_cadence_s": None, "gap_threshold_s": None, "gap_count": 0,
                "largest_gap_min": 0.0, "days_observed": 0,
            }
        ordered = sorted(times)
        deltas = [(b-a).total_seconds() for a,b in zip(ordered, ordered[1:]) if b > a]
        valid = [d for d in deltas if 0 < d <= 600]
        try: cadence = float(statistics.median(valid)) if valid else None
        except Exception: cadence = None
        gap_threshold = min(600.0, max(60.0, (cadence or 60.0) * 3.0))
        gaps = [x for x in deltas if x > gap_threshold]
        covered_s = sum(min(x, gap_threshold) for x in deltas)
        covered_s += min(gap_threshold, cadence or gap_threshold)
        span_s = max(0.0, (ordered[-1] - ordered[0]).total_seconds())
        span_pct = min(100.0, span_s / total_s * 100.0)
        data_pct = min(100.0, covered_s / total_s * 100.0)
        return {
            "requested_minutes": round(total_s / 60.0, 2),
            "span_minutes": round(span_s / 60.0, 2),
            "span_coverage_pct": round(span_pct, 2),
            "data_coverage_pct": round(data_pct, 2),
            "coverage_pct": round(data_pct, 2),
            "complete": data_pct >= 95.0,
            "samples": int(sample_count if sample_count is not None else len(ordered)),
            "first_ts": ordered[0].isoformat(), "last_ts": ordered[-1].isoformat(),
            "median_cadence_s": round(cadence, 2) if cadence is not None else None,
            "gap_threshold_s": round(gap_threshold, 2), "gap_count": len(gaps),
            "largest_gap_min": round((max(gaps) if gaps else 0.0) / 60.0, 2),
            "days_observed": len({x.date().isoformat() for x in ordered}),
        }

    @staticmethod
    def _coverage_from_sorted_times(ordered, start_dt, end_dt, sample_count=None):
        """Gap-aware coverage when caller already guarantees chronological order."""
        total_s = max(1.0, (end_dt - start_dt).total_seconds())
        if not ordered:
            return {
                "requested_minutes": round(total_s / 60.0, 2),
                "span_minutes": 0.0, "span_coverage_pct": 0.0, "data_coverage_pct": 0.0,
                "complete": False, "samples": 0, "first_ts": None, "last_ts": None,
                "median_cadence_s": None, "gap_threshold_s": None, "gap_count": 0,
                "largest_gap_min": 0.0, "days_observed": 0,
            }
        deltas = [(b-a).total_seconds() for a,b in zip(ordered, ordered[1:]) if b > a]
        valid = [d for d in deltas if 0 < d <= 600]
        try: cadence = float(statistics.median(valid)) if valid else None
        except Exception: cadence = None
        gap_threshold = min(600.0, max(60.0, (cadence or 60.0) * 3.0))
        gaps = [x for x in deltas if x > gap_threshold]
        covered_s = sum(min(x, gap_threshold) for x in deltas)
        covered_s += min(gap_threshold, cadence or gap_threshold)
        span_s = max(0.0, (ordered[-1] - ordered[0]).total_seconds())
        span_pct = min(100.0, span_s / total_s * 100.0)
        data_pct = min(100.0, covered_s / total_s * 100.0)
        return {
            "requested_minutes": round(total_s / 60.0, 2),
            "span_minutes": round(span_s / 60.0, 2),
            "span_coverage_pct": round(span_pct, 2),
            "data_coverage_pct": round(data_pct, 2),
            "coverage_pct": round(data_pct, 2),
            "complete": data_pct >= 95.0,
            "samples": int(sample_count if sample_count is not None else len(ordered)),
            "first_ts": ordered[0].isoformat(), "last_ts": ordered[-1].isoformat(),
            "median_cadence_s": round(cadence, 2) if cadence is not None else None,
            "gap_threshold_s": round(gap_threshold, 2), "gap_count": len(gaps),
            "largest_gap_min": round((max(gaps) if gaps else 0.0) / 60.0, 2),
            "days_observed": len({x.date().isoformat() for x in ordered}),
        }

    @staticmethod
    def _capacity_group_profiles(rows, current_rows, window_start_dt, window_end_dt):
        """Build capacity profiles with one grouping pass and shared per-entity scans.

        H2-C2 compute hardening: avoids reparsing/rescanning the same entity independently
        for durations, episodes, coverage and trend. Output semantics match capacity_analysis.
        """
        def num(value):
            if value is None or value == "": return None
            try: n=float(value)
            except (TypeError,ValueError): return None
            if n != n or n in (float("inf"),float("-inf")): return None
            return n
        def p95(values):
            if not values: return None
            ordered=sorted(values)
            idx=min(len(ordered)-1,max(0,round((len(ordered)-1)*0.95)))
            return ordered[idx]
        by_entity={}
        disorder=set()
        last_by_entity={}
        for row in rows or []:
            ent=str(row.get("entity") or "").strip()
            if not ent: continue
            try: ts=datetime.fromisoformat(str(row.get("ts") or ""))
            except Exception: continue
            v=num(row.get("occupancy"))
            prev=last_by_entity.get(ent)
            if prev is not None and ts < prev: disorder.add(ent)
            last_by_entity[ent]=ts
            by_entity.setdefault(ent,[]).append((ts,v,row))
        current_map={str(x.get("entity") or "").strip():x for x in (current_rows or []) if str(x.get("entity") or "").strip()}
        all_entities=list(dict.fromkeys(list(current_map.keys())+list(by_entity.keys())))
        out=[]
        for ent in all_entities:
            prs=by_entity.get(ent,[])
            if ent in disorder: prs.sort(key=lambda x:x[0])
            vals=[x[1] for x in prs if x[1] is not None]
            cur=current_map.get(ent) or {}; current=num(cur.get("occupancy"))
            if current is None and vals: current=vals[-1]
            pv=p95(vals); peak=max(vals) if vals else None; mean=(sum(vals)/len(vals)) if vals else None
            peak_pair=max((x for x in prs if x[1] is not None),key=lambda x:x[1],default=None)
            peak_ts=peak_pair[0].isoformat() if peak_pair is not None else None
            times=[x[0] for x in prs]
            deltas=[(b-a).total_seconds() for a,b in zip(times,times[1:]) if b>a]
            valid=[d for d in deltas if 0<d<=600]
            cadence=float(statistics.median(valid)) if valid else 60.0
            cap_s=min(600.0,max(60.0,cadence*3.0))
            mins75=mins85=mins90=0.0
            # Durations use the left sample exactly as the prior implementation.
            for a,b in zip(prs,prs[1:]):
                dt=(b[0]-a[0]).total_seconds()
                if dt<=0 or a[1] is None: continue
                eff=min(dt,cap_s); v=a[1]
                if v>=75: mins75+=eff
                if v>=85: mins85+=eff
                if v>=90: mins90+=eff
            ep_state={85:{"start":None,"count":0,"longest":0.0},90:{"start":None,"count":0,"longest":0.0}}
            recent_sum=recent_n=prev_sum=prev_n=0
            recent_start=(window_end_dt-timedelta(minutes=15)) if window_end_dt else None
            prev_start=(window_end_dt-timedelta(minutes=60)) if window_end_dt else None
            prev_ts=None
            def close_ep(state,end_dt):
                st=state.get("start")
                if st is None:return
                state["count"]+=1
                state["longest"]=max(state["longest"],max(0.0,(end_dt-st).total_seconds())/60.0)
                state["start"]=None
            for ts,v,_row in prs:
                if prev_ts is not None and (ts-prev_ts).total_seconds()>cap_s:
                    for state in ep_state.values():
                        if state["start"] is not None: close_ep(state,prev_ts+timedelta(seconds=cadence))
                for th,state in ep_state.items():
                    active=(v is not None and v>=th)
                    if active and state["start"] is None: state["start"]=ts
                    elif not active and state["start"] is not None: close_ep(state,ts)
                if v is not None and window_end_dt:
                    if ts>=recent_start: recent_sum+=v;recent_n+=1
                    elif prev_start<=ts<recent_start: prev_sum+=v;prev_n+=1
                prev_ts=ts
            if prs:
                final_dt=prs[-1][0]+timedelta(seconds=cadence)
                for state in ep_state.values():
                    if state["start"] is not None: close_ep(state,final_dt)
            active_count=sum(1 for v in vals if v>0)
            active_samples_pct=(active_count*100.0/len(vals)) if vals else None
            zero_samples_pct=(100.0-active_samples_pct) if active_samples_pct is not None else None

            # H3 Executive Semantics: zero-occupancy lifecycle per entity.
            # Zero occupancy is an observed condition, not an automatic fault diagnosis.
            # Gaps terminate continuity: missing data never becomes silence and a recovery
            # is only declared when a positive sample follows a zero run within cadence tolerance.
            current_zero_since=None
            current_zero_duration_min=None
            current_zero_left_censored=False
            current_zero_samples=0
            last_zero_episode_since=None
            last_zero_episode_end=None
            last_zero_episode_duration_min=None
            last_zero_episode_left_censored=False
            last_zero_recovery_at=None
            last_zero_recovery_pct=None
            zero_episodes_completed=0
            valid_prs=[(ts,v) for ts,v,_row in prs if v is not None]

            # Completed zero episodes, including the recovery observation. Only episodes
            # of at least 1 observed minute are promoted to the executive lifecycle fields.
            completed_zero=[]
            run_start_idx=None
            for vi,(ts,v) in enumerate(valid_prs):
                if vi>0:
                    gap=(ts-valid_prs[vi-1][0]).total_seconds()
                    if gap<=0 or gap>cap_s:
                        # A gap closes the inference chain. Never infer recovery across it.
                        run_start_idx=None
                if v <= 0:
                    if run_start_idx is None:
                        run_start_idx=vi
                    continue
                if run_start_idx is not None:
                    start_ts=valid_prs[run_start_idx][0]
                    duration_min=max(0.0,(ts-start_ts).total_seconds())/60.0
                    left_censored=False
                    if run_start_idx==0 and window_start_dt is not None:
                        left_censored=(start_ts-window_start_dt).total_seconds() <= cap_s
                    completed_zero.append({
                        "since":start_ts,
                        "end":ts,
                        "duration_min":duration_min,
                        "left_censored":left_censored,
                        "recovery_pct":v,
                        "zero_samples":vi-run_start_idx,
                    })
                    run_start_idx=None

            material_completed=[ep for ep in completed_zero if ep["duration_min"] >= 1.0]
            zero_episodes_completed=len(material_completed)
            if material_completed:
                ep=material_completed[-1]
                last_zero_episode_since=ep["since"].isoformat()
                last_zero_episode_end=ep["end"].isoformat()
                last_zero_episode_duration_min=round(ep["duration_min"],2)
                last_zero_episode_left_censored=bool(ep["left_censored"])
                last_zero_recovery_at=ep["end"].isoformat()
                last_zero_recovery_pct=round(float(ep["recovery_pct"]),2)

            # Current zero episode remains separately exposed when the entity is still at 0%.
            if current is not None and current <= 0 and valid_prs:
                last_ts,last_v=valid_prs[-1]
                if last_v <= 0:
                    zi=len(valid_prs)-1
                    while zi>0:
                        prev_ts,prev_v=valid_prs[zi-1]
                        this_ts,_this_v=valid_prs[zi]
                        gap=(this_ts-prev_ts).total_seconds()
                        if prev_v <= 0 and 0 < gap <= cap_s:
                            zi-=1
                            continue
                        break
                    zero_start=valid_prs[zi][0]
                    zero_end=last_ts
                    try:
                        cur_ts=datetime.fromisoformat(str(cur.get("ts") or ""))
                        if cur_ts >= zero_end and (cur_ts-zero_end).total_seconds() <= cap_s:
                            zero_end=cur_ts
                    except Exception:
                        pass
                    if window_end_dt and window_end_dt >= zero_end and (window_end_dt-zero_end).total_seconds() <= cap_s:
                        zero_end=window_end_dt
                    current_zero_since=zero_start.isoformat()
                    current_zero_duration_min=round(max(0.0,(zero_end-zero_start).total_seconds())/60.0,2)
                    current_zero_samples=len(valid_prs)-zi
                    if zi==0 and window_start_dt is not None:
                        current_zero_left_censored=(zero_start-window_start_dt).total_seconds() <= cap_s

            q=DataService._coverage_from_sorted_times(times,window_start_dt,window_end_dt,sample_count=len(prs)) if prs and window_start_dt and window_end_dt else {}
            trend_state="N/C";trend_delta=None
            if recent_n and prev_n:
                trend_delta=(recent_sum/recent_n)-(prev_sum/prev_n)
                trend_state="SUBINDO" if trend_delta>=5 else "ALIVIANDO" if trend_delta<=-5 else "ESTÁVEL"
            out.append({
                "entity":ent,"current_pct":round(current,2) if current is not None else None,
                "mean_pct":round(mean,2) if mean is not None else None,"p95_pct":round(pv,2) if pv is not None else None,
                "peak_pct":round(peak,2) if peak is not None else None,"peak_ts":peak_ts,"samples":len(vals),
                "headroom_p95_pct":round(max(0.0,100.0-pv),2) if pv is not None else None,
                "minutes_over_75":round(mins75/60.0,2),"minutes_over_85":round(mins85/60.0,2),"minutes_over_90":round(mins90/60.0,2),
                "episodes_over_85":ep_state[85]["count"],"episodes_over_90":ep_state[90]["count"],"longest_episode_over_85_min":round(ep_state[85]["longest"],2),
                "active_samples_pct":round(active_samples_pct,2) if active_samples_pct is not None else None,
                "zero_samples_pct":round(zero_samples_pct,2) if zero_samples_pct is not None else None,
                "current_zero_since":current_zero_since,
                "current_zero_duration_min":current_zero_duration_min,
                "current_zero_left_censored":bool(current_zero_left_censored),
                "current_zero_samples":int(current_zero_samples),
                "last_zero_episode_since":last_zero_episode_since,
                "last_zero_episode_end":last_zero_episode_end,
                "last_zero_episode_duration_min":last_zero_episode_duration_min,
                "last_zero_episode_left_censored":bool(last_zero_episode_left_censored),
                "last_zero_recovery_at":last_zero_recovery_at,
                "last_zero_recovery_pct":last_zero_recovery_pct,
                "zero_episodes_completed":int(zero_episodes_completed),
                "trend_15m_vs_prev45m":trend_state,"trend_delta_pct_points":round(trend_delta,2) if trend_delta is not None else None,
                "quality":q or {},"busy":cur.get("busy"),"total":cur.get("total"),
            })
        return out

    @classmethod
    def _coverage_with_gaps(cls, rows, start_dt, end_dt):
        times=[]
        for row in rows or []:
            try: times.append(datetime.fromisoformat(str(row.get("ts") or "")))
            except Exception: pass
        return cls._coverage_from_times(times, start_dt, end_dt, sample_count=len(rows or []))

    @staticmethod
    def _parsed_metric_series(rows, key):
        out=[]
        for row in rows or []:
            try:
                ts=datetime.fromisoformat(str(row.get("ts") or ""))
                value=float(row.get(key) or 0)
            except Exception:
                continue
            out.append((ts,value,row))
        out.sort(key=lambda x:x[0])
        return out

    @classmethod
    def _duration_over_threshold(cls, rows, key, threshold):
        parsed=cls._parsed_metric_series(rows,key)
        if not parsed: return 0.0
        deltas=[(b[0]-a[0]).total_seconds() for a,b in zip(parsed,parsed[1:]) if b[0]>a[0]]
        valid=[d for d in deltas if 0<d<=600]
        cadence=float(statistics.median(valid)) if valid else 60.0
        cap_s=min(600.0,max(60.0,cadence*3.0))
        seconds=0.0
        for a,b in zip(parsed,parsed[1:]):
            dt=(b[0]-a[0]).total_seconds()
            if a[1] >= threshold and dt>0:
                seconds += min(dt,cap_s)
        return round(seconds/60.0,2)

    @classmethod
    def _gauge_minutes(cls, rows, key):
        """Integral do gauge em valor*minuto. NÃO significa total de eventos."""
        parsed=cls._parsed_metric_series(rows,key)
        if not parsed: return 0.0
        deltas=[(b[0]-a[0]).total_seconds() for a,b in zip(parsed,parsed[1:]) if b[0]>a[0]]
        valid=[d for d in deltas if 0<d<=600]
        cadence=float(statistics.median(valid)) if valid else 60.0
        cap_s=min(600.0,max(60.0,cadence*3.0))
        total=0.0
        for a,b in zip(parsed,parsed[1:]):
            dt=(b[0]-a[0]).total_seconds()
            if dt>0: total += max(0.0,a[1])*min(dt,cap_s)/60.0
        return round(total,2)

    @classmethod
    def _episodes(cls, rows, key, threshold=0.0, limit=5000):
        parsed=cls._parsed_metric_series(rows,key)
        if not parsed: return []
        deltas=[(b[0]-a[0]).total_seconds() for a,b in zip(parsed,parsed[1:]) if b[0]>a[0]]
        valid=[d for d in deltas if 0<d<=600]
        cadence=float(statistics.median(valid)) if valid else 60.0
        cap_s=min(600.0,max(60.0,cadence*3.0))
        episodes=[];cur=None;prev_ts=None
        def close_episode(end_dt):
            nonlocal cur
            if cur is None:return
            cur["end"]=end_dt.isoformat()
            cur["duration_min"]=round(max(0.0,(end_dt-datetime.fromisoformat(cur["start"])).total_seconds())/60.0,2)
            episodes.append(cur);cur=None
        for ts,value,row in parsed:
            if prev_ts is not None and (ts-prev_ts).total_seconds()>cap_s and cur is not None:
                close_episode(prev_ts+timedelta(seconds=cadence))
            active=value>threshold if threshold==0 else value>=threshold
            if active:
                if cur is None: cur={"start":ts.isoformat(),"end":ts.isoformat(),"max":value,"max_ts":ts.isoformat()}
                elif value>float(cur.get("max") or 0): cur["max"]=value;cur["max_ts"]=ts.isoformat()
            elif cur is not None:
                close_episode(ts)
            prev_ts=ts
        if cur is not None:
            close_episode(parsed[-1][0]+timedelta(seconds=cadence))
        return episodes[-limit:]

    @classmethod
    def _threshold_lifecycle(cls, rows, key, threshold=20.0, limit=500):
        """Lifecycle exato de episódios >= threshold, sem inventar resolução através de gaps.

        RESOLVIDO exige a primeira amostra subsequente < threshold dentro da continuidade observada.
        Se houver gap acima da tolerância enquanto o episódio está aberto, o episódio é encerrado
        como INTERROMPIDO_POR_GAP e resolved_at permanece N/C.
        """
        parsed=cls._parsed_metric_series(rows,key)
        if not parsed:return []
        deltas=[(b[0]-a[0]).total_seconds() for a,b in zip(parsed,parsed[1:]) if b[0]>a[0]]
        valid=[d for d in deltas if 0<d<=600]
        cadence=float(statistics.median(valid)) if valid else 60.0
        cap_s=min(600.0,max(60.0,cadence*3.0))
        out=[];cur=None;prev_ts=None
        def close(status, resolved_at=None):
            nonlocal cur
            if cur is None:return
            cur["status"]=status
            cur["resolved_at"]=resolved_at.isoformat() if resolved_at is not None else None
            end_dt=resolved_at if resolved_at is not None else datetime.fromisoformat(cur["last_critical_ts"])
            cur["end"]=end_dt.isoformat()
            cur["duration_min"]=round(max(0.0,(end_dt-datetime.fromisoformat(cur["start"])).total_seconds())/60.0,2)
            out.append(cur);cur=None
        for ts,value,row in parsed:
            if prev_ts is not None and (ts-prev_ts).total_seconds()>cap_s and cur is not None:
                close("INTERROMPIDO_POR_GAP")
            active=value>=float(threshold)
            if active:
                if cur is None:
                    cur={"start":ts.isoformat(),"last_critical_ts":ts.isoformat(),"max":value,"max_ts":ts.isoformat(),"threshold":float(threshold)}
                else:
                    cur["last_critical_ts"]=ts.isoformat()
                    if value>float(cur.get("max") or 0):cur["max"]=value;cur["max_ts"]=ts.isoformat()
            elif cur is not None:
                close("RESOLVIDO",ts)
            prev_ts=ts
        if cur is not None:close("ATIVO")
        return out[-limit:]

    @classmethod
    def _temporal_metric_bundle(cls, rows, start_dt, end_dt, stat_keys=(), duration_specs=(), episode_specs=(), gauge_keys=()):
        """Parse/sort timestamps once and derive multiple temporal metrics in one bounded pass.

        duration_specs / episode_specs are iterable pairs (metric_key, threshold).
        Semantics intentionally mirror _duration_over_threshold, _episodes and _gauge_minutes.
        """
        rows=rows or []
        stat_keys=tuple(stat_keys or ())
        duration_specs=tuple(duration_specs or ())
        episode_specs=tuple(episode_specs or ())
        gauge_keys=tuple(gauge_keys or ())
        values={k:[] for k in stat_keys}
        parsed=[]; disorder=False; prev_seen=None
        for row in rows:
            for k in stat_keys:
                v=row.get(k)
                if v is not None: values[k].append(v)
            try: ts=datetime.fromisoformat(str(row.get("ts") or ""))
            except Exception: continue
            if prev_seen is not None and ts < prev_seen: disorder=True
            prev_seen=ts
            parsed.append((ts,row))
        if disorder: parsed.sort(key=lambda x:x[0])
        times=[x[0] for x in parsed]
        deltas=[(b-a).total_seconds() for a,b in zip(times,times[1:]) if b>a]
        valid=[d for d in deltas if 0<d<=600]
        cadence=float(statistics.median(valid)) if valid else 60.0
        cap_s=min(600.0,max(60.0,cadence*3.0))
        duration_seconds={(k,float(th)):0.0 for k,th in duration_specs}
        gauge_totals={k:0.0 for k in gauge_keys}
        for (ta,ra),(tb,rb) in zip(parsed,parsed[1:]):
            dt=(tb-ta).total_seconds()
            if dt<=0: continue
            eff=min(dt,cap_s)
            for k,th in duration_specs:
                try:v=float(ra.get(k) or 0)
                except Exception:continue
                if v>=float(th): duration_seconds[(k,float(th))]+=eff
            for k in gauge_keys:
                try:v=float(ra.get(k) or 0)
                except Exception:continue
                gauge_totals[k]+=max(0.0,v)*eff/60.0
        episodes_out={}
        for k,th_raw in episode_specs:
            th=float(th_raw); eps=[];cur=None;prev_ts=None
            def close(end_dt):
                nonlocal cur
                if cur is None:return
                cur["end"]=end_dt.isoformat()
                cur["duration_min"]=round(max(0.0,(end_dt-datetime.fromisoformat(cur["start"])).total_seconds())/60.0,2)
                eps.append(cur);cur=None
            for ts,row in parsed:
                if prev_ts is not None and (ts-prev_ts).total_seconds()>cap_s and cur is not None:
                    close(prev_ts+timedelta(seconds=cadence))
                try:value=float(row.get(k) or 0)
                except Exception:value=0.0
                active=value>th if th==0 else value>=th
                if active:
                    if cur is None:cur={"start":ts.isoformat(),"end":ts.isoformat(),"max":value,"max_ts":ts.isoformat()}
                    elif value>float(cur.get("max") or 0):cur["max"]=value;cur["max_ts"]=ts.isoformat()
                elif cur is not None:close(ts)
                prev_ts=ts
            if cur is not None and parsed:close(parsed[-1][0]+timedelta(seconds=cadence))
            episodes_out[(k,th)]=eps[-5000:]
        return {
            "stats":{k:cls._metric_stats(values[k]) for k in stat_keys},
            "coverage":cls._coverage_from_sorted_times(times,start_dt,end_dt,sample_count=len(rows)),
            "durations":{key:round(sec/60.0,2) for key,sec in duration_seconds.items()},
            "episodes":episodes_out,
            "gauges":{k:round(v,2) for k,v in gauge_totals.items()},
            "cadence_s":cadence,
        }

    @staticmethod
    def _peak_evidence(rows, key, entity_key=None):
        """Maior valor observado com timestamp/entidade, sem preencher gaps com zero."""
        best = None
        for row in rows or []:
            try:
                value = float(row.get(key))
            except Exception:
                continue
            if best is None or value > best["value"]:
                best = {
                    "value": round(value, 3),
                    "ts": row.get("ts"),
                    "entity": row.get(entity_key) if entity_key else None,
                }
        return best

    @staticmethod
    def _rows_between(rows, start_dt, end_dt):
        out=[]
        for row in rows or []:
            try: ts=datetime.fromisoformat(str(row.get("ts") or ""))
            except Exception: continue
            if start_dt <= ts <= end_dt: out.append(row)
        return out

    @staticmethod
    def _time_index(rows):
        pairs=[]
        for row in rows or []:
            try: pairs.append((datetime.fromisoformat(str(row.get("ts") or "")),row))
            except Exception: pass
        pairs.sort(key=lambda x:x[0])
        return [x[0] for x in pairs],[x[1] for x in pairs]

    @staticmethod
    def _slice_time_index(times, rows, start_dt, end_dt):
        if not times:return []
        lo=bisect.bisect_left(times,start_dt); hi=bisect.bisect_right(times,end_dt)
        return rows[lo:hi]

    @classmethod
    def _queue_context_summary(cls, rows):
        if not rows: return {"samples":0}
        def st(k): return cls._metric_stats([x.get(k) for x in rows])
        avail=st("agents_available"); dialing=st("calls_dialing"); buf=st("buffer")
        a=avail.get("mean"); d=dialing.get("mean"); b=buf.get("mean")
        return {
            "samples":len(rows), "p34":st("p34_pct"), "p135":st("p135_pct"),
            "hit_rate5":st("hit_rate5"), "hit_rate15":st("hit_rate15"),
            "calls_dialing":dialing, "calls_in_attendance":st("calls_in_attendance"),
            "buffer":buf, "dialing_rate":st("dialing_rate"), "agents_available":avail,
            "dialing_per_available_agent":round(float(d)/float(a),2) if a not in (None,0) and d is not None else None,
            "buffer_per_available_agent":round(float(b)/float(a),2) if a not in (None,0) and b is not None else None,
        }

    @classmethod
    def _event_context_from_index(cls, qtimes, qrows, start_iso, end_iso, before_min=15, after_min=15):
        try: st=datetime.fromisoformat(start_iso); en=datetime.fromisoformat(end_iso)
        except Exception: return {"before":{"samples":0},"during":{"samples":0},"after":{"samples":0}}
        return {
            "before":cls._queue_context_summary(cls._slice_time_index(qtimes,qrows,st-timedelta(minutes=before_min),st-timedelta(microseconds=1))),
            "during":cls._queue_context_summary(cls._slice_time_index(qtimes,qrows,st,en)),
            "after":cls._queue_context_summary(cls._slice_time_index(qtimes,qrows,en,en+timedelta(minutes=after_min))),
        }

    @classmethod
    def _event_context(cls, queue_rows, start_iso, end_iso, before_min=15, after_min=15):
        qtimes,qrows=cls._time_index(queue_rows)
        return cls._event_context_from_index(qtimes,qrows,start_iso,end_iso,before_min,after_min)

    @classmethod
    def _event_intelligence(cls, queue_rows, trunk_entity_rows, stuck_rows, congestion, stuck_eps, capacity_eps):
        p34_peak=cls._peak_evidence(queue_rows,"p34_pct"); p135_peak=cls._peak_evidence(queue_rows,"p135_pct")
        dialing_peak=cls._peak_evidence(queue_rows,"calls_dialing"); trunk_peak=cls._peak_evidence(trunk_entity_rows,"occupancy","entity")
        stuck_peak=cls._peak_evidence(stuck_rows,"quantity")
        # H2-C2 correction: build temporal indexes once. The previous code rescanned all queue/stuck
        # samples for every episode (O(events*samples)); this is now bounded slicing via bisect.
        qtimes,qrows=cls._time_index(queue_rows); stimes,srows=cls._time_index(stuck_rows)
        stuck_story=[]
        for ep in stuck_eps or []:
            try: st=datetime.fromisoformat(ep["start"]); en=datetime.fromisoformat(ep["end"])
            except Exception: continue
            sr=cls._slice_time_index(stimes,srows,st,en)
            item=dict(ep);item["channel_minutes"]=cls._gauge_minutes(sr,"quantity")
            item["queue_context"]=cls._event_context_from_index(qtimes,qrows,ep["start"],ep["end"])
            stuck_story.append(item)
        stuck_story=sorted(stuck_story,key=lambda x:(float(x.get("channel_minutes") or 0),float(x.get("max") or 0),float(x.get("duration_min") or 0)),reverse=True)
        highlights=[]
        if trunk_peak: highlights.append({"type":"TRUNK_PEAK","ts":trunk_peak.get("ts"),"value":trunk_peak.get("value"),"entity":trunk_peak.get("entity"),"label":"Maior pressão de troncos"})
        if p135_peak: highlights.append({"type":"P135_PEAK","ts":p135_peak.get("ts"),"value":p135_peak.get("value"),"label":"Maior congestionamento interno P135"})
        if p34_peak: highlights.append({"type":"P34_PEAK","ts":p34_peak.get("ts"),"value":p34_peak.get("value"),"label":"Maior congestionamento na companhia P34"})
        if stuck_peak: highlights.append({"type":"STUCK_PEAK","ts":stuck_peak.get("ts"),"value":stuck_peak.get("value"),"label":"Maior quantidade simultânea de canais travados"})
        if dialing_peak: highlights.append({"type":"DIALING_PEAK","ts":dialing_peak.get("ts"),"value":dialing_peak.get("value"),"label":"Maior volume simultâneo discando"})
        return {"version":"EVENT_INTELLIGENCE_V1","highlights":highlights,
            "peaks":{"p34":p34_peak,"p135":p135_peak,"calls_dialing":dialing_peak,"trunk":trunk_peak,"stuck":stuck_peak},
            "congestion":congestion,"stuck_episode_story":stuck_story[:50],"capacity_episodes":capacity_eps or [],
            "note":"Eventos são observacionais. Coincidência temporal entre sinais não comprova causalidade."}

    @staticmethod
    def _pressure_gateway_profile_usable(profile):
        """Only score observed, adequately covered Gateway occupancy."""
        try:
            value = float(profile.get("p95_pct"))
            coverage = float((profile.get("quality") or {}).get("data_coverage_pct"))
            samples = int(profile.get("samples") or 0)
            return math.isfinite(value) and 0 <= value <= 100 and coverage >= 60 and samples >= 5
        except (TypeError, ValueError):
            return False

    @classmethod
    def _operational_pressure(
        cls, queue_rows, qsummary, congestion, trunk_stats, trunk_entity_rows,
        stuck_stats, observed_route_capacity, infra_mapped, qcoverage, tcoverage,
        scoverage, capacity_operational=None, trunk_mapped=None, gateway_mapped=None,
        gcoverage=None,
    ):
        """Pressão Operacional explicável e period-aware.

        Mede esforço/pressão observada na janela selecionada, sem confundir P34 com
        P135 e sem atribuir causalidade automática. P34 (congestionamento na
        companhia) e P135 (congestionamento interno) são dimensões independentes.
        """
        def clamp(v):
            return max(0.0, min(100.0, float(v)))

        def val(metric, stat, default=None):
            try:
                x = (qsummary.get(metric) or {}).get(stat)
                return float(x) if x is not None else default
            except Exception:
                return default

        def minutes(metric, key):
            try:
                x = (congestion.get(metric) or {}).get(key)
                return float(x) if x is not None else 0.0
            except Exception:
                return 0.0

        observed_minutes = None
        for key in ("available_minutes", "observed_minutes", "span_minutes"):
            try:
                x = qcoverage.get(key)
                if x is not None:
                    observed_minutes = max(0.0, float(x))
                    break
            except Exception:
                pass
        if not observed_minutes:
            try:
                observed_minutes = max(1.0, float(qcoverage.get("requested_minutes") or 0.0))
            except Exception:
                observed_minutes = 1.0

        def pressure_index_meaning(score):
            if score is None:
                return ("N/C", "Evidência insuficiente para interpretar a pressão desta dimensão.")
            v = float(score)
            if v >= 85:
                txt = (
                    "Pressão crítica: o valor que 95% das leituras não ultrapassaram, o pico e/ou o tempo acima de 20% indicam exposição severa "
                    "durante a janela selecionada."
                )
                if v >= 99.5:
                    txt += " O índice atingiu o teto da escala; isso não significa P34/P135 de 100%."
                return ("CRÍTICA", txt)
            if v >= 70:
                return (
                    "ELEVADA",
                    "Pressão elevada: o valor que 95% das leituras não ultrapassaram, o pico e/ou o tempo acima de 20% mostram exposição relevante durante o período."
                )
            if v >= 45:
                return (
                    "MODERADA",
                    "Pressão moderada: houve aproximação ou ultrapassagem relevante do limiar crítico, mas sem severidade/persistência suficiente para a faixa elevada."
                )
            return (
                "BAIXA",
                "Pressão baixa: os componentes do índice mostram sinal limitado na janela e não sustentam exposição crítica relevante ou persistente."
            )

        def congestion_dimension(metric):
            p95 = val(metric, "p95")
            peak = val(metric, "max")
            mean = val(metric, "mean")
            over20 = minutes("p34" if metric == "p34_pct" else "p135", "minutes_over_20")
            episodes = int(minutes("p34" if metric == "p34_pct" else "p135", "episodes_over_20"))
            longest = minutes("p34" if metric == "p34_pct" else "p135", "longest_over_20_min")
            if p95 is None and peak is None:
                return None
            p95_score = clamp((p95 or 0.0) / 20.0 * 100.0)
            peak_score = clamp((peak or 0.0) / 20.0 * 100.0)
            # Persistência: 50% da janela acima de 20% já representa persistência máxima.
            persistence_score = clamp((over20 / max(observed_minutes, 1.0)) * 200.0)
            score = round(0.55 * p95_score + 0.25 * peak_score + 0.20 * persistence_score, 1)
            interpretation_label, interpretation_text = pressure_index_meaning(score)
            return {
                "score": score, "p95": p95, "peak": peak, "mean": mean,
                "interpretation_label": interpretation_label,
                "interpretation_text": interpretation_text,
                "minutes_over_20": round(over20, 2), "episodes_over_20": episodes,
                "longest_over_20_min": round(longest, 2),
                "persistence_pct_observed": round(over20 / max(observed_minutes, 1.0) * 100.0, 2),
                "score_components": {
                    "critical_threshold_pct": 20.0,
                    "p95_normalized_score": round(p95_score, 1),
                    "peak_normalized_score": round(peak_score, 1),
                    "persistence_normalized_score": round(persistence_score, 1),
                    "p95_weight": 0.55, "peak_weight": 0.25, "persistence_weight": 0.20,
                    "observed_minutes": round(observed_minutes, 2),
                },
                "score_explanation": (
                    "Índice de pressão da dimensão, não percentual bruto. "
                    "Combina o valor que 95% das leituras não ultrapassaram (55%), o pico (25%) e o tempo acima de 20% (20%); "
                    "cada componente é normalizado e limitado a 100."
                ),
            }

        p34_dim = congestion_dimension("p34_pct")
        p135_dim = congestion_dimension("p135_pct")
        cap=capacity_operational or {}
        historical_trunk=(cap.get("historical_pressure_group") or {})
        trunk_value=historical_trunk.get("p95_pct")
        if trunk_value is None:
            trunk_value=trunk_stats.get("p95")
        trunk_p95 = float(trunk_value) if trunk_value is not None else None
        if trunk_mapped is None:
            trunk_mapped = infra_mapped
        if gateway_mapped is None:
            gateway_mapped = infra_mapped and bool(cap.get("gateway_profiles"))
        trunk_dim = round(clamp(trunk_p95), 1) if trunk_p95 is not None and trunk_mapped else None
        trunk_entity=historical_trunk.get("entity") or (cap.get("critical_group") or {}).get("entity")
        gateway_profiles = [p for p in cap.get("gateway_profiles") or [] if cls._pressure_gateway_profile_usable(p)] if gateway_mapped else []
        gateway_profile = max(gateway_profiles, key=lambda p: float(p["p95_pct"]), default=None)
        gateway_p95 = float(gateway_profile["p95_pct"]) if gateway_profile else None
        gateway_dim = round(clamp(gateway_p95), 1) if gateway_p95 is not None else None
        gateway_entity = (gateway_profile or {}).get("entity")
        telecom_dim = max(v for v in (trunk_dim, gateway_dim) if v is not None) if trunk_dim is not None or gateway_dim is not None else None
        telecom_source = "GATEWAY" if gateway_dim is not None and (trunk_dim is None or gateway_dim > trunk_dim) else "TRUNK" if trunk_dim is not None else None
        gateway_exclusion = None if gateway_dim is not None else (
            "Sem vínculo comprovado entre fila e Gateway no período" if not gateway_mapped else
            "Leituras do Gateway sem cobertura mínima de 60% e 5 amostras válidas"
        )
        stuck_peak = float(stuck_stats.get("max")) if stuck_stats.get("max") is not None else None

        stuck_context_pct = None
        if stuck_peak is not None and observed_route_capacity and observed_route_capacity > 0:
            stuck_context_pct = round(clamp((stuck_peak / observed_route_capacity) * 100.0), 2)
        if not stuck_peak:
            stuck_state = "SEM EVIDÊNCIA"
        elif stuck_peak >= 100 or (stuck_context_pct is not None and stuck_context_pct >= 5.0):
            stuck_state = "AGRAVANTE"
        elif stuck_peak >= 20 or (stuck_context_pct is not None and stuck_context_pct >= 1.0):
            stuck_state = "PRESENTE"
        else:
            stuck_state = "BAIXA RELEVÂNCIA"

        scored=[]
        if p34_dim is not None:
            scored.append(("Congestionamento na Companhia · P34", p34_dim["score"], 0.35, "p34"))
        if p135_dim is not None:
            scored.append(("Congestionamento interno · P135", p135_dim["score"], 0.30, "p135"))
        if telecom_dim is not None:
            scored.append(("Pressão Telecom · troncos e Gateways", telecom_dim, 0.35, "telecom"))

        if scored:
            mx=max(v for _,v,_,_ in scored)
            den=sum(w for _,_,w,_ in scored)
            avg=sum(v*w for _,v,w,_ in scored)/den if den else mx
            # Um único gargalo severo não pode desaparecer por média global.
            score=round(0.55*mx + 0.45*avg, 1)
        else:
            mx=avg=None
            score=None
        if score is None: level="EVIDÊNCIA INSUFICIENTE"
        elif score>=85: level="CRÍTICA"
        elif score>=70: level="ELEVADA"
        elif score>=45: level="MODERADA"
        else: level="BAIXA"

        contributors=[]
        for name,v,w,key in sorted(scored,key=lambda x:x[1],reverse=True):
            detail = p34_dim if key=="p34" else p135_dim if key=="p135" else {
                "trunk_p95":trunk_p95 if trunk_dim is not None else None,
                "trunk_entity":trunk_entity if trunk_dim is not None else None,
                "gateway_p95":gateway_p95,"gateway_entity":gateway_entity,
                "selected_source":telecom_source,"gateway_exclusion":gateway_exclusion,
                "interpretation_label":pressure_index_meaning(v)[0],
                "interpretation_text":(
                    f"Em 95% das leituras, a ocupação de {'Gateway' if telecom_source == 'GATEWAY' else 'troncos'} "
                    f"ficou em até {v:.1f}%. Esse foi o maior valor válido do caminho Telecom relacionado à fila."
                ),
                "score_explanation":"A pressão Telecom usa a maior ocupação sustentada entre troncos e Gateways comprovadamente ligados à fila. Cada fonte representa o mesmo caminho e não recebe peso adicional.",
            } if key=="telecom" else None
            contributors.append({
                "name":name,"score":v,"weight":w,"scored":True,
                "level":"crítica" if v>=85 else "alta" if v>=70 else "moderada" if v>=45 else "baixa",
                "metric":key,"detail":detail,
            })
        if stuck_peak is not None:
            contributors.append({
                "name":"Canais travados da Instância","score":None,"weight":0.0,"scored":False,
                "level":stuck_state.lower(),"max_concurrent":stuck_peak,
                "capacity_context_pct":stuck_context_pct,"state":stuck_state,
                "note":(
                    "Canais travados reduzem capacidade física onde estão alocados. "
                    "Como a fonte atual não comprova o vínculo fila→canal, não são descontados automaticamente "
                    "da capacidade específica da fila."
                ),
            })

        dmean=val("calls_dialing","mean"); dpeak=val("calls_dialing","max")
        avail=val("agents_available","mean"); buf=val("buffer","mean"); hit=val("hit_rate15","mean")
        relations={
            "calls_dialing_mean":dmean,"calls_dialing_peak":dpeak,
            "agents_available_mean":avail,"buffer_mean":buf,"hit_rate15_mean":hit,
            "dialing_per_available_agent":round(dmean/avail,2) if dmean is not None and avail not in (None,0) else None,
            "buffer_per_available_agent":round(buf/avail,2) if buf is not None and avail not in (None,0) else None,
            "attendance_note":(
                "Discagem, Hit Rate, buffer e agentes ajudam a explicar o contexto. Sem baseline robusto por fila/horário, "
                "essas relações não recebem peso causal automático."
            ),
        }

        # Pior instante: vence o maior sintoma normalizado, sem favorecer P135 a priori.
        worst=None
        for r in queue_rows or []:
            p135=float(r.get("p135_pct") or 0); p34=float(r.get("p34_pct") or 0)
            p135_n=clamp(p135/20*100); p34_n=clamp(p34/20*100)
            sev=max(p135_n,p34_n)
            dominant="P34" if p34_n>=p135_n else "P135"
            if worst is None or sev>worst["score"]:
                worst={
                    "ts":r.get("ts"),"score":round(sev,1),"dominant_metric":dominant,
                    "p135_pct":round(p135,3),"p34_pct":round(p34,3),
                    "calls_dialing":r.get("calls_dialing"),"hit_rate15":r.get("hit_rate15"),
                    "buffer":r.get("buffer"),"agents_available":r.get("agents_available")
                }

        current = (queue_rows or [])[-1] if queue_rows else {}
        p34_now=float(current.get("p34_pct") or 0.0); p135_now=float(current.get("p135_pct") or 0.0)
        gw_p95=gateway_p95; gw_current=cap.get("gateway_current_max")
        gw_faults=[x.get("entity") for x in (cap.get("gateway_contexts") or [])
                   if str(x.get("canonical_status") or "").upper()=="FAULT_CONFIRMED_CONTEXT"]
        trunk_entity=(cap.get("historical_pressure_group") or {}).get("entity") or (cap.get("critical_group") or {}).get("entity")

        hypotheses=[]; evidence_for=[]; evidence_against=[]
        p34_score=(p34_dim or {}).get("score",0); p135_score=(p135_dim or {}).get("score",0)
        if p34_score>=70:
            if telecom_source == "GATEWAY" and gateway_dim >= 85:
                hypotheses.append({
                    "rank":1,"name":"Pressão localizada em Gateway relacionado à fila",
                    "strength":"ALTA","why":f"P34 relevante no período; em 95% das leituras, a ocupação do Gateway {gateway_entity} ficou em até {gateway_p95:.1f}%.",
                })
                evidence_for.append("P34 elevado coincidiu com pressão forte em Gateway relacionado à fila.")
            elif trunk_dim is not None and trunk_dim>=85:
                hypotheses.append({
                    "rank":1,"name":"Restrição/saturação localizada em rota ou grupo de troncos",
                    "strength":"ALTA","why":f"P34 relevante no período; em 95% das leituras, a ocupação dos troncos ficou em até {trunk_p95:.1f}%" + (f" em {trunk_entity}" if trunk_entity else "") + ".",
                })
                evidence_for.append("P34 elevado coincidiu com pressão forte em troncos relacionados à fila.")
            else:
                hypotheses.append({
                    "rank":1,"name":"Restrição no caminho de saída / distribuição de rotas",
                    "strength":"MÉDIA","why":"P34 foi relevante, mas a pressão de troncos relacionada não comprova sozinha o ponto de restrição.",
                })
        if dpeak is not None and dpeak>0 and p34_score>=45:
            strength="MÉDIA" if hit is not None and hit<3 else "BAIXA/MÉDIA"
            hypotheses.append({
                "rank":2,"name":"Carga/estratégia de discagem contribuindo para a pressão",
                "strength":strength,"why":f"Pico de {dpeak:.0f} chamadas discando" + (f" e Hit Rate 15' médio {hit:.2f}%" if hit is not None else "") + ".",
            })
        if p135_score>=70:
            hypotheses.append({
                "rank":2,"name":"Pressão operacional interna da fila",
                "strength":"ALTA" if p135_score>=85 else "MÉDIA/ALTA",
                "why":f"Em 95% das leituras, o P135 ficou em até {(p135_dim or {}).get('p95') or 0:.2f}%, com exposição crítica persistente no período.",
            })
        if stuck_state=="AGRAVANTE":
            hypotheses.append({
                "rank":3,"name":"Canais travados reduzindo capacidade útil em parte da infraestrutura",
                "strength":"MÉDIA","why":f"Pico de {stuck_peak:.0f} canais travados" + (f" ({stuck_context_pct:.2f}% da capacidade observada)" if stuck_context_pct is not None else "") + ".",
            })
        elif stuck_peak:
            evidence_against.append(f"Canais travados tiveram baixa materialidade ({stuck_peak:.0f} no pico); não sustentam causa principal isoladamente.")
        if avail is not None and avail>=3:
            evidence_against.append(f"Havia em média {avail:.1f} agentes disponíveis; baixa evidência de falta de agentes como causa dominante.")
        if not gw_faults and gw_p95 is not None and float(gw_p95)<80:
            evidence_against.append(f"Gateways não mostraram pressão generalizada no período (o maior valor não ultrapassado em 95% das leituras foi {float(gw_p95):.1f}%) nem falha confirmada no contexto atual.")
        elif gw_faults:
            evidence_for.append("Há Gateway(s) com falha confirmada no Ground Truth: " + ", ".join(str(x) for x in gw_faults if x) + ".")

        hypotheses=sorted(hypotheses,key=lambda x:(x.get("rank",99), x.get("strength")!="ALTA"))[:4]
        recommendations=[]
        if p34_score>=70 and telecom_source == "GATEWAY" and gateway_dim >= 85:
            recommendations.append({
                "title":"Validar Gateway e rota associados ao P34",
                "why":f"P34 foi dominante; no Gateway {gateway_entity}, 95% das leituras ficaram em até {gateway_p95:.1f}% de ocupação.",
                "expected":"Confirmar se a restrição coincide com a rota antes de redistribuir ou ampliar capacidade.","priority":"Alta",
            })
        elif p34_score>=70 and trunk_dim is not None and trunk_dim>=85:
            recommendations.append({
                "title":"Validar rota/grupo de troncos associado ao P34",
                "why":f"P34 foi dominante no período; em {trunk_entity or 'o grupo relacionado'}, 95% das leituras de ocupação ficaram em até {trunk_p95:.1f}%.",
                "expected":"Confirmar se a restrição está concentrada na rota antes de ampliar ou redistribuir capacidade.","priority":"Alta",
            })
        elif p34_score>=70:
            recommendations.append({
                "title":"Investigar distribuição de rotas do P34",
                "why":"O congestionamento na companhia foi relevante, mas o vínculo fila→rota não é suficiente para atribuir a causa automaticamente.",
                "expected":"Identificar o caminho de saída restritivo e descartar indisponibilidade específica de rota/circuito.","priority":"Alta",
            })
        if dpeak is not None and dpeak>0 and hit is not None and hit<3:
            recommendations.append({
                "title":"Revisar intensidade e eficiência da discagem",
                "why":f"Hit Rate 15' médio {hit:.2f}% com pico de {dpeak:.0f} chamadas discando no período.",
                "expected":"Reduzir tentativas improdutivas e verificar se o P34 responde à redução de carga.","priority":"Alta" if p34_score>=70 else "Média",
            })
        if stuck_state=="AGRAVANTE":
            recommendations.append({
                "title":"Correlacionar canais travados com o episódio de congestionamento",
                "why":f"O pico de canais travados foi material ({stuck_peak:.0f}).",
                "expected":"Confirmar se a redução de capacidade útil coincidiu temporalmente com P34/P135.","priority":"Média",
            })
        if not recommendations:
            recommendations.append({
                "title":"Manter observação e confirmar a hipótese dominante",
                "why":"A janela não fornece evidência suficiente para intervenção estrutural automática.",
                "expected":"Aumentar confiança causal antes de alterar discagem, rota ou infraestrutura.","priority":"Baixa",
            })

        coverage_sources=(qcoverage,tcoverage,scoverage,gcoverage if gateway_dim is not None else None)
        covs=[float(x.get("data_coverage_pct") or x.get("coverage_pct") or 0) for x in coverage_sources if x]
        confidence=round(sum(covs)/len(covs),1) if covs else 0.0
        primary=contributors[0]["name"] if contributors else "Evidência insuficiente"

        summary=[f"Pressão {str(level).lower()} no período."]
        if p34_dim and p34_dim.get("score",0)>=max((p135_dim or {}).get("score",0),45):
            summary.append(
                f"P34 foi o sintoma dominante: em 95% das leituras ficou em até {p34_dim.get('p95') or 0:.2f}%, com pico de {p34_dim.get('peak') or 0:.2f}% e "
                f"{p34_dim.get('minutes_over_20') or 0:.0f} min acima de 20%."
            )
        elif p135_dim and p135_dim.get("score",0)>=45:
            summary.append(
                f"P135 foi o sintoma dominante: em 95% das leituras ficou em até {p135_dim.get('p95') or 0:.2f}%, com pico de {p135_dim.get('peak') or 0:.2f}% e "
                f"{p135_dim.get('minutes_over_20') or 0:.0f} min acima de 20%."
            )
        if trunk_dim is not None and trunk_p95 is not None:
            summary.append(f"Em 95% das leituras, a ocupação dos troncos relacionados ficou em até {trunk_p95:.1f}%" + (f" ({trunk_entity})" if trunk_entity else "") + ".")
        if gateway_dim is not None:
            summary.append(f"No Gateway relacionado {gateway_entity}, 95% das leituras ficaram em até {gateway_p95:.1f}% de ocupação; "
                           + ("ele determinou a pressão Telecom." if telecom_source == "GATEWAY" else "o grupo de troncos determinou a pressão Telecom."))
        summary.append(f"Agora: P34 {p34_now:.2f}% e P135 {p135_now:.2f}%.")

        return {
            "version":"OPERATIONAL_PRESSURE_OBSERVATIONAL_V3",
            "score":score,"level":level,"primary_factor":primary,"contributors":contributors,
            "overall_breakdown":{
                "highest_dimension":round(mx,1) if mx is not None else None,
                "weighted_mean":round(avg,1) if avg is not None else None,
                "highest_weight":0.55,"mean_weight":0.45,
            },
            "dimensions":{
                "company_congestion_p34":p34_dim,
                "internal_congestion_p135":p135_dim,
                "trunk_pressure":trunk_dim,
                "gateway_pressure":gateway_dim,
                "telecom_pressure":telecom_dim,
                "telecom_source":telecom_source,
                "gateway_exclusion":gateway_exclusion,
                "stuck_instance_context_pct":stuck_context_pct,
                "stuck_state":stuck_state,
            },
            "relations":relations,"worst_observed":worst,"evidence_coverage_pct":confidence,
            "executive_summary":" ".join(summary),
            "hypotheses":hypotheses,"evidence_for":evidence_for,"evidence_against":evidence_against,
            "recommendations":recommendations[:3],
            "current":{"p34_pct":round(p34_now,3),"p135_pct":round(p135_now,3),"gateway_current_max":gw_current},
            "model_status":"HEURISTIC_EXPLAINABLE_NOT_ML",
            "score_method": {
                "dimension_method":"P34/P135: 55% do valor que 95% das leituras não ultrapassaram + 25% do pico + 20% do tempo acima de 20%; cada parcela é convertida à escala de 0 a 100.",
                "overall_method":"Pressão total: 55% da dimensão mais severa + 45% da média ponderada das dimensões disponíveis (P34 35%, P135 30%, Telecom 35%). Telecom usa a maior ocupação válida entre troncos e Gateways relacionados à fila, sem somá-los.",
                "important":"Os valores /100 são índices de pressão, não percentuais brutos, probabilidades ou causalidade.",
            },
            "methodology":(
                "Índice observacional do período. P34 e P135 são dimensões independentes e consideram o valor que 95% das leituras não ultrapassaram, o pico e o tempo acima de 20%. "
                "Ocupação de troncos e Gateways entra na dimensão Telecom somente quando há vínculo com a fila por Last Calls no próprio período; Gateways também exigem 60% de cobertura e 5 amostras válidas. "
                "Discagem, Hit Rate, agentes e canais travados qualificam hipóteses e recomendações sem virar causalidade automática."
            ),
            "causality":"NOT_PROVEN_HYPOTHESES_RANKED",
        }

    @staticmethod
    def _compact_stats(values):
        stats = DataService._metric_stats(values)
        return {
            "mean": stats.get("mean"),
            "p95": stats.get("p95"),
            "max": stats.get("max"),
            "min": stats.get("min"),
        }

    @classmethod
    def _temporal_profile(cls, queue_rows, trunk_rows, stuck_rows, bucket_minutes=15):
        bucket_count = int(24 * 60 / bucket_minutes)
        queue_keys = (
            "p34_pct","p135_pct","hit_rate5","hit_rate15",
            "calls_dialing","calls_in_attendance","buffer","dialing_rate",
        )
        buckets = [{
            "minute_of_day": i * bucket_minutes,
            "time": f"{(i*bucket_minutes)//60:02d}:{(i*bucket_minutes)%60:02d}",
            "_queue": {k: [] for k in queue_keys},
            "_queue_days": set(),
            "_trunk": [],
            "_trunk_days": set(),
            "_stuck": [],
            "_stuck_days": set(),
        } for i in range(bucket_count)]

        def bucket_idx(ts):
            try:
                dt = datetime.fromisoformat(ts)
                return min(bucket_count-1, (dt.hour*60 + dt.minute)//bucket_minutes), dt.date().isoformat()
            except Exception:
                return None, None

        for row in queue_rows:
            idx, day = bucket_idx(row.get("ts"))
            if idx is None:
                continue
            b = buckets[idx]
            b["_queue_days"].add(day)
            for key in queue_keys:
                v = row.get(key)
                if v is not None:
                    try:
                        b["_queue"][key].append(float(v))
                    except Exception:
                        pass

        for row in trunk_rows:
            idx, day = bucket_idx(row.get("ts"))
            if idx is None:
                continue
            try:
                if row.get("occupancy") is None:
                    continue
                buckets[idx]["_trunk"].append(float(row.get("occupancy")))
                buckets[idx]["_trunk_days"].add(day)
            except Exception:
                pass

        for row in stuck_rows:
            idx, day = bucket_idx(row.get("ts"))
            if idx is None:
                continue
            try:
                buckets[idx]["_stuck"].append(float(row.get("quantity") or 0))
                buckets[idx]["_stuck_days"].add(day)
            except Exception:
                pass

        out = []
        for b in buckets:
            out.append({
                "minute_of_day": b["minute_of_day"],
                "time": b["time"],
                "queue_days": len(b["_queue_days"]),
                "trunk_days": len(b["_trunk_days"]),
                "stuck_days": len(b["_stuck_days"]),
                "p34_pct": cls._compact_stats(b["_queue"]["p34_pct"]),
                "p135_pct": cls._compact_stats(b["_queue"]["p135_pct"]),
                "hit_rate15": cls._compact_stats(b["_queue"]["hit_rate15"]),
                "calls_dialing": cls._compact_stats(b["_queue"]["calls_dialing"]),
                "calls_in_attendance": cls._compact_stats(b["_queue"]["calls_in_attendance"]),
                "trunk_occupancy": cls._compact_stats(b["_trunk"]),
                "stuck_quantity": cls._compact_stats(b["_stuck"]),
            })
        return out

    def analytics_window(self, instancia, codigo, period="15m", start=None, end=None, cache_bypass=False):
        # H2-C2: cada estágio crítico é cronometrado. O objetivo é localizar a causa
        # do custo antes de qualquer nova estratégia de cache/rollup.
        _perf_total_t0=time.perf_counter();_perf_stage_t0=_perf_total_t0;_perf_stages={}
        def _perf_mark(name):
            nonlocal _perf_stage_t0
            now=time.perf_counter();_perf_stages[name]=round((now-_perf_stage_t0)*1000.0,2);_perf_stage_t0=now
        bounds = self._analytics_bounds(instancia, codigo, period, start, end)
        _perf_mark("bounds")
        start_dt = bounds["start_dt"]
        end_dt = bounds["end_dt"]
        if str(period).lower()=="custom":
            cache_slot=end_dt.isoformat()
        elif bounds["requested_minutes"]>=7*24*60:
            # Perfil longo muda pouco a cada segundo: alinha cache a 5 min e permite reuso real.
            minute=(end_dt.minute//ANALYTICS_LONG_CACHE_BUCKET_MINUTES)*ANALYTICS_LONG_CACHE_BUCKET_MINUTES
            cache_slot=end_dt.replace(minute=minute,second=0,microsecond=0).isoformat()
        else:
            cache_slot=end_dt.replace(second=0,microsecond=0).isoformat()
        cache_key=(str(instancia),str(codigo),str(period or "15m").lower(),str(start or ""),str(end or ""),cache_slot)
        cache_ttl=ANALYTICS_CACHE_TTL_LONG_S if bounds["requested_minutes"]>=7*24*60 else ANALYTICS_CACHE_TTL_SHORT_S
        cached=None if cache_bypass else self._cache_get(self._analytics_cache,cache_key,cache_ttl)
        _perf_mark("cache_lookup")
        if cached is not None:
            out=dict(cached);perf=dict(out.get("performance") or {})
            perf.update({"release":ANALYTICS_RELEASE,"cache_hit":True,"cache_key_slot":cache_slot,"request_total_ms":round((time.perf_counter()-_perf_total_t0)*1000.0,2),"stages_ms":dict(_perf_stages)})
            out["performance"]=perf
            return out

        queue_rows = self._queue_range(instancia, codigo, start_dt, end_dt)
        _perf_mark("queue_range")
        stuck_rows = self._stuck_range(instancia, start_dt, end_dt)
        _perf_mark("stuck_range")

        # R1.2.2: relação fila->infra respeita o próprio intervalo selecionado.
        # H2-C2 Closure: janelas longas usam fotografia as-of do sidecar derivado;
        # o LIVE permanece fallback READ ONLY.
        if bounds["requested_minutes"]>=ANALYTICS_SIDECAR_LONG_MINUTES:
            current_infra = self._analytics_infra_snapshot(instancia,end_dt)
        else:
            current_infra = self.infra(instancia, bounds["requested_minutes"])
        _perf_mark("infra_snapshot_current")
        infra_context = self.queue_infra_context(
            instancia, codigo, current_infra, start_dt=start_dt, end_dt=end_dt
        )
        _perf_mark("queue_infra_mapping")
        infra_rows = self._infra_range(instancia, start_dt, end_dt, infra_context)
        _perf_mark("infra_range")
        trunk_rows = infra_rows.get("trunk", [])
        gateway_rows = infra_rows.get("gateway", [])
        trunk_entity_rows = infra_rows.get("trunk_entities", [])
        gateway_entity_rows = infra_rows.get("gateway_entities", [])
        trunk_all_entity_rows = infra_rows.get("trunk_all_entities", [])
        gateway_all_entity_rows = infra_rows.get("gateway_all_entities", [])

        queue_keys = (
            "p34_pct","p135_pct","hit_rate5","hit_rate15",
            "calls_dialing","calls_in_attendance","buffer","dialing_rate",
            "agents_available","agents_talking","agents_acw",
            "agents_paused","agents_other","agents_dialing",
        )
        queue_bundle = self._temporal_metric_bundle(
            queue_rows, start_dt, end_dt, stat_keys=queue_keys,
            duration_specs=(("p34_pct",10),("p34_pct",20),("p135_pct",10),("p135_pct",20)),
            episode_specs=(("p34_pct",20),("p135_pct",20)),
        )
        qsummary = queue_bundle["stats"]
        qcoverage = queue_bundle["coverage"]
        p34_eps20 = queue_bundle["episodes"].get(("p34_pct",20.0),[])
        p135_eps20 = queue_bundle["episodes"].get(("p135_pct",20.0),[])
        p34_lifecycle = self._threshold_lifecycle(queue_rows,"p34_pct",20.0,limit=500)
        p135_lifecycle = self._threshold_lifecycle(queue_rows,"p135_pct",20.0,limit=500)
        congestion_analytics = {
            "p34": {
                "minutes_over_10": queue_bundle["durations"].get(("p34_pct",10.0),0.0),
                "minutes_over_20": queue_bundle["durations"].get(("p34_pct",20.0),0.0),
                "episodes_over_20": len(p34_eps20),
                "longest_over_20_min": max([x.get("duration_min", 0) for x in p34_eps20] or [0]),
                "incident_lifecycle": p34_lifecycle,
            },
            "p135": {
                "minutes_over_10": queue_bundle["durations"].get(("p135_pct",10.0),0.0),
                "minutes_over_20": queue_bundle["durations"].get(("p135_pct",20.0),0.0),
                "episodes_over_20": len(p135_eps20),
                "longest_over_20_min": max([x.get("duration_min", 0) for x in p135_eps20] or [0]),
                "incident_lifecycle": p135_lifecycle,
            },
        }
        stuck_bundle = self._temporal_metric_bundle(
            stuck_rows, start_dt, end_dt, stat_keys=("quantity",),
            duration_specs=(("quantity",1),), episode_specs=(("quantity",0),), gauge_keys=("quantity",),
        )
        scoverage = stuck_bundle["coverage"]
        stuck_stats = stuck_bundle["stats"]["quantity"]
        stuck_eps = stuck_bundle["episodes"].get(("quantity",0.0),[])
        stuck_affected_min = stuck_bundle["durations"].get(("quantity",1.0),0.0)
        stuck_channel_min = stuck_bundle["gauges"].get("quantity",0.0)

        trunk_bundle = self._temporal_metric_bundle(
            trunk_rows, start_dt, end_dt, stat_keys=("occupancy",),
            duration_specs=(("occupancy",85),("occupancy",90)), episode_specs=(("occupancy",90),),
        )
        gateway_bundle = self._temporal_metric_bundle(
            gateway_rows, start_dt, end_dt, stat_keys=("occupancy",),
        )
        tcoverage = trunk_bundle["coverage"]
        gcoverage = gateway_bundle["coverage"]
        trunk_stats = trunk_bundle["stats"]["occupancy"]
        gateway_stats = gateway_bundle["stats"]["occupancy"]
        over85 = trunk_bundle["durations"].get(("occupancy",85.0),0.0)
        over90 = trunk_bundle["durations"].get(("occupancy",90.0),0.0)
        capacity_eps = trunk_bundle["episodes"].get(("occupancy",90.0),[])
        _perf_mark("queue_event_stats")

        relevant_trunks = infra_context.get("trunks") or []
        relevant_gateways = infra_context.get("gateways") or []
        gateway_semantic_context = load_gateway_semantic_reference(instancia)

        # H2-C2 second compute correction: the queue-scoped entity series are static subsets
        # of the instance entity series for the selected window. Build per-entity temporal
        # profiles ONCE from the complete instance series and reuse exact subsets in both
        # capacity views. This removes a second full Python group/parse pass without changing
        # the queue->infra semantics established by Last Calls.
        instance_trunks = infra_context.get("instance_trunks") or []
        instance_gateways = infra_context.get("instance_gateways") or []
        pre_profiles=infra_rows.get("precomputed_profiles") or {}
        if pre_profiles:
            all_trunk_profiles=list(pre_profiles.get("trunk") or [])
            all_gateway_profiles=list(pre_profiles.get("gateway") or [])
            queue_trunk_profiles=list(pre_profiles.get("queue_trunk") or [])
            queue_gateway_profiles=list(pre_profiles.get("queue_gateway") or [])
        else:
            all_trunk_profiles = self._capacity_group_profiles(trunk_all_entity_rows, instance_trunks, start_dt, end_dt)
            all_gateway_profiles = self._capacity_group_profiles(gateway_all_entity_rows, instance_gateways, start_dt, end_dt)
            queue_trunk_entities = {str(r.get("entity") or "").strip() for r in trunk_entity_rows if str(r.get("entity") or "").strip()}
            queue_gateway_entities = {str(r.get("entity") or "").strip() for r in gateway_entity_rows if str(r.get("entity") or "").strip()}
            queue_trunk_profiles = [x for x in all_trunk_profiles if str(x.get("entity") or "").strip() in queue_trunk_entities]
            queue_gateway_profiles = [x for x in all_gateway_profiles if str(x.get("entity") or "").strip() in queue_gateway_entities]
        _perf_mark("capacity_profile_prepare")
        infra_mapped = bool(infra_context.get("inferred_from_lastcalls"))
        observed_total = sum(float(x.get("total") or 0) for x in relevant_trunks)
        observed_busy = sum(float(x.get("busy") or 0) for x in relevant_trunks)
        capacity_scope_mode = (
            "QUEUE_MAPPED_BY_LASTCALLS" if infra_mapped else "INSTANCE_FALLBACK"
        )
        map_usage = infra_context.get("usage") or {}
        mapping_sampling = int(map_usage.get("sampling_stride") or 1)
        mapping_complete = bool(map_usage.get("mapping_complete", True))
        capacity_scope_note = (
            "Escopo FILA: capacidade observada somente nos grupos/gateways relacionados à fila "
            "por evidência de Last Calls DENTRO do intervalo selecionado. Não confundir com o "
            "inventário completo da Instância nem com capacidade física/contratada consolidada."
            if infra_mapped else
            "Escopo INSTÂNCIA (fallback): não há relação fila→infra comprovada por Last Calls no "
            "intervalo selecionado; a infraestrutura completa da Instância é usada apenas como "
            "contexto observacional. Não interpretar a soma de canais como capacidade exclusiva da fila."
        )
        if mapping_sampling > 1:
            capacity_scope_note += f" Mapeamento temporal amostrado a cada {mapping_sampling} snapshot(s) de Last Calls."

        scoped_series = {
            "trunk": trunk_rows, "gateway": gateway_rows,
            "trunk_entities": trunk_entity_rows, "gateway_entities": gateway_entity_rows,
        }
        capacity_operational = self.capacity_analysis(
            {"trunks": relevant_trunks, "gateways": relevant_gateways},
            {}, scoped_series, window_minutes=bounds["requested_minutes"],
            latest=(queue_rows[-1] if queue_rows else {}),
            stuck_summary={**stuck_stats, "max": stuck_stats.get("max")},
            dialer_summary={},
            gateway_semantic_context=gateway_semantic_context,
            evidence_context={
                "infra_coverage": tcoverage,
                "gateway_coverage": gcoverage,
                "scope_mode": capacity_scope_mode,
                "window_start": start_dt.isoformat(),
                "window_end": end_dt.isoformat(),
                "freshness": {"status": "HISTORICAL_WINDOW", "usable": bool(tcoverage.get("samples"))},
                "precomputed_profiles": {"trunk": queue_trunk_profiles, "gateway": queue_gateway_profiles},
                "queue_summary": qsummary,
                "congestion_analytics": congestion_analytics,
            },
        )

        _perf_mark("capacity_queue_scope")
        # Fotografia/análise COMPLETA da Instância, separada do escopo da fila.
        instance_infra = {
            "trunks": instance_trunks,
            "gateways": instance_gateways,
        }
        instance_series = {
            "trunk_entities": trunk_all_entity_rows,
            "gateway_entities": gateway_all_entity_rows,
        }
        instance_capacity = self.capacity_analysis(
            instance_infra, {}, instance_series, window_minutes=bounds["requested_minutes"],
            latest=(queue_rows[-1] if queue_rows else {}),
            stuck_summary={**stuck_stats, "max": stuck_stats.get("max")},
            dialer_summary={}, gateway_semantic_context=gateway_semantic_context,
            evidence_context={
                "infra_coverage": self._coverage_with_gaps(
                    self._envelope_series(trunk_all_entity_rows), start_dt, end_dt
                ),
                "gateway_coverage": self._coverage_with_gaps(
                    self._envelope_series(gateway_all_entity_rows), start_dt, end_dt
                ),
                "scope_mode": "INSTANCE_FULL",
                "window_start": start_dt.isoformat(),
                "window_end": end_dt.isoformat(),
                "freshness": {"status": "HISTORICAL_WINDOW", "usable": bool(trunk_all_entity_rows or gateway_all_entity_rows)},
                "precomputed_profiles": {"trunk": all_trunk_profiles, "gateway": all_gateway_profiles},
                "queue_summary": qsummary,
                "congestion_analytics": congestion_analytics,
            },
        )

        _perf_mark("capacity_instance_scope")
        # H2-C: inteligência de eventos e pressão operacional explicável.
        event_intelligence = self._event_intelligence(
            queue_rows, trunk_entity_rows, stuck_rows, congestion_analytics, stuck_eps, capacity_eps
        )
        operational_pressure = self._operational_pressure(
            queue_rows, qsummary, congestion_analytics, trunk_stats, trunk_entity_rows,
            stuck_stats, observed_total, infra_mapped, qcoverage, tcoverage, scoverage,
            capacity_operational=capacity_operational,
            trunk_mapped=bool(map_usage.get("trunks")) and bool(queue_trunk_profiles),
            gateway_mapped=bool(map_usage.get("gateways")) and bool(queue_gateway_profiles),
            gcoverage=gcoverage,
        )
        _perf_mark("event_intelligence_pressure")
        operational_feedback = {
            "queue": load_operational_feedback(
                instancia=instancia, entity=f"QUEUE:{instancia}:{codigo}",
                start=start_dt.isoformat(), end=end_dt.isoformat()
            ),
            "stuck_channels": load_operational_feedback(
                instancia=instancia, entity=f"STUCK_CHANNELS:{instancia}",
                start=start_dt.isoformat(), end=end_dt.isoformat()
            ),
        }

        _perf_mark("operational_feedback")
        mode = bounds["mode"]
        profile = (
            self._temporal_profile(queue_rows, trunk_rows, stuck_rows, ANALYTICS_BUCKET_MINUTES)
            if mode == "PROFILE_24H" else []
        )

        _perf_mark("profile_or_rollup")
        # Em cronologia até 24h retornamos as séries. Nos horizontes longos,
        # o gráfico usa perfil 00:00..23:59 e não recebe milhares de pontos.
        chronological = {
            "queue": queue_rows if mode == "CHRONOLOGY" else [],
            "trunk": trunk_rows if mode == "CHRONOLOGY" else [],
            "gateway": gateway_rows if mode == "CHRONOLOGY" else [],
            "stuck": stuck_rows if mode == "CHRONOLOGY" else [],
        }

        period_coverage_pct=float(qcoverage.get("data_coverage_pct") or qcoverage.get("coverage_pct") or 0.0)
        observed_useful_minutes=round(bounds["requested_minutes"]*period_coverage_pct/100.0,2)
        period_semantics={
            "selected_period":bounds["period"],
            "selected_period_label":bounds["label"],
            "presentation_mode":mode,
            "requested_minutes":bounds["requested_minutes"],
            "observed_useful_minutes":observed_useful_minutes,
            "observed_span_minutes":qcoverage.get("span_minutes"),
            "historical_coverage_pct":round(period_coverage_pct,2),
            "historical_complete":bool(qcoverage.get("complete")),
            "historical_status":"COMPLETE" if qcoverage.get("complete") else ("PARTIAL" if qcoverage.get("samples") else "NO_DATA"),
            "live_overview_period":"15m",
            "live_freshness_is_independent":True,
            "queue_read_strategy":"PERSISTENT_LIGHT_SIDECAR" if bounds["requested_minutes"]>=ANALYTICS_SIDECAR_LONG_MINUTES else "ROWID_BOUNDED_RANGE",
            "queue_read_fallback":"ROWID_BOUNDED_RANGE",
            "long_window_rollup":"PROFILE_24H_15MIN" if mode == "PROFILE_24H" else None,
            "cache_bucket_minutes":ANALYTICS_LONG_CACHE_BUCKET_MINUTES if bounds["requested_minutes"]>=7*24*60 else 1,
            "note":"Freshness LIVE descreve quão recente é a fonte. Cobertura histórica descreve quanto do período selecionado foi realmente observado; uma não substitui a outra.",
        }

        payload={
            "schema": "toi_temporal_analytics_v1_3",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "instancia": str(instancia),
            "codigo": str(codigo),
            "period": bounds["period"],
            "period_label": bounds["label"],
            "mode": mode,
            "bounds": {
                "start": bounds["start"],
                "end": bounds["end"],
                "requested_minutes": bounds["requested_minutes"],
                "profile_axis_start": "00:00" if mode == "PROFILE_24H" else None,
                "profile_axis_end": "23:59" if mode == "PROFILE_24H" else None,
            },
            "coverage": {
                "queue": qcoverage,
                "stuck": scoverage,
                "trunk": tcoverage,
                "gateway": gcoverage,
            },
            "period_semantics": period_semantics,
            "queue_summary": qsummary,
            "congestion_analytics": congestion_analytics,
            "event_intelligence": event_intelligence,
            "operational_pressure": operational_pressure,
            "operational_recommendations": operational_pressure.get("recommendations", []),
            "operational_feedback": operational_feedback,
            "stuck_analytics": {
                **stuck_stats,
                "episodes_count": len(stuck_eps),
                "affected_minutes": stuck_affected_min,
                "channel_minutes": stuck_channel_min,
                "max_episode_minutes": max([x.get("duration_min", 0) for x in stuck_eps] or [0]),
                "episodes": stuck_eps,
                "coverage": scoverage,
                "granularity": "COUNT_BY_INSTANCE",
                "individual_channel_ids_available": False,
                "semantic": {
                    "max_concurrent":"Maior quantidade simultânea observada; não é total de canais únicos.",
                    "episodes":"Períodos contínuos em que a quantidade ficou acima de zero.",
                    "affected_minutes":"Tempo observado com pelo menos um canal travado.",
                    "channel_minutes":"Exposição acumulada: quantidade simultânea × tempo. Ex.: 10 canais por 5 min = 50 canal-min.",
                    "parking_analogy":"Pense nos canais como vagas de estacionamento. Cada canal travado é uma vaga interditada; cada minuto de interdição soma exposição.",
                },
                "note": (
                    "A aquisição congelada persiste quantidade por Instância. "
                    "O payload atual não identifica individualmente cada canal travado."
                ),
            },
            "capacity_analytics": {
                "current": trunk_stats.get("current"),
                "mean": trunk_stats.get("mean"),
                "p95": trunk_stats.get("p95"),
                "peak": trunk_stats.get("max"),
                "headroom_p95": (
                    round(max(0.0, 100.0 - float(trunk_stats["p95"])), 2)
                    if trunk_stats.get("p95") is not None else None
                ),
                "minutes_over_85": over85,
                "minutes_over_90": over90,
                "episodes_over_90": len(capacity_eps),
                "episodes": capacity_eps,
                "gateway_p95": gateway_stats.get("p95"),
                "observed_route_capacity_channels": round(observed_total, 2),
                "observed_route_busy_channels": round(observed_busy, 2),
                "relevant_trunk_groups": len(relevant_trunks),
                "coverage": tcoverage,
                "scope_mode": capacity_scope_mode,
                "scope_note": capacity_scope_note,
                "operational": capacity_operational,
                "critical_group": capacity_operational.get("critical_group"),
                "portfolio_current_occupancy_nominal": capacity_operational.get("portfolio_current_occupancy_nominal"),
                "portfolio_current_headroom_nominal": capacity_operational.get("portfolio_current_headroom_nominal"),
                "gateway_current_max": capacity_operational.get("gateway_current_max"),
                "gateway_p95_operational": capacity_operational.get("gateway_p95"),
                "resilience_status": capacity_operational.get("resilience_status"),
                "evidence_confidence": capacity_operational.get("evidence_confidence"),
                "bottleneck_domain": capacity_operational.get("bottleneck_domain"),
                "bottleneck_label": capacity_operational.get("bottleneck_label"),
                "capacity_effective_status": capacity_operational.get("capacity_effective_status"),
                "contingency_headroom_status": capacity_operational.get("contingency_headroom_status"),
                "historical_pressure_group": capacity_operational.get("historical_pressure_group"),
                "trunk_group_profiles": capacity_operational.get("trunk_group_profiles", []),
                "gateway_profiles": capacity_operational.get("gateway_profiles", []),
                "gateway_contexts": capacity_operational.get("gateway_contexts", []),
                "dimensions": capacity_operational.get("dimensions", {}),
            },
            "instance_analytics": {
                "scope_mode": "INSTANCE_FULL",
                "trunk_group_profiles": instance_capacity.get("trunk_group_profiles", []),
                "gateway_profiles": instance_capacity.get("gateway_profiles", []),
                "trunk_current_max": instance_capacity.get("current_max"),
                "trunk_p95_max": instance_capacity.get("p95_occupancy"),
                "historical_pressure_group": instance_capacity.get("historical_pressure_group"),
                "gateway_current_max": instance_capacity.get("gateway_current_max"),
                "gateway_p95_max": instance_capacity.get("gateway_p95"),
                "trunk_count": len(instance_infra.get("trunks") or []),
                "gateway_count": len(instance_infra.get("gateways") or []),
            },
            "gateway_health_analytics": {
                "available_on_demand": True,
                "endpoint": "/api/gateway/analytics",
                "mode": "LAZY_READ_ONLY_GATEWAY_SHADOW",
            },
            "profile": profile,
            "series": chronological,
            "metric_semantics": {
                "calls_dialing": {
                    "type": "GAUGE",
                    "aggregation": "mean/p95/max",
                    "period_total_available": False,
                    "note": "Chamadas Discando é simultaneidade. O total de chamadas iniciadas no período não pode ser obtido somando snapshots; a fonte atual não expõe contador/ID de início suficiente para esse total."
                },
                "hit_rate": {
                    "type":"SNAPSHOT_PERCENTAGE",
                    "aggregation":"mean_of_observed_samples/p95/max",
                    "weighted_by_call_volume":False,
                    "note":"A média do período é a média das porcentagens observadas nos snapshots; não é ponderada por volume de chamadas porque o denominador necessário não está disponível nesta fonte."
                },
                "calls_in_attendance": {
                    "type": "GAUGE",
                    "aggregation": "mean/p95/max",
                    "note": "Em Atendimento é simultaneidade; somar snapshots duplicaria chamadas."
                },
                "buffer": {
                    "type": "GAUGE",
                    "aggregation": "mean/p95/max",
                },
                "dialing_rate": {
                    "type": "RATE",
                    "aggregation": "mean/p95/max",
                },
                "stuck_channels": {
                    "type": "GAUGE_EVENT",
                    "aggregation": "episodes/channel-minutes/max-concurrent",
                    "note": "Canal-minuto mede exposição acumulada (quantidade simultânea × tempo); não é contagem de canais únicos. A fonte atual não identifica canais individualmente."
                },
                "trunk_occupancy": {
                    "type": "RATIO",
                    "aggregation": "mean/p95/max/time-above-threshold",
                },
                "profile_24h": {
                    "type":"AGGREGATED_TIME_OF_DAY_PROFILE",
                    "note":"O Perfil Horário Agregado consolida amostras observadas por faixa de horário; sua linha não representa a cronologia do pico bruto. Picos reais permanecem nos cards/Event Intelligence com data e hora."
                },
            },
            "infra_context": {
                "inferred_from_lastcalls": bool(infra_context.get("inferred_from_lastcalls")),
                "lastcalls_sample_ts": (infra_context.get("usage") or {}).get("sample_ts"),
                "calls_used_for_mapping": (infra_context.get("usage") or {}).get("calls", 0),
                "mapping_window_start": (infra_context.get("usage") or {}).get("window_start"),
                "mapping_window_end": (infra_context.get("usage") or {}).get("window_end"),
                "mapping_snapshots_total": (infra_context.get("usage") or {}).get("snapshots_total", 0),
                "mapping_snapshots_used": (infra_context.get("usage") or {}).get("snapshots_used", 0),
                "mapping_sampling_stride": (infra_context.get("usage") or {}).get("sampling_stride", 1),
                "mapping_complete": bool((infra_context.get("usage") or {}).get("mapping_complete", True)),
                "instance_trunk_count": len(infra_context.get("instance_trunks") or []),
                "instance_gateway_count": len(infra_context.get("instance_gateways") or []),
                "relevant_trunks": [
                    {
                        "entity": x.get("entity"),
                        "total": x.get("total"),
                        "busy": x.get("busy"),
                        "occupancy": x.get("occupancy"),
                    }
                    for x in relevant_trunks
                ],
                "relevant_gateways": [
                    {
                        "entity": x.get("entity"),
                        "total": x.get("total"),
                        "busy": x.get("busy"),
                        "occupancy": x.get("occupancy"),
                    }
                    for x in relevant_gateways
                ],
            },
            "gateway_semantic_context": gateway_semantic_context,
            "performance": {
                "cache_mode":"IN_MEMORY_PLUS_PERSISTENT_DERIVED_SIDECAR",
                "cache_ttl_s":cache_ttl,
                "hotfix":"R1.2.4 H2-C2 COMPUTE_PATH_CORRECTION",
                "release":ANALYTICS_RELEASE,
                "cache_hit":False,
                "cache_key_slot":cache_slot,
                "sidecar":dict(self._sidecar_status),
                "long_window_profile_buckets":len(profile) if mode=="PROFILE_24H" else 0,
                "queue_raw_transport":"JSON_EXTRACT_FIELDS_ONLY_WITH_FALLBACK",
                "rows_loaded":{
                    "queue":len(queue_rows),"stuck":len(stuck_rows),"trunk":len(trunk_rows),"gateway":len(gateway_rows),
                    "trunk_entities":len(trunk_entity_rows),"gateway_entities":len(gateway_entity_rows),
                    "trunk_all_entities":len(trunk_all_entity_rows),"gateway_all_entities":len(gateway_all_entity_rows),
                },
            },
            "safety": {
                "mode": "READ_ONLY_ANALYTICS_ON_LIVE_DB_WITH_DERIVED_SIDECAR",
                "live_db_read_only": True,
                "sidecar_path": str(ANALYTICS_SIDECAR_DB),
                "sql_mutation": False,
                "changes_diagnosis": False,
                "changes_alerts": False,
                "changes_acquisition": False,
            },
        }
        _perf_mark("payload_build")
        perf=payload.setdefault("performance",{})
        perf["stages_ms"]=dict(_perf_stages)
        perf["compute_total_ms"]=round((time.perf_counter()-_perf_total_t0)*1000.0,2)
        return self._cache_put(self._analytics_cache,cache_key,payload)

    @staticmethod
    def infra_period_summary(infra_series, requested_minutes):
        trunks = (infra_series or {}).get("trunk", [])
        gateways = (infra_series or {}).get("gateway", [])
        return {
            "trunk_occupancy": DataService._metric_stats([x.get("occupancy") for x in trunks]),
            "gateway_occupancy": DataService._metric_stats([x.get("occupancy") for x in gateways]),
            "trunk_coverage": DataService._coverage_metadata(trunks, requested_minutes),
            "gateway_coverage": DataService._coverage_metadata(gateways, requested_minutes),
        }

    @staticmethod
    def _age_seconds(ts):
        if not ts:
            return None
        try:
            dt = datetime.fromisoformat(str(ts))
            now = datetime.now().astimezone()
            if dt.tzinfo is None:
                dt = dt.astimezone()
            return round(max(0.0, (now - dt).total_seconds()), 1)
        except Exception:
            return None

    def freshness(self, instancia, codigo, latest, infra, stuck, dialer):
        queue_age = self._age_seconds((latest or {}).get("ts"))
        stuck_age = self._age_seconds((stuck or {}).get("ts"))
        dialer_age = self._age_seconds((dialer or {}).get("ts"))
        infra_ts = []
        for x in ((infra or {}).get("gateways", []) + (infra or {}).get("trunks", [])):
            if x.get("ts"):
                infra_ts.append(x.get("ts"))
        infra_age = self._age_seconds(max(infra_ts) if infra_ts else None)

        def item(age, warn, stale, source, required=True):
            base={"age_s":age,"warn_s":warn,"stale_s":stale,"source":source,"required":required}
            if age is None:
                return {**base,"status":"SEM_DADOS","fresh":False,"usable":False}
            if age <= warn:
                return {**base,"status":"LIVE","fresh":True,"usable":True}
            if age <= stale:
                return {**base,"status":"ATRASO","fresh":False,"usable":True}
            return {**base,"status":"STALE","fresh":False,"usable":False}

        ip = self.instance_policy(instancia)
        qp = self.queue_policy(instancia, codigo)
        queue_warn = float(qp.get("target_s", 20))
        queue_stale = float(qp.get("max_age_s", 35))
        infp = ip.get("infra") or {}
        infra_warn = max(float(infp.get("gateway_target_s", 60)), float(infp.get("trunk_target_s", 60))) + 15.0
        infra_stale = float(infp.get("max_age_s", 120))

        queue = item(queue_age, queue_warn, queue_stale, "queue", True)
        queue["target_s"] = queue_warn
        infra_item = item(infra_age, infra_warn, infra_stale, "infra", True)
        stuck_item = item(stuck_age, 25, 45, "stuck_channels", True)

        sp = ip.get("slow_path") or {}
        slow_enabled = bool(sp.get("enabled", True))
        if not slow_enabled:
            dialer_item = {
                "age_s": dialer_age, "status":"PAUSADO", "fresh":False,
                "usable":False, "required":False, "source":"dialer",
                "reason":"Coleta suspensa por política do slow-path"
            }
        else:
            # O Discador é propositalmente uma fonte lenta/background. Freshness
            # deve respeitar a própria cadência, sem contaminar o SLA da fila.
            dial_target = float(sp.get("dialer_s", 600))
            dial_warn = max(240.0, dial_target * 1.20)
            dial_stale = max(360.0, dial_target * 2.00)
            dialer_item = item(dialer_age, dial_warn, dial_stale, "dialer", False)
            dialer_item["target_s"] = dial_target

        sources={
            "queue":queue,"infra":infra_item,
            "stuck_channels":stuck_item,"dialer":dialer_item,
        }

        if queue["status"] in ("STALE","SEM_DADOS"):
            overall="STALE"
        elif queue["status"]=="ATRASO":
            overall="PARCIAL"
        elif any(sources[k]["status"] in ("ATRASO","STALE","SEM_DADOS") for k in ("infra","stuck_channels")):
            overall="PARCIAL"
        else:
            overall="LIVE"

        weights={"queue":40,"infra":30,"stuck_channels":15,"dialer":15}
        status_factor={"LIVE":1.0,"ATRASO":0.65,"STALE":0.0,"SEM_DADOS":0.0,"PAUSADO":0.35}
        quality_score=round(sum(weights[k]*status_factor.get(sources[k]["status"],0) for k in weights))
        excluded=[k for k,v in sources.items() if not v.get("usable")]
        return {
            "overall":overall,"sources":sources,"quality_score":quality_score,
            "excluded_sources":excluded,
            "checked_at":datetime.now().astimezone().isoformat(timespec="seconds"),
        }

    def overview(self, instancia, codigo, minutes):
        key=(str(instancia),str(codigo),int(minutes))
        with self._overview_lock:
            hit=self._cache_get(self._overview_cache,key,OVERVIEW_CACHE_TTL_S)
            if hit is not None:
                return hit
            value=self._overview_uncached(instancia,codigo,minutes)
            return self._cache_put(self._overview_cache,key,value)

    def _overview_uncached(self, instancia, codigo, minutes):
        # Uma unica leitura ROWID-bounded de 60 min alimenta latest, serie LIVE,
        # features 5/15/30/60 e incidentes. A versao anterior relia a mesma
        # queue_snapshot em pelo menos oito consultas independentes por poll.
        feature_minutes=max(60,int(minutes))
        feature_rows,queue_watermark=self.live_timeseries(instancia,codigo,feature_minutes)
        if not feature_rows:
            return {
                "db": str(self.db_path),
                "requested_minutes": minutes,
                "live_read_path": {
                    "mode": "FAST_NEGATIVE_NO_FULL_SCAN",
                    "queue_feature_window_minutes": feature_minutes,
                    "queue_rows_loaded_once": 0,
                    "queue_rows_returned": 0,
                    "queue_rowid_watermark": queue_watermark,
                    "overview_cache_ttl_s": OVERVIEW_CACHE_TTL_S,
                    "live_db_read_only": True,
                    "absent_queue_fast_negative": True,
                },
                "latest": None, "series": [], "period_summary": {}, "period_coverage": {},
                "infra": {"trunks": [], "gateways": []},
                "infra_all": {"trunks": [], "gateways": []},
                "infra_context": {"trunks": [], "gateways": [], "inferred_from_lastcalls": False},
                "infra_series": {"trunk": [], "gateway": [], "trunk_entities": [], "gateway_entities": []},
                "infra_period_summary": {}, "stuck": None, "stuck_series": [], "stuck_summary": {},
                "dialer": {}, "source_health": {},
                "freshness": {"overall": "STALE", "quality_score": 0, "excluded_sources": ["queue"]},
                "quality_gate": {"score": 0, "excluded_sources": ["queue"], "sources": {}},
                "diagnosis": {
                    "title": "Fila sem amostras no banco LIVE",
                    "severity": "DADOS", "confidence": 0, "technical_confidence": 0,
                    "quality_score": 0, "health_score": None, "factors": [], "scores": {},
                },
                "features": {},
                "capacity": {"status": "SEM_DADOS", "risk": "SEM_DADOS", "reason": "Fila sem amostras no banco LIVE"},
                "recommendations": [], "incidents": [], "ml_shadow": {},
            }
        latest=feature_rows[-1] if feature_rows else self.latest_queue(instancia,codigo)
        if latest and latest.get("ts"):
            end_dt=datetime.fromisoformat(str(latest["ts"]))
            start_dt=end_dt-timedelta(minutes=minutes)
            series=[r for r in feature_rows if start_dt.isoformat()<=str(r.get("ts") or "")<=end_dt.isoformat()]
        else:
            start_dt=end_dt=None
            series=[]
        period_summary = self.period_summary(instancia, codigo, minutes, series)

        infra = self.infra(instancia, minutes)
        try:
            context_start = datetime.fromisoformat(series[0]["ts"]) if series else None
            context_end = datetime.fromisoformat(series[-1]["ts"]) if series else None
        except Exception:
            context_start = context_end = None
        infra_context = self.queue_infra_context(
            instancia, codigo, infra, start_dt=context_start, end_dt=context_end
        )
        infra_series = self.infra_history(instancia, codigo, minutes, infra_context)
        infra_period = self.infra_period_summary(infra_series, minutes)

        relevant_infra = {
            "trunks": infra_context.get("trunks", []),
            "gateways": infra_context.get("gateways", []),
        }
        gateway_semantic_context = load_gateway_semantic_reference(instancia)

        stuck = self.stuck(instancia)
        stuck_series = self.stuck_history(instancia, minutes)
        stuck_summary = self.stuck_period_summary(instancia, minutes, stuck_series)

        dialer = self.dialer(instancia, codigo)
        dialer_summary = self.parse_dialer_summary(dialer)
        # Linhagem explícita: estes dois rótulos do painel aprovado vêm de
        # getqueuedetaildata, não do endpoint de campanhas do Discador.
        # Usamos exatamente o mesmo snapshot de fila do restante do dashboard.
        dialer_summary["queue_hit_rate5"] = (latest or {}).get("hit_rate5")
        dialer_summary["mailing_on_screen"] = (latest or {}).get("mailing_on_screen")
        dialer_summary["queue_source_ts"] = (latest or {}).get("ts")
        source_health = self.source_health(instancia, minutes, start_dt=context_start, end_dt=context_end)
        freshness = self.freshness(instancia, codigo, latest, relevant_infra, stuck, dialer)

        # Features fixas continuam existindo para o futuro ML Shadow.
        # A UI não usa mais fallback de 15m para os períodos longos.
        features = self._feature_windows_from_rows(feature_rows)

        # Diagnóstico da janela: preserva "agora" em latest, mas usa o
        # máximo observado de canais/infra na janela para não ignorar eventos históricos.
        stuck_diag = dict(stuck or {})
        if stuck_summary.get("max") is not None:
            stuck_diag["quantity"] = int(round(stuck_summary["max"]))

        diag_infra = {"trunks": [], "gateways": []}
        trunk_max = (infra_period.get("trunk_occupancy") or {}).get("max")
        gateway_max = (infra_period.get("gateway_occupancy") or {}).get("max")
        if trunk_max is not None:
            diag_infra["trunks"] = [{"occupancy": trunk_max}]
        elif relevant_infra.get("trunks"):
            diag_infra["trunks"] = relevant_infra["trunks"]
        if gateway_max is not None:
            diag_infra["gateways"] = [{"occupancy": gateway_max}]
        elif relevant_infra.get("gateways"):
            diag_infra["gateways"] = relevant_infra["gateways"]

        # QUALITY GATE: fontes não utilizáveis nunca entram no motor causal.
        infra_status = freshness["sources"]["infra"]["status"]
        stuck_status = freshness["sources"]["stuck_channels"]["status"]
        dialer_status = freshness["sources"]["dialer"]["status"]
        if infra_status in ("STALE","SEM_DADOS"):
            diag_infra = {"trunks": [], "gateways": []}
        if stuck_status in ("STALE","SEM_DADOS"):
            stuck_diag = {}
        if dialer_status in ("STALE","SEM_DADOS","PAUSADO"):
            dialer_summary_for_diag = {}
        else:
            dialer_summary_for_diag = dialer_summary

        diagnosis = self.diagnose(
            latest, series, diag_infra, stuck_diag, dialer_summary_for_diag, source_health, freshness
        )
        if freshness["sources"]["queue"]["status"] == "STALE":
            age = freshness["sources"]["queue"].get("age_s")
            diagnosis = {
                "title": "Dados desatualizados — aguardando coleta LIVE",
                "severity": "DADOS",
                "confidence": 0,
                "technical_confidence": 0,
                "quality_score": freshness.get("quality_score", 0),
                "quality_notes": ["Fila fora da janela de qualidade — diagnóstico operacional suspenso"],
                "health_score": None,
                "p135_avg": None,
                "max_trunk": None,
                "max_gateway": None,
                "factors": [{"impact": -20, "text": f"Última amostra da fila há {age:.0f}s" if age is not None else "Fila sem amostra recente"}],
                "scores": {},
            }
        if freshness["sources"]["infra"].get("usable"):
            capacity = self.capacity_analysis(
                relevant_infra, features, infra_series, window_minutes=minutes,
                latest=latest or {}, stuck_summary=stuck_summary,
                dialer_summary=dialer_summary_for_diag,
                gateway_semantic_context=gateway_semantic_context,
                evidence_context={
                    "infra_coverage": infra_period.get("trunk_coverage") or {},
                    "gateway_coverage": infra_period.get("gateway_coverage") or {},
                    "scope_mode": (
                        "QUEUE_MAPPED_BY_LASTCALLS"
                        if infra_context.get("inferred_from_lastcalls") else "INSTANCE_FALLBACK"
                    ),
                    "freshness": freshness.get("sources", {}).get("infra", {}),
                },
            )
        else:
            capacity = {
                "status":"SEM_DADOS","risk":"SEM_DADOS","current_max":None,
                "p95_occupancy":None,"peak_occupancy":None,"headroom":None,
                "minutes_over_90_est":None,
                "reason":f"Infra {freshness['sources']['infra']['status']} — não considerada pelo Quality Gate"
            }
        recommendations = self.build_recommendations(
            diagnosis, latest or {}, {"selected": period_summary}, capacity,
            dialer_summary_for_diag, freshness
        )
        if freshness["overall"] == "STALE":
            recommendations = []
        incidents = self._recent_incidents_from_rows(series)
        ml_shadow = self.ml_shadow_latest(instancia, codigo) or self.ml_shadow(features, diagnosis)

        return {
            "db": str(self.db_path),
            "requested_minutes": minutes,
            "live_read_path": {
                "mode": "ROWID_BOUNDED_SINGLE_SERIES",
                "queue_feature_window_minutes": feature_minutes,
                "queue_rows_loaded_once": len(feature_rows),
                "queue_rows_returned": len(series),
                "queue_rowid_watermark": queue_watermark,
                "overview_cache_ttl_s": OVERVIEW_CACHE_TTL_S,
                "live_db_read_only": True,
            },
            "latest": latest,
            "series": series,
            "period_summary": period_summary,
            "period_coverage": period_summary.get("coverage", {}),
            "infra": relevant_infra,
            "infra_all": infra,
            "infra_context": infra_context,
            "infra_series": infra_series,
            "infra_period_summary": infra_period,
            "stuck": stuck,
            "stuck_series": stuck_series,
            "stuck_summary": stuck_summary,
            "dialer": dialer_summary,
            "source_health": source_health,
            "freshness": freshness,
            "quality_gate": {
                "score": freshness.get("quality_score"),
                "excluded_sources": freshness.get("excluded_sources", []),
                "sources": freshness.get("sources", {}),
            },
            "diagnosis": diagnosis,
            "features": features,
            "capacity": capacity,
            "gateway_semantic_context": gateway_semantic_context,
            "recommendations": recommendations,
            "recent_incidents": incidents,
            "ml_shadow": ml_shadow,
            "data_status": {
                "queue": freshness["sources"]["queue"]["status"] if latest else "SEM_DADOS",
                "infra": freshness["sources"]["infra"]["status"] if (relevant_infra.get("gateways") or relevant_infra.get("trunks")) else "SEM_DADOS",
                "infra_relation": "OK" if infra_context.get("inferred_from_lastcalls") else "SEM_RELACAO_COMPROVADA",
                "dialer": (dialer_summary.get("data_state") if freshness["sources"]["dialer"].get("usable") else freshness["sources"]["dialer"]["status"]),
                "stuck_channels": freshness["sources"]["stuck_channels"]["status"] if stuck else "SEM_DADOS",
            },
        }

    @staticmethod
    def _feature_windows_from_rows(rows):
        """Calcula 5/15/30/60 min sobre uma unica serie ja carregada."""
        if not rows:return {}
        anchor=datetime.fromisoformat(str(rows[-1].get("ts")))
        out={}
        for label,minutes in (("5m",5),("15m",15),("30m",30),("60m",60)):
            start=anchor-timedelta(minutes=minutes)
            norm=[r for r in rows if r.get("ts") and datetime.fromisoformat(str(r["ts"]))>=start]
            if not norm:
                out[label]={"samples":0};continue
            def vals(key):return [float(x.get(key) or 0) for x in norm]
            def stats(values):
                if not values:return {}
                sv=sorted(values);idx=min(len(sv)-1,max(0,round((len(sv)-1)*0.95)))
                return {"current":round(values[-1],3),"mean":round(sum(values)/len(values),3),
                        "min":round(min(values),3),"max":round(max(values),3),"p95":round(sv[idx],3),
                        "delta":round(values[-1]-values[0],3) if len(values)>1 else 0}
            agents_total=[float(x.get("agents_available") or 0)+float(x.get("agents_talking") or 0)+
                          float(x.get("agents_acw") or 0)+float(x.get("agents_paused") or 0)+
                          float(x.get("agents_other") or 0)+float(x.get("agents_dialing") or 0) for x in norm]
            dial=vals("calls_dialing");buf=vals("buffer");avail=vals("agents_available")
            p34=vals("p34_pct");p135=vals("p135_pct")
            out[label]={"samples":len(norm),"p135":stats(p135),"p34":stats(p34),
                "hit_rate5":stats(vals("hit_rate5")),"hit_rate15":stats(vals("hit_rate15")),
                "calls_dialing":stats(dial),"calls_in_attendance":stats(vals("calls_in_attendance")),
                "buffer":stats(buf),"dialing_rate":stats(vals("dialing_rate")),
                "agents_available":stats(avail),"agents_total":stats(agents_total),
                "available_agent_pct":stats([(a/t*100.0) for a,t in zip(avail,agents_total) if t>0]),
                "dialing_per_available_agent":stats([d/a for d,a in zip(dial,avail) if a>0]),
                "buffer_per_available_agent":stats([b/a for b,a in zip(buf,avail) if a>0]),
                "minutes_p135_over_20_est":round(sum(1 for x in p135 if x>=20)*minutes/max(len(p135),1),2),
                "minutes_p34_over_20_est":round(sum(1 for x in p34 if x>=20)*minutes/max(len(p34),1),2)}
        return out

    def _recent_incidents_from_rows(self, rows, limit=12):
        merged=[]
        for typ,key in (("P34","p34_pct"),("P135","p135_pct")):
            for ep in self._threshold_lifecycle(rows,key,20.0,limit=max(100,limit*10)):
                item=dict(ep);item["type"]=typ;merged.append(item)
        def event_dt(x):
            try:return datetime.fromisoformat(str(x.get("resolved_at") or x.get("last_critical_ts") or x.get("start")))
            except Exception:return datetime.min
        merged.sort(key=event_dt,reverse=True)
        return merged[:limit]

    def feature_windows(self, instancia, codigo):
        """Features temporais para regras e futuro ML Shadow Mode."""
        return self._feature_windows_from_rows(self.timeseries(instancia,codigo,60))
        # Legacy implementation retained below for base-diff traceability; the
        # bounded return above is the only reachable path.
        with closing(self.connect()) as conn:
            max_ts = conn.execute("""
                SELECT MAX(ts) FROM queue_snapshot
                WHERE instancia=? AND codigo=?
            """, (str(instancia), str(codigo))).fetchone()[0]
            if not max_ts:
                return {}

            anchor = datetime.fromisoformat(max_ts)
            out = {}

            for label, minutes in (("5m", 5), ("15m", 15), ("30m", 30), ("60m", 60)):
                start = (anchor - timedelta(minutes=minutes)).isoformat()
                rows = conn.execute("""
                    SELECT p34_pct,p135_pct,calls_dialing,calls_in_attendance,
                           hit_rate5,hit_rate15,buffer,dialing_rate,
                           agents_available,agents_talking,agents_acw,agents_paused,
                           raw_queue
                    FROM queue_snapshot
                    WHERE instancia=? AND codigo=? AND ts>=?
                    ORDER BY ts
                """, (str(instancia), str(codigo), start)).fetchall()

                norm = [normalize_queue_raw(r) for r in rows]
                if not norm:
                    out[label] = {"samples": 0}
                    continue

                def vals(key):
                    return [float(x.get(key) or 0) for x in norm]

                def metric_stats(key):
                    v = vals(key)
                    if not v:
                        return {}
                    sv = sorted(v)
                    idx = min(len(sv)-1, max(0, round((len(sv)-1) * 0.95)))
                    return {
                        "current": round(v[-1], 3),
                        "mean": round(sum(v)/len(v), 3),
                        "min": round(min(v), 3),
                        "max": round(max(v), 3),
                        "p95": round(sv[idx], 3),
                        "delta": round(v[-1]-v[0], 3) if len(v) > 1 else 0,
                    }

                p135 = vals("p135_pct")
                p34 = vals("p34_pct")

                agents_total_values=[
                    float(x.get("agents_available") or 0)+float(x.get("agents_talking") or 0)+float(x.get("agents_acw") or 0)+
                    float(x.get("agents_paused") or 0)+float(x.get("agents_other") or 0)+float(x.get("agents_dialing") or 0)
                    for x in norm
                ]
                dial_vals=vals("calls_dialing"); buf_vals=vals("buffer"); avail_vals=vals("agents_available")
                def derived_stats(values):
                    if not values:return {}
                    sv=sorted(values);idx=min(len(sv)-1,max(0,round((len(sv)-1)*0.95)))
                    return {"current":round(values[-1],3),"mean":round(sum(values)/len(values),3),"min":round(min(values),3),"max":round(max(values),3),"p95":round(sv[idx],3),"delta":round(values[-1]-values[0],3) if len(values)>1 else 0}
                dial_per_agent=[d/a for d,a in zip(dial_vals,avail_vals) if a>0]
                buffer_per_agent=[b/a for b,a in zip(buf_vals,avail_vals) if a>0]
                available_pct=[(a/t*100.0) for a,t in zip(avail_vals,agents_total_values) if t>0]

                out[label] = {
                    "samples": len(norm),
                    "p135": metric_stats("p135_pct"),
                    "p34": metric_stats("p34_pct"),
                    "hit_rate5": metric_stats("hit_rate5"),
                    "hit_rate15": metric_stats("hit_rate15"),
                    "calls_dialing": metric_stats("calls_dialing"),
                    "calls_in_attendance": metric_stats("calls_in_attendance"),
                    "buffer": metric_stats("buffer"),
                    "dialing_rate": metric_stats("dialing_rate"),
                    "agents_available": metric_stats("agents_available"),
                    "agents_total": derived_stats(agents_total_values),
                    "available_agent_pct": derived_stats(available_pct),
                    "dialing_per_available_agent": derived_stats(dial_per_agent),
                    "buffer_per_available_agent": derived_stats(buffer_per_agent),
                    "minutes_p135_over_20_est": round(
                        sum(1 for x in p135 if x >= 20) * minutes / max(len(p135), 1), 2
                    ),
                    "minutes_p34_over_20_est": round(
                        sum(1 for x in p34 if x >= 20) * minutes / max(len(p34), 1), 2
                    ),
                }
            return out

    def recent_incidents(self, instancia, codigo, minutes=1440, limit=12):
        """Episódios P34 e P135 >=20% independentes, com resolução observada explícita.

        A resolução só existe quando há primeira amostra abaixo de 20% sem gap de continuidade.
        P34 e P135 não se anulam entre si por dominância relativa.
        """
        return self._recent_incidents_from_rows(self.timeseries(instancia,codigo,minutes),limit=limit)
        # Legacy implementation retained below for base-diff traceability; the
        # bounded return above is the only reachable path.
        with closing(self.connect()) as conn:
            max_ts = conn.execute("""
                SELECT MAX(ts) FROM queue_snapshot
                WHERE instancia=? AND codigo=?
            """, (str(instancia), str(codigo))).fetchone()[0]
            if not max_ts:
                return []
            anchor = datetime.fromisoformat(max_ts)
            start_dt = anchor - timedelta(minutes=minutes)
            rows = [dict(r) for r in conn.execute("""
                SELECT ts,p34_pct,p135_pct
                FROM queue_snapshot
                WHERE instancia=? AND codigo=? AND ts>=?
                ORDER BY ts
            """, (str(instancia), str(codigo), start_dt.isoformat())).fetchall()]
        merged=[]
        for typ,key in (("P34","p34_pct"),("P135","p135_pct")):
            for ep in self._threshold_lifecycle(rows,key,20.0,limit=max(100,limit*10)):
                item=dict(ep);item["type"]=typ
                merged.append(item)
        def event_dt(x):
            raw=x.get("resolved_at") or x.get("last_critical_ts") or x.get("start")
            try:return datetime.fromisoformat(str(raw))
            except Exception:return datetime.min
        merged.sort(key=event_dt,reverse=True)
        return merged[:limit]

    @staticmethod
    def capacity_analysis(
        infra, features, infra_series=None, window_minutes=15,
        latest=None, stuck_summary=None, dialer_summary=None,
        gateway_semantic_context=None, evidence_context=None,
    ):
        """Capacity Engine explicável v2.1 — Evidence Semantics Hardening.

        Princípios:
        - N/C nunca é convertido em zero;
        - maior grupo atual != maior percentil 95 histórico != ocupação nominal mapeada;
        - capacidade efetiva/contingência continuam N/C sem topologia comprovada;
        - confiança é explicável por freshness, cobertura, gaps, escopo e Ground Truth;
        - papel/lifecycle de Gateway qualifica resiliência sem alterar aquisição ou RAW;
        - não fabrica vínculo fila -> rota/gateway/canal ausente nas fontes.
        """
        infra = infra or {}
        latest = latest or {}
        stuck_summary = stuck_summary or {}
        dialer_summary = dialer_summary or {}
        evidence_context = evidence_context or {}
        semantic_rows = [dict(x) for x in (gateway_semantic_context or {}).get("gateways", [])]
        window_start = evidence_context.get("window_start")
        window_end = evidence_context.get("window_end")
        try: window_start_dt = datetime.fromisoformat(window_start) if window_start else None
        except Exception: window_start_dt = None
        try: window_end_dt = datetime.fromisoformat(window_end) if window_end else None
        except Exception: window_end_dt = None

        trunks = [dict(x) for x in (infra.get("trunks") or [])]
        gateways = [dict(x) for x in (infra.get("gateways") or [])]
        infra_series = infra_series or {}
        trunk_series = [dict(x) for x in (infra_series.get("trunk_entities") or infra_series.get("trunk") or [])]
        gateway_series = [dict(x) for x in (infra_series.get("gateway_entities") or infra_series.get("gateway") or [])]

        def _num(value):
            if value is None or value == "":
                return None
            try:
                n = float(value)
            except (TypeError, ValueError):
                return None
            if n != n or n in (float("inf"), float("-inf")):
                return None
            return n

        def _p95(values):
            vals = sorted(v for v in (_num(x) for x in values) if v is not None)
            if not vals:
                return None
            idx = min(len(vals)-1, max(0, round((len(vals)-1) * 0.95)))
            return vals[idx]

        precomputed = evidence_context.get("precomputed_profiles") or {}
        trunk_profiles = precomputed.get("trunk")
        gateway_profiles = precomputed.get("gateway")
        if trunk_profiles is None:
            trunk_profiles = DataService._capacity_group_profiles(trunk_series, trunks, window_start_dt, window_end_dt)
        if gateway_profiles is None:
            gateway_profiles = DataService._capacity_group_profiles(gateway_series, gateways, window_start_dt, window_end_dt)

        current_candidates = [x for x in trunk_profiles if x.get("current_pct") is not None]
        p95_candidates = [x for x in trunk_profiles if x.get("p95_pct") is not None]
        # H2-C2 Consistency Closure: não transforma empate/zero em entidade arbitrária.
        critical_group = None
        current_selection_reason = "NO_CURRENT_PRESSURE"
        if current_candidates:
            ordered_current = sorted(current_candidates, key=lambda x: float(x.get("current_pct") or 0.0), reverse=True)
            top = ordered_current[0]
            top_v = float(top.get("current_pct") or 0.0)
            second_v = float(ordered_current[1].get("current_pct") or 0.0) if len(ordered_current) > 1 else None
            if top_v > 0 and (second_v is None or abs(top_v-second_v) > 1e-9):
                critical_group = top
                current_selection_reason = "UNIQUE_MAX"
            elif top_v > 0:
                current_selection_reason = "TIED_MAX"
        historical_pressure_group = max(p95_candidates, key=lambda x: x["p95_pct"], default=None)

        current = (critical_group or {}).get("current_pct")
        p95 = (historical_pressure_group or {}).get("p95_pct")
        peak_values = [x.get("peak_pct") for x in trunk_profiles if x.get("peak_pct") is not None]
        peak = max(peak_values) if peak_values else None
        minutes_over90 = (historical_pressure_group or {}).get("minutes_over_90")
        headroom = (historical_pressure_group or {}).get("headroom_p95_pct")

        # Ocupação nominal mapeada usa SOMENTE grupos com busy e total conhecidos.
        nominal_pairs = []
        for row in trunks:
            busy = _num(row.get("busy"))
            total = _num(row.get("total"))
            if busy is None or total is None or total <= 0:
                continue
            nominal_pairs.append((max(0.0, busy), max(0.0, total)))
        nominal_busy = sum(x[0] for x in nominal_pairs) if nominal_pairs else None
        nominal_total = sum(x[1] for x in nominal_pairs) if nominal_pairs else None
        portfolio_occ = (
            round((nominal_busy / nominal_total) * 100.0, 2)
            if nominal_total is not None and nominal_total > 0 else None
        )
        portfolio_headroom = round(max(0.0, 100.0 - portfolio_occ), 2) if portfolio_occ is not None else None

        gw_current_candidates = [x for x in gateway_profiles if x.get("current_pct") is not None]
        gw_p95_candidates = [x for x in gateway_profiles if x.get("p95_pct") is not None]
        gateway_current_profile = max(gw_current_candidates, key=lambda x: x.get("current_pct"), default=None)
        gateway_p95_profile = max(gw_p95_candidates, key=lambda x: x.get("p95_pct"), default=None)
        gw_current = (gateway_current_profile or {}).get("current_pct")
        gw_p95 = (gateway_p95_profile or {}).get("p95_pct")

        # Comparativos de menor pressão/uso observado. São fatos comparativos, não prova de
        # capacidade alternativa ou de intercambialidade entre rotas.
        lowest_pressure_group = min(p95_candidates, key=lambda x: x.get("p95_pct"), default=None)
        lowest_current_group = min(current_candidates, key=lambda x: x.get("current_pct"), default=None)
        def _entity_key(v):
            return ''.join(ch for ch in str(v or '').upper() if ch.isalnum())
        semantic_status_by_entity = {
            _entity_key(x.get("entity")): str(x.get("canonical_status") or '').upper()
            for x in semantic_rows if x.get("entity")
        }
        gw_p95_comparable = [
            x for x in gw_p95_candidates
            if semantic_status_by_entity.get(_entity_key(x.get("entity"))) != "PLANNED_INACTIVE"
        ]
        gw_current_comparable = [
            x for x in gw_current_candidates
            if semantic_status_by_entity.get(_entity_key(x.get("entity"))) != "PLANNED_INACTIVE"
        ]
        gateway_lowest_p95_profile = min(gw_p95_comparable, key=lambda x: x.get("p95_pct"), default=None)
        gateway_lowest_current_profile = min(gw_current_comparable, key=lambda x: x.get("current_pct"), default=None)

        queue_summary = evidence_context.get("queue_summary") or {}
        congestion_period = evidence_context.get("congestion_analytics") or {}
        def _qstat(metric, stat):
            try:
                return _num((queue_summary.get(metric) or {}).get(stat))
            except Exception:
                return None

        scored_gateway_profiles = [x for x in gateway_profiles if DataService._pressure_gateway_profile_usable(x)]
        scored_gateway_profile = max(scored_gateway_profiles, key=lambda x: x["p95_pct"], default=None)
        scored_gateway_p95 = (scored_gateway_profile or {}).get("p95_pct")
        p95_for_risk = max((v for v in (p95, scored_gateway_p95) if v is not None), default=None)
        risk_source = "GATEWAY" if scored_gateway_p95 is not None and (p95 is None or scored_gateway_p95 > p95) else "TRUNK" if p95 is not None else None
        risk_entity = (scored_gateway_profile or {}).get("entity") if risk_source == "GATEWAY" else (historical_pressure_group or {}).get("entity")
        if p95_for_risk is None:
            risk = "N/C"
        elif p95_for_risk >= 90:
            risk = "ALTO"
        elif p95_for_risk >= 80:
            risk = "MÉDIO"
        else:
            risk = "BAIXO"
        trunk_risk = "N/C" if p95 is None else "ALTO" if p95 >= 90 else "MÉDIO" if p95 >= 80 else "BAIXO"
        gateway_risk = "N/C" if scored_gateway_p95 is None else "ALTO" if scored_gateway_p95 >= 90 else "MÉDIO" if scored_gateway_p95 >= 80 else "BAIXO"

        canonical = [str(x.get("canonical_status") or "").upper() for x in semantic_rows]
        fault_count = sum(1 for x in canonical if x == "FAULT_CONFIRMED_CONTEXT")
        planned_count = sum(1 for x in canonical if x == "PLANNED_INACTIVE")
        standby_count = sum(1 for x in canonical if x == "STANDBY_EXPECTED")
        partial_count = sum(1 for x in canonical if x == "OPERATIONAL_PARTIAL_VISIBILITY")

        if fault_count:
            resilience = "REDUZIDA"
        elif planned_count:
            resilience = "PLANEJADA"
        elif standby_count:
            resilience = "STANDBY MAPEADO"
        else:
            resilience = "SEM RESTRIÇÃO CONHECIDA"

        # Confiança explicável: nenhuma soma/score opaco.
        cov = evidence_context.get("infra_coverage") or {}
        cov_pct = _num(cov.get("data_coverage_pct"))
        if cov_pct is None:
            cov_pct = _num(cov.get("coverage_pct"))
        samples = _num(cov.get("samples"))
        largest_gap = _num(cov.get("largest_gap_min"))
        requested = _num(cov.get("requested_minutes")) or float(window_minutes or 0)
        fresh = evidence_context.get("freshness") or {}
        fresh_status = str(fresh.get("status") or "N/C").upper()
        fresh_usable = fresh.get("usable")
        scope_mode = str(evidence_context.get("scope_mode") or "N/C")
        valid_pressure = bool(current_candidates or p95_candidates or nominal_pairs or scored_gateway_profiles)

        confidence_reasons = []
        largest_gap_ratio_pct = round((largest_gap/requested)*100.0, 2) if largest_gap is not None and requested > 0 else None
        severe_gap = bool(largest_gap is not None and requested > 0 and largest_gap > max(120.0, requested * 0.10))
        relevant_gap = bool(largest_gap is not None and requested > 0 and largest_gap > max(30.0, requested * 0.05))
        if fresh_usable is False or fresh_status in ("STALE", "SEM_DADOS"):
            evidence_confidence = "INSUFICIENTE"
            confidence_reasons.append(f"Infra {fresh_status or 'N/C'} fora do Quality Gate")
        elif not valid_pressure:
            evidence_confidence = "INSUFICIENTE"
            confidence_reasons.append("Sem ocupação/busy-total válidos para sustentar pressão de capacidade")
        elif cov_pct is not None and cov_pct < 50:
            evidence_confidence = "FRÁGIL"
            confidence_reasons.append(f"Cobertura útil de apenas {cov_pct:.1f}% da janela")
        elif severe_gap:
            evidence_confidence = "FRÁGIL"
            confidence_reasons.append(f"Maior intervalo sem dados de {largest_gap:.1f} min ({largest_gap_ratio_pct:.1f}% da janela)")
        elif partial_count or scope_mode == "INSTANCE_FALLBACK":
            evidence_confidence = "PARCIAL"
            if partial_count:
                confidence_reasons.append(f"{partial_count} Gateway(s) com visibilidade operacional parcial")
            if scope_mode == "INSTANCE_FALLBACK":
                confidence_reasons.append("Fila→infra não comprovada; infraestrutura da Instância usada como contexto")
        elif cov_pct is not None and cov_pct < 80:
            evidence_confidence = "PARCIAL"
            confidence_reasons.append(f"Cobertura útil {cov_pct:.1f}% da janela")
        elif relevant_gap:
            evidence_confidence = "PARCIAL"
            confidence_reasons.append(f"Existe intervalo relevante sem dados: {largest_gap:.1f} min")
        else:
            evidence_confidence = "ADEQUADA"
            if cov_pct is not None:
                confidence_reasons.append(f"Cobertura útil {cov_pct:.1f}% sem buraco dominante")
            else:
                confidence_reasons.append("Sem limitação temporal dominante identificada")

        calls_dialing = _num(latest.get("calls_dialing")) or 0.0
        in_attendance = _num(latest.get("calls_in_attendance")) or 0.0
        agents_available = _num(latest.get("agents_available")) or 0.0
        agents_talking = _num(latest.get("agents_talking")) or 0.0
        agents_acw = _num(latest.get("agents_acw")) or 0.0
        agents_paused = _num(latest.get("agents_paused")) or 0.0
        buffer_now = _num(latest.get("buffer")) or 0.0
        hit15 = _num(latest.get("hit_rate15")) or 0.0
        dialing_rate = _num(latest.get("dialing_rate")) or 0.0
        p34 = _num(latest.get("p34_pct")) or 0.0
        p135 = _num(latest.get("p135_pct")) or 0.0
        stuck_max = _num(stuck_summary.get("max")) or 0.0
        stuck_current = _num(stuck_summary.get("current"))
        if stuck_current is None:
            stuck_current = _num(stuck_summary.get("quantity")) or 0.0

        # Materialidade de canais travados: quantidade absoluta + participação na capacidade nominal observada.
        # Esta classificação é calculada ANTES do gargalo para evitar que "20 canais" virem gargalo
        # automaticamente quando representam parcela pequena da infraestrutura.
        stuck_materiality_pct = None
        if nominal_total is not None and nominal_total > 0:
            stuck_materiality_pct = round((stuck_max / nominal_total) * 100.0, 2)
        if stuck_max <= 0:
            stuck_state = "SEM EVIDÊNCIA"
        elif stuck_max >= 100 or (stuck_materiality_pct is not None and stuck_materiality_pct >= 5.0):
            stuck_state = "AGRAVANTE"
        elif stuck_max >= 20 or (stuck_materiality_pct is not None and stuck_materiality_pct >= 1.0):
            stuck_state = "PRESENTE"
        else:
            stuck_state = "BAIXA RELEVÂNCIA"

        if p95_for_risk is not None and p95_for_risk >= 90:
            bottleneck = "TELECOM_GATEWAY" if risk_source == "GATEWAY" else "TELECOM_TRUNK"
            bottleneck_label = "Telecom · Gateway" if risk_source == "GATEWAY" else "Telecom · grupo de troncos"
        elif p95_for_risk is not None and p95_for_risk >= 80:
            bottleneck = "TELECOM_GATEWAY_PRESSURE" if risk_source == "GATEWAY" else "TELECOM_TRUNK_PRESSURE"
            bottleneck_label = "Telecom · pressão em Gateway" if risk_source == "GATEWAY" else "Telecom · pressão em grupo de troncos"
        elif fault_count:
            bottleneck = "INFRA_RESILIENCE"
            bottleneck_label = "Infraestrutura · resiliência reduzida"
        elif calls_dialing > 0 and hit15 > 0 and hit15 < 2:
            bottleneck = "DIALER_EFFICIENCY"
            bottleneck_label = "Discagem · baixa eficiência observada"
        elif in_attendance > 0 and agents_available <= 0:
            bottleneck = "ATTENDANCE_HEADROOM"
            bottleneck_label = "Atendimento · pouca folga imediata"
        elif stuck_state == "AGRAVANTE":
            bottleneck = "STUCK_CHANNELS"
            bottleneck_label = "Infraestrutura · canais travados materialmente relevantes"
        else:
            bottleneck = "NONE_OBSERVED"
            bottleneck_label = "Nenhum gargalo dominante comprovado"

        bottleneck_catalog = [
            {"code":"TELECOM_TRUNK","label":"Telecom · grupo de troncos","meaning":"O valor abaixo do qual ficaram 95% das leituras do grupo relacionado foi de pelo menos 90% de ocupação."},
            {"code":"TELECOM_TRUNK_PRESSURE","label":"Telecom · pressão em grupo de troncos","meaning":"O valor não ultrapassado em 95% das leituras ficou entre 80% e 90%, sinal de atenção de capacidade."},
            {"code":"TELECOM_GATEWAY","label":"Telecom · Gateway","meaning":"A ocupação sustentada de um Gateway relacionado foi de pelo menos 90% e superou a pressão dos troncos."},
            {"code":"TELECOM_GATEWAY_PRESSURE","label":"Telecom · pressão em Gateway","meaning":"A ocupação sustentada de um Gateway relacionado ficou entre 80% e 90% e superou a pressão dos troncos."},
            {"code":"INFRA_RESILIENCE","label":"Infraestrutura · resiliência reduzida","meaning":"Gateway com falha confirmada reduz redundância/resiliência observada."},
            {"code":"DIALER_EFFICIENCY","label":"Discagem · baixa eficiência observada","meaning":"Discagem ativa com Hit Rate 15' baixo; qualifica pressão, não prova falta física de capacidade."},
            {"code":"ATTENDANCE_HEADROOM","label":"Atendimento · pouca folga imediata","meaning":"Há atendimento em curso e nenhum agente disponível no estado atual."},
            {"code":"STUCK_CHANNELS","label":"Infraestrutura · canais travados materialmente relevantes","meaning":"Canais travados atingem materialidade AGRAVANTE por quantidade/proporção observada."},
            {"code":"NONE_OBSERVED","label":"Nenhum gargalo dominante comprovado","meaning":"Nenhuma dimensão atingiu evidência suficiente para dominar a leitura."},
        ]
        bottleneck_reason = {
            "TELECOM_TRUNK": f"No grupo mais pressionado, 95% das leituras ficaram em até {p95:.2f}% de ocupação (limiar alto ≥90%)." if p95 is not None else "Pressão Telecom alta.",
            "TELECOM_TRUNK_PRESSURE": f"No grupo mais pressionado, 95% das leituras ficaram em até {p95:.2f}% de ocupação (faixa de atenção 80–90%)." if p95 is not None else "Pressão Telecom em atenção.",
            "TELECOM_GATEWAY": f"No Gateway {risk_entity}, 95% das leituras ficaram em até {p95_for_risk:.2f}% de ocupação (limiar alto ≥90%)." if p95_for_risk is not None else "Pressão Telecom alta.",
            "TELECOM_GATEWAY_PRESSURE": f"No Gateway {risk_entity}, 95% das leituras ficaram em até {p95_for_risk:.2f}% de ocupação (faixa de atenção 80–90%)." if p95_for_risk is not None else "Pressão Telecom em atenção.",
            "INFRA_RESILIENCE": f"{fault_count} Gateway(s) com falha confirmada no contexto operacional.",
            "DIALER_EFFICIENCY": f"Discagem ativa com Hit Rate 15' atual {hit15:.2f}%.",
            "ATTENDANCE_HEADROOM": "Há chamadas em atendimento e nenhum agente disponível agora.",
            "STUCK_CHANNELS": f"Canais travados classificados como AGRAVANTE: pico {stuck_max:.0f}" + (f" · materialidade {stuck_materiality_pct:.2f}%" if stuck_materiality_pct is not None else "") + ".",
            "NONE_OBSERVED": "Nenhuma dimensão atingiu critério de dominância nesta janela.",
        }.get(bottleneck, "")

        if risk == "ALTO":
            overall = "ALTO"
        elif risk == "MÉDIO" or fault_count or stuck_state == "AGRAVANTE":
            overall = "ATENÇÃO"
        elif risk == "N/C":
            overall = "ATENÇÃO" if fault_count else "N/C"
        else:
            overall = "BAIXO"

        capacity_effective_status = "N/C — topologia/roteamento não comprovados"
        contingency_headroom_status = (
            "N/C — standby mapeado, absorção de demanda não comprovada"
            if standby_count else
            "N/C — topologia/roteamento não comprovados"
        )

        p34_period_p95=_qstat("p34_pct","p95"); p34_period_peak=_qstat("p34_pct","max")
        p135_period_p95=_qstat("p135_pct","p95"); p135_period_peak=_qstat("p135_pct","max")
        dialing_mean=_qstat("calls_dialing","mean"); dialing_peak=_qstat("calls_dialing","max")
        buffer_mean=_qstat("buffer","mean"); hit15_mean=_qstat("hit_rate15","mean")
        p34_over20=float((congestion_period.get("p34") or {}).get("minutes_over_20") or 0.0)
        p135_over20=float((congestion_period.get("p135") or {}).get("minutes_over_20") or 0.0)

        if p34_period_p95 is not None and p34_period_p95 >= 20 and (p135_period_p95 or 0) < p34_period_p95:
            congestion_state="P34 DOMINANTE"
        elif p135_period_p95 is not None and p135_period_p95 >= 20:
            congestion_state="P135 DOMINANTE"
        elif max(p34_period_peak or 0,p135_period_peak or 0) >= 20:
            congestion_state="EPISÓDIO CRÍTICO"
        elif max(p34_period_peak or 0,p135_period_peak or 0) > 0:
            congestion_state="PRESENTE"
        else:
            congestion_state="AUSENTE"

        dimensions = {
            "demand": {
                "label": "Demanda / Discagem",
                "state": "PRESSÃO OBSERVADA" if (dialing_peak or calls_dialing or buffer_now) else "BAIXA",
                "calls_dialing": round(calls_dialing, 2), "buffer": round(buffer_now, 2),
                "hit_rate15": round(hit15, 3), "dialing_rate": round(dialing_rate, 3),
                "period_calls_dialing_mean": round(dialing_mean,2) if dialing_mean is not None else None,
                "period_calls_dialing_peak": round(dialing_peak,2) if dialing_peak is not None else None,
                "period_buffer_mean": round(buffer_mean,2) if buffer_mean is not None else None,
                "period_hit_rate15_mean": round(hit15_mean,3) if hit15_mean is not None else None,
                "note": "Mostra quanta demanda a fila colocou na saída. Discagem alta sozinha não prova falta de capacidade; ganha relevância quando coincide com P34/P135 e pressão Telecom.",
            },
            "attendance": {
                "label": "Atendimento / Agentes",
                "state": "SEM FOLGA IMEDIATA" if in_attendance > 0 and agents_available <= 0 else "FOLGA PRESENTE" if agents_available > 0 else "SEM ATIVIDADE",
                "agents_available": round(agents_available, 2), "agents_talking": round(agents_talking, 2),
                "agents_acw": round(agents_acw, 2), "agents_paused": round(agents_paused, 2),
                "calls_in_attendance": round(in_attendance, 2),
                "note": "Capacidade humana é separada da Telecom. Agentes disponíveis ajudam a descartar falta de atendimento como causa dominante, mas não criam circuitos de saída.",
            },
            "trunks": {
                "label": "Telecom / Grupos de Troncos", "state": "PRESSÃO " + trunk_risk,
                "critical_group": (critical_group or {}).get("entity"),
                "critical_group_current_pct": current,
                "historical_pressure_group": (historical_pressure_group or {}).get("entity"),
                "critical_group_p95_pct": p95,
                "critical_group_peak_pct": round(peak, 2) if peak is not None else None,
                "critical_group_headroom_p95_pct": headroom,
                "lowest_p95_group": (lowest_pressure_group or {}).get("entity"),
                "lowest_p95_pct": (lowest_pressure_group or {}).get("p95_pct"),
                "lowest_current_group": (lowest_current_group or {}).get("entity"),
                "lowest_current_pct": (lowest_current_group or {}).get("current_pct"),
                "portfolio_current_occupancy_nominal_pct": portfolio_occ,
                "portfolio_current_headroom_nominal_pct": portfolio_headroom,
                "capacity_effective_status": capacity_effective_status,
                "note": "O valor abaixo do qual ficaram 95% das leituras do grupo mais pressionado e a folga nesse nível revelam gargalo localizado no período. A carga agregada busy/total da Instância é apenas contexto e pode diluir uma rota saturada.",
            },
            "gateways": {
                "label": "Gateways / Infraestrutura", "state": resilience,
                "occupancy_pressure_risk": gateway_risk,
                "current_max_pct": round(gw_current, 2) if gw_current is not None else None,
                "current_max_entity": (gateway_current_profile or {}).get("entity"),
                "p95_pct": round(gw_p95, 2) if gw_p95 is not None else None,
                "p95_entity": (gateway_p95_profile or {}).get("entity"),
                "lowest_p95_pct": (gateway_lowest_p95_profile or {}).get("p95_pct"),
                "lowest_p95_entity": (gateway_lowest_p95_profile or {}).get("entity"),
                "lowest_current_pct": (gateway_lowest_current_profile or {}).get("current_pct"),
                "lowest_current_entity": (gateway_lowest_current_profile or {}).get("entity"),
                "fault_confirmed": fault_count, "planned_inactive": planned_count,
                "standby_expected": standby_count, "partial_visibility": partial_count,
                "note": "Separa fotografia atual de comportamento no período. Ocupação baixa de Gateway não prova folga utilizável sem confirmar papel, rota e lifecycle.",
            },
            "stuck_channels": {
                "label": "Canais Travados", "state": stuck_state,
                "current": round(stuck_current, 2), "max": round(stuck_max, 2),
                "materiality_pct": stuck_materiality_pct,
                "note": "Canal travado reduz capacidade física onde está alocado. Sem vínculo fila→canal, o TOI não desconta automaticamente da capacidade específica da fila; materialidade combina quantidade e proporção observada.",
            },
            "congestion": {
                "label": "Sintomas P34 / P135", "state": congestion_state,
                "p34_pct": round(p34, 3), "p135_pct": round(p135, 3),
                "p34_period_p95": p34_period_p95, "p34_period_peak": p34_period_peak, "p34_minutes_over_20": round(p34_over20,2),
                "p135_period_p95": p135_period_p95, "p135_period_peak": p135_period_peak, "p135_minutes_over_20": round(p135_over20,2),
                "note": "P34 indica indisponibilidade/restrição no caminho de saída; P135 indica congestionamento interno. São sintomas para diagnóstico, não medidas de capacidade por si só.",
            },
            "resilience": {
                "label": "Resiliência / Contingência", "state": resilience,
                "confidence": evidence_confidence,
                "confidence_reasons": confidence_reasons,
                "coverage_pct": round(cov_pct, 2) if cov_pct is not None else None,
                "largest_gap_min": round(largest_gap, 2) if largest_gap is not None else None,
                "scope_mode": scope_mode,
                "contingency_headroom_status": contingency_headroom_status,
                "note": "Mostra se existe alternativa confiável para absorver degradação. Standby só conta como contingência quando topologia/roteamento comprovam que pode receber a demanda.",
            },
        }

        if bottleneck in ("TELECOM_TRUNK", "TELECOM_GATEWAY", "TELECOM_TRUNK_PRESSURE", "TELECOM_GATEWAY_PRESSURE"):
            recommendation = "Validar recorrência, distribuição de rotas e capacidade alternativa do elemento pressionado antes de recomendar expansão."
        elif bottleneck == "INFRA_RESILIENCE":
            recommendation = "Revalidar o Gateway com falha confirmada antes de tratá-lo como redundância saudável."
        elif bottleneck == "DIALER_EFFICIENCY":
            recommendation = "Revisar eficiência/taxa de discagem e observar resposta do P34/P135 antes de atribuir o cenário a falta de infraestrutura."
        elif bottleneck == "ATTENDANCE_HEADROOM":
            recommendation = "Validar disponibilidade de atendimento/agentes; essa restrição é humana e deve ser separada da capacidade Telecom."
        else:
            recommendation = "Manter observação multidimensional; não há evidência suficiente para recomendar expansão imediata."

        status = "OK" if valid_pressure else "SEM_DADOS"
        return {
            "schema": "toi_capacity_operational_v2", "engine_revision": "2.3",
            "status": status, "risk": risk, "overall_status": overall,
            "telecom_risk_basis": {"source":risk_source,"entity":risk_entity,"occupancy_pct":p95_for_risk,
                                   "gateway_eligible_count":len(scored_gateway_profiles),
                                   "method":"Maior ocupação sustentada válida entre grupo de troncos e Gateway; cobertura do Gateway >=60% e >=5 leituras."},
            "bottleneck_domain": bottleneck, "bottleneck_label": bottleneck_label,
            "bottleneck_reason": bottleneck_reason, "bottleneck_catalog": bottleneck_catalog,
            "current_max": round(current, 2) if current is not None else None,
            "p95_occupancy": round(p95, 2) if p95 is not None else None,
            "peak_occupancy": round(peak, 2) if peak is not None else None,
            "minutes_over_90_est": round(minutes_over90, 2) if minutes_over90 is not None else None,
            "headroom": round(headroom, 2) if headroom is not None else None,
            "current_pressure_selection_reason": current_selection_reason,
            "current_pressure_tied_or_zero": current_selection_reason in ("NO_CURRENT_PRESSURE", "TIED_MAX"),
            "critical_group": {
                "entity": (critical_group or {}).get("entity"),
                "occupancy": current,
                "p95": next((x.get("p95_pct") for x in trunk_profiles if x.get("entity") == (critical_group or {}).get("entity")), None),
                "busy": (critical_group or {}).get("busy"), "total": (critical_group or {}).get("total"),
            } if critical_group else None,
            "historical_pressure_group": historical_pressure_group,
            "lowest_pressure_group": lowest_pressure_group,
            "lowest_current_group": lowest_current_group,
            "trunk_group_profiles": sorted(trunk_profiles, key=lambda x: (x.get("p95_pct") is not None, x.get("p95_pct") or -1, x.get("current_pct") or -1), reverse=True),
            "portfolio_current_occupancy_nominal": portfolio_occ,
            "portfolio_current_headroom_nominal": portfolio_headroom,
            "portfolio_nominal_busy": round(nominal_busy, 2) if nominal_busy is not None else None,
            "portfolio_nominal_total": round(nominal_total, 2) if nominal_total is not None else None,
            "capacity_effective_status": capacity_effective_status,
            "contingency_headroom_status": contingency_headroom_status,
            "gateway_current_max": round(gw_current, 2) if gw_current is not None else None,
            "gateway_current_max_entity": (gateway_current_profile or {}).get("entity"),
            "gateway_p95": round(gw_p95, 2) if gw_p95 is not None else None,
            "gateway_p95_entity": (gateway_p95_profile or {}).get("entity"),
            "gateway_lowest_p95": (gateway_lowest_p95_profile or {}).get("p95_pct"),
            "gateway_lowest_p95_entity": (gateway_lowest_p95_profile or {}).get("entity"),
            "gateway_lowest_current": (gateway_lowest_current_profile or {}).get("current_pct"),
            "gateway_lowest_current_entity": (gateway_lowest_current_profile or {}).get("entity"),
            "stuck_materiality_pct": stuck_materiality_pct,
            "pressure_status": overall,
            "gateway_profiles": sorted(gateway_profiles, key=lambda x: (x.get("p95_pct") is not None, x.get("p95_pct") or -1, x.get("current_pct") or -1), reverse=True),
            "resilience_status": resilience,
            "evidence_confidence": evidence_confidence,
            "evidence_reasons": confidence_reasons,
            "evidence_coverage_pct": round(cov_pct, 2) if cov_pct is not None else None,
            "evidence_largest_gap_min": round(largest_gap, 2) if largest_gap is not None else None,
            "evidence_largest_gap_ratio_pct": largest_gap_ratio_pct,
            "evidence_scope_mode": scope_mode,
            "evidence_confidence_definitions": {
                "ADEQUADA": "Boa cobertura e sem buracos capazes de esconder um evento relevante.",
                "PARCIAL": "Cobertura razoável, mas existem intervalos relevantes sem observação.",
                "FRÁGIL": "A quantidade ou a concentração dos intervalos sem dados impede conclusão segura.",
                "INSUFICIENTE": "Não há evidência utilizável suficiente para sustentar uma conclusão operacional."
            },
            "gateway_contexts": semantic_rows,
            "dimensions": dimensions,
            "recommendation": recommendation,
        }

    @staticmethod
    def build_recommendations(diagnosis, latest, features, capacity, dialer_summary, quality_gate=None):
        recs = []
        scores = (diagnosis or {}).get("scores") or {}
        qsrc = ((quality_gate or {}).get("sources") or {})
        infra_usable = (qsrc.get("infra") or {}).get("usable", True)
        dialer_usable = (qsrc.get("dialer") or {}).get("usable", True)
        f15 = (features or {}).get("selected") or (features or {}).get("15m") or {}

        # v1.10.6.3.7.5 R1.1 — NULL hardening:
        # janelas sem amostras podem manter as chaves estatísticas com valor None.
        # Normalize antes de comparações para preservar /api/overview fora da
        # janela operacional e também com fontes STALE/SEM_DADOS.
        hr15_raw = (f15.get("hit_rate15") or {}).get("mean")
        if hr15_raw is None:
            hr15_raw = latest.get("hit_rate15")
        p135_mean_raw = (f15.get("p135_pct") or f15.get("p135") or {}).get("mean")
        if p135_mean_raw is None:
            p135_mean_raw = latest.get("p135_pct")
        dialing_delta_raw = (f15.get("calls_dialing") or {}).get("delta")

        hr15 = float(hr15_raw or 0.0)
        p135_mean = float(p135_mean_raw or 0.0)
        dialing_delta = float(dialing_delta_raw or 0.0)

        if scores.get("discagem", 0) >= 3 or (hr15 and hr15 < 2):
            why = (
                "Hit Rate baixo e pressão de tentativas reduzem eficiência operacional."
                if dialer_usable else
                "Hit Rate baixo observado na fila; Discador não está utilizável nesta janela e não compõe a causa."
            )
            recs.append({
                "title": "Revisar taxa e estratégia de discagem",
                "why": why,
                "expected": "Reduzir pressão sem perda desnecessária de contatos úteis.",
                "priority": "Alta",
            })

        if infra_usable and capacity.get("risk") in ("ALTO", "MÉDIO"):
            critical = (capacity.get("critical_group") or {}).get("entity") or "grupo mais pressionado"
            p95 = capacity.get("p95_occupancy")
            recs.append({
                "title": "Avaliar grupo mais pressionado e distribuição de rotas",
                "why": f"{critical}: 95% das leituras de ocupação ficaram em até {float(p95 or 0):.1f}%. O valor representa pressão localizada, não ocupação total dos troncos.",
                "expected": "Confirmar recorrência, distribuição e capacidade alternativa antes de ampliar infraestrutura.",
                "priority": "Alta" if capacity.get("risk") == "ALTO" else "Média",
            })

        if infra_usable and capacity.get("resilience_status") == "REDUZIDA":
            faults = [x.get("entity") for x in (capacity.get("gateway_contexts") or []) if x.get("canonical_status") == "FAULT_CONFIRMED_CONTEXT"]
            recs.append({
                "title": "Revalidar resiliência dos Gateways",
                "why": ("Gateway com falha confirmada no contexto operacional: " + ", ".join(faults)) if faults else "Há Ground Truth indicando resiliência reduzida na infraestrutura.",
                "expected": "Não contabilizar redundância não confirmada como headroom disponível e preservar a falha histórica.",
                "priority": "Alta",
            })

        if dialing_delta > 100 and p135_mean >= 10:
            recs.append({
                "title": "Correlacionar crescimento de discagem e P135",
                "why": "A janela mostra crescimento de chamadas discando junto com congestionamento relevante.",
                "expected": "Confirmar ou descartar causalidade antes de ampliar capacidade.",
                "priority": "Média",
            })

        if not recs:
            recs.append({
                "title": "Manter acompanhamento e consolidar histórico",
                "why": "Não há evidência suficiente para intervenção imediata nesta janela.",
                "expected": "Aumentar confiança antes de executar mudança operacional.",
                "priority": "Baixa",
            })

        return recs[:3]

    def ml_shadow_latest(self, instancia, codigo):
        try:
            with closing(self.connect()) as conn:
                if not self.table_exists(conn, "ml_shadow_prediction"):
                    return None
                row=self._current_tail_latest(
                    conn, "ml_shadow_prediction", "instancia=? AND codigo=?",
                    (str(instancia), str(codigo)),
                )
                if not row:
                    return None
                d=dict(row)
                try:
                    d["features"] = json.loads(d.pop("features_json") or "{}")
                except Exception:
                    d["features"] = {}
                d["mode"] = "SHADOW"
                return d
        except Exception:
            return None

    @staticmethod
    def ml_shadow(features, diagnosis):
        """Contrato inicial do ML Shadow Mode sem predições inventadas."""
        return {
            "mode": "SHADOW",
            "model_status": "AGUARDANDO_TREINO",
            "prediction": None,
            "confidence": None,
            "rule_engine_reference": (diagnosis or {}).get("title"),
            "feature_windows_available": [
                k for k, v in (features or {}).items()
                if (v or {}).get("samples", 0) > 0
            ],
            "note": "Pipeline de features ativo; primeiro modelo ainda não treinado.",
        }


class Handler(BaseHTTPRequestHandler):
    service: DataService = None
    radar = None

    def log_message(self, fmt, *args):
        print(f"[HTTP] {self.command} {urlparse(self.path).path}", flush=True)

    def session_cookie(self, token, max_age):
        secure = "; Secure" if self.headers.get("X-Forwarded-Proto", "").lower() == "https" else ""
        return ("toi_legacy_session=" + token + "; HttpOnly; SameSite=Strict;"
                " Path=/toi/; Max-Age=" + str(max_age) + secure)

    def send_json(self, obj, status=200, headers=None):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            # Cliente local fechou a conexão (refresh/troca rápida de período).
            # É ruído de transporte HTTP; não é falha da aquisição nem do writer LIVE.
            return None

    def admin_cookie(self):
        try:
            cookie = SimpleCookie(self.headers.get("Cookie", ""))
            return cookie["toi_legacy_session"].value if "toi_legacy_session" in cookie else ""
        except Exception:
            return ""

    def admin_user(self):
        return shared_admin.current(self.admin_cookie())

    def client_ip(self):
        peer = self.client_address[0]
        if peer in ("127.0.0.1", "::1"):
            forwarded = self.headers.get("X-Real-IP", "").strip()
            try:
                return str(ipaddress.ip_address(forwarded))
            except ValueError:
                pass
        return peer

    @staticmethod
    def allowed_instances(user):
        try:
            allowed = json.loads(user.get("instances") or "[]")
            return None if "*" in allowed else {str(item) for item in allowed}
        except (ValueError, TypeError, AttributeError):
            return set()

    def read_admin_body(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        if not 1 <= length <= 32768:
            raise ValueError("Payload inválido")
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(body, dict):
            raise ValueError("Payload inválido")
        return body

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/api/session":
            try:
                user = self.admin_user()
                return self.send_json({"user": shared_admin.safe_user(user)} if user else
                                      {"error": "Autenticação necessária"}, 200 if user else 401)
            except PermissionError as exc:
                return self.send_json({"error": str(exc)}, 403)
        # O supervisor local consulta esta rota sem sessão para monitorar o processo.
        if path == "/api/health":
            return self.send_json({"ok": True})
        allowed = None
        if path.startswith("/api/") and not path.startswith("/api/admin/"):
            try:
                user = self.admin_user()
                if not user:
                    return self.send_json({"error": "Autenticação necessária"}, 401)
                if user["must_change"]:
                    return self.send_json({"error": "Defina sua senha inicial no TOI antigo"}, 403)
                allowed = self.allowed_instances(user)
                monitor_scope=(path=="/api/radar/history" and qs.get("instancia",[""])[0] in ("SBC","MGW"))
                if allowed is not None and qs.get("instancia", [""])[0] and qs["instancia"][0] not in allowed and not monitor_scope:
                    return self.send_json({"error": "Instância não autorizada"}, 403)
                if allowed is not None and path in (
                        "/api/analytics/window", "/api/knowledge/cases",
                        "/api/operational/feedback", "/api/gateway/analytics",
                        "/api/overview") and not qs.get("instancia", [""])[0]:
                    return self.send_json({"error": "Informe a instância autorizada"}, 400)
            except PermissionError as exc:
                return self.send_json({"error": str(exc)}, 403)

        if path.startswith("/api/admin/"):
            try:
                user = self.admin_user()
                if path == "/api/admin/me":
                    if not user:
                        return self.send_json({"error": "Autenticação necessária"}, 401)
                    return self.send_json({"user": shared_admin.safe_user(user), "csrf": user["csrf"]})
                if user and user["must_change"]:
                    return self.send_json({"error": "Defina sua senha inicial no TOI antigo"}, 403)
                shared_admin.require_admin(user)
                if path == "/api/admin/users":
                    return self.send_json({"items": shared_admin.users()})
                if path == "/api/admin/audit":
                    return self.send_json({"items": shared_admin.audit_events()})
                if path == "/api/admin/settings":
                    return self.send_json(shared_admin.settings())
                return self.send_json({"error": "Rota inexistente"}, 404)
            except PermissionError as exc:
                return self.send_json({"error": str(exc)}, 403)
            except (OSError, sqlite3.Error, RuntimeError) as exc:
                return self.send_json({"error": "Administração indisponível: "+type(exc).__name__}, 503)

        if path == "/api/ipbx/blocked-calls/latest":
            with _BLOCKED_JOBS_LOCK:
                latest = next((job for job in reversed(list(_BLOCKED_JOBS.values()))
                               if job["login"] == user["login"] and job["state"] == "done"), None)
                payload = ({key: latest[key] for key in
                            ("queue", "rows", "period", "carriers")}
                           if latest else None)
                if payload is not None:
                    status=latest.get("ipbx_status",DEFAULT_STATUS)
                    payload.update(ipbx_status=status,ipbx_status_label=IPBX_STATUSES[status])
            if payload is None:
                payload = _load_blocked_summary(user["login"])
            return self.send_json(payload if payload is not None else
                                  {"error": "Nenhuma consulta concluída"},
                                  200 if payload is not None else 404)

        if path in ("/api/ipbx/blocked-calls/status", "/api/ipbx/blocked-calls/download"):
            job_id = qs.get("job", [""])[0]
            with _BLOCKED_JOBS_LOCK:
                job = _BLOCKED_JOBS.get(job_id)
                if not job or job["login"] != user["login"]:
                    payload = None
                else:
                    payload = {key: value for key, value in job.items()
                               if key not in ("output", "login")}
                    output = job.get("output")
            if payload is None:
                return self.send_json({"error": "Consulta não encontrada"}, 404)
            if path.endswith("/status"):
                return self.send_json(payload)
            if payload["state"] != "done":
                return self.send_json({"error": "Planilha ainda não disponível"}, 409)
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            filename=payload.get("filename","telefones_validados_consultaoperadora.xlsx")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(output)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(output)
            return

        if path == "/api/sbc-monitor":
            try:
                return self.send_json(sbc_monitor_feed())
            except (OSError, sqlite3.Error) as exc:
                return self.send_json({"error": "Monitor SBC indisponível: " + type(exc).__name__}, 503)

        if path == "/api/sbc-traffic":
            try:
                pair = qs.get("pair", [""])[0]
                day = qs.get("date", [None])[0]
                raw_hour = qs.get("hour", [None])[0]
                hour = int(raw_hour) if raw_hour is not None else None
                realm = qs.get("realm", [None])[0] or None
                return self.send_json(traffic_report(SBC_MONITOR_ROOT, pair, day, hour, realm))
            except (ValueError, TypeError):
                return self.send_json({"error": "Par, data, hora ou realm inválidos"}, 400)
            except OSError as exc:
                return self.send_json({"error": "Histórico SBC indisponível: " + type(exc).__name__}, 503)

        if path == "/api/sbc-live":
            try:
                return self.send_json(live_report(SBC_MONITOR_ROOT, qs.get("pair", [""])[0]))
            except ValueError:
                return self.send_json({"error": "Par SBC inválido"}, 400)
            except OSError as exc:
                return self.send_json({"error": "Leitura SBC indisponível: " + type(exc).__name__}, 503)

        if path in ("/api/radar", "/api/radar/history"):
            if self.radar is None:
                return self.send_json({"error":"Radar indisponivel"},503)
            try:
                if path == "/api/radar":
                    payload = self.radar.snapshot()
                    if allowed is not None:
                        payload["items"] = [x for x in payload["items"] if str(x.get("instancia")) in allowed or x.get("source") == "MONITOR_ALERTAS"]
                        payload["events"] = [x for x in payload["events"] if str(x.get("instancia")) in allowed or x.get("source") == "MONITOR_ALERTAS"]
                        payload["counts"] = {"portal_active": sum(x.get("active") and x.get("source")=="PORTAL_ORBE" for x in payload["items"]),
                                             "gateway_active": sum(x.get("active") and x.get("source")=="GATEWAY" for x in payload["items"]),
                                             "agiroute_active": sum(x.get("active") and x.get("source")=="AGIROUTE" for x in payload["items"]),
                                             "monitor_active": sum(x.get("active") and x.get("source")=="MONITOR_ALERTAS" for x in payload["items"]),
                                             "ack_pending": sum(bool(x.get("ack_pending")) or bool(x.get("normalization_ack_pending")) for x in payload["items"])}
                    return self.send_json(payload)
                payload = self.radar.history(
                    qs.get("start",[None])[0],qs.get("end",[None])[0],
                    qs.get("instancia",[""])[0],qs.get("codigo",[""])[0],
                    qs.get("before",[None])[0],
                    exact_scope=qs.get("exact_scope",[""])[0]=="1",
                    source=qs.get("source",[""])[0],allowed_instances=allowed,
                    occurrences=qs.get("occurrences",[""])[0]=="1")
                if allowed is not None:
                    payload["events"] = [x for x in payload["events"] if str(x.get("instancia")) in allowed or x.get("source") == "MONITOR_ALERTAS"]
                return self.send_json(payload)
            except (ValueError,TypeError) as exc:
                return self.send_json({"error":str(exc)},400)
            except Exception as exc:
                return self.send_json({"error":"Radar: "+type(exc).__name__},503)

        if path == "/api/queues":
            items = self.service.queues()
            return self.send_json(items if allowed is None else
                                  [x for x in items if str(x["instancia"]) in allowed])

        if path == "/api/portal-queue-status":
            inst, code = qs.get("instancia", [""])[0], qs.get("codigo", [""])[0]
            if not inst or not code:
                return self.send_json({"error": "Informe instância e fila"}, 400)
            try:
                return self.send_json(self.service.portal_queue_status(inst, code))
            except (OSError, ValueError, TypeError):
                return self.send_json({"available": False, "reason": "Estado do Portal Orbe indisponível"}, 503)

        if path == "/api/queues/live":
            try:
                rows = self.service._refresh_queue_tail_catalog(force=True)
                latest = {}
                for row in rows:
                    key = (str(row["instancia"]), str(row["codigo"]))
                    if key not in latest:
                        latest[key] = dict(row)
                with closing(self.service.connect()) as conn:
                    items = []
                    for row in latest.values():
                        sample = conn.execute("""SELECT ts,p34_pct,p135_pct,calls_dialing,
                            calls_in_attendance,agents_available,agents_talking,agents_acw,
                            agents_paused,agents_other,agents_dialing FROM queue_snapshot
                            WHERE rowid=?""", (row["rowid"],)).fetchone()
                        if sample:
                            items.append({"instancia": row["instancia"], "codigo": row["codigo"],
                                "nome": row["nome"], **dict(sample)})
                if allowed is not None:
                    items = [x for x in items if str(x["instancia"]) in allowed]
                return self.send_json({"items": sorted(items, key=lambda x:(str(x["instancia"]), x["nome"]))})
            except sqlite3.Error:
                return self.send_json({"error": "Filas LIVE indisponíveis"}, 503)

        if path in ("/api/infrastructure", "/api/sip-servers"):
            inst = qs.get("instancia", [""])[0]
            if not inst:
                return self.send_json({"error": "Informe a instância"}, 400)
            if path == "/api/sip-servers":
                from sip_inventory import inventory_report
                return self.send_json(inventory_report(APP_DIR.parent, inst))
            try:
                inventory = self.service.infra(inst, 15)
                sources = {}
                for name, key in (("gateways", "gateway_ts"), ("trunks", "trunk_ts")):
                    stamp = inventory.get("meta", {}).get(key)
                    age = self.service._age_seconds(stamp)
                    sources[name] = {"sample_at": stamp, "age_s": age,
                        "status": "SEM_DADOS" if age is None else "LIVE" if age <= 45 else "ATRASADO"}
                    if name == 'gateways':
                        sources[name] = self.service.gateway_freshness(inst, stamp)
                return self.send_json({"instance": inst, **inventory, "sources": sources,
                                       "source_reconciliation":self.service.gateway_source_reconciliation(inst,
                                           {"gateways":inventory.get("gateways", []), **sources['gateways']}),
                                       "gateway_investigation":self.service.gateway_balance_report(inst),
                                       "gateway_activity":self.service.gateway_activity_report(inst)})
            except sqlite3.Error:
                return self.send_json({"error": "Infraestrutura temporariamente indisponível"}, 503)

        if path == "/api/sip":
            inst, code = qs.get("instancia", [""])[0], qs.get("codigo", [""])[0]
            if not inst or not code:
                return self.send_json({"error": "Informe instância e fila"}, 400)
            try:
                counts = {}
                seen = set()
                sample_at = None
                with closing(self.service.connect()) as conn:
                    rows = conn.execute("""SELECT ts,raw_json FROM
                        (SELECT rowid,ts,instancia,codigo,raw_json FROM lastcalls_snapshot
                         ORDER BY rowid DESC LIMIT 3000)
                        WHERE instancia=? AND codigo=? ORDER BY rowid DESC LIMIT 80""", (inst, code))
                    for row in rows:
                        sample_at = sample_at or row["ts"]
                        try:
                            calls = json.loads(row["raw_json"] or "{}").get("lastCallsList") or []
                        except (ValueError, AttributeError):
                            continue
                        for call in calls:
                            if not isinstance(call, dict):
                                continue
                            identity = str(call.get("IdCallIPBX") or "")
                            if not identity or identity in seen:
                                continue
                            seen.add(identity)
                            sip = str(call.get("NmStatusSip") or "").strip()
                            if sip:
                                counts[sip] = counts.get(sip, 0) + 1
                return self.send_json({"items": [{"sip": key, "calls": value} for key, value in
                    sorted(counts.items(), key=lambda item: -item[1])], "sample_at": sample_at,
                    "observed": sum(counts.values()), "partial": True})
            except sqlite3.Error:
                return self.send_json({"error": "SIP indisponível"}, 503)

        if path == "/api/analytics/window":
            inst = qs.get("instancia", [""])[0]
            code = qs.get("codigo", [""])[0]
            period = qs.get("period", ["15m"])[0]
            start = qs.get("start", [None])[0]
            end = qs.get("end", [None])[0]
            cache_bypass = str(qs.get("cache_bypass", ["0"])[0]).lower() in ("1","true","yes")
            try:
                return self.send_json(
                    self.service.analytics_window(
                        inst, code, period=period, start=start, end=end, cache_bypass=cache_bypass
                    )
                )
            except sqlite3.OperationalError as exc:
                return self.send_json(
                    {"error": "Consulta temporariamente indisponível"},
                    503,
                )
            except ValueError as exc:
                return self.send_json({"error": str(exc)}, 400)
            except Exception as exc:
                return self.send_json(
                    {"error": "Análise indisponível"},
                    500,
                )

        if path == "/api/knowledge/cases":
            inst = qs.get("instancia", [None])[0]
            entity = qs.get("entity", [None])[0]
            return self.send_json(load_operational_knowledge_cases(instancia=inst, entity=entity))

        if path == "/api/operational/feedback":
            inst = qs.get("instancia", [None])[0]
            entity = qs.get("entity", [None])[0]
            start = qs.get("start", [None])[0]
            end = qs.get("end", [None])[0]
            return self.send_json(load_operational_feedback(instancia=inst, entity=entity, start=start, end=end))

        if path == "/api/gateway/analytics":
            inst = qs.get("instancia", [""])[0]
            period = qs.get("period", ["15m"])[0]
            start = qs.get("start", [None])[0]
            end = qs.get("end", [None])[0]
            codigo = qs.get("codigo", [None])[0]
            detail = qs.get("detail", ["full"])[0]
            entity = qs.get("entity", [None])[0]
            cache_bypass = str(qs.get("cache_bypass", ["0"])[0]).lower() in ("1","true","yes")
            try:
                return self.send_json(
                    self.service.gateway_analytics_bundle(
                        inst, period=period, start=start, end=end, codigo=codigo, detail=detail, entity=entity, cache_bypass=cache_bypass
                    )
                )
            except sqlite3.OperationalError as exc:
                return self.send_json({"error": "Gateway temporariamente indisponível"}, 503)
            except ValueError as exc:
                return self.send_json({"error": str(exc)}, 400)
            except Exception as exc:
                return self.send_json({"error": "Análise do Gateway indisponível"}, 500)

        if path == "/api/overview":
            inst = qs.get("instancia", [""])[0]
            code = qs.get("codigo", [""])[0]
            period = qs.get("period", ["15m"])[0]
            minutes = PERIOD_MAP.get(period, 15)
            try:
                return self.send_json({**self.service.overview(inst, code, minutes),
                                       "gateway_investigation":self.service.gateway_balance_report(inst),
                                       "gateway_activity":self.service.gateway_activity_report(inst)})
            except sqlite3.OperationalError as exc:
                return self.send_json(
                    {"error": "Consulta temporariamente indisponível"},
                    503,
                )
            except Exception as exc:
                return self.send_json(
                    {"error": "Visão geral indisponível"},
                    500,
                )

        # static
        rel = "index.html" if path in ("/", "/login") else path.lstrip("/")
        file_path = (STATIC_DIR / rel).resolve()
        if not file_path.is_relative_to(STATIC_DIR.resolve()):
            self.send_error(403)
            return
        if file_path.suffix == ".json":
            user = self.admin_user()
            if not user:
                return self.send_json({"error": "Autenticação necessária"}, 401)
            allowed = self.allowed_instances(user)
            agiroute_for_instance_6 = rel == "toi_agiroute_latest.json" and allowed is not None and "6" in allowed
            if user["must_change"] or (allowed is not None and not agiroute_for_instance_6):
                return self.send_json({"error": "Snapshot restrito"}, 403)
        if not file_path.exists() or not file_path.is_file():
            self.send_error(404)
            return

        ctype = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        body = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype + ("; charset=utf-8" if ctype.startswith("text/") else ""))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)


    def do_POST(self):
        parsed=urlparse(self.path)
        origin = self.headers.get("Origin")
        if origin and urlparse(origin).netloc != self.headers.get("Host"):
            return self.send_json({"error": "Origem inválida"}, 403)
        if parsed.path.startswith("/api/") and parsed.path != "/api/admin/login":
            try:
                session_user = self.admin_user()
                if not session_user:
                    return self.send_json({"error": "Autenticação necessária"}, 401)
                if not secrets.compare_digest(self.headers.get("X-CSRF-Token", ""),
                                              session_user["csrf"]):
                    return self.send_json({"error": "Sessão de formulário inválida"}, 403)
                if session_user["role"] == "Consulta" and parsed.path not in (
                        "/api/admin/change-password", "/api/admin/logout", "/api/ai/ask"):
                    return self.send_json({"error": "Operação não autorizada"}, 403)
                if session_user["must_change"] and parsed.path not in (
                        "/api/admin/change-password", "/api/admin/logout"):
                    return self.send_json({"error": "Defina sua senha inicial no TOI antigo"}, 403)
            except PermissionError as exc:
                return self.send_json({"error": str(exc)}, 403)
        if parsed.path == "/api/ai/ask":
            acquired = False
            try:
                length = int(self.headers.get("Content-Length", "0") or 0)
                if not 1 <= length <= 8192:
                    raise ValueError("Pergunta acima do limite permitido")
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(body, dict):
                    raise ValueError("Pergunta inválida")
                user = self.admin_user()
                allowed = self.allowed_instances(user)
                catalog = self.service.queues()
                if allowed is not None:
                    catalog = [q for q in catalog if str(q.get("instancia")) in allowed]
                acquired = _TOI_AI_QUERY_LOCK.acquire(blocking=False)
                if not acquired:
                    return self.send_json({"error": "A IA está atendendo outra consulta; tente novamente em instantes"}, 429)
                from toi_local_ai import LocalTOIAnalyst
                result = LocalTOIAnalyst(APP_DIR.parent).answer(
                    body.get("question", ""), allowed_instances=allowed,
                    queues=catalog, history=body.get("history", []) if isinstance(body.get("history"), list) else [])
                shared_admin.audit(user["login"], "toi_ai_query",
                    f"source={result.get('scope', {}).get('dataset', 'clarify')};status={result.get('status')}")
                return self.send_json(result)
            except PermissionError as exc:
                return self.send_json({"error": str(exc)}, 403)
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                return self.send_json({"error": str(exc)}, 400)
            except (OSError, sqlite3.Error, RuntimeError):
                return self.send_json({"error": "Consulta local indisponível; verifique o modelo e as fontes de dados"}, 503)
            finally:
                if acquired:
                    _TOI_AI_QUERY_LOCK.release()
        if parsed.path == "/api/ipbx/trace":
            job_id = secrets.token_hex(4)
            status = "?"
            try:
                user = self.admin_user()
                if self.allowed_instances(user) is not None:
                    raise PermissionError("Relatório IPBX exige acesso a todas as instâncias")
                origin = self.headers.get("Origin")
                if origin and urlparse(origin).netloc != self.headers.get("Host"):
                    raise PermissionError("Origem inválida")
                if not secrets.compare_digest(self.headers.get("X-CSRF-Token", ""), user["csrf"]):
                    raise PermissionError("Sessão de formulário inválida")
                status = parse_qs(parsed.query).get("status", [""])[0]
                if status not in ("19", "21"):
                    raise ValueError("Selecione Status IPBX 19 ou 21")
                if int(self.headers.get("Content-Length", "0") or 0):
                    raise ValueError("Esta consulta usa o Trace diretamente no Portal Orbe")
                if not _TRACE_REPORT_LOCK.acquire(blocking=False):
                    raise RuntimeError("Já existe uma consulta IPBX em andamento")
                try:
                    output, result = build_current_report(status, job_id=job_id)
                finally:
                    _TRACE_REPORT_LOCK.release()
                shared_admin.audit(user["login"], "ipbx_trace_generated",
                                   f"status={status};selecionados={result['selected']}")
                filename = f"TOI_IPBX_{status}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.send_header("Content-Length", str(len(output)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("X-TOI-Sample-Count", str(result["selected"]))
                self.send_header("X-TOI-Eligible", str(result["valid_distinct"]))
                self.end_headers()
                self.wfile.write(output)
                return
            except PermissionError as exc:
                log_trace("job_failed", status, job_id, error=type(exc).__name__)
                return self.send_json({"error": str(exc), "reference": job_id}, 403)
            except (ValueError, TypeError, IndexError) as exc:
                log_trace("job_failed", status, job_id, error=type(exc).__name__)
                return self.send_json({"error": str(exc), "reference": job_id}, 400)
            except RuntimeError as exc:
                log_trace("job_failed", status, job_id, error=type(exc).__name__)
                return self.send_json({"error": str(exc), "reference": job_id}, 503)
            except Exception as exc:
                log_trace("job_failed", status, job_id, error=type(exc).__name__)
                return self.send_json({"error": "Trace indisponível: " + type(exc).__name__,
                                       "reference": job_id}, 503)
        if parsed.path == "/api/ipbx/blocked-calls":
            job_id = secrets.token_hex(16)
            queue = parse_qs(parsed.query).get("queue", [""])[0]
            try:
                user = self.admin_user()
                if self.allowed_instances(user) is not None and "5" not in self.allowed_instances(user):
                    raise PermissionError("Fila IPBX não autorizada")
                origin = self.headers.get("Origin")
                if origin and urlparse(origin).netloc != self.headers.get("Host"):
                    raise PermissionError("Origem inválida")
                if not secrets.compare_digest(self.headers.get("X-CSRF-Token", ""), user["csrf"]):
                    raise PermissionError("Sessão de formulário inválida")
                if queue not in BLOCKED_QUEUES:
                    raise ValueError("Selecione a fila 0247400046 ou 0247400047")
                length = int(self.headers.get("Content-Length", "0") or 0)
                if not 1 <= length <= 1024:
                    raise ValueError("Informe o período da consulta")
                request = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(request, dict):
                    raise ValueError("Período inválido")
                ipbx_status=validate_ipbx_status(request.get("ipbx_status",DEFAULT_STATUS))
                try:
                    start_at = datetime.strptime(request["start"], "%d/%m/%Y %H:%M:%S")
                    end_at = datetime.strptime(request["end"], "%d/%m/%Y %H:%M:%S")
                except (KeyError, TypeError, ValueError):
                    raise ValueError("Use dd/mm/aaaa HH:MM:SS nas duas datas") from None
                if end_at <= start_at:
                    raise ValueError("Data Fim deve ser posterior à Data Início")
                if not _TRACE_REPORT_LOCK.acquire(blocking=False):
                    raise RuntimeError("Já existe uma consulta IPBX em andamento")
                try:
                    with _BLOCKED_JOBS_LOCK:
                        _BLOCKED_JOBS[job_id] = {"login": user["login"], "queue": queue,
                                                 "ipbx_status": ipbx_status,"ipbx_status_label":IPBX_STATUSES[ipbx_status],
                                                 "state": "running", "stage": "Iniciando consulta",
                                                 "period": request["start"] + " a " + request["end"]}
                    threading.Thread(target=_run_blocked_calls_job,
                                     args=(job_id, queue, user["login"], start_at, end_at, ipbx_status),
                                     daemon=True).start()
                except Exception:
                    _TRACE_REPORT_LOCK.release()
                    with _BLOCKED_JOBS_LOCK:
                        _BLOCKED_JOBS.pop(job_id, None)
                    raise
                return self.send_json({"job": job_id, "queue": queue, "state": "running",
                                       "ipbx_status":ipbx_status,"ipbx_status_label":IPBX_STATUSES[ipbx_status]}, 202)
            except PermissionError as exc:
                log_trace("job_failed", queue, job_id, error=type(exc).__name__)
                return self.send_json({"error": str(exc), "reference": job_id}, 403)
            except (ValueError, TypeError, IndexError, json.JSONDecodeError) as exc:
                log_trace("job_failed", queue, job_id, error=type(exc).__name__)
                return self.send_json({"error": str(exc), "reference": job_id}, 400)
            except RuntimeError as exc:
                log_trace("job_failed", queue, job_id, error=type(exc).__name__)
                return self.send_json({"error": str(exc), "reference": job_id}, 503)
            except Exception as exc:
                log_trace("job_failed", queue, job_id, error=type(exc).__name__)
                return self.send_json({"error": "Consulta indisponível: " + type(exc).__name__,
                                       "reference": job_id}, 503)
        if parsed.path.startswith("/api/admin/"):
            try:
                origin=self.headers.get("Origin")
                if origin and urlparse(origin).netloc != self.headers.get("Host"):
                    raise PermissionError("Origem inválida")
                body=self.read_admin_body()
                if parsed.path == "/api/admin/login":
                    result,error,status=shared_admin.login(body.get("login",""),body.get("password",""),self.client_ip())
                    if error:return self.send_json({"error":error},status)
                    token,csrf,user=result
                    return self.send_json({"user":user,"csrf":csrf},headers={
                        "Set-Cookie": self.session_cookie(token, 28800)})
                user=self.admin_user()
                if parsed.path == "/api/admin/logout":
                    if not user or self.headers.get("X-CSRF-Token", "") != user["csrf"]:
                        raise PermissionError("Sessão inválida")
                    shared_admin.logout(self.admin_cookie(),user)
                    return self.send_json({"ok":True},headers={
                        "Set-Cookie": self.session_cookie("", 0)})
                if parsed.path == "/api/admin/change-password":
                    if not user or self.headers.get("X-CSRF-Token", "") != user["csrf"]:
                        raise PermissionError("Sessão inválida")
                    shared_admin.change_password(user, body.get("current_password", ""),
                                                 body.get("new_password", ""))
                    return self.send_json({"ok": True}, headers={
                        "Set-Cookie": self.session_cookie("", 0)})
                shared_admin.require_admin(user,self.headers.get("X-CSRF-Token",""))
                if parsed.path == "/api/admin/users":
                    shared_admin.create_user(user,body)
                    return self.send_json({"ok":True})
                match=re.fullmatch(r"/api/admin/users/(\d+)",parsed.path)
                if match:
                    shared_admin.update_user(user,int(match.group(1)),body)
                    return self.send_json({"ok":True})
                if parsed.path == "/api/admin/settings":
                    shared_admin.save_settings(user,body)
                    return self.send_json({"ok":True})
                return self.send_json({"error":"Rota inexistente"},404)
            except PermissionError as exc:
                return self.send_json({"error":str(exc)},403)
            except (ValueError,TypeError,KeyError,sqlite3.IntegrityError) as exc:
                return self.send_json({"error":str(exc)},400)
            except (OSError,sqlite3.Error,RuntimeError) as exc:
                return self.send_json({"error":"Administração indisponível: "+type(exc).__name__},503)
        if parsed.path in ("/api/radar/ack", "/api/radar/ack-normalization"):
            if self.radar is None:
                return self.send_json({"error":"Radar indisponivel"},503)
            origin=self.headers.get("Origin")
            if origin and urlparse(origin).netloc != self.headers.get("Host"):
                return self.send_json({"error":"Origem invalida"},403)
            try:
                length=int(self.headers.get("Content-Length","0") or 0)
                if length<=0 or length>4096:
                    raise ValueError("Payload invalido")
                payload=json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(payload,dict):
                    raise ValueError("Payload invalido")
                allowed = self.allowed_instances(self.admin_user())
                if allowed is not None:
                    incident = next((item for item in self.radar.snapshot()["items"]
                                     if item["id"] == payload.get("incident_id")), None)
                    if incident is None or (str(incident.get("instancia")) not in allowed and incident.get("source") != "MONITOR_ALERTAS"):
                        return self.send_json({"error": "Incidente não autorizado"}, 403)
                if parsed.path == "/api/radar/ack-normalization":
                    return self.send_json(self.radar.acknowledge_normalization(payload.get("incident_id")))
                return self.send_json(self.radar.acknowledge(payload.get("incident_id")))
            except KeyError:
                return self.send_json({"error":"Incidente nao encontrado"},404)
            except (ValueError,TypeError) as exc:
                return self.send_json({"error":str(exc)},400)
            except Exception as exc:
                return self.send_json({"error":"Ciente nao gravado: "+type(exc).__name__},503)
        if parsed.path != "/api/operational/feedback":
            return self.send_json({"error":"Endpoint POST não encontrado"},404)
        try:
            length=int(self.headers.get("Content-Length","0") or 0)
            if length<=0 or length>65536:
                return self.send_json({"error":"Payload vazio ou acima de 64 KiB"},400)
            payload=json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, dict):
                return self.send_json({"error": "Payload inválido"}, 400)
            allowed = self.allowed_instances(self.admin_user())
            if allowed is not None and str(payload.get("instancia", "")) not in allowed:
                return self.send_json({"error": "Instância não autorizada"}, 403)
            rec=append_operational_feedback(payload)
            try: self.service._gateway_analytics_cache.clear()
            except Exception: pass
            return self.send_json({"ok":True,"record":rec},201)
        except ValueError as exc:
            return self.send_json({"error":str(exc)},400)
        except Exception as exc:
            return self.send_json({"error":"Registro operacional indisponível"},500)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db")
    ap.add_argument("--policy", default=str(APP_DIR / "toi_adaptive_policy_v1.json"))
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8088)
    args = ap.parse_args()

    db = Path(args.db) if args.db else find_latest_db()
    if not db:
        print("ERRO: nenhum toi_demo.sqlite encontrado.")
        print("Use --db CAMINHO_DO_SQLITE.")
        return 2

    Handler.service = DataService(db, Path(args.policy))
    Handler.service._gateway_monitor = GatewayMonitorSource()
    Handler.service._gateway_monitor.start()
    from toi_ml_weekly_review import start_weekly
    ml_review_stop = threading.Event()
    ml_review_thread = start_weekly(ml_review_stop, db)
    # R1.2.7: Radar has its own store; acquisition and D3 queries are unchanged.
    try:
        from toi_operational_radar_v1 import RadarService, configured_gateway_telegram
        Handler.radar = RadarService(APP_DIR, balance_provider=Handler.service.gateway_balance_inputs,
                                    telegram_sender=configured_gateway_telegram())
        Handler.service._gateway_balance_reader = Handler.radar.gateway_balance_snapshot
        Handler.service._gateway_activity_reader = Handler.radar.gateway_activity_snapshot
        Handler.radar.start()
    except Exception as exc:
        print("[RADAR] Inicializacao indisponivel: " + type(exc).__name__)
        Handler.radar = None
    server = ThreadingHTTPServer((args.host, args.port), Handler)

    print("=" * 78)
    print("TOI DASHBOARD v1.10.6")
    print("=" * 78)
    print(f"Banco : {db}")
    print(f"Policy: {args.policy}")
    print(f"URL   : http://{args.host}:{args.port}")
    print("CTRL+C para encerrar.")
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nEncerrando...")
    finally:
        server.server_close()
        Handler.service._gateway_monitor.stop()
        ml_review_stop.set()
        ml_review_thread.join(timeout=2)
        if Handler.radar is not None:
            Handler.radar.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
