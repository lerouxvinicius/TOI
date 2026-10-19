#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TOI Gateway Intelligence Shadow v0.2.4.1 R2.1 — DB Lock + Incremental LIVE Read Hardening G2
READ ONLY em relação ao toi_live.sqlite.

Evolução sobre Gateway Health v0.2.1:
- membership ativo continua vindo dinamicamente do gatewayList LIVE;
- ICMP robusto: 3 probes/ciclo, perda %, RTT mediana/p95/máximo e jitter robusto;
- Path Quorum v3: OK / DEGRADED / REDE + recovery grace, evitando falsos críticos coletivos;
- health multidimensional: reachability, service_state, traffic/call share e health final;
- silêncio funcional: Gateway alcançável sem tráfego quando a Instância/pares estão ativos;
- estado LEARNING possui limite temporal; não mascara silêncio indefinidamente;
- histórico rico gravado exclusivamente no banco Shadow;
- NÃO altera diagnóstico, recomendação, chamado, ação ou aquisição homologada.

Observação: traffic_share_pct é proxy por canais ocupados no gatewayList.
call_share_pct usa evidência NmGateway das lastCalls disponíveis no SQLite.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import socket
import sqlite3
import statistics
import subprocess
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(os.environ.get("TOI_ROOT", str(Path(__file__).resolve().parents[2])))
LIVE_DB = ROOT / "dados_toi" / "live" / "toi_live.sqlite"
SHADOW_DB = ROOT / "dados_toi" / "shadow" / "toi_gateway_health_shadow.sqlite"
SNAPSHOT = ROOT / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW" / "static" / "toi_gateway_health_latest.json"
DEFAULT_REF = Path(__file__).resolve().parents[1] / "gateway_identity_reference_v4.json"
DEFAULT_SEMANTIC = Path(__file__).resolve().parents[1] / "gateway_semantic_context_v1.json"

INSTANCES = ("1", "2", "4", "5", "6")
CADENCE_S = 10
PING_MAX_WORKERS = 8

PING_PROBES = 3
PING_TIMEOUT_MS = 700
PING_FAIL_CRITICAL = 3
PING_RECOVERY_SUCCESS_CYCLES = 2

INVENTORY_FRESH_S = 180
SENTINEL_IP = "10.90.10.101"
PATH_DEGRADED_RATIO = 0.25
PATH_DOWN_RATIO = 0.60
PATH_WIDE_DOWN_RATIO = 0.80
PATH_AFFECTED_INSTANCE_MIN = 2
PATH_RECOVERY_GRACE_CYCLES = 3
MEMBERSHIP_REMOVE_CONFIRM_CYCLES = 3

OP_START = "08:00"
OP_END = "21:00"
TRAFFIC_WARN_MIN = 10
TRAFFIC_CRIT_MIN = 20
LEARNING_MAX_MIN = 15
BASELINE_ACTIVE_SAMPLES = 30
PEER_ACTIVE_OCC_MIN = 5.0
PEER_ACTIVE_RATIO_MIN = 0.50

DNS_CACHE = {}
DNS_TTL_S = 300

# R2.1 — Snapshot Publish Hardening.
# O snapshot JSON é uma projeção para o Dashboard; falha transitória de lock no
# Windows NÃO pode derrubar a persistência histórica no Shadow DB.
SNAPSHOT_PUBLISH_RETRY_DELAYS_S = (0.05, 0.10, 0.25, 0.50, 1.00)
SNAPSHOT_PUBLISH_TRANSIENT_WINERRORS = {5, 32, 33}
SNAPSHOT_PUBLISH_FAILURES_TOTAL = 0
SNAPSHOT_PUBLISH_FAILED_CYCLES = 0
SNAPSHOT_PUBLISH_LAST_ERROR = None
SNAPSHOT_PUBLISH_LAST_SUCCESS_TS = None

# R2.1 — Shadow DB Lock Hardening.
# O Shadow usa journal_mode=DELETE por compatibilidade atual. Consultas READ ONLY
# podem segurar SHARED lock por alguns segundos; SQLITE_BUSY/LOCKED transitório
# nunca deve encerrar o Gateway Intelligence.
SHADOW_BUSY_TIMEOUT_MS = 5000
SHADOW_COMMIT_RETRY_DELAYS_S = (0.25, 0.50, 1.00, 2.00, 4.00, 5.00)
SHADOW_LOCK_FAILURES_TOTAL = 0
SHADOW_LOCK_SKIPPED_CYCLES = 0
SHADOW_LOCK_LAST_ERROR = None
SHADOW_LOCK_LAST_SUCCESS_TS = None


# v0.2.4 — thresholds SHADOW/provisórios; não são política homologada.
BASELINE_LOOKBACK_DAYS = 7
BASELINE_MAX_ROWS = 40000
BASELINE_REFRESH_S = 300
BASELINE_MIN_SAMPLES = 120
BASELINE_MIN_SPAN_MIN = 60
BASELINE_RECURRENT_RATIO = 0.25
BASELINE_OCCASIONAL_RATIO = 0.05
BASELINE_MIN_ACTIVE_SAMPLES = 20

HYST_ESCALATE_CONFIRM_CYCLES = 2
HYST_RECOVERY_CONFIRM_CYCLES = 3
HYST_SAME_SEVERITY_CONFIRM_CYCLES = 2

BASELINE_CACHE = {}

# Read Storm Hardening. O LIVE e append-only; depois da carga curta inicial,
# cada ciclo consome somente ROWIDs novos. Os limites cobrem com ampla margem
# a frota atual e nao alteram schema, dados ou journal_mode do LIVE.
LIVE_QUEUE_BOOTSTRAP_ROWS = 5000
LIVE_CALL_BOOTSTRAP_ROWS = 5000
LIVE_INFRA_BOOTSTRAP_ROWS = 12000


def now():
    return datetime.now().astimezone()


def iso(dt=None):
    return (dt or now()).isoformat(timespec="seconds")


def norm(v):
    return re.sub(r"[^A-Z0-9]", "", str(v or "").upper())


def parse_dt(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
        return dt.astimezone() if dt.tzinfo else dt.replace(tzinfo=now().tzinfo)
    except Exception:
        return None


def age_seconds(ts):
    dt = parse_dt(ts)
    return None if dt is None else max(0.0, (now() - dt).total_seconds())


def _snapshot_publish_is_transient(exc):
    """Somente locks/denials transitórios do Windows são tolerados.

    Erros estruturais (diretório inexistente, disco, JSON etc.) continuam
    fail-fast para não mascarar defeitos reais.
    """
    if isinstance(exc, PermissionError):
        return True
    return getattr(exc, "winerror", None) in SNAPSHOT_PUBLISH_TRANSIENT_WINERRORS


def publish_snapshot_atomic(payload):
    """Publica o snapshot com retry/backoff sem derrubar o writer Shadow.

    Retorna True se publicado. Em lock transitório persistente, retorna False
    e mantém o processo vivo; o próximo ciclo tentará novamente.
    """
    global SNAPSHOT_PUBLISH_FAILURES_TOTAL
    global SNAPSHOT_PUBLISH_FAILED_CYCLES
    global SNAPSHOT_PUBLISH_LAST_ERROR
    global SNAPSHOT_PUBLISH_LAST_SUCCESS_TS

    body = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp = SNAPSHOT.with_suffix(".tmp")
    attempts = len(SNAPSHOT_PUBLISH_RETRY_DELAYS_S) + 1

    for attempt in range(1, attempts + 1):
        try:
            # Reescreve o temporário a cada tentativa: se o replace anterior
            # falhou, o conteúdo continua íntegro para uma nova tentativa.
            tmp.write_text(body, encoding="utf-8")
            os.replace(tmp, SNAPSHOT)

            if SNAPSHOT_PUBLISH_FAILED_CYCLES:
                print(
                    f"  [RECOVERY] snapshot_publish recuperado após "
                    f"{SNAPSHOT_PUBLISH_FAILED_CYCLES} ciclo(s) com falha."
                )
            elif attempt > 1:
                print(f"  [RECOVERY] snapshot_publish concluído na tentativa {attempt}/{attempts}.")

            SNAPSHOT_PUBLISH_FAILED_CYCLES = 0
            SNAPSHOT_PUBLISH_LAST_ERROR = None
            SNAPSHOT_PUBLISH_LAST_SUCCESS_TS = iso()
            return True

        except OSError as exc:
            if not _snapshot_publish_is_transient(exc):
                raise

            SNAPSHOT_PUBLISH_FAILURES_TOTAL += 1
            SNAPSHOT_PUBLISH_LAST_ERROR = (
                f"{type(exc).__name__}: winerror={getattr(exc, 'winerror', None)}: {exc}"
            )

            if attempt < attempts:
                delay = SNAPSHOT_PUBLISH_RETRY_DELAYS_S[attempt - 1]
                print(
                    f"  [WARN] snapshot_publish bloqueado tentativa {attempt}/{attempts}: "
                    f"{SNAPSHOT_PUBLISH_LAST_ERROR}; retry em {delay:.2f}s"
                )
                time.sleep(delay)
                continue

            SNAPSHOT_PUBLISH_FAILED_CYCLES += 1
            print(
                f"  [WARN] snapshot_publish ADIADO após {attempts} tentativas: "
                f"{SNAPSHOT_PUBLISH_LAST_ERROR}. Shadow DB permanece ativo; "
                f"novo publish no próximo ciclo."
            )
            return False

    return False


def _sqlite_lock_transient(exc):
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    code = getattr(exc, "sqlite_errorcode", None)
    busy = getattr(sqlite3, "SQLITE_BUSY", 5)
    locked = getattr(sqlite3, "SQLITE_LOCKED", 6)
    if code in (busy, locked):
        return True
    msg = str(exc).lower()
    return "database is locked" in msg or "database table is locked" in msg or "database is busy" in msg


def configure_shadow_connection(con):
    """Configura tolerância de lock sem alterar journal_mode/schema/dados."""
    con.execute(f"PRAGMA busy_timeout={int(SHADOW_BUSY_TIMEOUT_MS)}")
    return con


def commit_shadow_with_retry(con, context="cycle", retry_delays=None, allow_skip=False):
    """Commit tolerante a SQLITE_BUSY/LOCKED.

    - lock transitório: espera e tenta novamente a MESMA transação;
    - erro estrutural: fail-fast;
    - se allow_skip=True e o lock persistir, faz rollback somente do ciclo e
      mantém o processo vivo para a próxima cadência.
    """
    global SHADOW_LOCK_FAILURES_TOTAL
    global SHADOW_LOCK_SKIPPED_CYCLES
    global SHADOW_LOCK_LAST_ERROR
    global SHADOW_LOCK_LAST_SUCCESS_TS

    delays = tuple(SHADOW_COMMIT_RETRY_DELAYS_S if retry_delays is None else retry_delays)
    attempts = len(delays) + 1
    for attempt in range(1, attempts + 1):
        try:
            con.commit()
            if attempt > 1:
                print(f"  [RECOVERY] shadow_db commit recuperado ({context}) na tentativa {attempt}/{attempts}.")
            SHADOW_LOCK_LAST_ERROR = None
            SHADOW_LOCK_LAST_SUCCESS_TS = iso()
            return True
        except sqlite3.OperationalError as exc:
            if not _sqlite_lock_transient(exc):
                raise
            SHADOW_LOCK_FAILURES_TOTAL += 1
            SHADOW_LOCK_LAST_ERROR = f"{type(exc).__name__}: {exc}"
            if attempt < attempts:
                delay = float(delays[attempt - 1])
                print(
                    f"  [WARN] shadow_db bloqueado ({context}) tentativa {attempt}/{attempts}: "
                    f"{SHADOW_LOCK_LAST_ERROR}; retry em {delay:.2f}s"
                )
                time.sleep(delay)
                continue

            if not allow_skip:
                raise

            try:
                con.rollback()
            finally:
                SHADOW_LOCK_SKIPPED_CYCLES += 1
            print(
                f"  [WARN] shadow_db commit ADIADO ({context}) após {attempts} tentativas: "
                f"{SHADOW_LOCK_LAST_ERROR}. Ciclo atual revertido; processo permanece vivo e tentará no próximo ciclo."
            )
            return False

    return False


def in_operational_window(t):
    sh, sm = map(int, OP_START.split(":"))
    eh, em = map(int, OP_END.split(":"))
    cur = t.hour * 60 + t.minute
    a = sh * 60 + sm
    b = eh * 60 + em
    return a <= cur < b if a < b else cur >= a or cur < b


def open_live():
    uri = f"file:{LIVE_DB.as_posix()}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=3)
    con.row_factory = sqlite3.Row
    return con


def active_inventory(row):
    """Converte a linha corrente já cacheada do gatewayList em inventário.

    A seleção da linha LIVE ocorre uma única vez por ciclo no LiveTailCache.
    Esta função não consulta o SQLite e, portanto, não pode reintroduzir scan
    por Instância.
    """
    if not row:
        return {"ts": None, "age_s": None, "fresh": False, "gateways": []}
    try:
        doc = json.loads(row["raw_json"] or "{}")
    except Exception:
        doc = {}
    arr = doc.get("gatewayList") or doc.get("gatewayDataList") or doc.get("GatewayDataList") or []
    gateways = []
    if isinstance(arr, list):
        for x in arr:
            name = str(x.get("NmGateway") or "").strip()
            if not name:
                continue

            def num(key):
                try:
                    return float(str(x.get(key, 0)).replace(",", "."))
                except Exception:
                    return 0.0

            gateways.append(
                {
                    "entity": name,
                    "total": num("QtValidsTotal"),
                    "busy": num("QtValidsBusy"),
                    "idle": num("QtValidsIdle"),
                    "occupancy_pct": num("VlPercValidsBusy"),
                    "invalid": num("QtInvalidsTotal"),
                }
            )
    age = age_seconds(row["ts"])
    return {
        "ts": row["ts"],
        "age_s": age,
        "fresh": age is not None and age <= INVENTORY_FRESH_S,
        "gateways": gateways,
    }


def fingerprint_calls(items):
    normalized = []
    for item in items:
        try:
            normalized.append(json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        except Exception:
            normalized.append(str(item))
    normalized.sort()
    return hashlib.sha1("\n".join(normalized).encode("utf-8", errors="ignore")).hexdigest()


def configured_instances(root):
    """Use the same curated/catalog scope as the LIVE collector."""
    app=Path(root)/'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'
    scope=json.loads((app/'toi_demo_scope_v1.json').read_text(encoding='utf-8'))
    catalog=json.loads((app/'portal_queue_catalog.json').read_text(encoding='utf-8'))
    instances={str(q['instancia']) for q in scope['queues']}
    instances.update(str(q['instancia']) for q in catalog.get('queues',[]) if all(
        str(q.get(key,'')).strip().isdigit() for key in ('instancia','codigo','projeto_id')))
    if not instances:raise RuntimeError('No configured gateway instances')
    return tuple(sorted(instances,key=int))


class LiveTailCache:
    """Projecao corrente incremental do LIVE, sem rescans temporais."""

    def __init__(self):
        self.queue_last_rowid = 0
        self.call_last_rowid = 0
        self.infra_last_rowid = 0
        self.queue_latest = {}
        self.call_rows = deque()
        self.gateway_latest = {}
        self.infra_bootstrap_scans_total = 0
        self.infra_bootstrap_completed = False
        self.infra_missing_instances = list(INSTANCES)
        self.calls_metadata_read = 0
        self.calls_stale_skipped = 0
        self.calls_backlog_capped = False

    @staticmethod
    def _latest_rowid(con, table):
        row=con.execute(f"SELECT rowid FROM {table} ORDER BY rowid DESC LIMIT 1").fetchone()
        return int(row["rowid"]) if row else 0

    def _refresh_queue(self, con):
        latest=self._latest_rowid(con,"queue_snapshot")
        if self.queue_last_rowid>latest:
            self.queue_last_rowid=0;self.queue_latest.clear()
        if self.queue_last_rowid==0:
            rows=list(con.execute("""SELECT rowid,instancia,codigo,ts,calls_dialing,calls_in_attendance
                FROM queue_snapshot ORDER BY rowid DESC LIMIT ?""",(LIVE_QUEUE_BOOTSTRAP_ROWS,)).fetchall())
            rows.reverse()
        else:
            rows=con.execute("""SELECT rowid,instancia,codigo,ts,calls_dialing,calls_in_attendance
                FROM queue_snapshot WHERE rowid>? ORDER BY rowid""",(self.queue_last_rowid,)).fetchall()
        for r in rows:
            self.queue_latest[(str(r["instancia"]),str(r["codigo"]))]=dict(r)
            self.queue_last_rowid=max(self.queue_last_rowid,int(r["rowid"]))
        return len(rows)

    def _refresh_calls(self, con, cycle, minutes):
        latest=self._latest_rowid(con,"lastcalls_snapshot")
        if self.call_last_rowid>latest:
            self.call_last_rowid=0;self.call_rows.clear()
        # Inspect dates before opening large call payloads. Cold starts previously
        # loaded/parsed thousands of old JSON documents only to discard them.
        start=max(self.call_last_rowid,latest-LIVE_CALL_BOOTSTRAP_ROWS)
        self.calls_backlog_capped=self.call_last_rowid>0 and start>self.call_last_rowid
        metadata=con.execute("""SELECT rowid,ts FROM lastcalls_snapshot NOT INDEXED
            WHERE rowid>? AND rowid<=? ORDER BY rowid""",(start,latest)).fetchall()
        cutoff=cycle-timedelta(minutes=minutes)
        selected=[]
        for row in metadata:
            dt=parse_dt(row['ts'])
            if dt is not None and cutoff<=dt<=cycle+timedelta(seconds=5):
                selected.append(int(row['rowid']))
        self.calls_metadata_read=len(metadata)
        self.calls_stale_skipped=len(metadata)-len(selected)
        rows=[]
        for offset in range(0,len(selected),100):
            ids=selected[offset:offset+100]
            placeholders=','.join('?' for _ in ids)
            rows.extend(con.execute(f"""SELECT rowid,ts,instancia,codigo,raw_json
                FROM lastcalls_snapshot WHERE rowid IN ({placeholders}) ORDER BY rowid""",ids).fetchall())
        for r in rows:
            dt=parse_dt(r["ts"])
            if dt is None:continue
            try:doc=json.loads(r["raw_json"] or "{}")
            except Exception:doc={}
            grouped=defaultdict(list)
            for item in (doc.get("lastCallsList") or []):
                gw=str(item.get("NmGateway") or "").strip()
                if gw:grouped[norm(gw)].append(item)
            compact={key:{"rows":len(items),"fingerprint":fingerprint_calls(items)} for key,items in grouped.items()}
            self.call_rows.append({"dt":dt,"ts":r["ts"],"instancia":str(r["instancia"]),"gateways":compact})
        self.call_last_rowid=latest
        while self.call_rows and self.call_rows[0]["dt"]<cutoff:
            self.call_rows.popleft()
        return len(rows)

    def _refresh_gateway_inventory(self, con):
        """Mantém o gatewayList corrente com um bootstrap e tail por ROWID.

        O bootstrap inspeciona somente uma cauda limitada por ROWID. Uma
        Instância sem inventário recente não provoca a leitura de todo o banco.
        Depois, lê somente o intervalo ROWID novo.
        """
        latest = self._latest_rowid(con, "infra_snapshot")
        reset = self.infra_last_rowid > latest
        if reset:
            self.infra_last_rowid = 0
            self.gateway_latest.clear()
            self.infra_bootstrap_completed = False

        if not self.infra_bootstrap_completed:
            missing = set(INSTANCES)
            rows_read = 0
            latest_rowids = {}
            cursor = con.execute(
                """SELECT rowid,instancia
                   FROM infra_snapshot NOT INDEXED
                   WHERE source='gateway' AND rowid>? AND rowid<=?
                   ORDER BY rowid DESC""",
                (max(0,latest-LIVE_INFRA_BOOTSTRAP_ROWS),latest),
            )
            try:
                for row in cursor:
                    rows_read += 1
                    inst = str(row["instancia"])
                    if inst not in missing:
                        continue
                    latest_rowids[inst] = int(row["rowid"])
                    missing.remove(inst)
                    if not missing:
                        break
            finally:
                cursor.close()

            # Busca o payload JSON somente para as linhas vencedoras. Durante
            # o scan reverso não transporta todo o histórico raw_json para o
            # Python.
            if latest_rowids:
                placeholders = ",".join("?" for _ in latest_rowids)
                sql = (
                    "SELECT rowid,instancia,ts,raw_json,latency FROM infra_snapshot "
                    f"WHERE rowid IN ({placeholders})"
                )
                for row in con.execute(sql, tuple(latest_rowids.values())):
                    self.gateway_latest[str(row["instancia"])] = dict(row)

            self.infra_last_rowid = latest
            self.infra_bootstrap_scans_total += 1
            self.infra_bootstrap_completed = True
            self.infra_missing_instances = sorted(missing)
            return {
                "rows": rows_read,
                "bootstrap": True,
                "reset": reset,
                "missing_instances": list(self.infra_missing_instances),
            }

        if latest <= self.infra_last_rowid:
            return {
                "rows": 0,
                "bootstrap": False,
                "reset": False,
                "missing_instances": list(self.infra_missing_instances),
            }

        rows = con.execute(
            """SELECT rowid,instancia,source,ts,raw_json,latency
               FROM infra_snapshot
               WHERE rowid>? AND rowid<=? AND source='gateway'
               ORDER BY rowid""",
            (self.infra_last_rowid, latest),
        ).fetchall()
        for row in rows:
            inst = str(row["instancia"])
            if inst in INSTANCES:
                self.gateway_latest[inst] = dict(row)
        self.infra_last_rowid = latest
        self.infra_missing_instances = [inst for inst in INSTANCES if inst not in self.gateway_latest]
        return {
            "rows": len(rows),
            "bootstrap": False,
            "reset": False,
            "missing_instances": list(self.infra_missing_instances),
        }

    def refresh(self, con, cycle, minutes=20):
        startup=not self.infra_bootstrap_completed
        def phase(label, action):
            started=time.monotonic()
            if startup:print(f'[Gateway startup] {label}: begin',flush=True)
            result=action()
            if startup:print(f'[Gateway startup] {label}: complete in {time.monotonic()-started:.2f}s',flush=True)
            return result
        return {
            "queue_rows":phase('queue metadata',lambda:self._refresh_queue(con)),
            "lastcalls_rows":phase('recent calls',lambda:self._refresh_calls(con,cycle,minutes)),
            "infra":phase('gateway inventory',lambda:self._refresh_gateway_inventory(con)),
        }

    def inventory_row(self, inst):
        return self.gateway_latest.get(str(inst))

    def activity(self, inst):
        rows=[r for (row_inst,_),r in self.queue_latest.items() if row_inst==str(inst)]
        total=sum(int(r.get("calls_dialing") or 0)+int(r.get("calls_in_attendance") or 0) for r in rows)
        newest=max([r.get("ts") for r in rows if r.get("ts")] or [None])
        age=age_seconds(newest);usable=age is not None and age<=120
        return {"active":bool(usable and total>0),"calls_now_proxy":total if usable else 0,
                "ts":newest,"age_s":age,"usable":usable}

    def evidence(self, inst):
        out=defaultdict(lambda:{"snapshots":0,"rows":0,"last_ts":None,"fingerprint":None})
        for row in self.call_rows:
            if row["instancia"]!=str(inst):continue
            for key,item in row["gateways"].items():
                info=out[key];info["snapshots"]+=1;info["rows"]+=int(item["rows"])
                info["last_ts"]=row["ts"];info["fingerprint"]=item["fingerprint"]
        return out


def reference_index(ref):
    idx = {}
    for name, item in (ref.get("gateways") or {}).items():
        for key in [name] + list(item.get("aliases") or []):
            idx[norm(key)] = {"canonical": name, **item}
    return idx


def semantic_index(doc):
    idx = {}
    for item in (doc.get("gateways") or []):
        inst = str(item.get("instancia") or "").strip()
        entity = str(item.get("entity") or "").strip()
        if inst and entity:
            idx[(inst, norm(entity))] = dict(item)
    return idx


def semantic_context_for(semantic_idx, inst, entity):
    item = dict(semantic_idx.get((str(inst), norm(entity))) or {})
    if not item:
        return {
            "known": False,
            "operational_role": "UNKNOWN",
            "lifecycle_state": "UNSPECIFIED",
            "traffic_expectation": "BASELINE_DERIVED",
            "coverage_level": "STANDARD",
            "evidence_source": "NONE",
            "evidence_confidence": "UNSPECIFIED",
            "note": "",
        }
    item["known"] = True
    return item


def semantic_overlay(ctx, *, service_state, stable_status, reachability, occupancy_active):
    """
    Camada semântica Shadow. Não altera raw_status/stable_status nem gera alertas.
    Apenas descreve papel/lifecycle/expectativa operacional de forma auditável.
    """
    role = str(ctx.get("operational_role") or "UNKNOWN").upper()
    lifecycle = str(ctx.get("lifecycle_state") or "UNSPECIFIED").upper()
    coverage = str(ctx.get("coverage_level") or "STANDARD").upper()
    machine = str(stable_status or "").upper()
    reach = str(reachability or "").upper()
    svc = str(service_state or "").upper()

    # SEMANTIC_ORTHOGONAL_TO_MACHINE_STATUS
    # A dimensão semântica descreve papel/lifecycle/cobertura e permanece presente
    # mesmo quando a dimensão de máquina aponta PATH_DEGRADED/ICMP. Isto NÃO mascara
    # a falha de rede: raw_status/stable_status/status e machine_alerts permanecem intactos,
    # e a supressão executiva continua restrita exclusivamente a NO_TRAFFIC funcional.
    # Falha humana confirmada é evidência operacional explícita; não é inferência automática.
    if lifecycle == "FAULT_CONFIRMED":
        status = "FAULT_CONFIRMED_CONTEXT"
    # Parada planejada/configuração não deve virar falha funcional automática.
    elif lifecycle in ("VENDOR_CONFIGURATION", "PLANNED_INACTIVE", "MAINTENANCE_PLANNED"):
        status = "PLANNED_INACTIVE"
    # Papel operacional e cobertura são uma dimensão ortogonal ao estado da rede.
    elif role == "SBC_LAST_ROUTE":
        status = "STANDBY_ACTIVE" if (svc == "ACTIVE" or occupancy_active) else "STANDBY_EXPECTED"
    elif coverage == "PARTIAL_QUEUE_SCOPE":
        status = "OPERATIONAL_PARTIAL_VISIBILITY" if svc == "ACTIVE" else "PARTIAL_VISIBILITY"
    else:
        status = None

    return {
        "semantic_status": status,
        "operational_role": role,
        "lifecycle_state": lifecycle,
        "traffic_expectation": str(ctx.get("traffic_expectation") or "BASELINE_DERIVED").upper(),
        "coverage_level": coverage,
        "evidence_source": str(ctx.get("evidence_source") or "NONE"),
        "evidence_confidence": str(ctx.get("evidence_confidence") or "UNSPECIFIED").upper(),
        "note": str(ctx.get("note") or ""),
    }


def resolve_target(name, ref_idx):
    item = ref_idx.get(norm(name)) or {}
    ip = item.get("ip")
    if ip:
        return ip, ip, "REFERENCE", item

    key = norm(name)
    cached = DNS_CACHE.get(key)
    if cached and time.time() - cached["at"] <= DNS_TTL_S:
        return cached["target"], cached["ip"], cached["source"], item

    for candidate in (name, name.lower()):
        try:
            resolved = socket.gethostbyname(candidate)
            DNS_CACHE[key] = {"at": time.time(), "target": candidate, "ip": resolved, "source": "DNS"}
            return candidate, resolved, "DNS", item
        except Exception:
            pass

    DNS_CACHE[key] = {"at": time.time(), "target": name, "ip": None, "source": "UNRESOLVED"}
    return name, None, "UNRESOLVED", item


def sync_membership(con, inst, inventory, cycle):
    """
    Sincroniza membership somente com fotografia Portal fresca.

    Adição é imediata. Remoção exige ausência confirmada em múltiplos ciclos frescos,
    evitando que uma fotografia parcial/transitória retire um Gateway do monitoramento.
    """
    if not inventory["fresh"]:
        return []

    current = {norm(g["entity"]): g["entity"] for g in inventory["gateways"]}
    rows = con.execute(
        "SELECT * FROM gateway_membership_v3 WHERE instancia=?",
        (inst,),
    ).fetchall()
    existing = {norm(r["entity"]): dict(r) for r in rows}
    events = []

    for key, entity in current.items():
        row = existing.get(key)
        if not row:
            con.execute(
                "INSERT INTO gateway_membership_v3(instancia,entity,is_active,first_seen_ts,last_seen_ts,last_change_ts,last_inventory_ts,missing_cycles) VALUES(?,?,?,?,?,?,?,?)",
                (inst, entity, 1, iso(cycle), iso(cycle), iso(cycle), inventory["ts"], 0),
            )
            events.append((entity, "DISCOVERED_LIVE", "Gateway descoberto automaticamente no gatewayList LIVE."))
        elif not int(row["is_active"] or 0):
            con.execute(
                """UPDATE gateway_membership_v3
                   SET entity=?,is_active=1,last_seen_ts=?,last_change_ts=?,last_inventory_ts=?,missing_cycles=0
                   WHERE instancia=? AND entity=?""",
                (entity, iso(cycle), iso(cycle), inventory["ts"], inst, row["entity"]),
            )
            events.append((entity, "ADDED_TO_PORTAL", "Gateway entrou novamente na fotografia LIVE."))
        else:
            con.execute(
                """UPDATE gateway_membership_v3
                   SET entity=?,last_seen_ts=?,last_inventory_ts=?,missing_cycles=0
                   WHERE instancia=? AND entity=?""",
                (entity, iso(cycle), inventory["ts"], inst, row["entity"]),
            )

    # Nunca remova todos os Gateways por uma fotografia vazia isolada.
    # O gatewayList atual conhecido possui pelo menos um item para as Instâncias em escopo.
    if current:
        for key, row in existing.items():
            if not int(row["is_active"] or 0) or key in current:
                continue
            miss = int(row.get("missing_cycles") or 0) + 1
            if miss >= MEMBERSHIP_REMOVE_CONFIRM_CYCLES:
                con.execute(
                    """UPDATE gateway_membership_v3
                       SET is_active=0,last_change_ts=?,last_inventory_ts=?,missing_cycles=?
                       WHERE instancia=? AND entity=?""",
                    (iso(cycle), inventory["ts"], miss, inst, row["entity"]),
                )
                events.append((
                    row["entity"], "REMOVED_FROM_PORTAL",
                    f"Gateway ausente em {miss} fotografias LIVE consecutivas; monitoramento ativo encerrado."
                ))
            else:
                con.execute(
                    """UPDATE gateway_membership_v3
                       SET last_inventory_ts=?,missing_cycles=?
                       WHERE instancia=? AND entity=?""",
                    (inventory["ts"], miss, inst, row["entity"]),
                )
                events.append((
                    row["entity"], "REMOVAL_PENDING",
                    f"Gateway ausente em {miss}/{MEMBERSHIP_REMOVE_CONFIRM_CYCLES} fotografias LIVE; aguardando confirmação."
                ))

    for entity, kind, detail in events:
        con.execute(
            "INSERT INTO gateway_inventory_event VALUES(?,?,?,?,?)",
            (iso(cycle), inst, entity, kind, detail),
        )
    return events


def membership_fallback(con, inst):
    rows = con.execute(
        "SELECT entity FROM gateway_membership_v3 WHERE instancia=? AND is_active=1 ORDER BY entity",
        (inst,),
    ).fetchall()
    return [r["entity"] for r in rows]


def percentile(values, p):
    vals = sorted(float(x) for x in values if x is not None)
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * float(p)
    lo = int(pos)
    hi = min(lo + 1, len(vals) - 1)
    frac = pos - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


def ping_probe(target, timeout_ms=PING_TIMEOUT_MS, probes=PING_PROBES):
    """Executa múltiplos ICMPs no ping da plataforma atual."""
    if not target:
        return {
            "ok": False, "sent": probes, "received": 0, "loss_pct": 100.0,
            "rtt_ms": None, "rtt_p50_ms": None, "rtt_p95_ms": None,
            "rtt_max_ms": None, "jitter_ms": None,
        }
    try:
        command = (["ping", "-n", str(int(probes)), "-w", str(int(timeout_ms)), str(target)]
                   if os.name == "nt" else
                   ["ping", "-n", "-c", str(int(probes)), "-W",
                    str(max(1, (int(timeout_ms) + 999) // 1000)), str(target)])
        cp = subprocess.run(
            command,
            capture_output=True,
            text=True,
            errors="ignore",
            timeout=max(5, int(probes) * 2 + 2),
        )
        txt = (cp.stdout or "") + "\n" + (cp.stderr or "")
        rtts = []
        # Português/inglês: tempo=43ms, time=43ms, tempo<1ms, time<1ms.
        for m in re.finditer(r"(?:tempo|time)\s*([=<])\s*(\d+(?:[.,]\d+)?)\s*ms", txt, re.I):
            op, raw = m.group(1), float(m.group(2).replace(",", "."))
            rtts.append(0.5 if op == "<" and raw <= 1 else raw)

        received = len(rtts)
        # Fallback raro: ping retornou 0 mas o texto não foi reconhecido.
        if received == 0 and cp.returncode == 0:
            received = probes

        sent = int(probes)
        loss = max(0.0, min(100.0, 100.0 * (sent - min(received, sent)) / max(sent, 1)))
        p50 = statistics.median(rtts) if rtts else None
        p95 = percentile(rtts, 0.95)
        rmax = max(rtts) if rtts else None
        jitter = (p95 - p50) if p95 is not None and p50 is not None else None
        return {
            "ok": received > 0,
            "sent": sent,
            "received": min(received, sent),
            "loss_pct": round(loss, 2),
            "rtt_ms": round(p50, 2) if p50 is not None else None,
            "rtt_p50_ms": round(p50, 2) if p50 is not None else None,
            "rtt_p95_ms": round(p95, 2) if p95 is not None else None,
            "rtt_max_ms": round(rmax, 2) if rmax is not None else None,
            "jitter_ms": round(jitter, 2) if jitter is not None else None,
        }
    except Exception:
        return {
            "ok": False, "sent": probes, "received": 0, "loss_pct": 100.0,
            "rtt_ms": None, "rtt_p50_ms": None, "rtt_p95_ms": None,
            "rtt_max_ms": None, "jitter_ms": None,
        }


def table_exists(con, name):
    return bool(con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone())



def init_shadow():
    """Inicializa somente o banco Shadow. Nunca altera o SQLite LIVE."""
    SHADOW_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(SHADOW_DB, timeout=SHADOW_BUSY_TIMEOUT_MS / 1000.0)
    configure_shadow_connection(con)
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS gateway_health_sample_v5(
          ts TEXT,instancia TEXT,entity TEXT,ip TEXT,ip_source TEXT,
          inventory_ts TEXT,inventory_age_s REAL,inventory_fresh INTEGER,
          occupancy_pct REAL,busy REAL,total REAL,
          probes_sent INTEGER,probes_received INTEGER,loss_pct REAL,
          rtt_p50_ms REAL,rtt_p95_ms REAL,rtt_max_ms REAL,rtt_jitter_ms REAL,
          ping_fail_count INTEGER,ping_success_count INTEGER,
          reachability_state TEXT,service_state TEXT,
          raw_status TEXT,stable_status TEXT,display_status TEXT,severity TEXT,
          hysteresis_candidate TEXT,hysteresis_count INTEGER,hysteresis_required INTEGER,
          path_suppressed INTEGER,
          call_evidence_rows INTEGER,call_evidence_snapshots INTEGER,last_call_evidence_ts TEXT,
          call_fingerprint TEXT,instance_active INTEGER,operational_window INTEGER,
          traffic_share_pct REAL,call_share_pct REAL,peer_active_ratio REAL,peer_median_occupancy REAL,
          baseline_class TEXT,baseline_ready INTEGER,baseline_samples INTEGER,
          baseline_active_samples INTEGER,baseline_active_ratio REAL,baseline_span_min REAL,
          baseline_occ_median REAL,baseline_occ_p95 REAL,
          baseline_traffic_share_median REAL,baseline_call_share_median REAL,
          silence_warn_min REAL,silence_critical_min REAL,
          path_state TEXT,reason TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_gwh_v5 ON gateway_health_sample_v5(instancia,entity,ts);
        CREATE INDEX IF NOT EXISTS ix_gwh_v5_ts ON gateway_health_sample_v5(ts);

        CREATE TABLE IF NOT EXISTS gateway_state_v5(
          instancia TEXT,entity TEXT,
          first_seen_ts TEXT,last_seen_ts TEXT,last_active_ts TEXT,
          last_call_evidence_ts TEXT,last_call_fingerprint TEXT,
          active_samples INTEGER DEFAULT 0,observed_samples INTEGER DEFAULT 0,
          ping_fail_count INTEGER DEFAULT 0,ping_success_count INTEGER DEFAULT 0,
          raw_status TEXT,stable_status TEXT,last_reliable_status TEXT,
          candidate_status TEXT,candidate_count INTEGER DEFAULT 0,
          stable_since_ts TEXT,last_transition_ts TEXT,
          total_cycles INTEGER DEFAULT 0,operational_cycles INTEGER DEFAULT 0,
          path_suppressed_cycles INTEGER DEFAULT 0,
          raw_warn_cycles INTEGER DEFAULT 0,raw_critical_cycles INTEGER DEFAULT 0,
          stable_warn_cycles INTEGER DEFAULT 0,stable_critical_cycles INTEGER DEFAULT 0,
          PRIMARY KEY(instancia,entity)
        );

        CREATE TABLE IF NOT EXISTS gateway_state_transition_v1(
          ts TEXT,instancia TEXT,entity TEXT,old_status TEXT,new_status TEXT,
          raw_status TEXT,reason TEXT,confirm_cycles INTEGER,
          baseline_class TEXT,path_state TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_gw_transition_v1 ON gateway_state_transition_v1(instancia,entity,ts);

        CREATE TABLE IF NOT EXISTS gateway_baseline_snapshot_v1(
          ts TEXT,instancia TEXT,entity TEXT,lookback_days INTEGER,
          sample_count INTEGER,active_samples INTEGER,active_ratio REAL,span_min REAL,
          baseline_class TEXT,ready INTEGER,
          occupancy_median REAL,occupancy_p95 REAL,
          traffic_share_median REAL,call_share_median REAL,
          rtt_p50_median REAL,loss_p95 REAL,
          source_versions TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_gw_baseline_v1 ON gateway_baseline_snapshot_v1(instancia,entity,ts);

        CREATE TABLE IF NOT EXISTS gateway_accounting_daily_v1(
          day TEXT,instancia TEXT,entity TEXT,
          cycles INTEGER DEFAULT 0,operational_cycles INTEGER DEFAULT 0,
          path_suppressed_cycles INTEGER DEFAULT 0,
          icmp_ok_cycles INTEGER DEFAULT 0,icmp_intermittent_cycles INTEGER DEFAULT 0,
          icmp_down_cycles INTEGER DEFAULT 0,icmp_nc_cycles INTEGER DEFAULT 0,
          service_active_cycles INTEGER DEFAULT 0,
          silence_observe_cycles INTEGER DEFAULT 0,silence_warn_cycles INTEGER DEFAULT 0,
          silence_critical_cycles INTEGER DEFAULT 0,
          stable_warn_cycles INTEGER DEFAULT 0,stable_critical_cycles INTEGER DEFAULT 0,
          occupancy_sum REAL DEFAULT 0,occupancy_samples INTEGER DEFAULT 0,
          traffic_share_sum REAL DEFAULT 0,traffic_share_samples INTEGER DEFAULT 0,
          call_share_sum REAL DEFAULT 0,call_share_samples INTEGER DEFAULT 0,
          transitions INTEGER DEFAULT 0,
          first_ts TEXT,last_ts TEXT,
          PRIMARY KEY(day,instancia,entity)
        );

        CREATE TABLE IF NOT EXISTS gateway_membership_v3(
          instancia TEXT,entity TEXT,is_active INTEGER,first_seen_ts TEXT,last_seen_ts TEXT,
          last_change_ts TEXT,last_inventory_ts TEXT,missing_cycles INTEGER DEFAULT 0,
          PRIMARY KEY(instancia,entity)
        );
        CREATE TABLE IF NOT EXISTS gateway_inventory_event(
          ts TEXT,instancia TEXT,entity TEXT,event_type TEXT,detail TEXT
        );
        CREATE TABLE IF NOT EXISTS gateway_path_sample_v1(
          ts TEXT,path_state TEXT,sentinel_ok INTEGER,resolved_gateways INTEGER,
          zero_reply_gateways INTEGER,partial_gateways INTEGER,fail_ratio REAL,partial_ratio REAL,
          affected_instances INTEGER,recovery_grace_remaining INTEGER
        );
        """
    )
    membership_cols = {r[1] for r in con.execute("PRAGMA table_info(gateway_membership_v3)").fetchall()}
    if "missing_cycles" not in membership_cols:
        con.execute("ALTER TABLE gateway_membership_v3 ADD COLUMN missing_cycles INTEGER DEFAULT 0")
    commit_shadow_with_retry(con, context="init", allow_skip=False)
    return con


def _state_defaults(inst, entity):
    return {
        "instancia": inst, "entity": entity,
        "first_seen_ts": None, "last_seen_ts": None, "last_active_ts": None,
        "last_call_evidence_ts": None, "last_call_fingerprint": None,
        "active_samples": 0, "observed_samples": 0,
        "ping_fail_count": 0, "ping_success_count": 0,
        "raw_status": None, "stable_status": None, "last_reliable_status": None,
        "candidate_status": None, "candidate_count": 0,
        "stable_since_ts": None, "last_transition_ts": None,
        "total_cycles": 0, "operational_cycles": 0, "path_suppressed_cycles": 0,
        "raw_warn_cycles": 0, "raw_critical_cycles": 0,
        "stable_warn_cycles": 0, "stable_critical_cycles": 0,
    }


def get_state(con, inst, entity):
    row = con.execute(
        "SELECT * FROM gateway_state_v5 WHERE instancia=? AND entity=? COLLATE NOCASE",
        (inst, entity),
    ).fetchone()
    if row:
        d = _state_defaults(inst, entity)
        d.update(dict(row))
        d["entity"] = entity
        return d

    d = _state_defaults(inst, entity)
    # Migração suave do v4 para preservar último ativo e aprendizado observado.
    if table_exists(con, "gateway_state_v4"):
        legacy = con.execute(
            "SELECT * FROM gateway_state_v4 WHERE instancia=? AND entity=? COLLATE NOCASE",
            (inst, entity),
        ).fetchone()
        if legacy:
            old = dict(legacy)
            for k in ("first_seen_ts","last_seen_ts","last_active_ts","last_call_evidence_ts","last_call_fingerprint"):
                d[k] = old.get(k)
            d["active_samples"] = int(old.get("active_samples") or 0)
            d["observed_samples"] = int(old.get("observed_samples") or 0)
            d["ping_fail_count"] = int(old.get("ping_fail_count") or 0)
            d["ping_success_count"] = int(old.get("ping_success_count") or 0)
            d["raw_status"] = old.get("last_status")
            d["stable_status"] = old.get("last_status")
            d["last_reliable_status"] = old.get("last_reliable_status") or old.get("last_status")
            d["stable_since_ts"] = old.get("last_seen_ts")
    return d


def save_state(con, s):
    cols = [
        "instancia","entity","first_seen_ts","last_seen_ts","last_active_ts",
        "last_call_evidence_ts","last_call_fingerprint","active_samples","observed_samples",
        "ping_fail_count","ping_success_count","raw_status","stable_status","last_reliable_status",
        "candidate_status","candidate_count","stable_since_ts","last_transition_ts",
        "total_cycles","operational_cycles","path_suppressed_cycles","raw_warn_cycles",
        "raw_critical_cycles","stable_warn_cycles","stable_critical_cycles"
    ]
    ph = ",".join("?" for _ in cols)
    upd = ",".join(f"{c}=excluded.{c}" for c in cols[2:])
    con.execute(
        f"INSERT INTO gateway_state_v5({','.join(cols)}) VALUES({ph}) "
        f"ON CONFLICT(instancia,entity) DO UPDATE SET {upd}",
        tuple(s.get(c) for c in cols),
    )


def severity(status):
    s = str(status or "").upper()
    if s in ("PING_DOWN", "NO_TRAFFIC_CRITICAL"):
        return "CRITICAL"
    if s in (
        "PING_INTERMITTENT", "NO_TRAFFIC_WARN", "PING_NC",
        "OPERACIONAL_ICMP_NC", "OPERACIONAL_ICMP_INTERMITTENT",
        "SERVICE_ACTIVE_ICMP_DOWN", "PATH_DEGRADED", "PING_RECOVERING",
    ):
        return "WARN"
    return "INFO"


def severity_rank(status):
    return {"INFO": 0, "WARN": 1, "CRITICAL": 2}.get(severity(status), 0)


def op_window_start_for(dt):
    sh, sm = map(int, OP_START.split(":"))
    return dt.replace(hour=sh, minute=sm, second=0, microsecond=0)


def _finite(values):
    out = []
    for v in values:
        if v is None:
            continue
        try:
            out.append(float(v))
        except Exception:
            pass
    return out


def baseline_for_gateway(con, inst, entity, cycle):
    """Baseline robusto de 7 dias, exclusivamente a partir do banco Shadow."""
    cache_key = (str(inst), norm(entity))
    cached = BASELINE_CACHE.get(cache_key)
    if cached and time.time() - cached["at"] < BASELINE_REFRESH_S:
        return cached["value"]

    cutoff = (cycle - timedelta(days=BASELINE_LOOKBACK_DAYS)).isoformat()
    rows = []
    # v5 tem precedência temporal futura; v4 preserva histórico já coletado.
    if table_exists(con, "gateway_health_sample_v5"):
        rows += con.execute(
            """SELECT ts,occupancy_pct,traffic_share_pct,call_share_pct,rtt_p50_ms,loss_pct,
                      service_state,instance_active,operational_window,path_state,inventory_fresh
               FROM gateway_health_sample_v5
               WHERE instancia=? AND entity=? COLLATE NOCASE AND ts>=?
               ORDER BY ts DESC LIMIT ?""",
            (str(inst), entity, cutoff, BASELINE_MAX_ROWS),
        ).fetchall()
    remaining = max(0, BASELINE_MAX_ROWS - len(rows))
    if remaining and table_exists(con, "gateway_health_sample_v4"):
        rows += con.execute(
            """SELECT ts,occupancy_pct,traffic_share_pct,call_share_pct,rtt_p50_ms,loss_pct,
                      service_state,instance_active,operational_window,path_state,inventory_fresh
               FROM gateway_health_sample_v4
               WHERE instancia=? AND entity=? COLLATE NOCASE AND ts>=?
               ORDER BY ts DESC LIMIT ?""",
            (str(inst), entity, cutoff, remaining),
        ).fetchall()

    eligible = []
    for r in rows:
        try:
            if int(r["operational_window"] or 0) != 1:
                continue
            # v0.2.4.1 Semantic Hardening:
            # instance_active era derivado das filas cadastradas no TOI e, portanto,
            # não representa demanda global da Instância quando a cobertura de filas é parcial.
            # Ele passa a ser CONTEXTO, não gate de elegibilidade do baseline do Gateway.
            if str(r["path_state"] or "").upper() != "OK":
                continue
            if int(r["inventory_fresh"] or 0) != 1:
                continue
            eligible.append(r)
        except Exception:
            continue

    ts_values = [parse_dt(r["ts"]) for r in eligible if parse_dt(r["ts"]) is not None]
    span_min = 0.0
    if len(ts_values) >= 2:
        span_min = max(0.0, (max(ts_values) - min(ts_values)).total_seconds() / 60.0)
    active_rows = [r for r in eligible if str(r["service_state"] or "").upper() == "ACTIVE" or float(r["occupancy_pct"] or 0) > 0]
    sample_count = len(eligible)
    active_samples = len(active_rows)
    active_ratio = active_samples / sample_count if sample_count else 0.0
    ready = sample_count >= BASELINE_MIN_SAMPLES and span_min >= BASELINE_MIN_SPAN_MIN

    if not ready:
        bclass = "LEARNING"
    elif active_samples >= BASELINE_MIN_ACTIVE_SAMPLES and active_ratio >= BASELINE_RECURRENT_RATIO:
        bclass = "ACTIVE_RECURRENT"
    elif active_samples >= BASELINE_MIN_ACTIVE_SAMPLES and active_ratio >= BASELINE_OCCASIONAL_RATIO:
        bclass = "ACTIVE_OCCASIONAL"
    else:
        bclass = "STANDBY_CANDIDATE"

    occs = _finite(r["occupancy_pct"] for r in active_rows)
    tshares = _finite(r["traffic_share_pct"] for r in active_rows)
    cshares = _finite(r["call_share_pct"] for r in active_rows)
    rtts = _finite(r["rtt_p50_ms"] for r in eligible)
    losses = _finite(r["loss_pct"] for r in eligible)
    value = {
        "class": bclass, "ready": bool(ready), "sample_count": sample_count,
        "active_samples": active_samples, "active_ratio": active_ratio, "span_min": span_min,
        "occupancy_median": statistics.median(occs) if occs else None,
        "occupancy_p95": percentile(occs, .95),
        "traffic_share_median": statistics.median(tshares) if tshares else None,
        "call_share_median": statistics.median(cshares) if cshares else None,
        "rtt_p50_median": statistics.median(rtts) if rtts else None,
        "loss_p95": percentile(losses, .95),
    }
    con.execute(
        """INSERT INTO gateway_baseline_snapshot_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            iso(cycle), str(inst), entity, BASELINE_LOOKBACK_DAYS,
            sample_count, active_samples, round(active_ratio, 6), round(span_min, 2),
            bclass, int(ready), value["occupancy_median"], value["occupancy_p95"],
            value["traffic_share_median"], value["call_share_median"],
            value["rtt_p50_median"], value["loss_p95"], "v5+v4",
        ),
    )
    BASELINE_CACHE[cache_key] = {"at": time.time(), "value": value}
    return value


def silence_thresholds(baseline):
    bclass = baseline.get("class")
    if bclass == "ACTIVE_RECURRENT":
        return 10.0, 20.0
    if bclass == "ACTIVE_OCCASIONAL":
        return 20.0, 45.0
    return None, None


def apply_hysteresis(con, state, raw_status, reason, baseline_class, path_state, cycle, path_suppressed=False):
    """Promove mudanças apenas após confirmação. Path transversal congela falhas individuais."""
    old = state.get("stable_status")
    if not old:
        state["stable_status"] = raw_status
        state["stable_since_ts"] = iso(cycle)
        state["candidate_status"] = None
        state["candidate_count"] = 0
        return raw_status, False, None, 0, 0

    if path_suppressed:
        state["candidate_status"] = None
        state["candidate_count"] = 0
        return old, False, None, 0, 0

    if raw_status == old:
        state["candidate_status"] = None
        state["candidate_count"] = 0
        return old, False, None, 0, 0

    if state.get("candidate_status") == raw_status:
        state["candidate_count"] = int(state.get("candidate_count") or 0) + 1
    else:
        state["candidate_status"] = raw_status
        state["candidate_count"] = 1

    old_rank = severity_rank(old)
    new_rank = severity_rank(raw_status)
    if new_rank > old_rank:
        required = HYST_ESCALATE_CONFIRM_CYCLES
    elif new_rank < old_rank:
        required = HYST_RECOVERY_CONFIRM_CYCLES
    else:
        required = HYST_SAME_SEVERITY_CONFIRM_CYCLES

    if int(state["candidate_count"] or 0) < required:
        return old, False, raw_status, int(state["candidate_count"] or 0), required

    state["stable_status"] = raw_status
    state["stable_since_ts"] = iso(cycle)
    state["last_transition_ts"] = iso(cycle)
    state["candidate_status"] = None
    state["candidate_count"] = 0
    con.execute(
        """INSERT INTO gateway_state_transition_v1
           (ts,instancia,entity,old_status,new_status,raw_status,reason,confirm_cycles,baseline_class,path_state)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (iso(cycle), state["instancia"], state["entity"], old, raw_status, raw_status,
         reason, required, baseline_class, path_state),
    )
    con.execute(
        "INSERT INTO gateway_inventory_event VALUES(?,?,?,?,?)",
        (iso(cycle), state["instancia"], state["entity"], "STABLE_STATE_CHANGE", f"{old} -> {raw_status}: {reason}"),
    )
    return raw_status, True, None, 0, required


def account_daily(con, cycle, inst, entity, *, inside, path_suppressed, reachability, service_state,
                  raw_status, stable_status, occupancy_pct, traffic_share_pct, call_share_pct, transitioned):
    day = cycle.date().isoformat()
    occ = float(occupancy_pct) if occupancy_pct is not None else None
    tsh = float(traffic_share_pct) if traffic_share_pct is not None else None
    csh = float(call_share_pct) if call_share_pct is not None else None
    raw_sev = severity(raw_status)
    stable_sev = severity(stable_status)
    vals = {
        "cycles":1,"operational_cycles":int(bool(inside)),"path_suppressed_cycles":int(bool(path_suppressed)),
        "icmp_ok_cycles":int(reachability=="OK"),"icmp_intermittent_cycles":int(reachability=="INTERMITTENT"),
        "icmp_down_cycles":int(reachability=="DOWN"),"icmp_nc_cycles":int(reachability=="NC"),
        "service_active_cycles":int(service_state=="ACTIVE"),
        "silence_observe_cycles":int(raw_status=="NO_TRAFFIC_OBSERVE"),
        "silence_warn_cycles":int(raw_status=="NO_TRAFFIC_WARN"),
        "silence_critical_cycles":int(raw_status=="NO_TRAFFIC_CRITICAL"),
        "stable_warn_cycles":int(stable_sev=="WARN"),"stable_critical_cycles":int(stable_sev=="CRITICAL"),
        "occupancy_sum":occ or 0.0,"occupancy_samples":int(occ is not None),
        "traffic_share_sum":tsh or 0.0,"traffic_share_samples":int(tsh is not None),
        "call_share_sum":csh or 0.0,"call_share_samples":int(csh is not None),
        "transitions":int(bool(transitioned)),
    }
    cols=list(vals)
    con.execute(
        f"""INSERT INTO gateway_accounting_daily_v1
        (day,instancia,entity,{','.join(cols)},first_ts,last_ts)
        VALUES(?,?,?,{','.join('?' for _ in cols)},?,?)
        ON CONFLICT(day,instancia,entity) DO UPDATE SET
        {','.join(f'{c}={c}+excluded.{c}' for c in cols)},last_ts=excluded.last_ts""",
        (day,str(inst),entity,*[vals[c] for c in cols],iso(cycle),iso(cycle)),
    )


def accounting_today(con, cycle, inst, entity):
    row = con.execute(
        "SELECT * FROM gateway_accounting_daily_v1 WHERE day=? AND instancia=? AND entity=? COLLATE NOCASE",
        (cycle.date().isoformat(), str(inst), entity),
    ).fetchone()
    if not row:
        return {}
    d=dict(row)
    for stem in ("occupancy","traffic_share","call_share"):
        n=int(d.get(stem+"_samples") or 0)
        d[stem+"_avg"] = round(float(d.get(stem+"_sum") or 0)/n,3) if n else None
    return d


def classify_path(sentinel, target_results, target_instances):
    """
    Path Quorum v3.

    A v0.2.2 contava instância afetada somente quando havia perda de 100%. Assim,
    um ciclo com 25/26 Gateways respondendo apenas 2 de 3 probes podia permanecer
    path=OK. Aqui qualquer perda parcial entra no quorum transversal.
    """
    resolved = len(target_results)
    zero = [t for t, p in target_results.items() if int(p.get("received") or 0) == 0]
    partial = [t for t, p in target_results.items()
               if 0 < int(p.get("received") or 0) < int(p.get("sent") or PING_PROBES)]
    impaired = set(zero) | set(partial)
    fail_ratio = len(zero) / resolved if resolved else 0.0
    partial_ratio = len(partial) / resolved if resolved else 0.0
    impairment_ratio = len(impaired) / resolved if resolved else 0.0

    # [total, zero, partial, impaired]
    per_inst = defaultdict(lambda: [0, 0, 0, 0])
    for target, probe in target_results.items():
        received = int(probe.get("received") or 0)
        sent = int(probe.get("sent") or PING_PROBES)
        for inst in target_instances.get(target, set()):
            per_inst[inst][0] += 1
            if received == 0:
                per_inst[inst][1] += 1
                per_inst[inst][3] += 1
            elif received < sent:
                per_inst[inst][2] += 1
                per_inst[inst][3] += 1

    affected_any = sum(1 for total, zero_n, partial_n, impaired_n in per_inst.values()
                       if total and impaired_n > 0)
    affected_major = sum(1 for total, zero_n, partial_n, impaired_n in per_inst.values()
                         if total and impaired_n / total >= 0.50)
    zero_affected = sum(1 for total, zero_n, partial_n, impaired_n in per_inst.values()
                        if total and zero_n > 0)
    sentinel_ok = int(sentinel.get("received") or 0) > 0

    if ((not sentinel_ok and fail_ratio >= PATH_DOWN_RATIO) or
            (fail_ratio >= PATH_WIDE_DOWN_RATIO and affected_major >= PATH_AFFECTED_INSTANCE_MIN)):
        state = "REDE"
    elif ((impairment_ratio >= PATH_DEGRADED_RATIO and affected_any >= PATH_AFFECTED_INSTANCE_MIN) or
          (not sentinel_ok and impairment_ratio > 0)):
        state = "DEGRADED"
    else:
        state = "OK"
    return {
        "state": state,
        "sentinel_ok": sentinel_ok,
        "resolved": resolved,
        "zero": len(zero),
        "partial": len(partial),
        "impaired": len(impaired),
        "fail_ratio": fail_ratio,
        "partial_ratio": partial_ratio,
        "impairment_ratio": impairment_ratio,
        "affected_instances": affected_any,
        "zero_affected_instances": zero_affected,
        "severely_affected_instances": affected_major,
    }





def main():
    global INSTANCES
    INSTANCES=configured_instances(ROOT)
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--reference", default=str(DEFAULT_REF))
    parser.add_argument("--semantic-context", default=str(DEFAULT_SEMANTIC))
    args = parser.parse_args()

    ref = json.loads(Path(args.reference).read_text(encoding="utf-8"))
    ref_idx = reference_index(ref)
    semantic_doc = json.loads(Path(args.semantic_context).read_text(encoding="utf-8"))
    semantic_idx = semantic_index(semantic_doc)
    semantic_hash = hashlib.sha256(Path(args.semantic_context).read_bytes()).hexdigest()
    shadow = init_shadow()
    SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 118)
    print("TOI GATEWAY INTELLIGENCE SHADOW v0.2.4.1 - SEMANTIC HARDENING / BASELINE QUALITY / ROLE-LIFECYCLE")
    print("=" * 118)
    print("LIVE DB :", LIVE_DB)
    print("SHADOW  :", SHADOW_DB)
    print("SNAPSHOT:", SNAPSHOT)
    print(f"ICMP: {PING_PROBES} probes/ciclo, cadência {CADENCE_S}s, 24x7. Membership: gatewayList LIVE.")
    print("Baseline: 7 dias; instance_active agora é contexto, não gate de elegibilidade do Gateway.")
    print("Histerese: escalada 2 ciclos, recuperação 3, troca lateral 2. Limiares ainda NÃO homologados.")
    print("Semantic: papel/lifecycle/expectativa/coverage versionados; não alteram raw/stable status nem alertas.")
    print("Full Accounting: preservado; somente banco Shadow.")
    print(f"DB Lock Hardening: busy_timeout={SHADOW_BUSY_TIMEOUT_MS}ms + commit retry/rollback de ciclo; journal_mode inalterado.")
    print("Read Storm G3: bootstrap limitado por ROWID; JSON de chamadas somente nos últimos 20 minutos; LIVE READ ONLY.")
    print("Ctrl+C para encerrar.")
    print()

    recovery_grace = 0
    live_tail = LiveTailCache()

    while True:
        cycle = now()
        inside = in_operational_window(cycle)
        live = open_live()
        inventory, activity, calls, dynamic = {}, {}, {}, []

        live_tail_stats=live_tail.refresh(live,cycle,minutes=20)
        startup=live_tail.infra_bootstrap_scans_total==1 and live_tail_stats['infra'].get('bootstrap',False)

        for inst in INSTANCES:
            inventory[inst] = active_inventory(live_tail.inventory_row(inst))
            activity[inst] = live_tail.activity(inst)
            calls[inst] = live_tail.evidence(inst)
            sync_membership(shadow, inst, inventory[inst], cycle)
            if inventory[inst]["fresh"] and inventory[inst]["gateways"]:
                dynamic.extend((inst, g) for g in inventory[inst]["gateways"])
            else:
                for entity in membership_fallback(shadow, inst):
                    dynamic.append((inst, {"entity":entity,"total":None,"busy":None,"idle":None,"occupancy_pct":None}))
        live.close()

        dedup = {}
        for inst, g in dynamic:
            dedup[(inst, norm(g["entity"]))] = (inst, g)
        dynamic = list(dedup.values())

        targets, audit_notes = {}, []
        target_instances = defaultdict(set)
        for inst, g in dynamic:
            target, resolved, source, ref_item = resolve_target(g["entity"], ref_idx)
            targets[(inst, norm(g["entity"]))] = (target, resolved, source, ref_item)
            if resolved:
                target_instances[target].add(inst)
            ref_inst = str(ref_item.get("reference_instance") or "")
            if ref_inst and ref_inst != inst:
                audit_notes.append({
                    "instancia":inst,"entity":g["entity"],"type":"REFERENCE_CONFLICT",
                    "message":f"{g['entity']}: Portal LIVE=Instância {inst}; referência={ref_inst}."
                })

        unique_targets = {v[0] for v in targets.values() if v[1]}
        unique_targets.add(SENTINEL_IP)
        ping = {}
        if startup:print('[Gateway startup] ICMP: begin',flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(PING_MAX_WORKERS,max(1,len(unique_targets)))) as pool:
            futures={}
            for target in sorted(unique_targets):
                futures[pool.submit(ping_probe,target)] = target
                time.sleep(0.03)
            for future in concurrent.futures.as_completed(futures):
                ping[futures[future]] = future.result()
        if startup:print('[Gateway startup] ICMP: complete; processing gateway baselines',flush=True)

        sentinel_probe = ping.get(SENTINEL_IP,{"received":0,"sent":PING_PROBES})
        gateway_probe_results = {t: ping.get(t,{"received":0,"sent":PING_PROBES}) for t in unique_targets if t!=SENTINEL_IP}
        path = classify_path(sentinel_probe,gateway_probe_results,target_instances)
        raw_path_state = path["state"]
        if raw_path_state != "OK":
            recovery_grace = PATH_RECOVERY_GRACE_CYCLES
            effective_path_state = raw_path_state
        elif recovery_grace > 0:
            effective_path_state = "RECOVERY"
            recovery_grace -= 1
        else:
            effective_path_state = "OK"
        shadow.execute(
            "INSERT INTO gateway_path_sample_v1 VALUES(?,?,?,?,?,?,?,?,?,?)",
            (iso(cycle),effective_path_state,int(path["sentinel_ok"]),path["resolved"],path["zero"],path["partial"],
             round(path["fail_ratio"],4),round(path["partial_ratio"],4),path["affected_instances"],recovery_grace)
        )

        inst_metrics={}
        for inst in INSTANCES:
            gs=[g for i,g in dynamic if i==inst]
            inst_metrics[inst]={
                "total_busy":sum(max(0.0,float(g.get("busy") or 0)) for g in gs),
                "call_total":sum(int(v.get("rows") or 0) for v in calls[inst].values()),
            }

        alerts, machine_alerts, semantic_suppressions, semantic_findings, samples = [], [], [], [], []
        for inst,g in dynamic:
            name=g["entity"]
            if startup:print(f'[Gateway startup] instance={inst} gateway={name}: analysis',flush=True)
            target,resolved,ip_source,ref_item=targets[(inst,norm(name))]
            probe=ping.get(target,{"ok":False,"sent":PING_PROBES,"received":0,"loss_pct":100.0,
                                   "rtt_p50_ms":None,"rtt_p95_ms":None,"rtt_max_ms":None,"jitter_ms":None}) if resolved else {
                                   "ok":False,"sent":0,"received":0,"loss_pct":None,"rtt_p50_ms":None,"rtt_p95_ms":None,
                                   "rtt_max_ms":None,"jitter_ms":None}
            state=get_state(shadow,inst,name)
            if not state["first_seen_ts"]: state["first_seen_ts"]=iso(cycle)
            state["last_seen_ts"]=iso(cycle)
            state["total_cycles"]=int(state.get("total_cycles") or 0)+1
            if inside: state["operational_cycles"]=int(state.get("operational_cycles") or 0)+1
            if inside and activity[inst]["usable"]:
                state["observed_samples"]=int(state.get("observed_samples") or 0)+1

            sent=int(probe.get("sent") or 0); received=int(probe.get("received") or 0); loss_pct=probe.get("loss_pct")
            path_suppressed=bool(resolved and effective_path_state in ("DEGRADED","REDE","RECOVERY") and received < sent)
            if path_suppressed:
                state["path_suppressed_cycles"]=int(state.get("path_suppressed_cycles") or 0)+1

            if resolved:
                if received >= max(1,sent):
                    state["ping_fail_count"]=0; state["ping_success_count"]=int(state.get("ping_success_count") or 0)+1
                elif path_suppressed:
                    state["ping_success_count"]=0
                elif received==0:
                    state["ping_fail_count"]=int(state.get("ping_fail_count") or 0)+1; state["ping_success_count"]=0
                else:
                    state["ping_success_count"]=0
            else:
                state["ping_fail_count"]=0; state["ping_success_count"]=0

            ce=calls[inst].get(norm(name),{"rows":0,"snapshots":0,"last_ts":None,"fingerprint":None})
            occ=g.get("occupancy_pct"); occ_float=None if occ is None else float(occ or 0)
            occupancy_active=occ_float is not None and occ_float>0
            current_fp=ce.get("fingerprint"); previous_fp=state.get("last_call_fingerprint")
            new_call_evidence=bool(current_fp and (previous_fp is None or current_fp!=previous_fp))
            if occupancy_active:
                state["last_active_ts"]=iso(cycle); state["active_samples"]=int(state.get("active_samples") or 0)+1
            elif new_call_evidence and ce.get("last_ts"):
                state["last_active_ts"]=ce["last_ts"]; state["active_samples"]=int(state.get("active_samples") or 0)+1
            if new_call_evidence and ce.get("last_ts"):
                state["last_call_evidence_ts"]=ce["last_ts"]; state["last_call_fingerprint"]=current_fp
            elif current_fp and previous_fp is None:
                state["last_call_fingerprint"]=current_fp

            last_active_dt=parse_dt(state.get("last_active_ts"))
            idle_anchor=last_active_dt
            if inside:
                start_today=op_window_start_for(cycle)
                if idle_anchor is None or idle_anchor < start_today:
                    idle_anchor=start_today
            idle_min=max(0.0,(cycle-idle_anchor).total_seconds()/60.0) if idle_anchor else None
            first_seen_dt=parse_dt(state.get("first_seen_ts"))
            observed_min=max(0.0,(cycle-first_seen_dt).total_seconds()/60.0) if first_seen_dt else 0.0

            total_busy=inst_metrics[inst]["total_busy"]; busy_now=max(0.0,float(g.get("busy") or 0))
            traffic_share_pct=100.0*busy_now/total_busy if total_busy>0 else 0.0
            call_total=inst_metrics[inst]["call_total"]
            call_share_pct=100.0*int(ce.get("rows") or 0)/call_total if call_total>0 else 0.0
            peers=[pg for pi,pg in dynamic if pi==inst and norm(pg["entity"])!=norm(name)]
            peer_occs=[float(pg.get("occupancy_pct") or 0) for pg in peers if pg.get("occupancy_pct") is not None]
            peer_active_ratio=sum(1 for x in peer_occs if x>0)/len(peer_occs) if peer_occs else 0.0
            peer_median_occ=statistics.median(peer_occs) if peer_occs else 0.0
            peers_expect_traffic=activity[inst]["active"] and peer_active_ratio>=PEER_ACTIVE_RATIO_MIN and peer_median_occ>=PEER_ACTIVE_OCC_MIN

            baseline=baseline_for_gateway(shadow,inst,name,cycle)
            warn_min,crit_min=silence_thresholds(baseline)

            if resolved is None:
                reachability="NC"
            elif path_suppressed:
                reachability="PATH_UNCERTAIN"
            elif loss_pct==0:
                reachability="OK"
            elif loss_pct is not None and 0<loss_pct<100:
                reachability="INTERMITTENT"
            elif state["ping_fail_count"]>=PING_FAIL_CRITICAL:
                reachability="DOWN"
            else:
                reachability="INTERMITTENT"

            if not inside:
                service_state="PASSIVE"
            elif occupancy_active or new_call_evidence:
                service_state="ACTIVE"
            elif not activity[inst]["usable"]:
                service_state="UNKNOWN"
            elif not activity[inst]["active"]:
                service_state="IDLE_EXPECTED"
            elif baseline["class"] in ("ACTIVE_RECURRENT","ACTIVE_OCCASIONAL") and crit_min is not None and idle_min is not None and idle_min>=crit_min:
                service_state="SILENT_CRITICAL"
            elif baseline["class"] in ("ACTIVE_RECURRENT","ACTIVE_OCCASIONAL") and warn_min is not None and idle_min is not None and idle_min>=warn_min:
                service_state="SILENT_WARN"
            elif peers_expect_traffic and baseline["class"] in ("STANDBY_CANDIDATE","LEARNING") and observed_min>=LEARNING_MAX_MIN:
                service_state="SILENT_SUSPECT"
            elif baseline["class"]=="LEARNING":
                service_state="LEARNING"
            else:
                service_state="IDLE_RECENT"

            if reachability=="PATH_UNCERTAIN":
                raw_status="PATH_DEGRADED"
                reason=f"ICMP inconclusivo por path={effective_path_state}; falha individual suprimida. Serviço={service_state}."
            elif reachability=="NC":
                raw_status="OPERACIONAL_ICMP_NC" if service_state=="ACTIVE" else "PING_NC"
                reason="Há tráfego atual, porém ICMP não está observável." if service_state=="ACTIVE" else "Gateway LIVE sem IP/DNS observável."
            elif reachability=="DOWN":
                raw_status="SERVICE_ACTIVE_ICMP_DOWN" if service_state=="ACTIVE" else "PING_DOWN"
                reason=("ICMP persistente DOWN, mas serviço/tráfego segue ativo; investigar plano de gestão/rede." if service_state=="ACTIVE"
                        else f"Sem resposta ICMP por {state['ping_fail_count']} ciclos confiáveis consecutivos.")
            elif reachability=="INTERMITTENT":
                raw_status="OPERACIONAL_ICMP_INTERMITTENT" if service_state=="ACTIVE" else "PING_INTERMITTENT"
                reason=f"ICMP intermitente (perda={loss_pct}%)." if service_state!="ACTIVE" else f"Serviço ativo, porém ICMP intermitente (perda={loss_pct}%)."
            elif state.get("stable_status") in ("PING_DOWN","SERVICE_ACTIVE_ICMP_DOWN") and int(state.get("ping_success_count") or 0)<PING_RECOVERY_SUCCESS_CYCLES:
                raw_status="PING_RECOVERING"; reason="ICMP voltou; aguardando confirmação sucessiva."
            elif service_state=="SILENT_CRITICAL":
                raw_status="NO_TRAFFIC_CRITICAL"; reason=f"Silêncio funcional {idle_min:.1f} min; baseline={baseline['class']} (crit={crit_min:.0f} min)."
            elif service_state=="SILENT_WARN":
                raw_status="NO_TRAFFIC_WARN"; reason=f"Silêncio funcional {idle_min:.1f} min; baseline={baseline['class']} (atenção={warn_min:.0f} min)."
            elif service_state=="SILENT_SUSPECT":
                raw_status="NO_TRAFFIC_OBSERVE"; reason=(f"Pares ativos ({peer_active_ratio*100:.0f}%; mediana {peer_median_occ:.1f}%), "
                    f"sem tráfego próprio; baseline={baseline['class']}. Observação Shadow, não alerta operacional.")
            elif service_state=="ACTIVE":
                raw_status="OPERACIONAL"; reason=f"ICMP OK e tráfego ativo. Baseline={baseline['class']}."
            elif service_state=="LEARNING":
                raw_status="LEARNING"; reason=f"ICMP OK; baseline em formação ({baseline['sample_count']} amostras elegíveis / {baseline['span_min']:.0f} min)."
            elif not inside:
                raw_status="REACHABLE"; reason="ICMP OK 24x7; fora da janela operacional o tráfego não é exigido."
            else:
                raw_status="REACHABLE"; reason=f"ICMP OK; sem falha funcional confirmada. Baseline={baseline['class']}."

            stable_status,transitioned,candidate,candidate_count,candidate_required = apply_hysteresis(
                shadow,state,raw_status,reason,baseline["class"],effective_path_state,cycle,path_suppressed=path_suppressed)
            state["raw_status"]=raw_status
            if effective_path_state=="OK" and stable_status!="PATH_DEGRADED":
                state["last_reliable_status"]=stable_status
            raw_sev=severity(raw_status); stable_sev=severity(stable_status)
            if raw_sev=="WARN": state["raw_warn_cycles"]=int(state.get("raw_warn_cycles") or 0)+1
            if raw_sev=="CRITICAL": state["raw_critical_cycles"]=int(state.get("raw_critical_cycles") or 0)+1
            if stable_sev=="WARN": state["stable_warn_cycles"]=int(state.get("stable_warn_cycles") or 0)+1
            if stable_sev=="CRITICAL": state["stable_critical_cycles"]=int(state.get("stable_critical_cycles") or 0)+1
            save_state(shadow,state)

            # Camada semântica é calculada antes da projeção de alertas Shadow.
            # O estado de máquina (raw/stable) permanece intocado e auditável.
            semantic_ctx = semantic_context_for(semantic_idx, inst, name)
            semantic = semantic_overlay(
                semantic_ctx, service_state=service_state, stable_status=stable_status,
                reachability=reachability, occupancy_active=occupancy_active
            )

            # Alertas de máquina são preservados integralmente em machine_alerts.
            # Somente NO_TRAFFIC funcional pode ser suprimido da projeção executiva Shadow
            # quando Ground Truth confirma standby/parada planejada/cobertura parcial.
            if stable_sev in ("WARN","CRITICAL") and stable_status!="PATH_DEGRADED":
                machine_alert={"instancia":inst,"entity":name,"severity":stable_sev,"type":stable_status,
                    "message":f"{name} ({resolved or 'IP/DNS N/C'}): {reason} [raw={raw_status} stable={stable_status} baseline={baseline['class']}]"}
                machine_alerts.append(machine_alert)
                suppress_semantic = (
                    stable_status in ("NO_TRAFFIC_WARN","NO_TRAFFIC_CRITICAL","NO_TRAFFIC_OBSERVE")
                    and semantic.get("semantic_status") in (
                        "STANDBY_EXPECTED","PLANNED_INACTIVE","PARTIAL_VISIBILITY","OPERATIONAL_PARTIAL_VISIBILITY"
                    )
                )
                if suppress_semantic:
                    semantic_suppressions.append({**machine_alert,
                        "semantic_status":semantic.get("semantic_status"),
                        "reason":"Ground Truth torna ausência de tráfego esperada ou observacionalmente incompleta; machine alert preservado, projeção executiva Shadow suprimida."})
                else:
                    alerts.append(machine_alert)

            if semantic.get("semantic_status") == "FAULT_CONFIRMED_CONTEXT":
                semantic_findings.append({
                    "instancia":inst,"entity":name,"severity":"CONTEXT_CONFIRMED",
                    "type":"FAULT_CONFIRMED_CONTEXT","message":semantic.get("note"),
                    "evidence_source":semantic.get("evidence_source"),
                    "evidence_confidence":semantic.get("evidence_confidence"),
                    "machine_status":stable_status,
                })

            account_daily(shadow,cycle,inst,name,inside=inside,path_suppressed=path_suppressed,reachability=reachability,
                          service_state=service_state,raw_status=raw_status,stable_status=stable_status,
                          occupancy_pct=occ_float,traffic_share_pct=traffic_share_pct,call_share_pct=call_share_pct,
                          transitioned=transitioned)
            acct=accounting_today(shadow,cycle,inst,name)

            inventory_fresh=bool(inventory[inst]["fresh"])
            display_status="PATH_DEGRADED" if path_suppressed else stable_status
            # Recalcula somente se Path Quorum alterou display_status; sem alterar stable_status.
            if display_status != stable_status:
                semantic = semantic_overlay(
                    semantic_ctx, service_state=service_state, stable_status=display_status,
                    reachability=reachability, occupancy_active=occupancy_active
                )
            sample={
                "instancia":inst,"entity":name,"config_name":ref_item.get("canonical") or name,"aliases":ref_item.get("aliases") or [],
                "role":"GW","ip":resolved,"ip_source":ip_source,
                "semantic_status":semantic["semantic_status"],"operational_role":semantic["operational_role"],
                "lifecycle_state":semantic["lifecycle_state"],"traffic_expectation":semantic["traffic_expectation"],
                "coverage_level":semantic["coverage_level"],"semantic_evidence_source":semantic["evidence_source"],
                "semantic_evidence_confidence":semantic["evidence_confidence"],"semantic_note":semantic["note"],
                "inventory_ts":inventory[inst]["ts"],"inventory_age_s":round(inventory[inst]["age_s"],1) if inventory[inst]["age_s"] is not None else None,
                "inventory_fresh":inventory_fresh,"ping_ok":received>0 if resolved else None,
                "probes_sent":sent if resolved else None,"probes_received":received if resolved else None,"loss_pct":loss_pct if resolved else None,
                "rtt_ms":probe.get("rtt_p50_ms") if resolved else None,"rtt_p50_ms":probe.get("rtt_p50_ms") if resolved else None,
                "rtt_p95_ms":probe.get("rtt_p95_ms") if resolved else None,"rtt_max_ms":probe.get("rtt_max_ms") if resolved else None,
                "rtt_jitter_ms":probe.get("jitter_ms") if resolved else None,"consecutive_ping_fail":state["ping_fail_count"],
                "consecutive_ping_success":state["ping_success_count"],"reachability_state":reachability,"service_state":service_state,
                "found_api":bool(inventory[inst]["gateways"]),"occupancy_pct":occ_float,"busy":g.get("busy"),"total":g.get("total"),
                "traffic_share_pct":round(traffic_share_pct,3),"call_share_pct":round(call_share_pct,3),
                "peer_active_ratio":round(peer_active_ratio,4),"peer_median_occupancy":round(peer_median_occ,3),
                "call_evidence_rows":ce.get("rows",0),"call_evidence_snapshots":ce.get("snapshots",0),
                "last_call_evidence_ts":state.get("last_call_evidence_ts"),"new_call_evidence":new_call_evidence,"active_now":occupancy_active,
                "last_active_ts":state.get("last_active_ts"),"idle_minutes":round(idle_min,2) if idle_min is not None else None,
                "observed_minutes":round(observed_min,2),
                "baseline_class":baseline["class"],"baseline_ready":baseline["ready"],"baseline_samples":baseline["sample_count"],
                "baseline_active_samples":baseline["active_samples"],"baseline_active_ratio":round(baseline["active_ratio"]*100,2),
                "baseline_span_min":round(baseline["span_min"],1),"baseline_occ_median":baseline["occupancy_median"],
                "baseline_occ_p95":baseline["occupancy_p95"],"baseline_traffic_share_median":baseline["traffic_share_median"],
                "baseline_call_share_median":baseline["call_share_median"],"silence_warn_min":warn_min,"silence_critical_min":crit_min,
                "instance_active":activity[inst]["active"],"instance_activity_age_s":activity[inst]["age_s"],"operational_window":inside,
                "path_state":effective_path_state,"path_suppressed":path_suppressed,
                "reference_instance":ref_item.get("reference_instance"),"reference_conflict":bool(ref_item.get("reference_instance") and str(ref_item.get("reference_instance"))!=inst),
                "raw_status":raw_status,"stable_status":stable_status,"status":display_status,"health_status":stable_status,"severity":stable_sev,
                "hysteresis":{"candidate":candidate,"count":candidate_count,"required":candidate_required,"transitioned":transitioned},
                "accounting_today":acct,"reason":reason,"detail":reason,
            }
            samples.append(sample)

            shadow.execute(
                """INSERT INTO gateway_health_sample_v5(
                ts,instancia,entity,ip,ip_source,inventory_ts,inventory_age_s,inventory_fresh,
                occupancy_pct,busy,total,probes_sent,probes_received,loss_pct,rtt_p50_ms,rtt_p95_ms,rtt_max_ms,rtt_jitter_ms,
                ping_fail_count,ping_success_count,reachability_state,service_state,raw_status,stable_status,display_status,severity,
                hysteresis_candidate,hysteresis_count,hysteresis_required,path_suppressed,
                call_evidence_rows,call_evidence_snapshots,last_call_evidence_ts,call_fingerprint,instance_active,operational_window,
                traffic_share_pct,call_share_pct,peer_active_ratio,peer_median_occupancy,
                baseline_class,baseline_ready,baseline_samples,baseline_active_samples,baseline_active_ratio,baseline_span_min,
                baseline_occ_median,baseline_occ_p95,baseline_traffic_share_median,baseline_call_share_median,
                silence_warn_min,silence_critical_min,path_state,reason)
                VALUES("""+",".join("?" for _ in range(54))+""" )""",
                (iso(cycle),inst,name,resolved,ip_source,inventory[inst]["ts"],inventory[inst]["age_s"],int(inventory_fresh),
                 occ_float,g.get("busy"),g.get("total"),sent if resolved else None,received if resolved else None,loss_pct if resolved else None,
                 probe.get("rtt_p50_ms") if resolved else None,probe.get("rtt_p95_ms") if resolved else None,probe.get("rtt_max_ms") if resolved else None,
                 probe.get("jitter_ms") if resolved else None,state["ping_fail_count"],state["ping_success_count"],reachability,service_state,
                 raw_status,stable_status,display_status,stable_sev,candidate,candidate_count,candidate_required,int(path_suppressed),
                 ce.get("rows",0),ce.get("snapshots",0),state.get("last_call_evidence_ts"),state.get("last_call_fingerprint"),
                 int(activity[inst]["active"]),int(inside),round(traffic_share_pct,3),round(call_share_pct,3),round(peer_active_ratio,4),round(peer_median_occ,3),
                 baseline["class"],int(baseline["ready"]),baseline["sample_count"],baseline["active_samples"],round(baseline["active_ratio"],6),round(baseline["span_min"],2),
                 baseline["occupancy_median"],baseline["occupancy_p95"],baseline["traffic_share_median"],baseline["call_share_median"],
                 warn_min,crit_min,effective_path_state,reason)
            )

        persisted = commit_shadow_with_retry(shadow, context="cycle", allow_skip=True)
        if not persisted:
            if args.once:
                shadow.close()
                return 4
            elapsed=max(0.0,(now()-cycle).total_seconds())
            time.sleep(max(0.2,CADENCE_S-elapsed))
            continue

        instance_summary={}
        for inst in INSTANCES:
            gs=[g for g in samples if g["instancia"]==inst]
            instance_summary[inst]={
                "inventory_ts":inventory[inst]["ts"],"inventory_age_s":inventory[inst]["age_s"],"inventory_fresh":inventory[inst]["fresh"],
                "gateway_count":len(inventory[inst]["gateways"]) or len(gs),"monitored_count":len(gs),
                "ping_resolved_count":sum(g["ip"] is not None for g in gs),"ping_ok_count":sum(g["reachability_state"]=="OK" for g in gs),
                "ping_intermit_count":sum(g["reachability_state"]=="INTERMITTENT" for g in gs),"ping_down_count":sum(g["reachability_state"]=="DOWN" for g in gs),
                "ping_nc_count":sum(g["reachability_state"]=="NC" for g in gs),"service_active_count":sum(g["service_state"]=="ACTIVE" for g in gs),
                "functional_silence_count":sum(g["service_state"] in ("SILENT_WARN","SILENT_CRITICAL","SILENT_SUSPECT") for g in gs),
                "baseline_ready_count":sum(bool(g["baseline_ready"]) for g in gs),"instance_active":activity[inst]["active"],
                "queue_activity_proxy":activity[inst]["calls_now_proxy"],
            }

        payload={
            "tool":"toi_gateway_intelligence_shadow_v0_2_4_1","schema":"gateway_intelligence_v0_2_4_1","generated_at":iso(cycle),
            "policy_status":"SHADOW_ONLY_PROVISIONAL_THRESHOLDS","inventory_mode":"DYNAMIC_PORTAL_GATEWAYLIST_LAST_KNOWN_GOOD",
            "operational_window":{"inside":inside,"start":OP_START,"end":OP_END,"icmp_24x7":True},
            "connectivity":{"status":effective_path_state,"raw_status":raw_path_state,"sentinel_ip":SENTINEL_IP,"sentinel_ping_ok":path["sentinel_ok"],
                "gateway_ping_fail_ratio":round(path["fail_ratio"],4),"gateway_ping_partial_ratio":round(path["partial_ratio"],4),
                "gateway_ping_impairment_ratio":round(path.get("impairment_ratio",0.0),4),"impaired_gateways":path.get("impaired",0),
                "affected_instances":path["affected_instances"],"severely_affected_instances":path.get("severely_affected_instances",0),
                "recovery_grace_remaining":recovery_grace},
            "baseline_policy":{"lookback_days":BASELINE_LOOKBACK_DAYS,"min_samples":BASELINE_MIN_SAMPLES,"min_span_min":BASELINE_MIN_SPAN_MIN,
                "recurrent_ratio":BASELINE_RECURRENT_RATIO,"occasional_ratio":BASELINE_OCCASIONAL_RATIO,"refresh_s":BASELINE_REFRESH_S,
                "instance_activity_gate":False,
                "eligibility":"operational_window=1 + path_state=OK + inventory_fresh=1; instance_active is context only",
                "classes":["LEARNING","ACTIVE_RECURRENT","ACTIVE_OCCASIONAL","STANDBY_CANDIDATE"]},
            "semantic_policy":{"mode":"SHADOW_HUMAN_GROUND_TRUTH","context_version":semantic_doc.get("version"),
                "context_sha256":semantic_hash,"changes_machine_status":False,"generates_alerts":False,
                "filters_shadow_functional_alert_projection":True,"machine_alerts_preserved":True},
            "hysteresis_policy":{"escalate_confirm_cycles":HYST_ESCALATE_CONFIRM_CYCLES,"recovery_confirm_cycles":HYST_RECOVERY_CONFIRM_CYCLES,
                "same_severity_confirm_cycles":HYST_SAME_SEVERITY_CONFIRM_CYCLES,"alerts_use_stable_status":True},
            "instances":instance_summary,"gateways":samples,
            "alerts":sorted(alerts,key=lambda a:(0 if a["severity"]=="CRITICAL" else 1,a["instancia"],norm(a["entity"]))),
            "machine_alerts":sorted(machine_alerts,key=lambda a:(0 if a["severity"]=="CRITICAL" else 1,a["instancia"],norm(a["entity"]))),
            "semantic_suppressions":semantic_suppressions,"semantic_findings":semantic_findings,
            "inventory_notes":audit_notes,
            "shadow_persistence":{
                "status":"OK","journal_mode":"DELETE","busy_timeout_ms":SHADOW_BUSY_TIMEOUT_MS,
                "lock_failures_total":SHADOW_LOCK_FAILURES_TOTAL,
                "skipped_cycles_total":SHADOW_LOCK_SKIPPED_CYCLES,
                "last_lock_error":SHADOW_LOCK_LAST_ERROR,
                "last_commit_success_ts":SHADOW_LOCK_LAST_SUCCESS_TS,
            },
            "live_read_path":{
                "mode":"ROWID_BOUNDED_RECENT_PAYLOAD_G3",
                "queue_watermark":live_tail.queue_last_rowid,
                "lastcalls_watermark":live_tail.call_last_rowid,
                "infra_watermark":live_tail.infra_last_rowid,
                "queue_rows_read_cycle":live_tail_stats.get("queue_rows",0),
                "lastcalls_rows_read_cycle":live_tail_stats.get("lastcalls_rows",0),
                "lastcalls_metadata_read_cycle":live_tail.calls_metadata_read,
                "lastcalls_stale_skipped_cycle":live_tail.calls_stale_skipped,
                "lastcalls_backlog_capped":live_tail.calls_backlog_capped,
                "infra_rows_read_cycle":live_tail_stats.get("infra",{}).get("rows",0),
                "infra_bootstrap_cycle":live_tail_stats.get("infra",{}).get("bootstrap",False),
                "infra_bootstrap_completed":live_tail.infra_bootstrap_completed,
                "infra_bootstrap_scans_total":live_tail.infra_bootstrap_scans_total,
                "infra_missing_instances":list(live_tail.infra_missing_instances),
                "gateway_inventory_cached_instances":sorted(live_tail.gateway_latest),
                "cached_lastcalls_rows":len(live_tail.call_rows),
                "live_db_read_only":True,
            },
            "feature_contract":{
                "icmp":["loss_pct","rtt_p50_ms","rtt_p95_ms","rtt_max_ms","rtt_jitter_ms","consecutive_ping_fail"],
                "service":["service_state","idle_minutes","traffic_share_pct","call_share_pct","peer_active_ratio","peer_median_occupancy"],
                "baseline":["baseline_class","baseline_ready","baseline_active_ratio","baseline_occ_median","baseline_occ_p95","baseline_traffic_share_median"],
                "hysteresis":["raw_status","stable_status","hysteresis","path_suppressed"],
                "accounting":["accounting_today"],
                "context":["path_state","instance_active","operational_window","inventory_fresh"],
                "semantic":["semantic_status","operational_role","lifecycle_state","traffic_expectation","coverage_level",
                            "semantic_evidence_source","semantic_evidence_confidence"]},
            "note":"Shadow observacional. v0.2.4.1 remove instance_active como gate do baseline porque cobertura de filas pode ser parcial. Papel/lifecycle são contexto humano versionado; raw/stable e machine_alerts são preservados. Apenas a projeção executiva Shadow de NO_TRAFFIC pode ser suprimida quando Ground Truth comprova standby/parada planejada/cobertura parcial. Não altera diagnóstico/recomendação/chamado/ação."
        }
        publish_snapshot_atomic(payload)

        counts={i:sum(1 for g in samples if g["instancia"]==i) for i in INSTANCES}
        resolved_n=sum(g["ip"] is not None for g in samples); icmp_ok_n=sum(g["reachability_state"]=="OK" for g in samples)
        inter_n=sum(g["reachability_state"]=="INTERMITTENT" for g in samples); down_n=sum(g["reachability_state"]=="DOWN" for g in samples)
        nc_n=sum(g["reachability_state"]=="NC" for g in samples); ready_n=sum(bool(g["baseline_ready"]) for g in samples)
        warn_n=sum(a["severity"]=="WARN" for a in alerts); crit_n=sum(a["severity"]=="CRITICAL" for a in alerts)
        candidates=sum(bool(g.get("hysteresis",{}).get("candidate")) for g in samples)
        print(f"[{cycle:%H:%M:%S}] path={effective_path_state} | inventário={counts} | ip={resolved_n}/{len(samples)} "
              f"icmp_ok={icmp_ok_n} inter={inter_n} down={down_n} nc={nc_n} | baseline={ready_n}/{len(samples)} ready | "
              f"hyst_pending={candidates} warn={warn_n} crit={crit_n}")
        if audit_notes:
            print(f"  [AUDIT] {len(audit_notes)} divergência(s) referência x LIVE; sem impacto automático no health.")
        if args.once: break
        elapsed=max(0.0,(now()-cycle).total_seconds()); time.sleep(max(0.2,CADENCE_S-elapsed))

    shadow.close(); return 0


if __name__ == "__main__":
    raise SystemExit(main())
