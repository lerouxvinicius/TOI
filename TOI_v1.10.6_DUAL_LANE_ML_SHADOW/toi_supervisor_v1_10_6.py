#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""TOI v1.10.6 — Runtime Lifecycle Hardening.

Escopo desta revisão:
- NÃO altera a aquisição congelada nem seus algoritmos;
- desacopla falhas de Collector, Dashboard e ML Shadow;
- reinicia somente o componente que falhou;
- detecta Collector vivo com writer/fast-path sem avanço no SQLite LIVE;
- evita restart storm com budget/cooldown;
- publica estado/forense somente em APP/runtime (fora do SQLite LIVE).

O arquivo mantém o nome operacional histórico ``toi_supervisor_v1_10_6.py`` para que
``INICIAR_TOI_V1_10_6.bat`` continue sendo o launcher oficial.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import traceback
from collections import deque
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen

APP = Path(__file__).resolve().parent
RUNTIME_DIR = APP / "runtime"
STATUS_FILE = RUNTIME_DIR / "toi_supervisor_runtime_status_v1.json"
EVENT_FILE = RUNTIME_DIR / "toi_supervisor_runtime_events_v1.jsonl"
EVENT_ROTATE_BYTES = 5 * 1024 * 1024

DEFAULT_CHECK_S = 10.0
DEFAULT_QUEUE_STALE_RESTART_S = 150.0
DEFAULT_QUEUE_STALE_CONSECUTIVE = 3
DEFAULT_DASHBOARD_FAIL_CONSECUTIVE = 3
DEFAULT_RESTART_COOLDOWN_S = 30.0
DEFAULT_RESTART_WINDOW_S = 600.0
DEFAULT_MAX_RESTARTS_WINDOW = 3
DEFAULT_POST_RESTART_GRACE_S = 75.0


def now_iso():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def age_seconds(ts):
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts))
        if dt.tzinfo is None:
            dt = dt.astimezone()
        return round(max(0.0, (datetime.now().astimezone() - dt).total_seconds()), 1)
    except Exception:
        return None


def atomic_json(path: Path, doc):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def append_event(kind, component, message, **extra):
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    try:
        if EVENT_FILE.exists() and EVENT_FILE.stat().st_size >= EVENT_ROTATE_BYTES:
            old = EVENT_FILE.with_suffix(".jsonl.1")
            try:
                if old.exists():
                    old.unlink()
                os.replace(EVENT_FILE, old)
            except OSError:
                pass
        row = {
            "ts": now_iso(),
            "kind": str(kind),
            "component": str(component),
            "message": str(message),
            **extra,
        }
        with EVENT_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        # Nunca deixar a telemetria do próprio supervisor derrubar o supervisor.
        pass


def sqlite_seed(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return False
    print(f"[SEED] Copiando histórico com SQLite backup:\n       {src}\n    -> {dst}")
    source = sqlite3.connect(str(src), timeout=10)
    target = sqlite3.connect(str(dst), timeout=10)
    try:
        source.backup(target)
        target.commit()
        target.execute("PRAGMA journal_mode=WAL")
        target.commit()
    finally:
        target.close()
        source.close()
    return True


def _ro_connect(db: Path):
    # Supervisor nunca escreve no LIVE DB.
    uri = db.resolve().as_uri() + "?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=2)
    con.row_factory = sqlite3.Row
    return con


def _table_exists(con, name):
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (str(name),)
    ).fetchone()
    return bool(row)


def _last_row(con, table, where_sql="", params=()):
    if not _table_exists(con, table):
        return None
    sql = f"SELECT rowid,ts FROM {table}"
    if where_sql:
        sql += " WHERE " + where_sql
    sql += " ORDER BY rowid DESC LIMIT 1"
    row = con.execute(sql, tuple(params)).fetchone()
    return dict(row) if row else None


def _last_scheduler(con):
    # Atividade recente do fast-path ajuda a diferenciar thread/lane parada de
    # fonte externa que continua gerando ERROR/COOLDOWN.
    candidates = []
    if _table_exists(con, "scheduler_health"):
        row = con.execute(
            """SELECT rowid,ts,state,source,codigo,detail
               FROM scheduler_health
               WHERE source IN ('queue','deadline_queue')
               ORDER BY rowid DESC LIMIT 1"""
        ).fetchone()
        if row:
            candidates.append(dict(row))
    if _table_exists(con, "dual_lane_health"):
        row = con.execute(
            """SELECT rowid,ts,result AS state,'dual_lane' AS source,codigo,detail
               FROM dual_lane_health ORDER BY rowid DESC LIMIT 1"""
        ).fetchone()
        if row:
            candidates.append(dict(row))
    if not candidates:
        return None
    candidates.sort(key=lambda x: int(x.get("rowid") or 0), reverse=True)
    return candidates[0]


def _queue_key(instancia, codigo):
    return f"{str(instancia)}:{str(codigo)}"


def load_watch_queues(config_path: Path, policy_path: Path | None = None, default_stale_s=150.0):
    """Carrega as filas esperadas sem alterar a configuração congelada.

    O watchdog usa a lista somente como contrato de observabilidade. O limite nunca
    fica abaixo de 150s nesta revisão para evitar reinício por jitter transitório.
    """
    out = []
    try:
        cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
    except Exception:
        cfg = {}
    try:
        policy = json.loads(Path(policy_path).read_text(encoding="utf-8")) if policy_path else {}
    except Exception:
        policy = {}
    inst_policy = (policy or {}).get("instancias") or {}
    for q in (cfg.get("queues") or []):
        inst = str(q.get("instancia") or "").strip()
        code = str(q.get("codigo") or "").strip()
        if not inst or not code:
            continue
        qp = (((inst_policy.get(inst) or {}).get("queues") or {}).get(code) or {})
        max_age = qp.get("max_age_s")
        try:
            semantic_limit = max(float(default_stale_s), float(max_age) * 2.5) if max_age is not None else float(default_stale_s)
        except Exception:
            semantic_limit = float(default_stale_s)
        out.append({
            "instancia": inst,
            "codigo": code,
            "nome": str(q.get("nome") or code),
            "critical": bool(q.get("full_detail")),
            "stale_restart_s": round(semantic_limit, 1),
        })
    return out


def _last_queue_entity(con, instancia, codigo):
    if not _table_exists(con, "queue_snapshot"):
        return None
    # ix_queue_snapshot(instancia,codigo,ts) torna esta busca bounded/indexada.
    row = con.execute(
        """SELECT rowid,ts FROM queue_snapshot
           WHERE instancia=? AND codigo=?
           ORDER BY ts DESC LIMIT 1""",
        (str(instancia), str(codigo)),
    ).fetchone()
    return dict(row) if row else None


def _last_queue_scheduler_entity(con, instancia, codigo):
    if not _table_exists(con, "scheduler_health"):
        return None
    row = con.execute(
        """SELECT rowid,ts,state,source,codigo,detail
           FROM scheduler_health
           WHERE instancia=? AND codigo=? AND source IN ('queue','deadline_queue')
           ORDER BY ts DESC LIMIT 1""",
        (str(instancia), str(codigo)),
    ).fetchone()
    return dict(row) if row else None


def probe_live(db: Path, expected_queues=None):
    out = {
        "db_exists": db.exists(),
        "checked_at": now_iso(),
        "queue": None,
        "infra": None,
        "stuck": None,
        "dialer": None,
        "queue_scheduler": None,
        "queue_entities": {},
        "error": None,
    }
    if not db.exists():
        return out
    con = None
    try:
        con = _ro_connect(db)
        for key, table in (
            ("queue", "queue_snapshot"),
            ("infra", "infra_snapshot"),
            ("stuck", "stuck_channels_snapshot"),
            ("dialer", "dialer_snapshot"),
        ):
            row = _last_row(con, table)
            if row:
                row["age_s"] = age_seconds(row.get("ts"))
            out[key] = row
        sched = _last_scheduler(con)
        if sched:
            sched["age_s"] = age_seconds(sched.get("ts"))
        out["queue_scheduler"] = sched
        for spec in (expected_queues or []):
            inst = str(spec.get("instancia") or "")
            code = str(spec.get("codigo") or "")
            key = _queue_key(inst, code)
            row = _last_queue_entity(con, inst, code)
            if row:
                row["age_s"] = age_seconds(row.get("ts"))
            sched_row = _last_queue_scheduler_entity(con, inst, code)
            if sched_row:
                sched_row["age_s"] = age_seconds(sched_row.get("ts"))
            out["queue_entities"][key] = {
                **dict(spec),
                "latest": row,
                "scheduler": sched_row,
            }
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    return out


def latest_age(db: Path):
    p = probe_live(db)
    q = p.get("queue") or {}
    return q.get("age_s")


class RestartBudget:
    def __init__(self, max_restarts=DEFAULT_MAX_RESTARTS_WINDOW, window_s=DEFAULT_RESTART_WINDOW_S):
        self.max_restarts = max(1, int(max_restarts))
        self.window_s = max(60.0, float(window_s))
        self._events = deque()

    def _prune(self, now=None):
        now = time.monotonic() if now is None else float(now)
        while self._events and now - self._events[0] > self.window_s:
            self._events.popleft()
        return now

    def allow(self, now=None):
        self._prune(now)
        return len(self._events) < self.max_restarts

    def record(self, now=None):
        now = self._prune(now)
        self._events.append(now)

    def remaining(self, now=None):
        self._prune(now)
        return max(0, self.max_restarts - len(self._events))


class RuntimeSupervisor:
    def __init__(self, args):
        self.args = args
        self.db = Path(args.db).resolve()
        self.config = Path(args.config).resolve()
        self.port = int(args.port)
        self.freeze_marker = APP / "homologacao" / "acquisition_frozen_v1_10_6.json"
        self.stop_requested = False

        self.collector_cmd = [
            sys.executable, str(APP / "toi_live_collector_v1_10_6.py"),
            "--config", str(self.config), "--db", str(self.db),
        ]
        self.dashboard_cmd = [
            sys.executable, str(APP / "toi_dashboard_server_v1_10_6.py"),
            "--db", str(self.db), "--policy", str(APP / "toi_adaptive_policy_v1.json"),
            "--port", str(self.port),
        ]
        self.ml_cmd = [
            sys.executable, str(APP / "toi_ml_shadow_v1_10_6.py"),
            "--db", str(self.db), "--freeze-marker", str(self.freeze_marker),
        ]

        self.proc = {"collector": None, "dashboard": None, "ml": None}
        self.budget = {
            k: RestartBudget(args.max_restarts_window, args.restart_window_s)
            for k in self.proc
        }
        self.last_restart_mono = {k: 0.0 for k in self.proc}
        self.last_restart_reason = {k: None for k in self.proc}
        self.queue_stale_checks = 0
        self.dashboard_fail_checks = 0
        self.collector_grace_until = 0.0
        self.last_probe = None
        self.last_status_write = 0.0
        self.watch_queues = load_watch_queues(
            self.config, APP / "toi_adaptive_policy_v1.json", args.queue_stale_restart_s
        )
        self.queue_entity_stale_checks = {
            _queue_key(x["instancia"], x["codigo"]): 0 for x in self.watch_queues
        }
        self.systemic_stale_checks = 0

    def _spawn(self, component, cmd, reason="startup"):
        p = subprocess.Popen(cmd, cwd=str(APP))
        self.proc[component] = p
        self.last_restart_reason[component] = reason
        print(f"[SUPERVISOR] {component.upper()} PID {p.pid} ({reason})")
        append_event("START", component, reason, pid=p.pid)
        return p

    def _terminate(self, component, timeout_s=8.0):
        p = self.proc.get(component)
        if not p or p.poll() is not None:
            return
        print(f"[SUPERVISOR] Encerrando {component} PID {p.pid}...")
        try:
            p.terminate()
        except Exception:
            pass
        deadline = time.time() + max(1.0, timeout_s)
        while p.poll() is None and time.time() < deadline:
            time.sleep(0.2)
        if p.poll() is None:
            try:
                p.kill()
            except Exception:
                pass
        append_event("STOP", component, "terminated_by_supervisor", pid=p.pid, returncode=p.poll())

    def _restart(self, component, reason):
        now = time.monotonic()
        if now - self.last_restart_mono[component] < self.args.restart_cooldown_s:
            return False
        if not self.budget[component].allow(now):
            append_event(
                "RESTART_SUPPRESSED", component,
                "restart budget exceeded; aguardando janela de proteção",
                reason=reason,
                remaining=self.budget[component].remaining(now),
            )
            return False
        self.budget[component].record(now)
        self.last_restart_mono[component] = now
        self._terminate(component)
        time.sleep(1.0)
        cmd = {
            "collector": self.collector_cmd,
            "dashboard": self.dashboard_cmd,
            "ml": self.ml_cmd,
        }[component]
        self._spawn(component, cmd, reason=f"restart: {reason}")
        if component == "collector":
            self.collector_grace_until = time.monotonic() + self.args.post_restart_grace_s
            self.queue_stale_checks = 0
            self.systemic_stale_checks = 0
            for key in self.queue_entity_stale_checks:
                self.queue_entity_stale_checks[key] = 0
        return True

    def _dashboard_ok(self):
        try:
            with urlopen(f"http://127.0.0.1:{self.port}/api/health", timeout=2.5) as r:
                if r.status != 200:
                    return False
                doc = json.loads(r.read().decode("utf-8", errors="replace"))
                return bool(doc.get("ok"))
        except Exception:
            return False

    def start(self):
        self._spawn("collector", self.collector_cmd, "startup")
        # Dá tempo para autenticação e primeira amostra, mas não impede o dashboard
        # de subir se o Portal estiver temporariamente degradado.
        deadline = time.time() + 75
        while time.time() < deadline and not self.stop_requested:
            cp = self.proc["collector"]
            if cp.poll() is not None:
                self._restart("collector", f"exit during startup rc={cp.returncode}")
                deadline = time.time() + 75
            age = latest_age(self.db)
            if age is not None and age <= 35:
                print(f"[SUPERVISOR] Primeira amostra LIVE confirmada: idade {age:.1f}s")
                break
            time.sleep(2)
        self.collector_grace_until = time.monotonic() + self.args.post_restart_grace_s

        self._spawn("dashboard", self.dashboard_cmd, "startup")
        if self.freeze_marker.exists():
            self._spawn("ml", self.ml_cmd, "startup")
        else:
            print("[SUPERVISOR] ML Shadow aguardando homologação/freeze da aquisição.")
        print(f"[SUPERVISOR] URL: http://127.0.0.1:{self.port}")
        print("[SUPERVISOR] Watchdog ativo: processo + progresso real do writer LIVE.")
        print("CTRL+C encerra os processos supervisionados.")

    def _monitor_processes(self):
        for component in ("collector", "dashboard", "ml"):
            p = self.proc.get(component)
            if component == "ml" and p is None:
                if self.freeze_marker.exists():
                    self._restart("ml", "ML ausente com freeze presente")
                continue
            if p is not None and p.poll() is not None:
                rc = p.returncode
                self._restart(component, f"process exited rc={rc}")

    def _monitor_writer(self):
        probe = probe_live(self.db, self.watch_queues)
        self.last_probe = probe
        q = probe.get("queue") or {}
        q_age = q.get("age_s")
        if q_age is None:
            # Durante criação/semente do DB, não reiniciar agressivamente.
            return
        if time.monotonic() < self.collector_grace_until:
            self.queue_stale_checks = 0
            self.systemic_stale_checks = 0
            for key in self.queue_entity_stale_checks:
                self.queue_entity_stale_checks[key] = 0
            return

        # 1) Contrato global: nenhum queue_snapshot novo em todo o TOI.
        if q_age <= self.args.queue_stale_restart_s:
            self.queue_stale_checks = 0
        else:
            self.queue_stale_checks += 1
            sched = probe.get("queue_scheduler") or {}
            sched_age = sched.get("age_s")
            sched_state = str(sched.get("state") or "N/C")
            print(
                f"[WATCHDOG] Writer global de fila sem avanço há {q_age:.1f}s "
                f"({self.queue_stale_checks}/{self.args.queue_stale_consecutive}); "
                f"scheduler age={sched_age} state={sched_state}"
            )
            append_event(
                "STALE", "collector", "queue writer global sem avanço",
                queue_age_s=q_age,
                scheduler_age_s=sched_age,
                scheduler_state=sched_state,
                consecutive=self.queue_stale_checks,
                infra_age_s=((probe.get("infra") or {}).get("age_s")),
                stuck_age_s=((probe.get("stuck") or {}).get("age_s")),
                dialer_age_s=((probe.get("dialer") or {}).get("age_s")),
            )
            if self.queue_stale_checks >= self.args.queue_stale_consecutive:
                self._restart(
                    "collector",
                    f"LIVE queue writer global stale {q_age:.0f}s; scheduler={sched_state}/{sched_age}",
                )
                return

        # 2) Contrato por fila: evita o falso saudável em que UMA fila/lane congela
        # enquanto outras filas continuam inserindo no mesmo queue_snapshot.
        entities = probe.get("queue_entities") or {}
        stale = []
        worker_stalled = []
        fresh_error = []
        healthy_count = 0
        for key, info in entities.items():
            latest = info.get("latest") or {}
            age = latest.get("age_s")
            limit = float(info.get("stale_restart_s") or self.args.queue_stale_restart_s)
            sched = info.get("scheduler") or {}
            sched_age = sched.get("age_s")
            sched_state = str(sched.get("state") or "N/C").upper()
            if age is not None and age <= limit:
                self.queue_entity_stale_checks[key] = 0
                healthy_count += 1
                continue
            self.queue_entity_stale_checks[key] = self.queue_entity_stale_checks.get(key, 0) + 1
            item = {
                "key": key,
                "age_s": age,
                "limit_s": limit,
                "scheduler_age_s": sched_age,
                "scheduler_state": sched_state,
                "consecutive": self.queue_entity_stale_checks[key],
                "name": info.get("nome"),
            }
            stale.append(item)
            # Scheduler sem evento recente = thread/lane silenciosamente parada.
            scheduler_live_limit = max(90.0, self.args.check_s * 6.0)
            if sched_age is None or sched_age > scheduler_live_limit:
                worker_stalled.append(item)
            elif sched_state not in ("OK", "OVERRUN"):
                fresh_error.append(item)

        if not stale:
            self.systemic_stale_checks = 0
            return

        total = max(1, len(entities))
        stale_ratio = len(stale) / total
        persistent_stalled = [
            x for x in worker_stalled
            if x["consecutive"] >= self.args.queue_stale_consecutive
        ]
        systemic_persistent = all(
            x["consecutive"] >= self.args.queue_stale_consecutive for x in stale
        ) and stale_ratio >= self.args.queue_systemic_ratio

        append_event(
            "QUEUE_ENTITY_STALE", "collector",
            "uma ou mais filas configuradas estão sem snapshot novo",
            stale_count=len(stale), total_queues=total, stale_ratio=round(stale_ratio, 3),
            healthy_count=healthy_count, worker_stalled=worker_stalled[:8],
            fresh_scheduler_errors=fresh_error[:8], stale=stale[:12],
        )

        if persistent_stalled:
            # Thread/lane morta ou scheduler deixou de avançar: reinício do Collector
            # é a ação correta porque recria autenticação, sessões e todas as lanes.
            keys=','.join(x['key'] for x in persistent_stalled[:5])
            self._restart("collector", f"queue worker/lane stalled: {keys}")
            return

        if systemic_persistent:
            self.systemic_stale_checks += 1
            if self.systemic_stale_checks >= 1:
                keys=','.join(x['key'] for x in stale[:5])
                self._restart(
                    "collector",
                    f"systemic queue stale ratio={stale_ratio:.0%}; queues={keys}",
                )
                return
        else:
            self.systemic_stale_checks = 0

        # Uma fila isolada com scheduler vivo e ERROR/COOLDOWN é tratada como
        # degradação da fonte, não como morte do Collector. Reiniciar toda a coleta
        # repetidamente aqui esconderia a causa e poderia prejudicar filas saudáveis.
        for x in fresh_error[:3]:
            if x["consecutive"] == self.args.queue_stale_consecutive:
                print(
                    f"[WATCHDOG] {x['key']} stale, mas scheduler continua "
                    f"{x['scheduler_state']} — fonte/sessão específica sob observação."
                )

    def _monitor_dashboard(self):
        p = self.proc.get("dashboard")
        if not p or p.poll() is not None:
            return
        if self._dashboard_ok():
            self.dashboard_fail_checks = 0
            return
        self.dashboard_fail_checks += 1
        append_event(
            "HEALTH_FAIL", "dashboard", "api/health sem resposta válida",
            consecutive=self.dashboard_fail_checks,
        )
        if self.dashboard_fail_checks >= self.args.dashboard_fail_consecutive:
            if self._restart("dashboard", "api/health unresponsive"):
                self.dashboard_fail_checks = 0

    def _write_status(self):
        now = time.monotonic()
        if now - self.last_status_write < 5.0:
            return
        self.last_status_write = now
        doc = {
            "schema": "toi_supervisor_runtime_status_v1",
            "generated_at": now_iso(),
            "supervisor_pid": os.getpid(),
            "components": {},
            "watchdog": {
                "queue_stale_restart_s": self.args.queue_stale_restart_s,
                "queue_stale_checks": self.queue_stale_checks,
                "restart_cooldown_s": self.args.restart_cooldown_s,
                "restart_window_s": self.args.restart_window_s,
                "max_restarts_window": self.args.max_restarts_window,
                "queue_systemic_ratio": self.args.queue_systemic_ratio,
                "watched_queue_count": len(self.watch_queues),
            },
            "live_probe": self.last_probe,
        }
        for k, p in self.proc.items():
            doc["components"][k] = {
                "pid": p.pid if p else None,
                "running": bool(p and p.poll() is None),
                "returncode": None if not p else p.poll(),
                "last_restart_reason": self.last_restart_reason[k],
                "restart_budget_remaining": self.budget[k].remaining(),
            }
        try:
            atomic_json(STATUS_FILE, doc)
        except Exception:
            pass

    def run(self):
        self.start()
        try:
            while not self.stop_requested:
                try:
                    self._monitor_processes()
                    self._monitor_writer()
                    self._monitor_dashboard()
                    self._write_status()
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    # Uma falha do próprio watchdog NÃO encerra os filhos.
                    msg = f"{type(exc).__name__}: {exc}"
                    print(f"[SUPERVISOR] ERRO INTERNO RECUPERÁVEL: {msg}")
                    append_event("INTERNAL_ERROR", "supervisor", msg, traceback=traceback.format_exc()[-4000:])
                time.sleep(max(1.0, self.args.check_s))
        except KeyboardInterrupt:
            print("\n[SUPERVISOR] Encerramento solicitado pelo operador.")
            self.stop_requested = True
        finally:
            # Somente encerramento explícito do supervisor encerra a árvore.
            for component in ("ml", "dashboard", "collector"):
                self._terminate(component)
            self._write_status()
        return 0


def build_parser():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(APP / "toi_demo_scope_v1.json"))
    ap.add_argument("--db", default=str((APP / "../dados_toi/live/toi_live.sqlite").resolve()))
    ap.add_argument("--seed-db", default="")
    ap.add_argument("--port", type=int, default=8088)
    ap.add_argument("--check-s", type=float, default=DEFAULT_CHECK_S)
    ap.add_argument("--queue-stale-restart-s", type=float, default=DEFAULT_QUEUE_STALE_RESTART_S)
    ap.add_argument("--queue-stale-consecutive", type=int, default=DEFAULT_QUEUE_STALE_CONSECUTIVE)
    ap.add_argument("--queue-systemic-ratio", type=float, default=0.50)
    ap.add_argument("--dashboard-fail-consecutive", type=int, default=DEFAULT_DASHBOARD_FAIL_CONSECUTIVE)
    ap.add_argument("--restart-cooldown-s", type=float, default=DEFAULT_RESTART_COOLDOWN_S)
    ap.add_argument("--restart-window-s", type=float, default=DEFAULT_RESTART_WINDOW_S)
    ap.add_argument("--max-restarts-window", type=int, default=DEFAULT_MAX_RESTARTS_WINDOW)
    ap.add_argument("--post-restart-grace-s", type=float, default=DEFAULT_POST_RESTART_GRACE_S)
    return ap


def main():
    args = build_parser().parse_args()
    db = Path(args.db).resolve()
    if args.seed_db and not db.exists():
        seed = Path(args.seed_db).resolve()
        if seed.exists():
            sqlite_seed(seed, db)
        else:
            print(f"[SEED] Ignorado: arquivo não encontrado: {seed}")

    print("=" * 96)
    print("TOI v1.10.6 — RUNTIME LIFECYCLE HARDENING")
    print("Collector/Dashboard/ML desacoplados + watchdog de progresso do writer LIVE")
    print("Aquisição congelada permanece inalterada.")
    print("=" * 96)
    append_event("START", "supervisor", "runtime lifecycle hardening iniciado", supervisor_pid=os.getpid())
    sup = RuntimeSupervisor(args)
    return sup.run()


if __name__ == "__main__":
    raise SystemExit(main())
