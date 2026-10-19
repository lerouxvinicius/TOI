"""Fast, read-only TOI questions over curated operational measurements.

Natural language only selects a fixed source, metric and operation. It never
becomes SQL. All database names, columns and filters come from this registry.
"""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import sqlite3
from statistics import median
import time

from toi_local_ai import TZ, _date_time, _display_number, _display_time, _plain, _readonly


LIVE = "dados_toi/live/toi_live.sqlite"
GATEWAY = "dados_toi/shadow/toi_gateway_health_shadow.sqlite"

# A source can be added here only after its data meaning and access scope are
# reviewed. Time-series tables are observed snapshots, not call detail records.
SERIES = {
    "gateway_quality": {
        "path": GATEWAY, "table": "gateway_health_sample_v5", "time": "ts",
        "instance": "instancia", "entity": "entity", "label": "saúde de gateways",
        "metrics": {
            "loss_pct": ("perda de pacotes (%)", "perda de pacotes", "perda", "ping"),
            "rtt_p95_ms": ("latência p95 (ms)", "rtt p95", "latencia p95", "tempo de resposta"),
            "rtt_p50_ms": ("latência mediana (ms)", "rtt mediano", "latencia mediana"),
            "rtt_jitter_ms": ("variação de latência (ms)", "jitter", "variacao de latencia"),
            "occupancy_pct": ("ocupação (%)", "ocupacao", "canais ocupados"),
            "traffic_share_pct": ("participação no tráfego (%)", "participacao no trafego", "balanceamento"),
            "call_share_pct": ("participação nas chamadas (%)", "participacao nas chamadas"),
            "silence_critical_min": ("silêncio crítico (min)", "silencio critico", "sem trafego"),
        },
    },
    "gateway_daily": {
        "path": GATEWAY, "table": "gateway_accounting_daily_v1", "time": "day",
        "instance": "instancia", "entity": "entity", "label": "resumo diário de gateways",
        "metrics": {
            "icmp_down_cycles": ("ciclos sem resposta ICMP", "icmp", "sem resposta", "ping"),
            "silence_critical_cycles": ("ciclos com silêncio crítico", "silencio critico", "sem trafego"),
            "stable_critical_cycles": ("ciclos em estado crítico", "critico", "critical"),
            "transitions": ("mudanças de estado", "transicoes", "mudancas de estado"),
            "operational_cycles": ("ciclos operacionais", "ciclos operacionais"),
        },
    },
    "gateway_path": {
        "path": GATEWAY, "table": "gateway_path_sample_v1", "time": "ts",
        "instance": None, "entity": None, "label": "caminho de rede dos gateways",
        "metrics": {
            "zero_reply_gateways": ("gateways sem resposta", "sem resposta", "falha de rede"),
            "fail_ratio": ("proporção de falhas", "proporcao de falhas", "falha compartilhada"),
            "affected_instances": ("instâncias afetadas", "instancias afetadas"),
        },
    },
    "ml_risk": {
        "path": LIVE, "table": "ml_shadow_prediction", "time": "ts",
        "instance": "instancia", "entity": "codigo", "label": "modelo de risco em sombra",
        "metrics": {
            "risk_score": ("score de risco", "risco", "score de risco"),
            "anomaly_score": ("score de anomalia", "anomalia", "score de anomalia"),
            "p34_signal": ("sinal P34", "sinal p34"),
            "p135_signal": ("sinal P135", "sinal p135"),
            "capacity_signal": ("sinal de capacidade", "sinal de capacidade"),
        },
    },
    "heartbeat": {
        "path": LIVE, "table": "live_heartbeat", "time": "ts",
        "instance": "instancia", "entity": "source", "label": "saúde das fontes de coleta",
        "metrics": {"latency": ("latência da coleta (s)", "latencia", "tempo de coleta")},
    },
    "scheduler": {
        "path": LIVE, "table": "scheduler_health", "time": "ts",
        "instance": "instancia", "entity": "source", "label": "agendamento do coletor",
        "metrics": {
            "start_lag_s": ("atraso no início da coleta (s)", "atraso", "agendamento"),
            "duration_s": ("duração da coleta (s)", "duracao", "tempo de coleta"),
            "recovery_delay_s": ("atraso de recuperação (s)", "recuperacao"),
        },
    },
    "worker": {
        "path": LIVE, "table": "worker_health", "time": "ts",
        "instance": None, "entity": "worker", "label": "saúde dos workers",
        "metrics": {"consecutive_failures": ("falhas consecutivas", "falhas", "erros", "consecutivas")},
    },
    "dual_lane": {
        "path": LIVE, "table": "dual_lane_health", "time": "ts",
        "instance": "instancia", "entity": "lane", "label": "lanes do coletor",
        "metrics": {
            "deadline_lag_s": ("atraso da lane (s)", "atraso", "deadline"),
            "coalesced_slots": ("slots agrupados", "slots"),
            "duration_s": ("duração da coleta (s)", "duracao"),
        },
    },
    "slow_path": {
        "path": LIVE, "table": "slow_path_health", "time": "ts",
        "instance": "instancia", "entity": "kind", "label": "caminho lento do coletor",
        "metrics": {
            "duration_s": ("duração (s)", "duracao", "tempo"),
            "fast_age_s": ("idade do caminho rápido (s)", "idade", "caminho rapido"),
        },
    },
    "adaptive": {
        "path": LIVE, "table": "adaptive_dispatcher_health", "time": "ts",
        "instance": "instancia", "entity": "codigo", "label": "despacho adaptativo",
        "metrics": {
            "age_before_s": ("idade antes do despacho (s)", "idade", "atraso"),
            "duration_s": ("duração do despacho (s)", "duracao"),
            "urgency": ("urgência", "urgencia"),
        },
    },
    "queue_ranking": {
        "path": LIVE, "table": "queue_snapshot", "time": "ts",
        "instance": "instancia", "entity": "codigo", "label": "filas",
        "metrics": {
            "p135_pct": ("congestionamento interno P135 (%)", "congestionamento interno", "p135"),
            "p34_pct": ("congestionamento na companhia P34 (%)", "congestionamento", "p34"),
            "p135_qtd": ("chamadas em P135", "chamadas p135"),
            "calls_dialing": ("chamadas discando", "discando", "discagens"),
            "agents_available": ("agentes disponíveis", "agentes disponiveis"),
            "latency_queue": ("latência da fila (s)", "latencia"),
        },
    },
}


def _response(answer, status, source, scope, rows=(), coverage=()):
    return {"status": status, "answer": answer, "scope": scope,
            "coverage": list(coverage), "evidence": [{"source": source, "rows": list(rows)[:5]}],
            "local_only": True}


def _period(scope, now, *, days=7):
    start = _date_time(str(scope.get("start") or "")) or now - timedelta(days=days)
    end = _date_time(str(scope.get("end") or ""), end=True) or now
    if end > now and end.date() == now.date() + timedelta(days=1) and end.hour == 0:
        end = now
    if start >= end or end > now + timedelta(minutes=1):
        raise ValueError("Período inválido")
    if end - start > timedelta(days=32):
        raise ValueError("Consulte no máximo 32 dias por pergunta")
    return start, end


def _rowid_at(conn, table, column, target):
    # Called only with table and column literals from SERIES.
    last = conn.execute(f"SELECT rowid FROM {table} ORDER BY rowid DESC LIMIT 1").fetchone()
    if not last:
        return 1
    lo, hi = 1, int(last[0]) + 1
    while lo < hi:
        mid = (lo + hi) // 2
        row = conn.execute(f"SELECT rowid,{column} FROM {table} WHERE rowid>=? ORDER BY rowid LIMIT 1", (mid,)).fetchone()
        if row is None:
            hi = mid
        elif str(row[1]) < target:
            lo = int(row[0]) + 1
        else:
            hi = mid
    return lo


def _operation(question):
    q = _plain(question)
    if any(x in q for x in ("quais ", "ranking", "top ", "mais afetad")) and any(
            x in q for x in ("maior", "mais ", "pico", "critico", "perda")):
        return "rank"
    if any(x in q for x in ("pico", "maior", "maximo", "mais alta", "mais alto")):
        return "peak"
    if any(x in q for x in ("menor", "minimo", "mais baixa", "mais baixo")):
        return "minimum"
    if "media" in q:
        return "average"
    if any(x in q for x in ("quantas amostras", "quantos registros", "quantas medicoes")):
        return "count"
    return "latest"


def _metric(question, spec):
    q = _plain(question)
    best = None
    best_len = 0
    for name, (_label, *aliases) in spec["metrics"].items():
        for alias in aliases:
            normalized = _plain(alias)
            if normalized and normalized in q and len(normalized) > best_len:
                best, best_len = name, len(normalized)
    return best or next(iter(spec["metrics"]))


def _source(question):
    q = _plain(question)
    if "gateway" in q:
        if any(x in q for x in ("ciclo", "por dia", "diario")):
            return "gateway_daily"
        if any(x in q for x in ("rede", "caminho", "compartilhada", "varios gateways")):
            return "gateway_path"
        if any(x in q for x in ("perda", "ping", "rtt", "jitter", "latencia", "trafego", "balanceamento", "ocupacao", "silencio")):
            return "gateway_quality"
    if any(x in q for x in ("anomalia", "score de risco", "previsao", "predicao", "modelo em sombra")):
        return "ml_risk"
    if "worker" in q:
        return "worker"
    if "heartbeat" in q or "fonte de coleta" in q:
        return "heartbeat"
    if any(x in q for x in ("agendamento", "scheduler")):
        return "scheduler"
    if "lane" in q:
        return "dual_lane"
    if "caminho lento" in q:
        return "slow_path"
    if "despacho adaptativo" in q:
        return "adaptive"
    if any(x in q for x in ("quais filas", "qual fila", "ranking de filas", "filas da instancia", "filas com maior")):
        return "queue_ranking"
    return None


def _entity(question, scope, key):
    if key in ("gateway_quality", "gateway_daily"):
        return scope.get("entity") or ""
    if key == "worker":
        match = re.search(r"\bworker\s+([a-z0-9._-]+)", _plain(question))
        return match.group(1) if match else ""
    if key == "dual_lane":
        match = re.search(r"\blane\s+([a-z0-9._-]+)", _plain(question))
        return match.group(1) if match else ""
    if key == "queue_ranking":
        return ""
    return ""


def _series(root, question, scope, allowed, queues, now, key):
    spec = SERIES[key]
    if spec["instance"] is None and allowed is not None:
        raise PermissionError("Essa fonte exige acesso a todas as instâncias")
    instance = str(scope.get("instance") or "")
    if instance and allowed is not None and instance not in allowed:
        raise PermissionError("Instância não autorizada")
    if allowed is not None and not allowed:
        raise PermissionError("Nenhuma instância autorizada")
    start, end = _period(scope, now, days=1)
    operation = _operation(question)
    metric = _metric(question, spec)
    entity = _entity(question, scope, key)
    table, stamp = spec["table"], spec["time"]
    date_only = stamp == "day"
    lower = start.date().isoformat() if date_only else start.isoformat(timespec="seconds")
    upper = (end.date() + (timedelta(days=1) if end.hour or end.minute or end.second else timedelta())).isoformat() if date_only else end.isoformat(timespec="seconds")
    path = root / spec["path"]
    where = [f"{stamp}>=?", f"{stamp}<?"]
    args = [lower, upper]
    if instance and spec["instance"]:
        where.append(f"{spec['instance']}=?"); args.append(instance)
    elif allowed is not None and spec["instance"]:
        where.append(spec["instance"] + " IN (" + ",".join("?" for _ in allowed) + ")")
        args.extend(sorted(allowed))
    if entity and spec["entity"]:
        where.append(f"{spec['entity']}=? COLLATE NOCASE"); args.append(entity)
    with closing(_readonly(path, seconds=8)) as conn:
        if not date_only:
            lo = _rowid_at(conn, table, stamp, lower)
            hi = _rowid_at(conn, table, stamp, upper)
            where.extend(("rowid>=?", "rowid<?")); args.extend((lo, hi))
        where.append(f"{metric} IS NOT NULL")
        predicate = " AND ".join(where)
        args = tuple(args)
        identity = (f"{spec['instance']} AS instancia," if spec["instance"] else "") + (
            f"{spec['entity']} AS entity," if spec["entity"] else "")
        if operation == "latest" and spec["entity"] and not entity:
            grouped = ",".join(c for c in (spec["instance"], spec["entity"]) if c)
            rows = [dict(r) for r in conn.execute(
                f"SELECT {identity} {stamp} AS ts,{metric} AS value FROM {table} "
                f"WHERE rowid IN (SELECT MAX(rowid) FROM {table} WHERE {predicate} GROUP BY {grouped}) "
                f"ORDER BY value DESC LIMIT 5", args)]
            operation = "rank"
        elif operation == "rank" and spec["entity"]:
            rows = [dict(r) for r in conn.execute(
                f"SELECT {identity} MAX({metric}) AS value, COUNT(*) AS samples FROM {table} "
                f"WHERE {predicate} GROUP BY " + ",".join(c for c in (spec["instance"], spec["entity"]) if c) +
                " ORDER BY value DESC LIMIT 5", args)]
        elif operation in ("peak", "minimum", "latest", "rank"):
            order = f"{metric} DESC,{stamp} ASC" if operation in ("peak", "rank") else (
                f"{metric} ASC,{stamp} ASC" if operation == "minimum" else f"{stamp} DESC")
            rows = [dict(r) for r in conn.execute(
                f"SELECT {identity} {stamp} AS ts,{metric} AS value FROM {table} "
                f"WHERE {predicate} ORDER BY {order} LIMIT 1", args)]
        else:
            expression = "AVG" if operation == "average" else "COUNT"
            rows = [dict(r) for r in conn.execute(
                f"SELECT {expression}({metric}) AS value,COUNT(*) AS samples FROM {table} "
                f"WHERE {predicate}", args)]
    rows = [r for r in rows if r.get("value") is not None]
    label = spec["metrics"][metric][0]
    prefix = {"peak": "Pico", "minimum": "Mínimo", "average": "Média",
              "latest": "Última medição", "count": "Amostras", "rank": "Maiores valores"}[operation]
    if not rows:
        answer, status = "Não há dados válidos para essa consulta no período selecionado.", "NO_DATA"
    else:
        parts = []
        for row in rows:
            location = []
            if row.get("entity"):
                location.append(str(row["entity"]))
            if row.get("instancia"):
                location.append("instância " + str(row["instancia"]))
            text = (", ".join(location) + ": " if location else "") + _display_number(row["value"])
            if row.get("ts"):
                text += " em " + (datetime.strptime(row["ts"], "%Y-%m-%d").strftime("%d/%m/%Y")
                                     if date_only else _display_time(row["ts"]))
            parts.append(text)
        answer = f"{prefix} de {label}: " + "; ".join(parts) + "."
        status = "OK"
        if key == "ml_risk":
            answer += " Indicador experimental em sombra."
    scope_result = {"dataset": key, "metric": metric, "operation": operation,
                    "instancia": instance or None, "entity": entity or None,
                    "start_display": _display_time(start), "end_display": _display_time(end)}
    return _response(answer, status, spec["path"] + ":" + table, scope_result, rows)


def _incidents(root, question, scope, allowed, now):
    q = _plain(question)
    if "incidente" not in q and "alerta" not in q:
        return None
    instance = str(scope.get("instance") or "")
    if instance and allowed is not None and instance not in allowed:
        raise PermissionError("Instância não autorizada")
    if allowed is not None and not allowed:
        raise PermissionError("Nenhuma instância autorizada")
    active = any(x in q for x in ("ativo", "aberto", "agora", "em andamento"))
    start, end = _period(scope, now)
    where = ["active=1"] if active else ["started>=?", "started<?"]
    args = [] if active else [start.timestamp(), end.timestamp()]
    if instance:
        where.append("instancia=?"); args.append(instance)
    elif allowed is not None:
        where.append("instancia IN (" + ",".join("?" for _ in allowed) + ")")
        args.extend(sorted(allowed))
    if "gateway" in q:
        where.append("source='GATEWAY'")
    with closing(_readonly(root / "dados_toi/operational/toi_radar_v1.sqlite", seconds=5)) as db:
        rows = [dict(r) for r in db.execute(
            "SELECT source,entity_key,instancia,codigo,started FROM incidents WHERE " +
            " AND ".join(where) + " ORDER BY started DESC LIMIT 6", tuple(args))]
        count = db.execute("SELECT COUNT(*) FROM incidents WHERE " + " AND ".join(where), tuple(args)).fetchone()[0]
    title = "Incidentes ativos" if active else "Incidentes no período"
    if not count:
        answer = f"{title}: nenhum registro encontrado."
    else:
        items = [f"{r['entity_key']} (instância {r['instancia']}, desde {_display_time(r['started'])})"
                 for r in rows[:5]]
        answer = f"{title}: {count}. " + "; ".join(items) + ("; e outros." if count > 5 else ".")
    return _response(answer, "OK" if count else "NO_DATA",
        "dados_toi/operational/toi_radar_v1.sqlite:incidents",
        {"dataset": "incidents", "instancia": instance or None,
         "start_display": _display_time(start), "end_display": _display_time(end)}, rows)


def _servers(root, question, allowed):
    q = _plain(question)
    if "servidor" not in q or any(x in q for x in ("sip", "sbc", "agi", "nap")):
        return None
    if allowed is not None:
        raise PermissionError("Essa fonte exige acesso a todas as instâncias")
    path = root / "dados/status_servidores.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    servers = data.get("servidores") or {}
    if not isinstance(servers, dict):
        raise ValueError("Estado dos servidores inválido")
    down = sorted(str(name) for name, up in servers.items() if up is False)
    answer = ("Servidores indisponíveis: " + ", ".join(down) + "." if down else
              f"Todos os {len(servers)} servidores monitorados estão disponíveis.")
    answer += " Última atualização: " + _display_time(data.get("atualizado_em")) + "."
    return _response(answer, "OK", "dados/status_servidores.json",
                     {"dataset": "servers"})


def _gateway_status(root, question, scope, allowed):
    q = _plain(question)
    if "gateway" not in q or not any(x in q for x in
            ("critico", "critical", "warn", "estado", "status", "indisponivel", "saudavel", "normal")):
        return None
    instance = str(scope.get("instance") or "")
    if instance and allowed is not None and instance not in allowed:
        raise PermissionError("Instância não autorizada")
    if allowed is not None and not allowed:
        raise PermissionError("Nenhuma instância autorizada")
    where, args = [], []
    if instance:
        where.append("instancia=?"); args.append(instance)
    elif allowed is not None:
        where.append("instancia IN (" + ",".join("?" for _ in allowed) + ")")
        args.extend(sorted(allowed))
    entity = str(scope.get("entity") or "")
    if entity:
        where.append("entity=? COLLATE NOCASE"); args.append(entity)
    sql = "SELECT instancia,entity,stable_status,last_seen_ts FROM gateway_state_v5"
    if where:
        sql += " WHERE " + " AND ".join(where)
    with closing(_readonly(root / GATEWAY, seconds=4)) as db:
        rows = [dict(row) for row in db.execute(sql, tuple(args))]
    desired = ("CRITICAL" if any(x in q for x in ("critico", "critical", "indisponivel")) else
               "WARN" if "warn" in q or "alerta" in q else None)
    if desired:
        rows = [row for row in rows if desired in str(row["stable_status"]).upper()]
    if not rows:
        answer = (f"Nenhum gateway em estado {desired} no escopo consultado." if desired else
                  "Não há estado de gateway registrado para esse escopo.")
        status = "NO_DATA"
    else:
        rows.sort(key=lambda row: (str(row["instancia"]), str(row["entity"])))
        items = [f"{row['entity']} (instância {row['instancia']}: {row['stable_status']})" for row in rows[:8]]
        answer = f"Gateways {desired or 'monitorados'}: {len(rows)}. " + "; ".join(items)
        if len(rows) > 8:
            answer += "; e outros"
        answer += "."
        status = "OK"
    return _response(answer, status, GATEWAY + ":gateway_state_v5",
                     {"dataset": "gateway_status", "instancia": instance or None}, rows)


def _gateway_recovery(root, question, scope, allowed, now):
    q = _plain(question)
    if "gateway" not in q or not any(x in q for x in ("retorn", "voltou", "voltar", "recuper")):
        return None
    instance = str(scope.get("instance") or "")
    if instance and allowed is not None and instance not in allowed:
        raise PermissionError("Instância não autorizada")
    if allowed is not None and not allowed:
        raise PermissionError("Nenhuma instância autorizada")
    start, end = _period(scope, now, days=1)
    where = ["ts>=?", "ts<?", "new_status='OPERACIONAL'",
             "old_status IN ('PING_DOWN','PING_RECOVERING','PING_INTERMITTENT',"
             "'NO_TRAFFIC_CRITICAL','NO_TRAFFIC_WARN','NO_TRAFFIC_OBSERVE',"
             "'SERVICE_ACTIVE_ICMP_DOWN','OPERACIONAL_ICMP_INTERMITTENT',"
             "'OPERACIONAL_ICMP_NC','PATH_DEGRADED','PING_NC')"]
    args = [start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")]
    if instance:
        where.append("instancia=?"); args.append(instance)
    elif allowed is not None:
        where.append("instancia IN (" + ",".join("?" for _ in allowed) + ")")
        args.extend(sorted(allowed))
    entity = str(scope.get("entity") or "")
    if entity:
        where.append("entity=? COLLATE NOCASE"); args.append(entity)
    with closing(_readonly(root / GATEWAY, seconds=5)) as db:
        rows = [dict(row) for row in db.execute(
            "SELECT ts,instancia,entity,old_status,new_status FROM gateway_state_transition_v1 "
            "WHERE " + " AND ".join(where) + " ORDER BY ts DESC LIMIT 200", tuple(args))]
    latest = {}
    for row in rows:
        latest.setdefault((row["instancia"], row["entity"]), row)
    selected = list(latest.values())[:6]
    if selected:
        items = [f"{row['entity']} (instância {row['instancia']}) às {_display_time(row['ts'])}"
                 for row in selected]
        answer = "Retorno ao estado operacional registrado: " + "; ".join(items)
        if len(latest) > len(selected):
            answer += "; e outros"
        answer += ". O estado operacional não confirma, por si só, discagens bem-sucedidas."
        status = "OK"
    else:
        answer = "Não há transição de gateway para o estado operacional registrada no período consultado."
        status = "NO_DATA"
    return _response(answer, status, GATEWAY + ":gateway_state_transition_v1",
                     {"dataset": "gateway_recovery", "instancia": instance or None,
                      "start_display": _display_time(start), "end_display": _display_time(end)}, selected)


def _queue_sql(root, instance, code, metric, start, end, *, summary=False):
    if metric not in ("p34_pct", "p135_pct"):
        raise ValueError("Métrica de fila inválida")
    lower = start.replace(tzinfo=None).isoformat(timespec="seconds")
    upper = end.replace(tzinfo=None).isoformat(timespec="seconds")
    sidecar = root / "dados_toi/cache/toi_analytics_light_v1.sqlite"
    candidates = [(sidecar, "queue_light", False), (root / LIVE, "queue_snapshot", True)]
    for path, table, live in candidates:
        if not path.is_file():
            continue
        try:
            with closing(_readonly(path, seconds=8)) as db:
                where = ["instancia=?", "ts>=?", "ts<?", f"{metric} IS NOT NULL"]
                args = [instance, lower, upper]
                if live and not code:
                    lo = _rowid_at(db, table, "ts", lower)
                    hi = _rowid_at(db, table, "ts", upper)
                    where.extend(("rowid>=?", "rowid<?"))
                    args.extend((lo, hi))
                if code:
                    where.append("codigo=?"); args.append(code)
                predicate = " AND ".join(where)
                table_ref = table + (" INDEXED BY ix_qs" if live and code else "")
                if summary:
                    rows = [dict(row) for row in db.execute(
                        f"SELECT codigo,MAX({metric}) AS value FROM {table_ref} "
                        f"WHERE {predicate} GROUP BY codigo ORDER BY value DESC", tuple(args))]
                    if rows:
                        winner = db.execute(
                            f"SELECT ts,codigo,{metric} AS value FROM {table_ref} "
                            f"WHERE {predicate} ORDER BY {metric} DESC,ts ASC LIMIT 1",
                            tuple(args)).fetchone()
                        rows[0]["ts"] = winner["ts"]
                else:
                    rows = [dict(row) for row in db.execute(
                        f"SELECT ts,codigo,{metric} AS value FROM {table_ref} "
                        f"WHERE {predicate} ORDER BY ts", tuple(args))]
            if rows or live:
                return rows, str(path.relative_to(root)) + ":" + table
        except sqlite3.OperationalError:
            if not live:
                continue
            raise
    raise FileNotFoundError("Não há base de filas legível")


def _queue_duration_stats(root, instance, code, metric, limit, start, end):
    """Aggregate in SQLite and read only a short tail to infer sample cadence."""
    if metric not in ("p34_pct", "p135_pct"):
        raise ValueError("Métrica de fila inválida")
    lower = start.replace(tzinfo=None).isoformat(timespec="seconds")
    upper = end.replace(tzinfo=None).isoformat(timespec="seconds")
    candidates = [
        (root / "dados_toi/cache/toi_analytics_light_v1.sqlite", "queue_light",
         "ix_queue_light_inst_code_ts"),
        (root / LIVE, "queue_snapshot", "ix_qs"),
    ]
    for path, table, index in candidates:
        if not path.is_file():
            continue
        try:
            with closing(_readonly(path, seconds=12)) as db:
                predicate = "instancia=? AND codigo=? AND ts>=? AND ts<?"
                args = (instance, code, lower, upper)
                row = db.execute(
                    f"SELECT COUNT({metric}) AS valid,"
                    f"SUM(CASE WHEN {metric}>? THEN 1 ELSE 0 END) AS above,"
                    f"MAX({metric}) AS peak,MIN(ts) AS first,MAX(ts) AS last "
                    f"FROM {table} INDEXED BY {index} WHERE {predicate}",
                    (limit, *args)).fetchone()
                tail = [str(item["ts"]) for item in db.execute(
                    f"SELECT ts FROM {table} INDEXED BY {index} WHERE {predicate} "
                    "ORDER BY ts DESC LIMIT 128", args)]
            if row["valid"] or table == "queue_snapshot":
                return dict(row), tail, str(path.relative_to(root)) + ":" + table
        except sqlite3.OperationalError:
            if table == "queue_light":
                continue
            raise
    raise FileNotFoundError("Não há base de filas legível")


def _queue_congestion_summary(root, instance, codes, start, end):
    """Use the (instance, code, time) index for every authorized catalog queue."""
    lower = start.replace(tzinfo=None).isoformat(timespec="seconds")
    upper = end.replace(tzinfo=None).isoformat(timespec="seconds")
    candidates = [
        (root / "dados_toi/cache/toi_analytics_light_v1.sqlite", "queue_light",
         "ix_queue_light_inst_code_ts"),
        (root / LIVE, "queue_snapshot", "ix_qs"),
    ]
    for path, table, index in candidates:
        if not path.is_file():
            continue
        try:
            with closing(_readonly(path, seconds=15)) as db:
                rows = []
                for offset in range(0, len(codes), 50):
                    batch = codes[offset:offset + 50]
                    placeholders = ",".join("?" for _ in batch)
                    rows.extend(dict(row) for row in db.execute(
                        f"SELECT codigo,MAX(p135_pct) AS value FROM {table} INDEXED BY {index} "
                        f"WHERE instancia=? AND codigo IN ({placeholders}) AND ts>=? AND ts<? "
                        "AND p135_pct IS NOT NULL GROUP BY codigo",
                        (instance, *batch, lower, upper)))
                rows.sort(key=lambda row: float(row["value"]), reverse=True)
                if rows:
                    winner = rows[0]["codigo"]
                    point = db.execute(
                        f"SELECT ts FROM {table} INDEXED BY {index} "
                        "WHERE instancia=? AND codigo=? AND ts>=? AND ts<? AND p135_pct=? "
                        "ORDER BY ts LIMIT 1",
                        (instance, winner, lower, upper, rows[0]["value"])).fetchone()
                    rows[0]["ts"] = point["ts"] if point else None
            if rows or table == "queue_snapshot":
                return rows, str(path.relative_to(root)) + ":" + table
        except sqlite3.OperationalError:
            if table == "queue_light":
                continue
            raise
    raise FileNotFoundError("Não há base de filas legível")


def _queue_question(root, question, scope, allowed, queues, now):
    q = _plain(question)
    instance = str(scope.get("instance") or "")
    if "fila" not in q or not instance:
        return None
    if allowed is not None and instance not in allowed:
        raise PermissionError("Instância não autorizada")
    start, end = _period(scope, now, days=1)
    threshold = re.search(r"\bacima\s+d[eo]\s*(\d+(?:[.,]\d+)?)\s*%", q)
    duration = any(x in q for x in ("por quanto tempo", "quanto tempo", "duracao"))
    if duration and threshold and ("p34" in q or "p135" in q):
        from toi_local_ai import LocalTOIAnalyst
        raw = str(scope.get("queue") or "")
        if not raw:
            raise ValueError("Informe a fila para calcular a duração")
        _, code = LocalTOIAnalyst._resolve_queue(raw, instance, list(queues))
        metric = "p135_pct" if "p135" in q else "p34_pct"
        limit = float(threshold.group(1).replace(",", "."))
        if not 0 <= limit <= 100:
            raise ValueError("Percentual fora de 0 a 100%")
        stats, tail, source = _queue_duration_stats(
            root, instance, code, metric, limit, start, end)
        if not stats["valid"]:
            answer, status = "Não há amostras válidas da fila nesse período.", "NO_DATA"
        else:
            stamps = [datetime.fromisoformat(value) for value in reversed(tail)]
            gaps = [(b - a).total_seconds() for a, b in zip(stamps, stamps[1:])]
            short_gaps = [gap for gap in gaps if 0 < gap <= 60]
            cadence = median(short_gaps) if short_gaps else 15
            seconds = round((stats["above"] or 0) * cadence)
            label = "P135" if metric == "p135_pct" else "P34"
            answer = (f"Fila {code}, instância {instance}: {label} acima de "
                      f"{_display_number(limit)}% por cerca de {seconds // 3600}h "
                      f"{seconds % 3600 // 60}min {seconds % 60}s. "
                      f"Pico observado: {_display_number(stats['peak'])}%. "
                      "Estimativa pela cadência das amostras; lacunas sem leitura não entram no tempo.")
            status = "OK"
        coverage = ([{"samples": stats["valid"], "first": _display_time(stats["first"]),
                      "last": _display_time(stats["last"])}] if stats["valid"] else [])
        return _response(answer, status, source,
                         {"dataset": "queue_duration", "instancia": instance, "codigo": code,
                          "start_display": _display_time(start), "end_display": _display_time(end)},
                         coverage=coverage)
    if scope.get("queue") or "congestionamento interno" not in q or not any(
            x in q for x in ("alguma fila", "algumas filas", "quais filas", "houve", "tivemos")):
        return None
    codes = sorted({str(row.get("codigo") or "") for row in queues
                    if str(row.get("instancia")) == instance and row.get("codigo")})
    if not codes:
        raise ValueError("Catálogo de filas da instância indisponível")
    rows, source = _queue_congestion_summary(root, instance, codes, start, end)
    affected = [row for row in rows if float(row["value"]) > 0]
    if not rows:
        answer, status = "Não há leitura de P135 das filas no período consultado.", "NO_DATA"
    elif not affected:
        answer, status = f"Não. Nenhuma fila da instância {instance} registrou P135 acima de 0% no período.", "OK"
    else:
        peak = affected[0]
        answer = (f"Sim. {len(affected)} fila(s) da instância {instance} registraram "
                  f"congestionamento interno (P135 acima de 0%). Maior pico: fila {peak['codigo']}, "
                  f"{_display_number(float(peak['value']))}% em {_display_time(peak['ts'])}.")
        status = "OK"
    return _response(answer, status, source,
                     {"dataset": "queue_congestion", "instancia": instance,
                      "start_display": _display_time(start), "end_display": _display_time(end)})


def _sip(root, question, scope, allowed, now):
    q = _plain(question)
    if not any(x in q for x in ("servidor sip", "servidores sip", "capacidade sip", "agentes sip")):
        return None
    instance = str(scope.get("instance") or "")
    if instance and allowed is not None and instance not in allowed:
        raise PermissionError("Instância não autorizada")
    instances = [instance] if instance else (sorted(allowed) if allowed is not None else [str(n) for n in range(1, 7)])
    if not instances:
        raise PermissionError("Nenhuma instância autorizada")
    rows = []
    with closing(_readonly(root / "dados/portal_sip.sqlite", seconds=5)) as db:
        for item in instances:
            latest = db.execute("SELECT ts,payload,error FROM snapshots WHERE instance=? ORDER BY id DESC LIMIT 1", (item,)).fetchone()
            if not latest or latest["error"] or not latest["payload"]:
                continue
            try:
                payload = json.loads(latest["payload"])
            except (TypeError, ValueError):
                continue
            for server in payload.get("items", []):
                if not isinstance(server, dict):
                    continue
                rows.append({"instancia": item, "server": str(server.get("server") or ""),
                             "agents": server.get("agents"), "capacity": server.get("capacity"),
                             "occupancy": server.get("occupancy"), "ts": latest["ts"]})
    rows = [row for row in rows if row["server"]]
    if not rows:
        answer, status = "Não há leitura SIP válida e atual para o escopo solicitado.", "NO_DATA"
    elif any(x in q for x in ("quantos", "quantas", "numero de servidores")):
        answer, status = f"Servidores SIP observados: {len(rows)}.", "OK"
    else:
        ranked = sorted(rows, key=lambda row: (row["occupancy"] is not None,
                                               row["occupancy"] or 0), reverse=True)
        items = [f"{row['server']} (instância {row['instancia']}): "
                 f"{_display_number(row['occupancy'])}% de ocupação, "
                 f"{row['agents']}/{row['capacity']} agentes" for row in ranked[:5]
                 if row["occupancy"] is not None]
        answer = ("Servidores SIP com maior ocupação: " + "; ".join(items) + "." if items else
                  "Há servidores SIP registrados, mas sem ocupação válida.")
        newest = max(row["ts"] for row in rows)
        answer += " Medição de " + _display_time(newest) + "."
        if now.timestamp() - newest > 45:
            answer += " A leitura está desatualizada."
        status = "OK" if items else "NO_DATA"
    return _response(answer, status, "dados/portal_sip.sqlite:snapshots",
                     {"dataset": "sip_servers", "instancia": instance or None}, rows)


def _nap(root, question, scope, allowed, now):
    q = _plain(question)
    if "nap" not in q and not ("rota" in q and any(x in q for x in ("queda", "indisponivel", "caiu"))):
        return None
    if allowed is not None:
        raise PermissionError("Essa fonte exige acesso a todas as instâncias")
    start, end = _period(scope, now, days=32)
    with closing(_readonly(root / "dados/mgw_monitor.db", seconds=4)) as db:
        events = [dict(row) for row in db.execute(
            "SELECT servidor,nap_id,rota,caiu_em,voltou_em,duracao_segundos,status "
            "FROM eventos_indisponibilidade ORDER BY id DESC LIMIT 2000")]
    def observed(value):
        stamp = datetime.fromisoformat(value)
        return stamp if stamp.tzinfo else stamp.replace(tzinfo=TZ)
    events = [row for row in events if row.get("caiu_em") and
              start <= observed(row["caiu_em"]) < end]
    if not events:
        answer, status = "Não há queda de NAP registrada nesse período.", "NO_DATA"
    elif any(x in q for x in ("mais tempo", "maior duracao", "mais longa")):
        longest = max(events, key=lambda row: row.get("duracao_segundos") or 0)
        answer = (f"Maior indisponibilidade de NAP: {longest['rota']} no servidor {longest['servidor']}, "
                  f"{longest.get('duracao_segundos') or 0} segundos, iniciada em "
                  f"{_display_time(longest['caiu_em'])}.")
        status = "OK"
    else:
        answer = f"Quedas de NAP registradas: {len(events)}. " + "; ".join(
            f"{row['rota']} ({row['servidor']}) em {_display_time(row['caiu_em'])}"
            for row in events[:4]) + ("; e outras." if len(events) > 4 else ".")
        status = "OK"
    return _response(answer, status, "dados/mgw_monitor.db:eventos_indisponibilidade",
        {"dataset": "nap_outages", "start_display": _display_time(start),
         "end_display": _display_time(end)}, events)


def _help(question):
    q = _plain(question)
    if not any(x in q for x in ("o que posso perguntar", "que perguntas", "quais dados", "quais informacoes", "o que o toi monitora")):
        return None
    return _response(
        "Consulte picos e médias de filas e realms; ocupação, perdas e estados de gateways; "
        "incidentes; saúde de coletas; servidores SIP; NAPs; e scores do modelo em sombra. "
        "Informe instância, entidade e período quando quiser um recorte específico.",
        "OK", "catálogo local do TOI", {"dataset": "capabilities"})


def _agiroute(root, question, allowed, now):
    q = _plain(question)
    if not any(x in q for x in ("agiroute", "agiroutey", "proxysql")):
        return None
    if allowed is not None and "6" not in allowed:
        raise PermissionError("Instância 6 não autorizada")
    path = root / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW/static/toi_agiroute_latest.json"
    if not path.is_file():
        return _response("Ainda não há leitura dos serviços Agiroute na instância 6.",
                         "NO_DATA", str(path.relative_to(root)), {"dataset": "agiroute"})
    data = json.loads(path.read_text(encoding="utf-8"))
    try:
        stamp = datetime.fromisoformat(str(data.get("generated_at") or "").replace("Z", "+00:00"))
    except ValueError:
        return _response("A leitura dos serviços Agiroute está inválida.", "NO_DATA",
                         str(path.relative_to(root)), {"dataset": "agiroute"})
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=TZ)
    stale = now.timestamp() - stamp.timestamp() > 180
    hosts = data.get("hosts") if isinstance(data.get("hosts"), list) else []
    if not data.get("configured"):
        answer = "A verificação de agiroutey e proxysql ainda não está configurada no TOI."
        status = "NO_DATA"
    else:
        items = []
        for host in hosts:
            ip = str(host.get("ip") or "")
            state = str(host.get("status") or "UNKNOWN")
            services = host.get("services") if isinstance(host.get("services"), dict) else {}
            if services:
                states = ", ".join(f"{name}: {value.get('status', 'UNKNOWN')}"
                                   for name, value in services.items() if isinstance(value, dict))
                items.append(f"{ip} — {states}")
            else:
                items.append(f"{ip} — {state}; serviços sem confirmação")
        answer = "Agiroute da instância 6: " + "; ".join(items) + "."
        status = "OK" if items and not stale else "NO_DATA"
    answer += " Leitura de " + _display_time(stamp) + "."
    if stale:
        answer += " A leitura está desatualizada."
    return _response(answer, status, str(path.relative_to(root)),
                     {"dataset": "agiroute", "instancia": "6"})


def answer_extended(root, question, scope, allowed_instances, queues, now):
    """Return an answer for a confidently routed question, or None for legacy AI."""
    root = Path(root)
    result = _help(question)
    if result is not None:
        return result
    result = _agiroute(root, question, allowed_instances, now)
    if result is not None:
        return result
    for handler in (_incidents,):
        result = handler(root, question, scope, allowed_instances, now)
        if result is not None:
            return result
    result = _servers(root, question, allowed_instances)
    if result is not None:
        return result
    result = _gateway_recovery(root, question, scope, allowed_instances, now)
    if result is not None:
        return result
    result = _gateway_status(root, question, scope, allowed_instances)
    if result is not None:
        return result
    result = _queue_question(root, question, scope, allowed_instances, queues, now)
    if result is not None:
        return result
    result = _sip(root, question, scope, allowed_instances, now)
    if result is not None:
        return result
    result = _nap(root, question, scope, allowed_instances, now)
    if result is not None:
        return result
    key = _source(question)
    if key is None:
        return None
    return _series(root, question, scope, allowed_instances, queues, now, key)
