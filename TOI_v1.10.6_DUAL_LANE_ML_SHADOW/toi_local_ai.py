"""Local language planner over bounded, read-only TOI measurements.

The model chooses a validated measurement and scope. It never receives a SQL
connection or executes text as SQL. Numeric answers come only from SQLite.
"""
from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, time as day_time, timedelta
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import unicodedata
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo


TZ = ZoneInfo("America/Sao_Paulo")
MODEL = os.environ.get("TOI_LOCAL_AI_MODEL", "qwen3:1.7b")
URL = os.environ.get("TOI_LOCAL_AI_URL", "http://127.0.0.1:11434/api/chat")


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


LOCAL_HTTP = build_opener(ProxyHandler({}), _NoRedirect)
SOURCES = {
    "queue": {"label": "filas do Portal Orbe", "metrics": {
        "p135_pct": "congestionamento interno P135 (%)", "p34_pct": "congestionamento na companhia P34 (%)",
        "p135_qtd": "chamadas em congestionamento interno P135", "p34_qtd": "chamadas em congestionamento P34",
        "calls_dialing": "chamadas discando", "calls_in_attendance": "chamadas em atendimento",
        "calls_last5": "chamadas nos últimos 5 minutos", "buffer": "chamadas em buffer",
        "agents_available": "agentes disponíveis", "agents_talking": "agentes em atendimento",
        "agents_acw": "agentes em pós-atendimento", "agents_paused": "agentes pausados",
        "agents_other": "agentes em outros estados", "agents_dialing": "agentes discando",
        "dialing_rate": "taxa de discagem",
        "hit_rate5": "hit rate em 5 minutos (%)", "hit_rate15": "hit rate em 15 minutos (%)",
        "latency_queue": "latência da fila (s)", "latency_stats": "latência das estatísticas (s)",
        "cycle_lag_s": "atraso do ciclo de coleta (s)"}},
    "realm": {"label": "sessões dos realms nos SBCs", "metrics": {
        "both_active": "sessões ativas de entrada e saída, separadamente",
        "in_active": "sessões ativas de entrada", "out_active": "sessões ativas de saída",
        "in_total": "contador acumulado de entrada", "out_total": "contador acumulado de saída"}},
    "infra": {"label": "gateways e troncos", "metrics": {
        "occupancy": "ocupação (%)", "busy": "canais ocupados", "idle": "canais livres",
        "total": "canais totais", "invalid": "canais inválidos", "latency": "latência"}},
    "lastcalls": {"label": "últimas chamadas observadas por fila", "metrics": {
        "calls": "chamadas observadas na amostra"}},
    "stuck": {"label": "canais travados", "metrics": {"quantity": "quantidade de canais travados"}},
}
OPERATIONS = {"peak", "minimum", "average", "latest", "count"}
def _plain(value: str) -> str:
    return "".join(char for char in unicodedata.normalize("NFKD", str(value or "").casefold())
                   if not unicodedata.combining(char)).strip()


def _display_time(value) -> str:
    """Render every user-visible timestamp in the TOI's São Paulo timezone."""
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        stamp = datetime.fromtimestamp(value, TZ)
    elif isinstance(value, datetime):
        stamp = value
    else:
        raw = str(value).strip()
        try:
            stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return raw
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=TZ)
    return stamp.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")


def _display_number(value) -> str:
    if isinstance(value, float):
        return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return str(value)


def _date_time(value: str, *, end=False) -> datetime | None:
    if not value:
        return None
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
        try:
            value = datetime.strptime(value, "%d/%m/%Y").date().isoformat()
        except ValueError as exc:
            raise ValueError("Data inválida; use DD/MM/AAAA ou AAAA-MM-DD") from exc
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            selected = date.fromisoformat(value)
            return datetime.combine(selected + timedelta(days=1) if end else selected,
                                    day_time.min, TZ)
        except ValueError as exc:
            raise ValueError("Data inválida; use AAAA-MM-DD") from exc
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=TZ)
        return parsed.astimezone(TZ)
    except ValueError as exc:
        raise ValueError("Data inválida; use AAAA-MM-DD") from exc


def _readonly(path: Path, *, seconds=10):
    if not path.is_file():
        raise FileNotFoundError(path.name)
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=2000")
    deadline = time.monotonic() + seconds
    conn.set_progress_handler(lambda: 1 if time.monotonic() > deadline else 0, 10000)
    return conn


def _rowid_lower_bound(conn, table: str, target: str) -> int:
    if table not in ("queue_snapshot", "infra_snapshot", "lastcalls_snapshot",
                     "stuck_channels_snapshot"):
        raise ValueError("Tabela fora do catálogo")
    hi_row = conn.execute(f"SELECT rowid FROM {table} ORDER BY rowid DESC LIMIT 1").fetchone()
    if not hi_row:
        return 1
    lo, hi = 1, int(hi_row[0]) + 1
    while lo < hi:
        mid = (lo + hi) // 2
        row = conn.execute(f"SELECT rowid,ts FROM {table} WHERE rowid>=? ORDER BY rowid LIMIT 1", (mid,)).fetchone()
        if row is None:
            hi = mid
        elif str(row["ts"]) < target:
            lo = int(row["rowid"]) + 1
        else:
            hi = mid
    return lo


class LocalTOIAnalyst:
    def __init__(self, root: Path, *, planner=None, now=None):
        self.root = Path(root)
        self.planner = planner or self._model_plan
        self.now = now or (lambda: datetime.now(TZ))

    def _scope(self, question: str, history: list[dict]) -> dict:
        """Resolve explicit identifiers and dates without letting a model invent them."""
        contextual = re.search(r"^(?:e\b|agora\b|ness[ae]\b|mesm[ao]\b|tambem\b)",
                               _plain(question)) is not None
        previous = next((str(item.get("content") or "") for item in reversed(history)
                         if contextual and isinstance(item, dict) and item.get("role") == "user"), "")
        scope = {"instance": "", "queue": "", "realm": "", "entity": "",
                 "infra_source": "", "start": "", "end": ""}
        for source in (question, previous):
            if not source:
                continue
            normalized = _plain(source)
            if not scope["instance"]:
                match = re.search(r"\binstancia\s*(?:n[.º°]?\s*)?([1-6])\b", normalized)
                if match:
                    scope["instance"] = match.group(1)
            if not scope["realm"]:
                match = re.search(r"\breal(?:m)?\s+([a-z0-9._-]+)", normalized)
                if match:
                    scope["realm"] = match.group(1)
            if not scope["queue"]:
                match = re.search(r"\bfila\s+(.+?)(?=,|\s+d[ao]\s+instancia\b|\s+no\s+dia\b|\s+em\s+\d{2}/|\s+(?:hoje|ontem|teve|atingiu|esta|estava)\b|\?|$)", normalized)
                if match:
                    candidate = match.group(1).strip(" .")
                    if candidate.split(" ", 1)[0] not in ("com", "que", "alguma", "algumas", "teve", "tem"):
                        scope["queue"] = candidate
            if not scope["infra_source"]:
                if "gateway" in normalized:
                    scope["infra_source"] = "gateway"
                elif "tronco" in normalized:
                    scope["infra_source"] = "trunk"
            if not scope["entity"] and scope["infra_source"]:
                pattern = r"\b(?:gateway|tronco)\s+([a-z0-9._-]+)"
                match = re.search(pattern, normalized)
                if match and match.group(1) not in ("da", "do", "de", "na", "no", "com", "teve", "tiveram", "esta", "estava", "qual", "mais", "maior", "menor"):
                    scope["entity"] = match.group(1)
            if not scope["start"]:
                dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b|\b\d{2}/\d{2}/\d{4}\b", source)
                if dates:
                    scope["start"] = dates[0]
                    scope["end"] = dates[1] if len(dates) > 1 else dates[0]
                elif re.search(r"\bhoje\b", normalized):
                    scope["start"] = scope["end"] = self.now().date().isoformat()
                elif re.search(r"\bontem\b", normalized):
                    scope["start"] = scope["end"] = (self.now().date() - timedelta(days=1)).isoformat()
        return scope

    def _model_plan(self, question: str, history: list[dict]) -> dict:
        if urlparse(URL).hostname not in ("127.0.0.1", "localhost", "::1"):
            raise RuntimeError("O modelo deve permanecer no próprio TOI")
        scope = self._scope(question, history)
        normalized = _plain(question)
        peak = any(word in normalized for word in ("pico", "maior", "maximo"))
        if peak and scope["realm"]:
            return {**scope, "dataset": "realm", "metric": "both_active", "operation": "peak"}
        if peak and scope["queue"] and "congestionamento interno" in normalized:
            return {**scope, "dataset": "queue", "metric": "p135_pct", "operation": "peak"}
        operation = ("peak" if peak else "minimum" if any(x in normalized for x in ("menor", "minimo"))
                     else "average" if "media" in normalized else "count" if "quantas amostras" in normalized
                     else "latest")
        if scope["realm"] and any(x in normalized for x in ("sess", "chamad", "contador")):
            direction = "out" if "saida" in normalized else "in" if "entrada" in normalized else "both"
            total = "contador" in normalized or "acumulad" in normalized
            metric = direction + "_total" if total and direction != "both" else (
                direction + "_active" if direction != "both" else "both_active")
            return {**scope, "dataset": "realm", "metric": metric, "operation": operation}
        if scope["queue"]:
            queue_aliases = (
                ("quantidade de chamadas p135", "p135_qtd"), ("chamadas em p135", "p135_qtd"),
                ("quantidade de chamadas p34", "p34_qtd"), ("chamadas em p34", "p34_qtd"),
                ("congestionamento interno", "p135_pct"), ("p135", "p135_pct"),
                ("congestionamento na companhia", "p34_pct"), ("p34", "p34_pct"),
                ("agentes disponiveis", "agents_available"), ("agentes em atendimento", "agents_talking"),
                ("agentes pausados", "agents_paused"), ("agentes discando", "agents_dialing"),
                ("chamadas discando", "calls_dialing"), ("chamadas em atendimento", "calls_in_attendance"),
                ("chamadas nos ultimos 5", "calls_last5"), ("hit rate de 15", "hit_rate15"),
                ("hit rate de 5", "hit_rate5"), ("hit rate", "hit_rate5"),
                ("taxa de discagem", "dialing_rate"), ("buffer", "buffer"),
                ("latencia da fila", "latency_queue"),
                ("latencia das estatisticas", "latency_stats"),
                ("atraso do ciclo", "cycle_lag_s"),
                ("ultimas chamadas", "lastcalls:calls"),
            )
            for phrase, target in queue_aliases:
                if phrase in normalized:
                    dataset, metric = target.split(":", 1) if ":" in target else ("queue", target)
                    return {**scope, "dataset": dataset, "metric": metric, "operation": operation}
        if "canais travados" in normalized:
            return {**scope, "dataset": "stuck", "metric": "quantity", "operation": operation}
        if scope["infra_source"] == "trunk" or "tronco" in normalized:
            for phrase, metric in (("ocupacao", "occupancy"), ("ocupados", "busy"),
                                   ("livres", "idle"), ("total", "total"), ("invalidos", "invalid")):
                if phrase in normalized:
                    return {**scope, "dataset": "infra", "metric": metric, "operation": operation}
        catalog = "; ".join(f"{name}:" + ",".join(f"{key}={label}" for key, label in spec["metrics"].items())
                            for name, spec in SOURCES.items())
        system = (
            "Classifique a pergunta TOI. Responda JSON em uma linha sem espaços, só chaves "
            "dataset,metric,operation. Não gere datas, nomes nem números. "
            "Escolha dataset e metric do catálogo. operation: peak para maior/pico, "
            "minimum para menor, average para média, latest para último, count para quantidade de amostras. "
            "Congestionamento interno=queue/p135_pct. Pico de chamadas de realm=realm/both_active "
            "(entrada e saída separadas). Se não houver métrica adequada, dataset=clarify e "
            "clarification com pergunta breve. Catálogo: " + catalog + "."
        )
        contextual = re.search(r"^(?:e\b|agora\b|ness[ae]\b|mesm[ao]\b|tambem\b)",
                               _plain(question)) is not None
        previous = next((str(item.get("content") or "")[:200] for item in reversed(history)
                         if contextual and isinstance(item, dict) and item.get("role") == "user"), "")
        user_text = ("Pergunta anterior: " + previous + "\n" if previous else "") + question
        payload = {"model": MODEL, "messages": [{"role": "system", "content": system},
                   {"role": "user", "content": user_text}], "format": "json",
                   "stream": False, "think": False, "keep_alive": "5m",
                   "options": {"temperature": 0, "num_ctx": 1024,
                               "num_predict": 100, "num_thread": 4}}
        request = Request(URL, data=json.dumps(payload).encode(),
                          headers={"Content-Type": "application/json"}, method="POST")
        with LOCAL_HTTP.open(request, timeout=120) as response:
            result = json.load(response)
        intent = json.loads(result["message"]["content"])
        if not isinstance(intent, dict):
            raise ValueError("Plano da IA inválido")
        dataset = str(intent.get("dataset") or "")
        metric = str(intent.get("metric") or "")
        if ":" in metric:
            source, candidate = metric.split(":", 1)
            if source in SOURCES and candidate in SOURCES[source]["metrics"]:
                dataset, metric = source, candidate
        return {**scope, "dataset": dataset,
                "metric": metric,
                "operation": intent.get("operation", ""),
                "clarification": intent.get("clarification", "")}

    @staticmethod
    def _resolve_queue(raw: str, instance: str, catalog: list[dict]) -> tuple[str, str]:
        candidates = [row for row in catalog if not instance or str(row.get("instancia")) == instance]
        key = _plain(raw)
        exact = [row for row in candidates if _plain(row.get("codigo")) == key or _plain(row.get("nome")) == key]
        found = exact or [row for row in candidates if key and key in _plain(row.get("nome"))]
        unique = {(str(row.get("instancia")), str(row.get("codigo"))): row for row in found}
        if len(unique) != 1:
            raise ValueError("Informe o código da fila e a instância para uma consulta inequívoca")
        row = next(iter(unique.values()))
        return str(row["instancia"]), str(row["codigo"])

    def answer(self, question: str, *, allowed_instances: set[str] | None,
               queues: list[dict] = (), history: list[dict] = ()) -> dict:
        question = str(question or "").strip()
        if not 3 <= len(question) <= 1000:
            raise ValueError("Pergunta deve ter de 3 a 1.000 caracteres")
        if len(history) > 8:
            history = history[-8:]
        from toi_ai_extended import answer_extended
        extended = answer_extended(self.root, question, self._scope(question, history),
                                   allowed_instances, queues, self.now().astimezone(TZ))
        if extended is not None:
            return extended
        try:
            plan = self.planner(question, history)
        except (OSError, TimeoutError, KeyError, json.JSONDecodeError) as exc:
            raise RuntimeError("Modelo local de IA indisponível") from exc
        if not isinstance(plan, dict):
            raise ValueError("Plano da IA inválido")
        dataset = plan.get("dataset")
        if dataset == "clarify":
            return {"answer": str(plan.get("clarification") or "Especifique a fonte, o período e a entidade.")[:300],
                    "status": "CLARIFY", "evidence": []}
        if dataset not in SOURCES or plan.get("operation") not in OPERATIONS:
            raise ValueError("A pergunta não corresponde a uma medição consultável")
        metric = str(plan.get("metric") or "")
        if metric not in SOURCES[dataset]["metrics"]:
            raise ValueError("Métrica não reconhecida na fonte escolhida")
        if dataset == "realm" and allowed_instances is not None:
            raise PermissionError("Sessões dos SBCs exigem acesso a todas as instâncias")
        instance = str(plan.get("instance") or "").strip()
        if instance and (not instance.isdigit() or not 1 <= int(instance) <= 6):
            raise ValueError("Instância inválida")
        if instance and allowed_instances is not None and instance not in allowed_instances:
            raise PermissionError("Instância não autorizada")
        if dataset in ("queue", "lastcalls"):
            if not plan.get("queue"):
                raise ValueError("Informe o código ou nome da fila")
            instance, code = self._resolve_queue(str(plan["queue"]), instance, list(queues))
            if allowed_instances is not None and instance not in allowed_instances:
                raise PermissionError("Fila não autorizada")
        else:
            code = ""
        plan = dict(plan)
        if dataset == "lastcalls":
            plan["queue"] = code
        now = self.now().astimezone(TZ)
        start = _date_time(str(plan.get("start") or ""))
        end = _date_time(str(plan.get("end") or ""), end=True)
        if start is None and dataset != "realm":
            start = now - timedelta(days=7)
        if end is None:
            end = now
        if end > now and end.date() == now.date() + timedelta(days=1) and end.time() == day_time.min:
            end = now
        if start and (start >= end or end > now + timedelta(minutes=1)):
            raise ValueError("Período inválido")
        if start and end - start > timedelta(days=32) and dataset != "realm":
            raise ValueError("Consulte no máximo 32 dias por pergunta")
        if dataset == "realm":
            records, source = self._realm(plan, metric, start, end)
        elif dataset == "queue":
            records, source = self._queue(instance, code, metric, plan["operation"], start, end)
        else:
            records, source = self._sidecar(dataset, metric, plan, instance,
                                            allowed_instances, start, end)
        return self._response(question, plan, records, source, start, end,
                              instance=instance, code=code)

    @staticmethod
    def _aggregate(conn, table: str, metric: str, operation: str,
                   where: str, args: tuple) -> list[dict]:
        # Table and metric always come from fixed internal allowlists.
        if operation in ("peak", "minimum", "latest"):
            order = f"{metric} DESC, ts ASC" if operation == "peak" else (
                f"{metric} ASC, ts ASC" if operation == "minimum" else "ts DESC")
            sql = f"SELECT ts,{metric} AS value FROM {table} WHERE {where} AND {metric} IS NOT NULL ORDER BY {order} LIMIT 1"
        else:
            expression = {"average": f"AVG({metric})", "sum": f"SUM({metric})",
                          "count": f"COUNT({metric})"}[operation]
            sql = f"SELECT MIN(ts) AS first_at,MAX(ts) AS last_at,{expression} AS value,COUNT({metric}) AS samples FROM {table} WHERE {where}"
        rows = [dict(row) for row in conn.execute(sql, args).fetchall()]
        if operation in ("peak", "minimum", "latest") and rows:
            coverage = conn.execute(f"SELECT COUNT({metric}),MIN(ts),MAX(ts) FROM {table} WHERE {where} AND {metric} IS NOT NULL", args).fetchone()
            rows[0].update(samples=coverage[0], first_at=coverage[1], last_at=coverage[2])
        return rows

    def _queue(self, instance: str, code: str, metric: str, operation: str,
               start: datetime, end: datetime) -> tuple[list[dict], str]:
        source = "dados_toi/cache/toi_analytics_light_v1.sqlite:queue_light"
        sidecar = self.root / "dados_toi/cache/toi_analytics_light_v1.sqlite"
        recent = start >= self.now().astimezone(TZ) - timedelta(days=31)
        sidecar_metrics = {"p34_pct", "p135_pct", "hit_rate5", "hit_rate15", "calls_dialing",
                           "calls_in_attendance", "calls_last5", "buffer", "dialing_rate",
                           "agents_available", "agents_talking", "agents_acw", "agents_paused",
                           "agents_other", "agents_dialing"}
        if recent and metric in sidecar_metrics and sidecar.is_file():
            try:
                with closing(_readonly(sidecar)) as conn:
                    rows = self._aggregate(conn, "queue_light", metric, operation,
                        "instancia=? AND codigo=? AND ts>=? AND ts<?",
                        (instance, code, start.replace(tzinfo=None).isoformat(),
                         end.replace(tzinfo=None).isoformat()))
                return rows, source
            except sqlite3.OperationalError:
                pass  # An inaccessible or unhealthy cache must not hide LIVE data.
        source = "dados_toi/live/toi_live.sqlite:queue_snapshot (recorte por ROWID)"
        with closing(_readonly(self.root / "dados_toi/live/toi_live.sqlite")) as conn:
            lo = _rowid_lower_bound(conn, "queue_snapshot", start.replace(tzinfo=None).isoformat())
            hi = _rowid_lower_bound(conn, "queue_snapshot", end.replace(tzinfo=None).isoformat())
            rows = self._aggregate(conn, "queue_snapshot", metric, operation,
                "rowid>=? AND rowid<? AND instancia=? AND codigo=? AND ts>=? AND ts<?",
                (lo, hi, instance, code, start.replace(tzinfo=None).isoformat(),
                 end.replace(tzinfo=None).isoformat()))
        return rows, source

    def _realm(self, plan: dict, metric: str, start: datetime | None,
               end: datetime) -> tuple[list[dict], str]:
        from sbc_traffic_history import PAIRS
        requested = _plain(plan.get("realm"))
        matches = [(pair, name) for pair, cfg in PAIRS.items() for name in cfg["realms"]
                   if _plain(name) == requested]
        if len(matches) != 1:
            raise ValueError("Informe um realm válido pelo nome completo")
        pair, realm = matches[0]
        metrics = ("in_active", "out_active") if metric == "both_active" else (metric,)
        rows = []
        with closing(_readonly(self.root / "dados/sbc_sessions.sqlite")) as conn:
            for field in metrics:
                where = "c.pair=? AND r.realm=? AND c.source_ok=1 AND r.error IS NULL"
                args: list = [pair, realm]
                if start:
                    where += " AND c.ts>=?"; args.append(start.timestamp())
                where += " AND c.ts<?"; args.append(end.timestamp())
                op = str(plan["operation"])
                if op in ("peak", "minimum", "latest"):
                    order = (f"r.{field} DESC,c.ts ASC" if op == "peak" else
                             f"r.{field} ASC,c.ts ASC" if op == "minimum" else "c.ts DESC")
                    query = (f"SELECT c.ts AS stamp,r.{field} AS value FROM cycles c JOIN readings r ON r.cycle_id=c.id "
                             f"WHERE {where} AND r.{field} IS NOT NULL ORDER BY {order} LIMIT 1")
                    point = conn.execute(query, args).fetchone()
                    if point:
                        count = conn.execute(f"SELECT COUNT(r.{field}),MIN(c.ts),MAX(c.ts) FROM cycles c JOIN readings r ON r.cycle_id=c.id WHERE {where} AND r.{field} IS NOT NULL", args).fetchone()
                        rows.append({"metric": field, "value": point["value"],
                                     "ts": datetime.fromtimestamp(point["stamp"], TZ).isoformat(timespec="seconds"),
                                     "samples": count[0],
                                     "first_at": datetime.fromtimestamp(count[1], TZ).isoformat(timespec="seconds"),
                                     "last_at": datetime.fromtimestamp(count[2], TZ).isoformat(timespec="seconds")})
                else:
                    expr = {"average": f"AVG(r.{field})", "sum": f"SUM(r.{field})",
                            "count": f"COUNT(r.{field})"}[op]
                    point = conn.execute(f"SELECT {expr} AS value,COUNT(r.{field}) AS samples,MIN(c.ts) AS first_at,MAX(c.ts) AS last_at FROM cycles c JOIN readings r ON r.cycle_id=c.id WHERE {where}", args).fetchone()
                    if point["samples"]:
                        rows.append({"metric": field, "value": point["value"],
                                     "samples": point["samples"],
                                     "first_at": datetime.fromtimestamp(point["first_at"], TZ).isoformat(timespec="seconds"),
                                     "last_at": datetime.fromtimestamp(point["last_at"], TZ).isoformat(timespec="seconds")})
        return rows, "dados/sbc_sessions.sqlite:cycles+readings (ACLI, 15 s)"

    def _sidecar(self, dataset: str, metric: str, plan: dict, instance: str,
                 allowed: set[str] | None, start: datetime, end: datetime,
                 *, force_live=False) -> tuple[list[dict], str]:
        recent = start >= self.now().astimezone(TZ) - timedelta(days=31)
        sidecar = self.root / "dados_toi/cache/toi_analytics_light_v1.sqlite"
        live = force_live or not recent or not sidecar.is_file()
        table = ({"infra": "infra_snapshot", "lastcalls": "lastcalls_snapshot",
                  "stuck": "stuck_channels_snapshot"} if live else
                 {"infra": "infra_light", "lastcalls": "lastcalls_light",
                  "stuck": "stuck_light"})[dataset]
        where = ["ts>=?", "ts<?"]
        args: list = [start.replace(tzinfo=None).isoformat(), end.replace(tzinfo=None).isoformat()]
        if live:
            path = self.root / "dados_toi/live/toi_live.sqlite"
            with closing(_readonly(path)) as bounds:
                lo = _rowid_lower_bound(bounds, table, args[0])
                hi = _rowid_lower_bound(bounds, table, args[1])
            where.extend(("rowid>=?", "rowid<?")); args.extend((lo, hi))
            if dataset == "lastcalls":
                metric = "(CASE WHEN json_valid(raw_json) THEN COALESCE(json_array_length(raw_json,'$.lastCallsList'),0) END)"
        else:
            path = sidecar
        if instance:
            where.append("instancia=?"); args.append(instance)
        elif allowed is not None:
            if not allowed:
                raise PermissionError("Nenhuma instância autorizada")
            where.append("instancia IN (" + ",".join("?" for _ in allowed) + ")")
            args.extend(sorted(allowed))
        if dataset == "infra":
            if plan.get("infra_source"):
                source = str(plan["infra_source"])
                if source not in ("gateway", "trunk"):
                    raise ValueError("Fonte de infraestrutura inválida")
                where.append("source=?"); args.append(source)
            if plan.get("entity"):
                where.append("entity=?"); args.append(str(plan["entity"]).strip())
        if dataset == "lastcalls":
            where.append("codigo=?"); args.append(str(plan.get("queue")))
        try:
            with closing(_readonly(path)) as conn:
                rows = self._aggregate(conn, table, metric, str(plan["operation"]), " AND ".join(where), tuple(args))
        except sqlite3.OperationalError:
            if not live:
                return self._sidecar(dataset, str(plan["metric"]), plan, instance,
                                     allowed, start, end, force_live=True)
            raise
        source = ("dados_toi/live/toi_live.sqlite" if live else
                  "dados_toi/cache/toi_analytics_light_v1.sqlite")
        return rows, f"{source}:{table}" + (" (recorte por ROWID)" if live else " (32 dias)")

    def _response(self, question: str, plan: dict, records: list[dict], source: str,
                  start: datetime | None, end: datetime, *, instance: str, code: str) -> dict:
        dataset, metric, operation = plan["dataset"], plan["metric"], plan["operation"]
        valid = [row for row in records if row.get("value") is not None and row.get("samples")]
        if not valid:
            answer = "Não há dados válidos para responder a essa pergunta no período selecionado."
            status = "NO_DATA"
        else:
            parts = []
            for row in valid:
                label = SOURCES[dataset]["metrics"].get(row.get("metric", metric), "medição")
                text = f"{label}: {_display_number(row['value'])}"
                if row.get("ts"):
                    text += f" em {_display_time(row['ts'])}"
                parts.append(text)
            title = {"peak": "Pico", "minimum": "Mínimo",
                     "average": "Média", "sum": "Soma",
                     "latest": "Última medição", "count": "Amostras"}[operation]
            if dataset == "realm" and plan.get("realm"):
                title += f" do realm {plan['realm']}"
            elif dataset in ("queue", "lastcalls") and code:
                title += f" da fila {code}"
            elif instance:
                title += f" da instância {instance}"
            answer = title + ": " + "; ".join(parts) + "."
            if dataset == "realm" and metric == "both_active":
                answer += " As sessões de entrada e saída não são somadas."
            status = "OK"
        coverage = []
        for row in valid:
            coverage.append({"metric": row.get("metric", metric),
                             "samples": row.get("samples"),
                             "first": _display_time(row.get("first_at")),
                             "last": _display_time(row.get("last_at"))})
        return {"status": status, "answer": answer,
                "scope": {"dataset": dataset, "metric": metric, "operation": operation,
                          "instancia": instance or None, "codigo": code or None,
                          "realm": plan.get("realm") or None,
                          "start": start.isoformat() if start else None, "end": end.isoformat(),
                          "start_display": _display_time(start) if start else None,
                          "end_display": _display_time(end)},
                "coverage": coverage,
                "evidence": [{"source": source, "rows": records[:2]}],
                "model": MODEL, "local_only": True}
