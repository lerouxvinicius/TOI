#!/usr/bin/env python3
"""Agrega IPBX 19/21 por fila a partir do LIVE TOI, sem rede ou telefones na saída.

O dashboard atualiza o agregado ao receber novas amostras do Portal Orbe,
em uma janela móvel de 60 minutos. Requer phonenumbers na .venv do TOI.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    TZ = ZoneInfo("America/Sao_Paulo")
except ZoneInfoNotFoundError:
    # Python em Windows pode não trazer a base IANA. Instalar tzdata é recomendado.
    TZ = timezone(timedelta(hours=-3))
STATUS_RE = re.compile(r"^\s*(19|21)(?:\D|$)")
BR_DATE = ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S")
OPERADORAS = ("GAMA", "BETA", "ALFA")


def datetime_orbe(value):
    if not value:
        return None
    s = str(value).strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        dt = None
        for fmt in BR_DATE:
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        return None
    return dt.replace(tzinfo=TZ) if dt.tzinfo is None else dt.astimezone(TZ)


def normalizar(numero):
    digits = re.sub(r"[^0-9]", "", str(numero or ""))
    if digits.startswith("55") and len(digits) in (12, 13):
        digits = digits[2:]
    return digits if len(digits) in (10, 11) else None


def estimar_operadora(numero):
    """Retorna operadora estimada e origem; NÃO identifica a prestadora pós-portabilidade."""
    import phonenumbers
    from phonenumbers import carrier

    digits = normalizar(numero)
    if not digits:
        return "DESCONHECIDA", "formato_invalido"
    try:
        obj = phonenumbers.parse("+55" + digits, None)
    except phonenumbers.NumberParseException:
        return "DESCONHECIDA", "numero_invalido"
    if not phonenumbers.is_valid_number(obj):
        return "DESCONHECIDA", "numero_invalido"
    label = carrier.name_for_number(obj, "pt") or ""
    upper = label.upper()
    for op in OPERADORAS:
        if op in upper:
            return op, "metadados_prefixo"
    if label:
        return "OUTRA / DESCONHECIDA", "metadados_prefixo"
    # A regra fornecida pelo usuário é uma hipótese de prefixo, nunca um cadastro Regulador.
    if len(digits) == 11 and digits[2] == "9":
        hint = {**dict.fromkeys("897", "BETA"),
                **dict.fromkeys("123", "GAMA"),
                **dict.fromkeys("456", "ALFA")}.get(digits[3])
        if hint:
            return hint, "heuristica_prefixo_nao_verificada"
    return "OUTRA / DESCONHECIDA", "metadados_ausentes"


def percent(value, total):
    return round(100.0 * value / total, 2) if total else None


def analisar(db_path, end=None, limit=100, max_snapshots=5000, classifier=estimar_operadora, rolling=False):
    if not 1 <= limit <= 100 or max_snapshots < 1:
        raise ValueError("Limite por status e fila deve estar entre 1 e 100")
    end = (end or datetime.now(TZ)).astimezone(TZ)
    if not rolling:
        end = end.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    db_path = Path(db_path).resolve(strict=True)
    uri = db_path.as_uri() + "?mode=ro"
    source = sqlite3.connect(uri, uri=True, timeout=3)
    source.row_factory = sqlite3.Row
    queue_candidates = defaultdict(lambda: defaultdict(dict))
    seen_calls = set()
    snapshots = 0
    invalid_rows = 0
    missing_call_time = 0
    oldest_snapshot = None
    rows_examined = 0
    scope_snapshots = defaultdict(int)
    scope_latest = {}
    try:
        # Lê apenas o final do banco grande; se o limite truncar a hora, avisa na saída.
        rows = source.execute("""
            SELECT ts, instancia, codigo, raw_json FROM
            (SELECT rowid, ts, instancia, codigo, raw_json
             FROM lastcalls_snapshot ORDER BY rowid DESC LIMIT ?)
            ORDER BY rowid DESC
        """, (max_snapshots,))
        for row in rows:
            rows_examined += 1
            snapshot_dt = datetime_orbe(row["ts"])
            if snapshot_dt is None:
                invalid_rows += 1
                continue
            oldest_snapshot = snapshot_dt if oldest_snapshot is None else min(oldest_snapshot, snapshot_dt)
            scope_key = (str(row["instancia"]), str(row["codigo"]))
            scope_latest[scope_key] = max(scope_latest.get(scope_key, snapshot_dt), snapshot_dt)
            if not start <= snapshot_dt < end:
                continue
            snapshots += 1
            try:
                calls = json.loads(row["raw_json"] or "{}").get("lastCallsList", [])
            except (ValueError, TypeError, AttributeError):
                invalid_rows += 1
                continue
            if not isinstance(calls, list):
                invalid_rows += 1
                continue
            scope_snapshots[scope_key] += 1
            queue_candidates[scope_key]
            for call in calls:
                if not isinstance(call, dict):
                    continue
                # Latest snapshot wins for a call whose final status changed.
                call_id = str(call.get("IdCallIPBX") or "").strip()
                identity = (*scope_key, call_id) if call_id else None
                if identity in seen_calls:
                    continue
                if identity:
                    seen_calls.add(identity)
                match = STATUS_RE.match(str(call.get("NmStatusIpbx") or ""))
                if not match:
                    continue
                phone = normalizar(call.get("NuTelephone"))
                if not phone:
                    invalid_rows += 1
                    continue
                call_time = datetime_orbe(call.get("DtStart"))
                if call_time is None:
                    missing_call_time += 1
                    continue  # não atribuir chamadas antigas à hora errada
                if not start <= call_time < end:
                    continue
                status = match.group(1)
                call_id = str(call.get("IdCallIPBX") or "").strip()
                unique = (str(row["instancia"]), str(row["codigo"]),
                          call_id or (phone + call_time.isoformat()), status)
                if unique in seen_calls:
                    continue
                seen_calls.add(unique)
                queue_candidates[(str(row["instancia"]), str(row["codigo"]))][status][phone] = phone[:2]
    finally:
        source.close()

    # Cada fila recebe até 100 números distintos em cada status. A mesma pessoa
    # pode aparecer em duas filas; a classificação é calculada apenas uma vez.
    selected_by_scope = {}
    for scope_key, subset in queue_candidates.items():
        selected_by_scope[scope_key] = {
            status: set(sorted(subset[status],
                               key=lambda n: hashlib.sha256(n.encode("ascii")).digest())[:limit])
            for status in ("19", "21")
        }
    selected = set().union(*(phones for statuses in selected_by_scope.values()
                             for phones in statuses.values())) if selected_by_scope else set()
    classifications = {phone: classifier(phone) for phone in selected}

    def summarize(source_candidates, scope_selected):
        buckets = {}
        for status in ("19", "21"):
            eligible = set(source_candidates[status]) & scope_selected[status]
            breakdown = {op: {"quantidade": 0, "ddd": defaultdict(int),
                              "metodos": defaultdict(int)} for op in OPERADORAS}
            unknown = 0
            for phone in eligible:
                op, method = classifications[phone]
                if op not in breakdown:
                    unknown += 1
                    continue
                breakdown[op]["quantidade"] += 1
                breakdown[op]["ddd"][phone[:2]] += 1
                breakdown[op]["metodos"][method] += 1
            for op in OPERADORAS:
                entry = breakdown[op]
                entry["percentual_amostra"] = percent(entry["quantidade"], len(eligible))
                entry["ddd_percentual_na_operadora"] = {
                    ddd: percent(count, entry["quantidade"])
                    for ddd, count in sorted(entry["ddd"].items())
                }
                entry["ddd_percentual_na_amostra"] = {
                    ddd: percent(count, len(eligible))
                    for ddd, count in sorted(entry["ddd"].items())
                }
                entry["ddd"] = dict(sorted(entry["ddd"].items()))
                entry["metodos"] = dict(entry["metodos"])
            buckets[status] = {"numeros_elegiveis": len(source_candidates[status]),
                               "numeros_na_amostra": len(eligible),
                               "desconhecidos": unknown,
                               "operadoras": breakdown}
        return buckets

    for key in scope_latest:
        queue_candidates[key]
        selected_by_scope.setdefault(key, {"19": set(), "21": set()})
    queues = [{"instancia": inst, "codigo": code, "status": summarize(subset, selected_by_scope[(inst, code)]),
               "snapshots": scope_snapshots[(inst, code)],
               "fonte_ultima_coleta": scope_latest[(inst, code)].isoformat()}
              for (inst, code), subset in sorted(queue_candidates.items())]
    sample_total = sum(bucket["numeros_na_amostra"] for row in queues
                       for bucket in row["status"].values())
    eligible_total = sum(bucket["numeros_elegiveis"] for row in queues
                         for bucket in row["status"].values())
    truncated = (rows_examined >= max_snapshots and oldest_snapshot is not None
                 and oldest_snapshot > start)
    return {"schema_version": 2, "origem": "toi_live.sqlite:lastcalls_snapshot",
            "classificacao": "ESTIMATIVA_POR_PREFIXO; NAO_CONFIRMA_PORTABILIDADE",
            "janela_inicio": start.isoformat(), "janela_fim": end.isoformat(),
            "gerado_em": datetime.now(timezone.utc).isoformat(),
            "maximo_numeros_por_fila_status": limit,
            "numeros_elegiveis": eligible_total,
            "numeros_amostrados": sample_total,
            "numeros_distintos_classificados": len(selected),
            "cobertura_pct": percent(sample_total, eligible_total),
            "cobertura_referente_a": "numeros_observados_em_lastcalls_snapshot",
            "amostra_parcial": True,  # a própria tela Últimas Ligações pode limitar resultados
            "limite_numeros_atingido": eligible_total > sample_total,
            "snapshots_lidos_na_janela": snapshots,
            "limite_snapshots_possivelmente_truncado": truncated,
            "registros_invalidos": invalid_rows,
            "chamadas_sem_horario": missing_call_time,
            "filas": queues}


def payload_card(result, instancia, codigo, status, now=None):
    """Contrato já consumido por app.js/ipbxCard; sem números individuais."""
    status = str(status)
    if status not in ("19", "21"):
        raise ValueError("Status IPBX precisa ser 19 ou 21")
    scope = next((q for q in result["filas"]
                  if str(q["instancia"]) == str(instancia) and str(q["codigo"]) == str(codigo)), None)
    bucket = (scope or {}).get("status", {}).get(status)
    bucket = bucket or {"numeros_elegiveis": 0, "numeros_na_amostra": 0,
                        "desconhecidos": 0, "operadoras": {}}
    count = bucket["numeros_na_amostra"]
    carriers = []
    for op in OPERADORAS:
        entry = bucket["operadoras"].get(op, {})
        carriers.append({"name": op, "numbers": entry.get("quantidade", 0) if count else None,
                         "share": entry.get("percentual_amostra") if count else None,
                         "ddds": [{"ddd": ddd, "numbers": amount,
                                   "share": entry.get("ddd_percentual_na_operadora", {}).get(ddd, 0.0),
                                   "sample_share": entry.get("ddd_percentual_na_amostra", {}).get(ddd, 0.0)}
                                  for ddd, amount in sorted(entry.get("ddd", {}).items())],
                         "methods": entry.get("metodos", {})})
    now = now or datetime.now(TZ)
    finish = datetime_orbe(result.get("janela_fim"))
    stale = finish is not None and (now - finish).total_seconds() > 3900
    source_at = datetime_orbe((scope or {}).get("fonte_ultima_coleta"))
    source_stale = source_at is not None and finish is not None and (finish-source_at).total_seconds() > 600
    state = ("Fonte indisponível" if result.get("erro_fonte") else
             "Fonte desatualizada" if stale or source_stale else
             "Sem coleta de Últimas Ligações nesta hora" if not scope or not scope.get("snapshots") else
             "Sem chamadas deste status nesta hora" if not bucket["numeros_elegiveis"] else
             "Limite de snapshots: amostra truncada" if result.get("limite_snapshots_possivelmente_truncado") else
             "Sem números selecionados para esta fila" if not count else
             "Amostra parcial" if bucket["numeros_elegiveis"] > count else "Amostra disponível")
    return {"queue": str(codigo), "instance": str(instancia), "status": int(status),
            "carriers": carriers, "numbers": count,
            "eligible": bucket["numeros_elegiveis"],
            "unknown": bucket["desconhecidos"],
            "coverage": percent(count, bucket["numeros_elegiveis"]),
            "limit": result.get("maximo_numeros_por_fila_status", 100),
            "estimated": True,
            "state": state, "partial": bucket["numeros_elegiveis"] > count, "statuses": [],
            "quality": {"state": state, "at": (scope or {}).get("fonte_ultima_coleta")},
            "integration": "Estimativa local por prefixo — não confirma a operadora atual após portabilidade",
            "sample_at": result["janela_fim"] if not result.get("erro_fonte") else None,
            "window_start": result["janela_inicio"] if not result.get("erro_fonte") else None,
            "window_end": result["janela_fim"] if not result.get("erro_fonte") else None,
            "source_coverage_note": "Amostra de Últimas Ligações; pode não incluir todas as chamadas",
            "source_truncated": result["limite_snapshots_possivelmente_truncado"]}


def salvar_atomico(out, result):
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    temp = out.with_name(out.name + ".tmp." + str(os.getpid()))
    try:
        with temp.open("w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, out)
    finally:
        temp.unlink(missing_ok=True)


def vazio(error=None):
    end = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
    return {"schema_version": 2, "filas": [], "janela_inicio": (end-timedelta(hours=1)).isoformat(),
            "janela_fim": end.isoformat(), "gerado_em": datetime.now(TZ).isoformat(),
            "snapshots_lidos_na_janela": 0, "limite_snapshots_possivelmente_truncado": False,
            "erro_fonte": error, "numeros_amostrados": 0, "numeros_elegiveis": 0,
            "amostra_parcial": True}


def ler_resultado(path):
    try:
        result = json.loads(Path(path).read_text(encoding="utf-8"))
        if result.get("schema_version") != 2 or not isinstance(result.get("filas"), list):
            raise ValueError("Contrato inválido")
        if not all(datetime_orbe(result.get(k)) for k in ("janela_inicio", "janela_fim")):
            raise ValueError("Janela ausente")
        if "limite_snapshots_possivelmente_truncado" not in result:
            raise ValueError("Qualidade ausente")
        return result
    except (OSError, ValueError, AttributeError):
        return vazio("Agregado indisponível")


def executar(db, out, limit=100, max_snapshots=5000):
    """Process lock released by Windows on exit; completed hours are immutable."""
    import msvcrt
    target = Path(out)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.with_suffix(".lock").open("a+b") as lock:
        if lock.tell() == 0:
            lock.write(b"0"); lock.flush()
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return None
        try:
            previous = ler_resultado(target)
            closed = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
            if not previous.get("erro_fonte") and datetime_orbe(previous["janela_fim"]) == closed:
                return previous
            try:
                result = analisar(db, limit=limit, max_snapshots=max_snapshots)
            except (OSError, sqlite3.Error, ImportError):
                # Never retain old percentages under a new successful timestamp.
                salvar_atomico(target, vazio("Leitura da fonte indisponível"))
                raise
            salvar_atomico(target, result)
            return result
        finally:
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True, help="Caminho do toi_live.sqlite (somente leitura)")
    ap.add_argument("--out", required=True, help="JSON agregado consumido pelo TOI antigo")
    ap.add_argument("--limite", type=int, default=100)
    ap.add_argument("--max-snapshots", type=int, default=5000)
    args = ap.parse_args(argv)
    if not 1 <= args.limite <= 100 or args.max_snapshots < 1:
        ap.error("--limite deve ser de 1 a 100; --max-snapshots deve ser positivo")
    try:
        result = executar(args.db, args.out, limit=args.limite, max_snapshots=args.max_snapshots)
    except (OSError, sqlite3.Error, ImportError) as exc:
        print(f"Falha na análise de operadoras: {type(exc).__name__}", file=sys.stderr)
        return 1
    if result is None:
        print("Execução anterior em andamento")
        return 0
    print("Concluído: {numeros_amostrados}/{numeros_elegiveis} números; "
          "janela {janela_inicio} a {janela_fim}; amostra_parcial={amostra_parcial}".format(**result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
