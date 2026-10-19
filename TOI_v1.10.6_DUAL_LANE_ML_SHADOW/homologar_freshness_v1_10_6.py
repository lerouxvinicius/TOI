#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Quality Gate de freshness da aquisição v1.10.6.

Somente leitura do SQLite. Gera um artefato de homologação exigido pelo passo
de congelamento da camada de aquisição.

A v1.10.6 valida a JANELA solicitada de verdade (por padrão 20 minutos):
- idade da última amostra;
- P95 dos intervalos entre snapshots dentro da janela;
- cobertura mínima de amostras esperadas para ALFA_COB_2/ALFA_COB_3;
- freshness atual de Infra e IPBX.

O Discador permanece não bloqueante para o freeze do núcleo porque é uma fonte
background com cadência própria; seu estado é registrado para auditoria.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

APP = Path(__file__).resolve().parent
CRITICAL = ("0249700009", "0249700010")
CRITICAL_TARGET_S = 18.0
QUEUE_MAX_AGE_S = 40.0
QUEUE_INTERVAL_P95_LIMIT_S = 30.0
MIN_COVERAGE_RATIO = 0.60


def as_aware(ts):
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(str(ts))
    except Exception:
        return None
    if d.tzinfo is None:
        d = d.astimezone()
    return d


def age_s(ts, now=None):
    d = as_aware(ts)
    if d is None:
        return None
    n = now or datetime.now().astimezone()
    return max(0.0, (n - d).total_seconds())


def p95(values):
    if not values:
        return None
    xs = sorted(float(v) for v in values)
    # Mesmo critério discreto usado nos auditores do projeto.
    idx = min(len(xs) - 1, max(0, round((len(xs) - 1) * 0.95)))
    return xs[idx]


def table_exists(con, name):
    return bool(con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone())


def rows_in_window(con, instancia, codigo, start, end):
    # Fazemos a filtragem temporal em Python para não depender das particularidades
    # de comparação lexical/offset do SQLite. A consulta fica limitada a um volume
    # seguro e depois é recortada exatamente pela janela pedida.
    rows = con.execute(
        """SELECT ts FROM queue_snapshot
           WHERE instancia=? AND codigo=?
           ORDER BY ts DESC LIMIT 5000""",
        (str(instancia), str(codigo)),
    ).fetchall()
    selected = []
    for r in rows:
        d = as_aware(r["ts"])
        if d is None:
            continue
        if d < start:
            # Como a consulta está DESC, daqui em diante só haverá dados mais antigos.
            break
        if d <= end:
            selected.append((d, r["ts"]))
    selected.sort(key=lambda x: x[0])
    return selected


def latest_source(con, table, instancia, limit_s, now, extra_where="", params=()):
    if not table_exists(con, table):
        return {
            "latest_ts": None,
            "age_s": None,
            "limit_s": float(limit_s),
            "pass": False,
            "status": "SEM_DADOS",
        }
    sql = f"SELECT MAX(ts) FROM {table} WHERE instancia=?"
    bind = [str(instancia)]
    if extra_where:
        sql += " AND " + extra_where
        bind.extend(params)
    try:
        ts = con.execute(sql, tuple(bind)).fetchone()[0]
    except Exception:
        ts = None
    a = age_s(ts, now=now)
    ok = bool(a is not None and a <= float(limit_s))
    return {
        "latest_ts": ts,
        "age_s": round(a, 1) if a is not None else None,
        "limit_s": float(limit_s),
        "pass": ok,
        "status": "OK" if ok else ("SEM_DADOS" if a is None else "ATRASO"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str((APP / "../dados_toi/live/toi_live.sqlite").resolve()))
    ap.add_argument("--minutes", type=int, default=20)
    ap.add_argument("--output", default=str(APP / "homologacao/freshness_v1_10_6.json"))
    args = ap.parse_args()

    if args.minutes <= 0:
        raise SystemExit("--minutes deve ser maior que zero.")

    db = Path(args.db).resolve()
    out = Path(args.output).resolve()
    con = sqlite3.connect(str(db), timeout=3)
    con.row_factory = sqlite3.Row

    if not table_exists(con, "queue_snapshot"):
        con.close()
        raise SystemExit("Tabela queue_snapshot não encontrada no SQLite informado.")

    now = datetime.now().astimezone()
    start = now - timedelta(minutes=args.minutes)
    window_seconds = float(args.minutes) * 60.0
    expected_samples = max(1, math.floor(window_seconds / CRITICAL_TARGET_S))
    min_samples = max(3, math.floor(expected_samples * MIN_COVERAGE_RATIO))

    rep = {
        "version": "1.10.6",
        "generated_at": now.isoformat(timespec="seconds"),
        "db": str(db),
        "window_minutes": args.minutes,
        "window_start": start.isoformat(timespec="seconds"),
        "window_end": now.isoformat(timespec="seconds"),
        "criteria": {
            "critical_queues": list(CRITICAL),
            "target_s": CRITICAL_TARGET_S,
            "latest_age_max_s": QUEUE_MAX_AGE_S,
            "interval_p95_max_s": QUEUE_INTERVAL_P95_LIMIT_S,
            "minimum_coverage_ratio": MIN_COVERAGE_RATIO,
            "expected_samples_approx": expected_samples,
            "minimum_samples": min_samples,
        },
        "queues": {},
        "sources": {},
        "approved": False,
    }

    print("=" * 108)
    print("TOI v1.10.6 — QUALITY GATE DE FRESHNESS — JANELA REAL")
    print("=" * 108)
    print(f"Janela validada : últimos {args.minutes} min")
    print(f"Início          : {rep['window_start']}")
    print(f"Fim             : {rep['window_end']}")
    print(
        f"Critério filas  : idade <= {QUEUE_MAX_AGE_S:.0f}s | "
        f"P95 intervalo <= {QUEUE_INTERVAL_P95_LIMIT_S:.0f}s | "
        f"cobertura >= {MIN_COVERAGE_RATIO*100:.0f}% (~{min_samples}/{expected_samples} amostras)"
    )
    print()

    all_ok = True
    for code in CRITICAL:
        selected = rows_in_window(con, "4", code, start, now)
        times = [x[0] for x in selected]
        raw_ts = [x[1] for x in selected]
        intervals = [
            (times[i] - times[i - 1]).total_seconds()
            for i in range(1, len(times))
            if (times[i] - times[i - 1]).total_seconds() >= 0
        ]
        latest = raw_ts[-1] if raw_ts else None
        latest_age = age_s(latest, now=now)
        p95_interval = p95(intervals)
        coverage = len(selected) / expected_samples if expected_samples else 0.0

        rec = {
            "window_samples": len(selected),
            "expected_samples_approx": expected_samples,
            "minimum_samples": min_samples,
            "coverage_ratio": round(coverage, 4),
            "first_ts": raw_ts[0] if raw_ts else None,
            "latest_ts": latest,
            "latest_age_s": round(latest_age, 1) if latest_age is not None else None,
            "interval_mean_s": round(sum(intervals) / len(intervals), 1) if intervals else None,
            "interval_p95_s": round(p95_interval, 1) if p95_interval is not None else None,
            "interval_max_s": round(max(intervals), 1) if intervals else None,
        }
        rec["age_pass"] = bool(latest_age is not None and latest_age <= QUEUE_MAX_AGE_S)
        rec["interval_pass"] = bool(
            p95_interval is not None and p95_interval <= QUEUE_INTERVAL_P95_LIMIT_S
        )
        rec["coverage_pass"] = len(selected) >= min_samples
        rec["pass"] = bool(rec["age_pass"] and rec["interval_pass"] and rec["coverage_pass"])
        rep["queues"][code] = rec
        all_ok = all_ok and rec["pass"]

        print(
            f"Fila {code}: n={len(selected):3d}/{expected_samples} "
            f"({coverage*100:5.1f}%) | idade={rec['latest_age_s']}s | "
            f"P95={rec['interval_p95_s']}s | máx={rec['interval_max_s']}s | "
            f"{'OK' if rec['pass'] else 'FALHA'}"
        )

    rep["sources"]["infra"] = latest_source(
        con, "infra_snapshot", "4", 135, now
    )
    rep["sources"]["ipbx"] = latest_source(
        con, "stuck_channels_snapshot", "4", 45, now,
        extra_where="status='OK'",
    )

    # Discador é background/opcional para o freeze do núcleo. Registra estado e
    # freshness, mas NÃO reprova a homologação do fast-path.
    if table_exists(con, "dialer_snapshot"):
        d = latest_source(con, "dialer_snapshot", "4", 1200, now)
        d["blocking"] = False
        rep["sources"]["dialer"] = d
    else:
        rep["sources"]["dialer"] = {
            "latest_ts": None,
            "age_s": None,
            "limit_s": 1200.0,
            "pass": False,
            "blocking": False,
            "status": "SEM_DADOS",
        }

    for key in ("infra", "ipbx"):
        item = rep["sources"][key]
        all_ok = all_ok and item["pass"]
        print(
            f"{key.upper():5s}: idade={item['age_s']}s limite={item['limit_s']:.0f}s | "
            f"{'OK' if item['pass'] else 'FALHA'}"
        )

    d = rep["sources"]["dialer"]
    print(
        f"DISC : idade={d.get('age_s')}s limite={d.get('limit_s', 1200):.0f}s | "
        f"{'OK' if d.get('pass') else d.get('status','SEM_DADOS/ATRASO')} | não bloqueante"
    )

    rep["approved"] = bool(all_ok)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")

    print("-" * 108)
    print("FRESHNESS:", "APROVADA" if rep["approved"] else "NÃO APROVADA")
    print("Relatório:", out)
    con.close()
    return 0 if rep["approved"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
