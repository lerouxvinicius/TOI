#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Valida Discador TOI x Portal Orbe para uma fila — v1.10.6.

Regras:
- Cache diferente = TEMPORAL, nunca falha de fidelidade.
- Cache coincidente = exige igualdade do payload de campanhas e dos agregados.
- Somente GET após o login.

O validador usa exclusivamente campos expostos por getdialerstatisticsdata.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from dotenv import load_dotenv

import toi_collector_core_v3 as core

APP = Path(__file__).resolve().parent


def parse_pair(v):
    m = re.findall(r"\d{2}:\d{2}:\d{2}", str(v or ""))
    return tuple(m[-2:]) if len(m) >= 2 else (None, None)


def age_s(ts):
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(str(ts))
        if d.tzinfo is None:
            d = d.astimezone()
        return max(0.0, (datetime.now().astimezone() - d).total_seconds())
    except Exception:
        return None


def canonical_campaigns(doc):
    arr = (doc or {}).get("dialerStatisticsList")
    if not isinstance(arr, list):
        return []
    # Ordem pode mudar sem alterar os dados. Ordenamos por identificadores e depois
    # serializamos com chaves ordenadas para comparar o conteúdo lógico completo.
    def key(row):
        if not isinstance(row, dict):
            return ("", "", "")
        return (
            str(row.get("CodeNmCampaign") or ""),
            str(row.get("CodeNmAttendGroup") or ""),
            str(row.get("DeStatus") or ""),
        )
    return sorted(arr, key=key)


def summary(doc):
    arr = canonical_campaigns(doc)
    out = {
        "campaigns": len(arr),
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
        "blocked_total": 0,
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
    }
    fields = {
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
        "blocked_total": "QtBlockedTotal",
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
        for out_key, portal_key in fields.items():
            out[out_key] += int(core.parse_num(row.get(portal_key)) or 0)
        h = core.parse_num(row.get("VlHitRateLast15"))
        if h is not None:
            out["hit15_values"].append(float(h))
    out["hit15_avg"] = round(
        sum(out["hit15_values"]) / len(out["hit15_values"]), 6
    ) if out["hit15_values"] else 0.0
    del out["hit15_values"]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str((APP / "../dados_toi/live/toi_live.sqlite").resolve()))
    ap.add_argument("--instancia", required=True)
    ap.add_argument("--fila", required=True)
    ap.add_argument("--projeto", required=True)
    args = ap.parse_args()

    for p in (Path.cwd(), APP, APP.parent, APP.parent.parent):
        if (p / ".env").exists():
            load_dotenv(p / ".env")
            break

    user = (os.getenv("ORBE_LOGIN") or "").strip()
    pwd = os.getenv("ORBE_PASSWORD") or ""
    base = (os.getenv("ORBE_BASE_URL") or core.DEFAULT_BASE_URL).rstrip("/")
    if not user or not pwd:
        print("ERRO: credenciais ORBE ausentes.")
        return 2

    con = sqlite3.connect(str(Path(args.db).resolve()), timeout=3)
    con.row_factory = sqlite3.Row
    row = con.execute(
        """SELECT * FROM dialer_snapshot
           WHERE instancia=? AND codigo=?
           ORDER BY ts DESC LIMIT 1""",
        (str(args.instancia), str(args.fila)),
    ).fetchone()
    if not row:
        print("TOI ainda não possui snapshot de Discador para esta fila.")
        con.close()
        return 3

    try:
        toi_doc = json.loads(row["raw_json"] or "{}")
    except Exception:
        print("Snapshot TOI inválido.")
        con.close()
        return 3

    ps = core.PortalSession(args.instancia, user, pwd, base, timeout=90)
    ps.auth()
    params = {"pCodeQueue": args.fila, "pIdProject": args.projeto}
    ref = urljoin(ps.base, "metrics/dashboardqueuedetail") + (
        f"?pCodeQueue={args.fila}&pIdProject={args.projeto}"
    )
    portal, lat, _ = ps.get_json(
        "metrics/dashboardqueuedetail/getdialerstatisticsdata",
        params, ref, timeout=90, retries=0,
    )

    portal_cache = parse_pair(portal.get("dtCacheAndServerDialerStatisticsData"))
    toi_cache = parse_pair(toi_doc.get("dtCacheAndServerDialerStatisticsData"))
    # Primeiro relógio = cache/fonte. O segundo pode ser o instante de resposta do servidor.
    aligned = bool(portal_cache[0] and toi_cache[0] and portal_cache[0] == toi_cache[0])

    a = summary(portal)
    b = summary(toi_doc)
    payload_equal = canonical_campaigns(portal) == canonical_campaigns(toi_doc)
    toi_age = age_s(row["ts"])

    print("=" * 108)
    print("TOI v1.10.6 — VALIDAÇÃO DO DISCADOR x PORTAL ORBE")
    print("=" * 108)
    print(f"Fila          : inst{args.instancia} {args.fila}")
    print(f"GET Portal    : {lat:.2f}s")
    print(f"Snapshot TOI  : {row['ts']} | idade={toi_age:.1f}s" if toi_age is not None else f"Snapshot TOI  : {row['ts']}")
    print(f"Cache TOI     : {toi_cache}")
    print(f"Cache Portal  : {portal_cache}")
    print(f"Comparação    : {'ALINHADO' if aligned else 'TEMPORAL'}")
    print()

    ok = True
    print("Campo".ljust(30), "Portal".rjust(14), "TOI".rjust(14), "Resultado".rjust(18))
    print("-" * 80)
    for key in a:
        same = abs(float(a[key]) - float(b[key])) < 1e-6
        result = "OK ALINHADO" if (aligned and same) else ("FALHA REAL" if aligned else "TEMPORAL")
        if aligned and not same:
            ok = False
        print(key.ljust(30), str(a[key]).rjust(14), str(b[key]).rjust(14), result.rjust(18))

    print()
    if aligned:
        print(
            "Payload completo das campanhas: "
            + ("OK — conteúdo lógico idêntico" if payload_equal else "FALHA — conteúdo lógico diferente")
        )
        ok = ok and payload_equal
    else:
        print("Payload completo das campanhas: TEMPORAL — caches diferentes, comparação estrita não aplicada.")

    con.close()
    if not aligned:
        print("Resultado: TEMPORAL — repita até haver coincidência de cache; não é falha de fidelidade.")
        return 4
    print("Resultado:", "100% FIEL" if ok else "FALHA DE FIDELIDADE")
    return 0 if ok else 5


if __name__ == "__main__":
    raise SystemExit(main())
