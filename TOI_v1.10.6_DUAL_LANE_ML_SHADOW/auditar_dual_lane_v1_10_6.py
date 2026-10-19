#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Auditoria operacional da v1.10.6 — Dual-Lane + Deadline Coalescing.

Somente lê o SQLite. Não acessa o Portal e não executa ação corretiva.
Critério principal de homologação da Instância 4:
- ALFA_COB_2 (0249700009): P95 do intervalo entre snapshots <= 30s
- ALFA_COB_3 (0249700010): P95 do intervalo entre snapshots <= 30s
- nenhuma fila crítica em lane duplicada
- integridade da grade: sem catch-up storm; coalescing é contabilizado, não reexecutado.
"""
from __future__ import annotations

import argparse, json, sqlite3, statistics
from datetime import datetime, timedelta
from pathlib import Path

APP=Path(__file__).resolve().parent
CRITICAL={"0249700009":"ALFA_COB_2","0249700010":"ALFA_COB_3"}


def p95(values):
    if not values: return None
    v=sorted(float(x) for x in values)
    return v[min(len(v)-1, max(0, round((len(v)-1)*0.95)))]


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", default=str((APP/"../dados_toi/live/toi_live.sqlite").resolve()))
    ap.add_argument("--policy", default=str(APP/"toi_adaptive_policy_v1.json"))
    ap.add_argument("--minutes", type=int, default=20)
    ap.add_argument("--json", default="")
    args=ap.parse_args()

    db=Path(args.db).resolve(); pol=Path(args.policy).resolve()
    policy=json.loads(pol.read_text(encoding="utf-8"))
    ip=((policy.get("instancias") or {}).get("4") or {})
    lanes=(ip.get("dual_lane") or {}).get("lanes") or {}
    lane_by_code={}
    duplicates=[]
    for lane,codes in lanes.items():
        for code in map(str,codes):
            if code in lane_by_code: duplicates.append(code)
            lane_by_code[code]=lane

    con=sqlite3.connect(str(db),timeout=3); con.row_factory=sqlite3.Row
    tables={r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "queue_snapshot" not in tables:
        print("ERRO: queue_snapshot ausente."); return 2

    max_ts=con.execute("SELECT MAX(ts) FROM queue_snapshot WHERE instancia='4'").fetchone()[0]
    if not max_ts:
        print("ERRO: sem snapshots da Instância 4."); return 2
    start=(datetime.fromisoformat(max_ts)-timedelta(minutes=args.minutes)).isoformat()

    qpol=ip.get("queues") or {}
    report={"version":"1.10.6","generated_at":datetime.now().astimezone().isoformat(timespec="seconds"),
            "window_minutes":args.minutes,"duplicates":duplicates,"queues":{},"lanes":{},"approved":False}

    print("="*112)
    print("TOI v1.10.6 — DUAL-LANE / DEADLINE COALESCING HEALTH — INSTÂNCIA 4")
    print(f"Janela: últimos {args.minutes} minutos")
    print("="*112)

    codes=list(qpol.keys())
    for code in codes:
        rows=con.execute("SELECT ts,latency_queue,latency_stats FROM queue_snapshot WHERE instancia='4' AND codigo=? AND ts>=? ORDER BY ts",(code,start)).fetchall()
        dts=[datetime.fromisoformat(r["ts"]) for r in rows]
        intervals=[(dts[i]-dts[i-1]).total_seconds() for i in range(1,len(dts))]
        durations=[float(r["latency_queue"] or 0)+float(r["latency_stats"] or 0) for r in rows]
        dl=[]
        if "dual_lane_health" in tables:
            dl=con.execute("SELECT duration_s,deadline_lag_s,coalesced_slots,result FROM dual_lane_health WHERE instancia='4' AND codigo=? AND ts>=? ORDER BY ts",(code,start)).fetchall()
        target=float((qpol.get(code) or {}).get("target_s",0))
        rec={
            "name":(qpol.get(code) or {}).get("nome",code),"lane":lane_by_code.get(code,"?"),"target_s":target,
            "samples":len(rows),"interval_mean_s":round(statistics.mean(intervals),2) if intervals else None,
            "interval_p95_s":round(p95(intervals),2) if intervals else None,"interval_max_s":round(max(intervals),2) if intervals else None,
            "duration_mean_s":round(statistics.mean(durations),2) if durations else None,"duration_p95_s":round(p95(durations),2) if durations else None,
            "deadline_lag_p95_s":round(p95([r["deadline_lag_s"] for r in dl]),2) if dl else None,
            "coalesced_slots":sum(int(r["coalesced_slots"] or 0) for r in dl),
            "errors":sum(1 for r in dl if r["result"] not in ("OK",)),
        }
        rec["critical_pass"]=(code not in CRITICAL) or (rec["interval_p95_s"] is not None and rec["interval_p95_s"]<=30.0)
        report["queues"][code]=rec
        print(f"{code} {rec['name'][:18]:18s} lane={rec['lane']} n={rec['samples']:3d} target={target:4.0f}s | "
              f"int média={rec['interval_mean_s']!s:>6} P95={rec['interval_p95_s']!s:>6} máx={rec['interval_max_s']!s:>6} | "
              f"GET P95={rec['duration_p95_s']!s:>6} | coalesced={rec['coalesced_slots']:3d} err={rec['errors']:2d}")

    if "dual_lane_health" in tables:
        for lane in lanes:
            rows=con.execute("SELECT target_s,duration_s,result FROM dual_lane_health WHERE instancia='4' AND lane=? AND ts>=?",(lane,start)).fetchall()
            ok=[r for r in rows if r["result"]=="OK"]
            # carga aproximada por job: soma da duração média por fila / target.
            load=0.0
            for code in lanes[lane]:
                rr=con.execute("SELECT AVG(duration_s),MAX(target_s) FROM dual_lane_health WHERE instancia='4' AND lane=? AND codigo=? AND ts>=? AND result='OK'",(lane,str(code),start)).fetchone()
                if rr and rr[0] is not None and rr[1]: load += float(rr[0])/float(rr[1])*100.0
            report["lanes"][lane]={"samples":len(rows),"ok":len(ok),"estimated_load_pct":round(load,1)}
            print(f"LANE {lane}: amostras={len(rows)} | OK={len(ok)} | carga estimada={load:.1f}%")

    critical_ok=all(report["queues"].get(c,{}).get("critical_pass") for c in CRITICAL)
    report["approved"]=bool(critical_ok and not duplicates)
    print("-"*112)
    print("CRITÉRIO COB_2/COB_3 P95 <= 30s:","APROVADO" if report["approved"] else "NÃO APROVADO")

    if args.json:
        rp=Path(args.json)
        if not rp.is_absolute(): rp=(APP/rp).resolve()
        rp.parent.mkdir(parents=True,exist_ok=True)
        rp.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
        print("Relatório:",rp)
    con.close()
    return 0 if report["approved"] else 3

if __name__=="__main__":
    raise SystemExit(main())
