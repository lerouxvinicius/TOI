#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""TOI v1.10.6 — ML Shadow Mode v0.

Primeira inteligência de máquina: detecção não supervisionada por baseline robusto
(mediana + MAD) usando histórico real do próprio TOI. O processo é SOMENTE SOMBRA:
- não altera score operacional;
- não altera diagnóstico determinístico;
- não executa recomendação;
- não envia alerta;
- apenas grava predições/anomalias em ml_shadow_prediction.

O processo só inicia se o manifesto de freeze da aquisição existir e autorizar ML.
"""
from __future__ import annotations
import argparse, hashlib, json, math, sqlite3, statistics, time
from datetime import datetime, timedelta
from pathlib import Path

APP=Path(__file__).resolve().parent
FIELDS=("p34_pct","p135_pct","hit_rate15","calls_dialing","dialing_rate")

def sha256_file(path: Path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""):
            h.update(chunk)
    return h.hexdigest()

def verify_freeze_manifest(doc):
    """Revoga o freeze em runtime se qualquer arquivo de aquisição mudou."""
    if str(doc.get("version")) != "1.10.6":
        return False, "versão do manifesto diferente de 1.10.6"
    files=doc.get("files") or {}
    if not files:
        return False, "manifesto sem hashes da camada de aquisição"
    for name,meta in files.items():
        path=APP/name
        if not path.exists():
            return False, f"arquivo do freeze ausente: {name}"
        expected=str((meta or {}).get("sha256") or "").lower()
        if not expected:
            return False, f"hash ausente no manifesto: {name}"
        actual=sha256_file(path).lower()
        if actual != expected:
            return False, f"freeze revogado: {name} foi alterado após homologação"
    return True, "hashes da aquisição íntegros"

def median_abs_dev(vals):
    if not vals:return 0.0
    med=statistics.median(vals); return statistics.median([abs(x-med) for x in vals])

def robust_z(x, vals):
    if x is None or len(vals)<8:return 0.0
    med=statistics.median(vals); mad=median_abs_dev(vals)
    if mad<=1e-9:
        sd=statistics.pstdev(vals) if len(vals)>1 else 0
        return 0.0 if sd<=1e-9 else (x-med)/sd
    return 0.6745*(x-med)/mad

def clip(v,lo,hi):return max(lo,min(hi,v))

def ensure(con):
    con.executescript('''
    CREATE TABLE IF NOT EXISTS ml_shadow_prediction(
      ts TEXT,
      instancia TEXT,
      codigo TEXT,
      model_version TEXT,
      risk_score REAL,
      anomaly_score REAL,
      risk_class TEXT,
      p34_signal REAL,
      p135_signal REAL,
      capacity_signal REAL,
      explanation TEXT,
      features_json TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_ml_shadow ON ml_shadow_prediction(instancia,codigo,ts);
    '''); con.commit()

def latest_trunk(con,inst):
    row=con.execute("SELECT MAX(ts) FROM infra_snapshot WHERE instancia=? AND source='trunk'",(str(inst),)).fetchone()
    if not row or not row[0]:return None
    ts=row[0]
    try:
        age=(datetime.now().astimezone()-datetime.fromisoformat(ts)).total_seconds()
    except (TypeError,ValueError):
        return None
    if age < -30 or age > 135:
        return None
    rows=con.execute("SELECT occupancy,raw_json,entity FROM infra_snapshot WHERE instancia=? AND source='trunk' AND ts=?",(str(inst),ts)).fetchall()
    vals=[]
    for r in rows:
        if r[2] != '__RAW__': vals.append(float(r[0] or 0))
        else:
            try:
                doc=json.loads(r[1] or '{}'); arr=doc.get('trunkGrouplist') or doc.get('trunkGroupList') or []
                for x in arr: vals.append(float(str(x.get('VlPercValidsBusy') or 0).replace(',','.').replace('%','')))
            except Exception: pass
    return max(vals or [0.0])

def build_prediction(con,inst,code,history_hours,min_samples):
    last=con.execute("SELECT * FROM queue_snapshot WHERE instancia=? AND codigo=? ORDER BY ts DESC LIMIT 1",(str(inst),str(code))).fetchone()
    if not last:return None
    try:
        end=datetime.fromisoformat(last['ts'])
        age=(datetime.now().astimezone()-end).total_seconds()
    except (TypeError,ValueError):
        return None
    if age < -30 or age > 120 or any(last[f] is None for f in FIELDS):
        return None
    start=(end-timedelta(hours=history_hours)).isoformat()
    rows=con.execute(f"SELECT ts,{','.join(FIELDS)} FROM queue_snapshot WHERE instancia=? AND codigo=? AND ts>=? ORDER BY ts",(str(inst),str(code),start)).fetchall()
    if len(rows)<min_samples:return None
    base={f:[float(r[f]) for r in rows[:-1] if r[f] is not None] for f in FIELDS}
    if any(len(values)<min_samples-1 for values in base.values()):return None
    cur={f:float(last[f]) for f in FIELDS}
    z={f:robust_z(cur[f],base[f]) for f in FIELDS}

    # Direções relevantes: congestionamento/tentativas/taxa para cima; hit rate para baixo.
    p34_sig=max(0.0,z['p34_pct'])
    p135_sig=max(0.0,z['p135_pct'])
    hit_sig=max(0.0,-z['hit_rate15'])
    calls_sig=max(0.0,z['calls_dialing'])
    rate_sig=max(0.0,z['dialing_rate'])
    trunk=latest_trunk(con,inst)
    if trunk is None:return None
    cap_sig=clip((trunk-70.0)/10.0,0.0,4.0)

    anomaly=clip(max(p34_sig,p135_sig,hit_sig,calls_sig,rate_sig,cap_sig),0,8)
    # score em 0..100, ainda sem calibração de probabilidade — apenas ranking de risco shadow.
    weighted=0.28*p135_sig+0.18*p34_sig+0.17*hit_sig+0.15*calls_sig+0.07*rate_sig+0.15*cap_sig
    risk=round(clip(weighted/4.0*100.0,0,100),1)
    clazz='ALTO' if risk>=70 else 'MÉDIO' if risk>=40 else 'BAIXO'
    reasons=[]
    if p135_sig>=2:reasons.append(f'P135 acima do baseline ({p135_sig:.1f} MAD-z)')
    if p34_sig>=2:reasons.append(f'P34 acima do baseline ({p34_sig:.1f} MAD-z)')
    if hit_sig>=2:reasons.append(f'Hit Rate 15 abaixo do baseline ({hit_sig:.1f} MAD-z)')
    if calls_sig>=2:reasons.append(f'Discando acima do baseline ({calls_sig:.1f} MAD-z)')
    if cap_sig>=1:reasons.append(f'Pressão de troncos {trunk:.1f}%')
    if not reasons:reasons.append('Sem anomalia dominante no baseline robusto')
    features={"current":cur,"robust_z":{k:round(v,3) for k,v in z.items()},"trunk_max_pct":round(trunk,2),"samples":len(rows),"history_hours":history_hours,"source_ts":last['ts']}
    return (datetime.now().astimezone().isoformat(timespec='seconds'),str(inst),str(code),'robust_mad_shadow_v0',risk,round(anomaly,3),clazz,
            round(p34_sig,3),round(p135_sig,3),round(cap_sig,3),'; '.join(reasons),json.dumps(features,ensure_ascii=False))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--db',default=str((APP/'../dados_toi/live/toi_live.sqlite').resolve()))
    ap.add_argument('--freeze-marker',default=str(APP/'homologacao/acquisition_frozen_v1_10_6.json'))
    ap.add_argument('--interval-s',type=float,default=30)
    ap.add_argument('--history-hours',type=float,default=6)
    ap.add_argument('--min-samples',type=int,default=30)
    ap.add_argument('--once',action='store_true',help='Executa um ciclo e encerra.')
    args=ap.parse_args()
    marker=Path(args.freeze_marker)
    if not marker.exists():
        print('ML SHADOW BLOQUEADO: camada de aquisição ainda não foi congelada.'); return 3
    doc=json.loads(marker.read_text(encoding='utf-8'))
    if doc.get('status')!='FROZEN' or not doc.get('ml_shadow_authorized'):
        print('ML SHADOW BLOQUEADO: manifesto inválido/não autorizado.'); return 3
    freeze_ok,freeze_reason=verify_freeze_manifest(doc)
    if not freeze_ok:
        print(f'ML SHADOW BLOQUEADO: {freeze_reason}.'); return 3
    print(f'[FREEZE] OK — {freeze_reason}.')

    con=sqlite3.connect(str(Path(args.db).resolve()),timeout=5); con.row_factory=sqlite3.Row
    ensure(con)
    print('='*96); print('TOI v1.10.6 — ML SHADOW MODE — robust_mad_shadow_v0'); print('='*96)
    print('Somente observação: nenhuma predição altera diagnóstico/recomendação.')
    print('CTRL+C para encerrar.')
    last_source={}
    try:
        while True:
            queues=con.execute("SELECT instancia,codigo,MAX(ts) ts FROM queue_snapshot GROUP BY instancia,codigo ORDER BY CAST(instancia AS INT),codigo").fetchall()
            for q in queues:
                pred=build_prediction(con,q['instancia'],q['codigo'],args.history_hours,args.min_samples)
                if pred:
                    source_ts=json.loads(pred[11])["source_ts"]
                    key=(q['instancia'],q['codigo'])
                    if last_source.get(key)==source_ts:
                        continue
                    con.execute("INSERT INTO ml_shadow_prediction VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",pred)
                    last_source[key]=source_ts
                    print(f"[ML/SHADOW] inst{q['instancia']} {q['codigo']} risco={pred[4]:5.1f} {pred[6]:5s} | {pred[10]}")
            con.commit()
            if args.once: break
            time.sleep(max(10,args.interval_s))
    except KeyboardInterrupt:
        print('\nML Shadow encerrado.')
    finally: con.close()
    return 0
if __name__=='__main__': raise SystemExit(main())
