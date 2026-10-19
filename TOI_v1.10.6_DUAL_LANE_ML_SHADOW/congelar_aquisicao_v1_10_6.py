#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Congela formalmente a camada de aquisição v1.10.6 após homologação.

Não altera coletor nem banco. Apenas cria um manifesto assinado por hashes SHA-256
quando os critérios obrigatórios estão aprovados.
"""
from __future__ import annotations
import argparse, hashlib, json
from datetime import datetime
from pathlib import Path

APP=Path(__file__).resolve().parent

def sha256(path:Path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
    return h.hexdigest()

def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--freshness',default=str(APP/'homologacao/freshness_v1_10_6.json'))
    ap.add_argument('--fidelity-cob2',default=str(APP/'homologacao/fidelidade_ALFA_COB_2_v1_10_6.json'))
    ap.add_argument('--fidelity-cob3',default=str(APP/'homologacao/fidelidade_ALFA_COB_3_v1_10_6.json'))
    ap.add_argument('--output',default=str(APP/'homologacao/acquisition_frozen_v1_10_6.json'))
    args=ap.parse_args()
    req={'freshness':Path(args.freshness),'fidelity_cob2':Path(args.fidelity_cob2),'fidelity_cob3':Path(args.fidelity_cob3)}
    missing=[k for k,p in req.items() if not p.exists()]
    if missing:
        print('NÃO CONGELADO. Relatórios ausentes:',', '.join(missing)); return 2
    docs={k:load(p) for k,p in req.items()}
    bad=[k for k,d in docs.items() if not bool(d.get('approved'))]
    if bad:
        print('NÃO CONGELADO. Homologações não aprovadas:',', '.join(bad)); return 3

    files=[APP/'toi_live_collector_v1_10_6.py',APP/'toi_collector_core_v3.py',APP/'toi_ipbx_readonly.py',APP/'toi_adaptive_policy_v1.json',APP/'toi_demo_scope_v1.json']
    manifest={
      'version':'1.10.6','status':'FROZEN','frozen_at':datetime.now().astimezone().isoformat(timespec='seconds'),
      'criteria':{
        'freshness':docs['freshness'],
        'fidelity_ALFA_COB_2':docs['fidelity_cob2'],
        'fidelity_ALFA_COB_3':docs['fidelity_cob3'],
      },
      'files':{p.name:{'sha256':sha256(p),'bytes':p.stat().st_size} for p in files},
      'rule':'Qualquer alteração em arquivos hashados revoga o freeze e exige nova homologação.',
      'ml_shadow_authorized':True,
    }
    out=Path(args.output); out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print('='*88); print('CAMADA DE AQUISIÇÃO v1.10.6 CONGELADA'); print('='*88)
    for p in files: print(f'{p.name:38s} {manifest["files"][p.name]["sha256"][:16]}...')
    print('ML Shadow Mode: AUTORIZADO'); print('Manifesto:',out)
    return 0
if __name__=='__main__': raise SystemExit(main())
