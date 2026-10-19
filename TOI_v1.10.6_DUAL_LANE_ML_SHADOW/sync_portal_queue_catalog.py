"""Atualiza o catálogo de filas do TOI a partir do inventário autenticado do Portal Orbe.

Este catálogo é somente descoberta. Não modifica o escopo de aquisição homologado.
"""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

APP = Path(__file__).resolve().parent
ROOT = APP.parent
sys.path.insert(0, str(ROOT))
from monitor_portal_orbe_v1_gerencial import PortalOrbe  # noqa: E402


def main():
    load_dotenv(ROOT / '.env')
    login = os.getenv('ORBE_LOGIN')
    password = os.getenv('ORBE_PASSWORD')
    if not login or not password:
        raise RuntimeError('Credenciais do Portal Orbe ausentes')
    rows = []
    counts = {}
    for instancia in ('1', '2', '4', '5', '6'):
        client = PortalOrbe(instancia, login, password, timeout=20)
        client.autenticar()
        queues = client.listar_filas()
        counts[instancia] = len(queues)
        for queue in queues:
            rows.append({key: str(queue[key]).strip() for key in ('codigo', 'nome', 'projeto_id')}
                        | {'instancia': instancia})
    if len(rows) < 14 or not all(counts.values()):
        raise RuntimeError('Inventário incompleto; catálogo anterior preservado')
    doc = {'schema': 'toi_portal_queue_catalog_v1',
           'captured_at': datetime.now(timezone.utc).isoformat(),
           'counts': counts, 'queues': rows}
    path = APP / 'portal_queue_catalog.json'
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    temporary.replace(path)
    print(json.dumps({'captured_at': doc['captured_at'], 'counts': counts, 'total': len(rows)}))


if __name__ == '__main__':
    main()
