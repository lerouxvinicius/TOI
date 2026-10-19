"""Verify independent infrastructure and SIP inventory for every instance."""
from pathlib import Path
import json
import sys
import time

ROOT = Path('/var/www/toi')
APP = ROOT / 'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'
sys.path.insert(0, str(APP))
import toi_dashboard_server_v1_10_6 as server
from sip_inventory import inventory_report
server.ANALYTICS_DIAGNOSTIC_READONLY = True
service = server.DataService(ROOT / 'dados_toi/live/toi_live.sqlite')
start = time.monotonic()
catalog = service.queues()
elapsed = time.monotonic() - start
assert catalog and elapsed < 10, 'Queue catalogue missing or slow'
report = {'catalog_seconds': round(elapsed, 3), 'instances': {}}
for instance in sorted({str(row['instancia']) for row in catalog}):
    inventory = service.infra(instance, 15)
    age = service._age_seconds(inventory['meta']['gateway_ts'])
    assert age is not None and age <= 60, f'{instance}: gateway source stale'
    sip = inventory_report(ROOT, instance)
    assert sip['fresh'], f'{instance}: SIP Servers not fresh: {sip["status"]}'
    report['instances'][instance] = {'gateways': len(inventory['gateways']),
        'gateway_age_s': age, 'sip_servers': len(sip['items']), 'sip_age_s': sip['age_s']}
print(json.dumps(report, ensure_ascii=False))
