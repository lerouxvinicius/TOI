"""Publish tracked TOI code over SSH, preserving private data and a Git rollback."""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
from io import BytesIO
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time
import zipfile

from remote_access import connect, run, sync_units, ROOT

LOCAL = Path(__file__).resolve().parents[1]


def service_states(client):
    states = {}
    for unit in ('toi-core.service', 'toi-sbc-live.service', 'toi-sbc-sessions.service', 'toi-sip-inventory.service', 'toi-gateway.service'):
        values = run(client, f'systemctl show -p LoadState -p ActiveState -p SubState {unit}')
        states[unit] = dict(line.split('=', 1) for line in values.splitlines() if '=' in line)
    return states


def save_audit(name, value):
    path = LOCAL.parent / 'outputs' / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    return path


def reload_dashboard(client, password, *, check=False, component='dashboard'):
    code = (LOCAL / 'deploy/reload_dashboard.py').read_text(encoding='utf-8')
    output = run(client, "sudo -S -p '' -u toi python3 -c " + shlex.quote(code) +
                 (' --check' if check else '') + (' --collector' if component == 'collector' else ''),
                 password, timeout_s=90)
    return json.loads(output)


GATEWAY_ROLLOUT_SCOPE = r'''import json, sqlite3, time
from contextlib import closing
from datetime import datetime
from pathlib import Path
root=Path('/var/www/toi')
app=root/'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'
scope=json.loads((app/'toi_demo_scope_v1.json').read_text())
catalog=json.loads((app/'portal_queue_catalog.json').read_text())
configured={str(q['instancia']) for q in scope['queues']}
configured.update(str(q['instancia']) for q in catalog.get('queues',[]) if all(
    str(q.get(key,'')).strip().isdigit() for key in ('instancia','codigo','projeto_id')))
instances=sorted(configured,key=int)
if not instances:
    raise RuntimeError('No configured instances to verify')
'''


GATEWAY_CADENCE_CHECK = GATEWAY_ROLLOUT_SCOPE + r'''
policy=json.loads((app/'toi_adaptive_policy_v1.json').read_text())
targets={}
for inst in instances:
    settings=dict(policy.get('infra_defaults') or {})
    settings.update(policy.get('instancias',{}).get(inst,{}).get('infra') or {})
    targets[inst]=float(settings.get('gateway_target_s',15))
if any(target!=10 for target in targets.values()):
    raise RuntimeError('Unexpected gateway targets: '+json.dumps(targets))
deadline=time.monotonic()+75
report={}
while time.monotonic()<deadline:
    with closing(sqlite3.connect((root/'dados_toi/live/toi_live.sqlite').as_uri()+'?mode=ro',uri=True,timeout=3)) as conn:
        conn.execute('PRAGMA query_only=ON')
        maximum=conn.execute('SELECT MAX(rowid) FROM infra_snapshot').fetchone()[0] or 0
        rows=conn.execute("SELECT instancia,ts FROM infra_snapshot NOT INDEXED WHERE rowid>=? AND source='gateway' AND entity!='__RAW__' ORDER BY rowid DESC",(max(0,maximum-12000),)).fetchall()
        last=conn.execute('SELECT MAX(rowid) FROM scheduler_health').fetchone()[0] or 0
        scheduled=conn.execute("SELECT instancia,scheduled_interval_s,ts FROM scheduler_health NOT INDEXED WHERE rowid>=? AND source='deadline_infra' AND codigo='gateway' AND state='OK' ORDER BY rowid DESC",(max(0,last-4000),)).fetchall()
        report={'instances':{},'all_verified':True}
        for inst in instances:
            stamps=sorted({ts for row_inst,ts in rows if str(row_inst)==inst},reverse=True)[:3]
            points=[datetime.fromisoformat(stamp).timestamp() for stamp in stamps]
            gaps=[round(a-b,2) for a,b in zip(points,points[1:])]
            scheduler=next(((target,ts) for row_inst,target,ts in scheduled if str(row_inst)==inst),None)
            age=time.time()-points[0] if points else None
            scheduler_age=time.time()-datetime.fromisoformat(scheduler[1]).timestamp() if scheduler else None
            valid=len(gaps)==2 and all(0<gap<=30 for gap in gaps) and age is not None and -5<=age<=30 and scheduler is not None and scheduler[0]==targets[inst] and -5<=scheduler_age<=30
            report['instances'][inst]={'configured_target_s':targets[inst],'scheduler_target_s':scheduler[0] if scheduler else None,
                'samples_at':stamps,'observed_gaps_s':gaps,'latest_age_s':round(age,2) if age is not None else None,
                'scheduler_age_s':round(scheduler_age,2) if scheduler_age is not None else None,'verified':valid}
            report['all_verified']=report['all_verified'] and valid
        if report['all_verified']:
            print(json.dumps(report));break
    time.sleep(3)
else:
    raise RuntimeError('Gateway cadence not confirmed: '+json.dumps(report))
'''


GATEWAY_SHADOW_CHECK = GATEWAY_ROLLOUT_SCOPE + r'''
path=root/'dados_toi/shadow/toi_gateway_health_shadow.sqlite'
deadline=time.monotonic()+75
verify_after=time.time()
report={}
while time.monotonic()<deadline:
    if path.is_file():
        try:
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=3)) as conn:
                conn.execute('PRAGMA query_only=ON')
                maximum=conn.execute('SELECT MAX(rowid) FROM gateway_health_sample_v5').fetchone()[0] or 0
                rows=conn.execute('SELECT instancia,MAX(ts) FROM gateway_health_sample_v5 NOT INDEXED WHERE rowid>=? GROUP BY instancia',(max(0,maximum-4000),)).fetchall()
                latest_by_instance={str(inst):ts for inst,ts in rows}
                report={'instances':{},'all_verified':True}
                for inst in instances:
                    latest=latest_by_instance.get(inst)
                    stamp=datetime.fromisoformat(latest).timestamp() if latest else None
                    age=time.time()-stamp if stamp is not None else None
                    valid=stamp is not None and stamp>=verify_after and -5<=age<=45
                    report['instances'][inst]={'technical_sample_at':latest,'age_s':round(age,2) if age is not None else None,'verified':valid}
                    report['all_verified']=report['all_verified'] and valid
                if report['all_verified']:
                    print(json.dumps(report));break
        except sqlite3.OperationalError as error:
            if not any(reason in str(error).lower() for reason in ('locked','busy','no such table')):
                raise
            report={'waiting_for_database':str(error),'all_verified':False}
    time.sleep(3)
else:
    raise RuntimeError('Gateway technical collection not confirmed: '+json.dumps(report))
'''


GATEWAY_TELEGRAM_PREFLIGHT = r'''import json, subprocess
from pathlib import Path
import requests
pid=int(subprocess.check_output(['systemctl','show','toi-core.service','-p','MainPID','--value'],text=True).strip())
if pid<=0: raise RuntimeError('TOI supervisor is unavailable')
env=dict(item.split(b'=',1) for item in Path('/proc/'+str(pid)+'/environ').read_bytes().split(b'\0') if b'=' in item)
token=env.get(b'TELEGRAM_BOT_TOKEN',b'').decode()
chat=env.get(b'TELEGRAM_CHAT_ID',b'').decode()
if not token or not chat: raise RuntimeError('Telegram bot/group not configured in the running TOI environment')
for method,parameters in [('getMe',{}),('getChat',{'chat_id':chat})]:
    try:
        doc=requests.post('https://api.telegram.org/bot'+token+'/'+method,json=parameters,timeout=(5,10)).json()
    except Exception:
        raise RuntimeError('Telegram read-only configuration check unavailable') from None
    if not isinstance(doc,dict) or doc.get('ok') is not True:
        raise RuntimeError('Telegram bot/group configuration rejected')
print(json.dumps({'configured':True,'bot_verified':True,'destination_verified':True,'secrets_disclosed':False}))
'''

GATEWAY_TELEGRAM_CHECK = r'''import json,sqlite3,time
from contextlib import closing
from pathlib import Path
started=time.time()
root=Path('/var/www/toi')
report={}
for attempt in range(25):
    status=json.loads((root/'TOI_v1.10.6_DUAL_LANE_ML_SHADOW/runtime/toi_supervisor_runtime_status_v1.json').read_text())
    from datetime import datetime
    def stamp(value):
        return datetime.fromisoformat(value.replace('Z','+00:00')).timestamp() if value else 0
    try:
        with closing(sqlite3.connect((root/'dados_toi/operational/toi_radar_v1.sqlite').as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
            row=db.execute("SELECT value FROM kv WHERE key='gateway:telegram:worker'").fetchone()
            worker=json.loads(row[0]) if row else {}
            # Match the actual supervisor child, not an old receipt/heartbeat.
            dashboard=status.get('components',{}).get('dashboard',{})
            pid=dashboard.get('pid')
            if worker.get('pid')!=pid or stamp(worker.get('heartbeat_at'))<started or not worker.get('configured') or worker.get('release')!='R1.2.13 RADAR GATEWAY ACTIVITY':
                time.sleep(2);continue
            sent=[]
            pending=[]
            for iid,kind,state,receipt,created,due in db.execute('SELECT incident_id,kind,state,receipt,created,due FROM gateway_telegram_outbox WHERE created>=?',(started-30,)):
                if state=='SENT':
                    evidence=json.loads(receipt or '{}')
                    if not evidence.get('message_ids') or not evidence.get('sent_at'): raise RuntimeError('Missing Telegram delivery receipt')
                    sent.append({'incident_id':iid,'kind':kind,'message_ids':evidence['message_ids'],'sent_at':evidence['sent_at']})
                elif state not in ('CANCELLED',):
                    pending.append({'incident_id':iid,'kind':kind,'state':state,'retry_at':due if state=='RETRY' else None})
            eligible=[]
            for body, in db.execute("SELECT payload FROM incidents WHERE source='GATEWAY' AND active=1"):
                rec=json.loads(body)
                if rec.get('evidence_available') is True and rec.get('status')=='ACTIVE' and -5<=time.time()-stamp(rec.get('last_sample_at'))<=45:
                    job=db.execute("SELECT state,receipt FROM gateway_telegram_outbox WHERE incident_id=? AND kind='OPENED'",(rec['id'],)).fetchone()
                    evidence=json.loads(job[1] or '{}') if job else {}
                    if not job or job[0]!='SENT' or not evidence.get('message_ids') or not evidence.get('sent_at'):
                        eligible.append(rec['id'])
                    elif not any(x['incident_id']==rec['id'] and x['kind']=='OPENED' for x in sent):
                        sent.append({'incident_id':rec['id'],'kind':'OPENED','message_ids':evidence['message_ids'],'sent_at':evidence['sent_at']})
            report={'configured':True,'worker_pid':pid,'worker_heartbeat_at':worker.get('heartbeat_at'),
                    'scope':'all_configured_instances','confirmed_deliveries':sent,'pending':pending,'eligible_without_receipt':eligible,
                    'delivery_confirmed':not eligible and not pending}
            if report['delivery_confirmed']:
                print(json.dumps(report));break
            if any(x['state'] in ('FAILED','UNCONFIRMED') for x in pending):
                print(json.dumps(report));break
    except sqlite3.OperationalError:
        pass
    time.sleep(2)
else:
    if not report.get('configured'): raise RuntimeError('Gateway Telegram worker not confirmed on the current dashboard')
    print(json.dumps(report))
'''


GATEWAY_ACTIVITY_CHECK = GATEWAY_ROLLOUT_SCOPE + r'''
from contextlib import closing
import sys
sys.path.insert(0,str(app))
from toi_dashboard_server_v1_10_6 import GatewayMonitorSource,DataService,gateway_monitor_name
from datetime import datetime
def stamp(value):
    return datetime.fromisoformat(value.replace('Z','+00:00')).timestamp() if value else 0
report={}
for attempt in range(25):
    with closing(sqlite3.connect((root/'dados_toi/operational/toi_radar_v1.sqlite').as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        current={}
        for key,body in db.execute("SELECT key,value FROM kv WHERE key>=? AND key<?",('gateway:activity:','gateway:activity:\uffff')):
            rest=key[len('gateway:activity:'):]
            inst,entity,counter=rest.split(':',2)
            value=json.loads(body)
            if inst in instances and counter=='PORTAL_VALID_BUSY' and -5<=time.time()-stamp(value.get('last_observed_at'))<=45:
                current.setdefault(inst,[]).append(entity)
        clock_samples=[]
        for body, in db.execute("SELECT payload FROM incidents WHERE source='GATEWAY' AND instancia='4' ORDER BY started DESC LIMIT 100"):
            rec=json.loads(body)
            if rec.get('kind')=='NO_TRAFFIC' and rec.get('entity')=='GWS845-DC1':
                clock_samples.append({key:rec.get(key) for key in ('entity','started_at','active','traffic_returned_at','resolved_at')});break
    monitor=GatewayMonitorSource()
    monitor.refresh()
    observed=monitor.snapshot('5')
    portal=DataService(root/'dados_toi/live/toi_live.sqlite').gateway_current_snapshot('5')
    matched=[row for row in observed.get('gateways',[]) if row['entity']=='GWS826-DC1']
    identity=bool(gateway_monitor_name('GWS826-DC1Y')=='GWS826-DC1' and len(matched)==1 and
        observed.get('status')=='CONSULTADO' and portal.get('status')=='LIVE' and
        any(row.get('entity')=='GWS826-DC1' for row in portal.get('gateways',[])))
    report={'instances':{inst:{'gateway_event_clocks':len(current.get(inst,[])),'verified':bool(current.get(inst))} for inst in instances},
        'v00826_identity_verified':identity,'known_incident_clock':clock_samples,
        'all_verified':identity and all(current.get(inst) for inst in instances)}
    if report['all_verified']:
        print(json.dumps(report));break
    time.sleep(2)
else:
    raise RuntimeError('Gateway event clocks/identity not confirmed: '+json.dumps(report))
'''


GATEWAY_IDENTITY_CHECK = r'''import json, sys, time
from pathlib import Path
root=Path('/var/www/toi')
sys.path.insert(0,str(root/'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'))
from toi_dashboard_server_v1_10_6 import GatewayMonitorSource,DataService,gateway_monitor_name
canonical='GWS803-DC1'
alias='GWS803-DC1'
if gateway_monitor_name(alias,'6')!=canonical or any(
    gateway_monitor_name(alias,inst)!=alias for inst in ('1','2','3','4','5')):
    raise RuntimeError('Instance-scoped gateway identity is incorrect')
monitor=GatewayMonitorSource()
service=DataService(root/'dados_toi/live/toi_live.sqlite')
service._gateway_monitor=monitor
report={}
for attempt in range(12):
    monitor.refresh()
    portal=service.gateway_current_snapshot('6')
    comparison=service.gateway_source_reconciliation('6',portal)
    matched=[row for row in comparison['rows'] if row['entity']==canonical]
    verified=bool(portal.get('status')=='LIVE' and comparison['monitor']['status']=='CONSULTADO'
        and len(matched)==1 and matched[0]['monitor_calls'] is not None
        and not any(flag in matched[0]['flags'] for flag in ('NOME_SEM_CORRESPONDENCIA','NOME_AMBIGUO'))
        and not any(row['entity'] in (alias,canonical) for row in comparison['unmatched_monitor']))
    report={'verified':verified,'instancia':'6','entity':canonical,'monitor_alias':alias,
        'instance_scoped':True,'portal_status':portal.get('status'),
        'monitor_status':comparison['monitor']['status'],'matched_rows':len(matched),
        'portal_valid_busy':matched[0]['portal_valid_busy'] if len(matched)==1 else None,
        'monitor_calls':matched[0]['monitor_calls'] if len(matched)==1 else None}
    if verified:
        print(json.dumps(report));break
    time.sleep(2)
else:
    raise RuntimeError('Gateway F00803/V00803 identity not confirmed: '+json.dumps(report))
'''


def backup_state(client):
    output = run(client, 'systemctl show toi-backup.service -p ActiveState -p Result -p InvocationID',
                 timeout_s=15)
    return dict(line.split('=', 1) for line in output.splitlines() if '=' in line)


def create_backup(client, password, *, timeout_s=1800):
    before = backup_state(client)
    already_running = before['ActiveState'] in ('active', 'activating')
    if not already_running:
        # The SSH channel must not wait on systemd's synchronous start job.
        run(client, "sudo -S -p '' systemctl start --no-block toi-backup.service",
            password, timeout_s=15)
    deadline = time.monotonic() + timeout_s
    next_progress = 0
    while time.monotonic() < deadline:
        state = backup_state(client)
        invoked = already_running or (state.get('InvocationID') and
                                      state['InvocationID'] != before.get('InvocationID'))
        if invoked and state['ActiveState'] not in ('active', 'activating'):
            if state['ActiveState'] != 'inactive' or state['Result'] != 'success':
                raise RuntimeError('Backup TOI não aprovado: ' + str(state))
            print(run(client, "sudo -S -p '' python3 -c " + shlex.quote(VERIFY_RECENT_BACKUP),
                      password, timeout_s=90).strip(), flush=True)
            return
        if time.monotonic() >= next_progress:
            print('Backup online em andamento; painel e coleta continuam ativos.', flush=True)
            next_progress = time.monotonic() + 30
        time.sleep(3)
    raise TimeoutError('Backup ainda não concluído; confira o serviço antes de retomar')

VERIFY_RECENT_BACKUP = '''from pathlib import Path
import hashlib, time
directory = Path("/var/backups/toi")
archives = sorted(directory.glob("toi-????????_??????.tar.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
for archive in archives:
    age = time.time() - archive.stat().st_mtime
    if not 0 <= age <= 12 * 3600:
        continue
    checksum = archive.with_suffix("").with_suffix(".sha256")
    fields = checksum.read_text(encoding="ascii").split()
    if len(fields) != 2 or fields[1] != archive.name:
        raise RuntimeError("Backup checksum manifest invalid")
    with archive.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if digest != fields[0]:
        raise RuntimeError("Backup checksum mismatch")
    print(f"Verified backup {archive.name}; age={age / 60:.0f} min")
    break
else:
    raise RuntimeError("No verified backup from the last twelve hours")
'''


def package() -> bytes:
    names = subprocess.check_output(["git", "-C", str(LOCAL), "ls-files", "-z"])
    paths = [name.decode() for name in names.split(b"\0") if name]
    if not paths:
        raise RuntimeError("No tracked files; commit the TOI project first")
    buf = BytesIO()
    manifest = {}
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in paths:
            path = LOCAL / name
            if not path.is_file() or path.is_symlink():
                raise RuntimeError(f"Missing or linked tracked file: {name}")
            data = path.read_bytes()
            manifest[name.replace("\\", "/")] = hashlib.sha256(data).hexdigest()
            archive.writestr(name.replace("\\", "/"), data)
        archive.writestr("__manifest__.json", json.dumps({"files": manifest}))
    print(f"Pacote de código: {len(paths)} arquivos, {len(buf.getvalue())} bytes")
    return buf.getvalue()


INCIDENT_OCCURRENCES_CHECK = r'''import json, os, sqlite3, sys, time
from contextlib import closing
from pathlib import Path
root=Path('/var/www/toi')
app=root/'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'
sys.path.insert(0,str(app))
from toi_operational_radar_v1 import RadarService, epoch
store=root/'dados_toi/operational/toi_radar_v1.sqlite'
directory=root/'relatorios/incident_history'
directory.mkdir(parents=True,exist_ok=True)
backup=directory/('radar_before_20260929_reconciliation_'+str(time.time_ns())+'.sqlite')
with closing(sqlite3.connect(store.as_uri()+'?mode=ro',uri=True,timeout=5)) as source, closing(sqlite3.connect(backup)) as target:
    source.backup(target)
    if target.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
        raise RuntimeError('Incident backup integrity failed')
os.chmod(backup,0o600)
radar=RadarService(app)
report=radar.reconcile_historical_incidents('2026-09-29T00:00:00-03:00',audit_key='operator_reconciliation_20260929_v1')
with radar.connect() as db:
    if db.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
        raise RuntimeError('Incident store integrity failed')
    if db.execute('SELECT COUNT(*) FROM incidents WHERE active=1 AND started<?',(epoch(report['before']),)).fetchone()[0]:
        raise RuntimeError('Historical incidents still active')
report.update(verified=True,backup=str(backup),release='R1.2.14 INCIDENT OCCURRENCES')
print(json.dumps(report))
'''


CALL_SEARCH_RESULT_PREFIX = 'TOI_CALL_SEARCH_RESULT_V2 '


def read_call_search_verification(output):
    """Only the framed receipt is data; Portal/XLS diagnostics may use stdout."""
    frames=[line[len(CALL_SEARCH_RESULT_PREFIX):] for line in output.splitlines()
            if line.startswith(CALL_SEARCH_RESULT_PREFIX)]
    if len(frames)!=1:
        raise RuntimeError('Verificação da busca de chamadas não retornou um comprovante único.')
    try:
        report=json.loads(frames[0])
    except (ValueError,TypeError):
        raise RuntimeError('Comprovante da busca de chamadas inválido.') from None
    if not isinstance(report,dict):
        raise RuntimeError('Comprovante da busca de chamadas inválido.')
    if report.get('verified') is not True:
        stage={'IMPORTS':'inicialização','PORTAL_EXPORT':'exportação do Portal','XLS_VALIDATION':'validação da planilha'}.get(report.get('stage'),'verificação')
        queue=report.get('queue')
        status=report.get('ipbx_status')
        detail=f' · Fila {queue} · Rel{status}' if queue in ('0247400046','0247400047') and status in ('21','16') else ''
        raise RuntimeError('Falha na busca de chamadas: '+stage+detail+'.')
    checks=report.get('checks')
    expected={(q,s) for q in ('0247400046','0247400047') for s in ('21','16')}
    valid=(isinstance(checks,list) and len(checks)==4 and all(
        isinstance(item,dict) and item.get('portal_export_verified') is True and
        item.get('queue') in ('0247400046','0247400047') and item.get('ipbx_status') in ('21','16') and
        isinstance(item.get('rows'),int) and not isinstance(item['rows'],bool) and item['rows']>=0
        for item in checks))
    if not valid or {(item.get('queue'),item.get('ipbx_status')) for item in checks}!=expected:
        raise RuntimeError('Verificação da busca de chamadas incompleta: são necessárias as duas filas e os dois status.')
    return report


CALL_SEARCH_CHECK = r'''import json, sys
from datetime import datetime, timedelta
from pathlib import Path
root=Path('/var/www/toi')
app=root/'TOI_v1.10.6_DUAL_LANE_ML_SHADOW'
sys.path.insert(0,str(app))
results=[]
stage='IMPORTS'
queue=status=None
try:
    from dotenv import load_dotenv
    load_dotenv(root/'private/toi.env')
    from toi_ipbx_blocked_calls_v1 import QUEUES, IPBX_STATUSES, fetch_blocked_calls, add_phone_column
    end=datetime.now()-timedelta(minutes=2)
    start=end-timedelta(minutes=1)
    for queue in QUEUES:
        for status in IPBX_STATUSES:
            stage='PORTAL_EXPORT'
            data,filename=fetch_blocked_calls(queue,start,end,'call-search-rollout-'+queue+'-'+status,ipbx_status=status)
            stage='XLS_VALIDATION'
            prepared,rows=add_phone_column(data,queue)
            results.append({'queue':queue,'ipbx_status':status,'rows':rows,'portal_export_verified':True})
    report={'verified':True,'checks':results,'verification_version':2}
except Exception as error:
    report={'verified':False,'checks':results,'stage':stage,'queue':queue,'ipbx_status':status,
            'error_type':type(error).__name__,'verification_version':2}
print('\nTOI_CALL_SEARCH_RESULT_V2 '+json.dumps(report))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--core-only", action="store_true",
                        help="Restart only toi-core when no other TOI service code changed")
    parser.add_argument("--audit-sbc", action="store_true",
                        help="Save a read-only SBC source audit after publishing")
    parser.add_argument("--sbc-sessions", action="store_true",
                        help="Enable and verify the isolated read-only ACLI session counter service")
    parser.add_argument('--dashboard-only', action='store_true',
                        help='Reload only the supervised dashboard, keeping acquisition running')
    parser.add_argument('--reload-collector', action='store_true',
                        help='With dashboard-only, reload the verified collector child to apply the 10s gateway policy')
    parser.add_argument('--start-gateway-shadow', action='store_true',
                        help='Start the existing technical gateway service and verify recent historical samples')
    parser.add_argument('--gateway-telegram', action='store_true',
                        help='Verify the existing Telegram destination and delivery worker for all gateway alerts')
    parser.add_argument('--gateway-activity', action='store_true',
                        help='Verify gateway counter transition clocks and the approved V00826 identity mapping')
    parser.add_argument('--gateway-identity', action='store_true',
                        help='Verify the approved F00803/V00803 identity in instance 6')
    parser.add_argument('--incident-occurrences', action='store_true',
                        help='Apply the operator-confirmed historical closure before 29/09/2026 with a separate online backup')
    parser.add_argument('--call-search', action='store_true',
                        help='Verify the Portal exports for both queues and IPBX statuses 21/16')
    parser.add_argument('--sip-inventory', action='store_true',
                        help='Enable and verify independent SIP server inventory collection')
    parser.add_argument("--reuse-verified-backup", action="store_true",
                        help="Reuse a checksum-verified backup from the last twelve hours")
    parser.add_argument("--stop-stalled-backup", action="store_true",
                        help="Stop the audited, stalled backup after verifying a completed one")
    args = parser.parse_args()
    if (args.reload_collector or args.start_gateway_shadow or args.gateway_telegram or args.gateway_activity or args.gateway_identity or args.incident_occurrences or args.call_search) and not args.dashboard_only:
        raise RuntimeError('Targeted gateway rollout requires --dashboard-only')
    if args.dashboard_only:
        args.core_only = True
    payload = package()
    client, password = connect()
    try:
        if run(client, f"git -C {ROOT} status --porcelain").strip():
            raise RuntimeError("Remote code has uncommitted changes; review them first")
        previous = run(client, f"git -C {ROOT} rev-parse HEAD").strip()
        states = service_states(client)
        save_audit('toi_sbc_publish_preflight.json', {
            'observed_at': datetime.now().astimezone().isoformat(),
            'commit': previous, 'services': states})
        print('Estado antes da publicação:', json.dumps(states, ensure_ascii=False))
        if args.dashboard_only:
            print('Pré-verificação do painel:', reload_dashboard(client, password, check=True))
            if args.reload_collector:
                print('Pré-verificação do coletor:', reload_dashboard(client, password, check=True, component='collector'))
        if args.gateway_telegram:
            telegram_config=json.loads(run(client,"sudo -S -p '' /var/www/toi/.venv/bin/python -c "+
                shlex.quote(GATEWAY_TELEGRAM_PREFLIGHT),password,timeout_s=45))
            print('Bot e destino Telegram verificados:',json.dumps(telegram_config))
        if args.sbc_sessions:
            # The existing SNMP source must be healthy before a counter-only
            # rollout; a failed prior rollout cannot silently leave it stopped.
            for unit in ('toi-core.service', 'toi-sbc-live.service'):
                if states[unit]['ActiveState'] != 'active':
                    raise RuntimeError(f'{unit}: serviço indisponível antes da publicação')
        if args.stop_stalled_backup and not args.reuse_verified_backup:
            raise RuntimeError("--stop-stalled-backup requires --reuse-verified-backup")
        if args.reuse_verified_backup:
            print(run(client, "sudo -S -p '' python3 -c " + shlex.quote(VERIFY_RECENT_BACKUP),
                      password, timeout_s=90).strip())
            if args.stop_stalled_backup:
                state = run(client, "systemctl show -p ActiveState --value toi-backup.service").strip()
                if state == "activating":
                    print("Parando backup comprovadamente preso antes da publicação...")
                    run(client, "sudo -S -p '' systemctl stop toi-backup.service", password,
                        timeout_s=60)
        else:
            print("Criando backup online antes da atualização...")
            create_backup(client, password)
        remote = f"{ROOT}/.incoming/release-{datetime.now():%Y%m%d_%H%M%S}.zip"
        sftp = client.open_sftp()
        try:
            try:
                sftp.mkdir(f"{ROOT}/.incoming")
            except OSError:
                pass
            sftp.chmod(f"{ROOT}/.incoming", 0o700)
            with sftp.open(remote, "wb") as target:
                target.write(payload)
        finally:
            sftp.close()
        services = ("toi-core.service" if args.core_only else
                    "toi-alerts.service toi-portal.service toi-gateway.service toi-core.service")
        if args.dashboard_only:
            services = ''
        sip_unit = 'toi-sip-inventory.service'
        had_sip_unit = (args.sip_inventory and
            run(client, f'systemctl show -p LoadState --value {sip_unit}').strip() == 'loaded')
        sip_active = (had_sip_unit and
            run(client, f'systemctl show -p ActiveState --value {sip_unit}').strip() == 'active')
        sbc_unit = "toi-sbc-live.service"
        sessions_unit = "toi-sbc-sessions.service"
        had_sessions_unit = (args.sbc_sessions and
            run(client, f"systemctl show -p LoadState --value {sessions_unit}").strip() == "loaded")
        sessions_active = (had_sessions_unit and
            run(client, f"systemctl show -p ActiveState --value {sessions_unit}").strip() == "active")
        had_sbc_unit = (not args.core_only and
                        run(client, f"systemctl show -p LoadState --value {sbc_unit}").strip() == "loaded")
        stopped_services = services + (f" {sbc_unit}" if had_sbc_unit else "")
        if sessions_active:
            stopped_services += f" {sessions_unit}"
        if sip_active:
            stopped_services += f' {sip_unit}'
        if stopped_services.strip():
            run(client, f"sudo -S -p '' systemctl stop {stopped_services}", password)
        collector_reload_attempted = False
        gateway_start_attempted = False
        try:
            run(client, f"python3 {ROOT}/deploy/apply_release.py {shlex.quote(remote)}")
            run(client, f"git -C {ROOT} add -A")
            changed = run(client, f"git -C {ROOT} diff --cached --name-only").strip()
            if args.dashboard_only:
                # Acquisition changes require an explicit verified collector reload.
                allowed = ('deploy/', 'tests/',
                           'TOI_v1.10.6_DUAL_LANE_ML_SHADOW/static/')
                exact = {'README.md', 'mgw/portal_sip.py',
                         'TOI_v1.10.6_DUAL_LANE_ML_SHADOW/sip_inventory.py',
                         'TOI_v1.10.6_DUAL_LANE_ML_SHADOW/toi_operational_radar_v1.py',
                         'TOI_v1.10.6_DUAL_LANE_ML_SHADOW/toi_dashboard_server_v1_10_6.py'}
                if args.call_search:
                    exact.add('TOI_v1.10.6_DUAL_LANE_ML_SHADOW/toi_ipbx_blocked_calls_v1.py')
                if args.reload_collector:
                    exact.add('TOI_v1.10.6_DUAL_LANE_ML_SHADOW/toi_adaptive_policy_v1.json')
                    exact.add('TOI_v1.10.6_DUAL_LANE_ML_SHADOW/toi_live_collector_v1_10_6.py')
                if args.start_gateway_shadow:
                    exact.add('TOI_v1.10.6.3.7.4.1_CONTINUOUS_ACTIVATION_R2/tools_readonly/toi_gateway_intelligence_shadow_v0_2_4_1_r2_1.py')
                unsafe = [p for p in changed.splitlines()
                          if p not in exact and not p.startswith(allowed)]
                if unsafe:
                    raise RuntimeError('Dashboard-only rollout includes other code: ' + ', '.join(unsafe))
            if changed:
                run(client, f"git -C {ROOT} -c user.name='TOI deployment' "
                    f"-c user.email='toi@localhost' commit -m 'Deploy TOI code'")
            sync_units(client, password)
            started_services = services
            if not args.core_only:
                run(client, f"sudo -S -p '' systemctl enable {sbc_unit}", password)
                started_services += f" {sbc_unit}"
            if started_services.strip():
                run(client, f"sudo -S -p '' systemctl start {started_services}", password)
            if args.dashboard_only:
                if args.reload_collector:
                    collector_reload_attempted = True
                    print('Coletor recarregado pelo supervisor:', reload_dashboard(client, password, component='collector'))
                reloaded = reload_dashboard(client, password)
                save_audit('toi_dashboard_reload.json', reloaded)
                print('Painel atualizado; coletor preservado:', reloaded)
                if args.reload_collector:
                    check = json.loads(run(client, "sudo -S -p '' -u toi python3 -c " +
                        shlex.quote(GATEWAY_CADENCE_CHECK), password, timeout_s=90))
                    save_audit('toi_gateway_cadence_verified.json', check)
                    print('Cadência dos gateways verificada:', json.dumps(check))
                if args.start_gateway_shadow:
                    gateway_start_attempted = True
                    operation='restart' if states.get('toi-gateway.service', {}).get('ActiveState')=='active' else 'start'
                    run(client, "sudo -S -p '' systemctl "+operation+" toi-gateway.service", password, timeout_s=45)
                    run(client, 'systemctl is-active --quiet toi-gateway.service')
                    technical = json.loads(run(client, "sudo -S -p '' -u toi python3 -c " +
                        shlex.quote(GATEWAY_SHADOW_CHECK), password, timeout_s=90))
                    save_audit('toi_gateway_technical_verified.json', technical)
                    print('Análise técnica dos gateways verificada:', json.dumps(technical))
            if args.sip_inventory:
                run(client, f"sudo -S -p '' systemctl enable --now {sip_unit}", password)
            if args.sbc_sessions:
                run(client, f"sudo -S -p '' systemctl enable --now {sessions_unit}", password)
            for attempt in range(15):
                try:
                    run(client, "curl --fail --silent --show-error --max-time 8 "
                        "http://127.0.0.1:8088/api/health")
                    break
                except RuntimeError:
                    if attempt == 14:
                        raise
                    time.sleep(2)
            if not args.core_only:
                for unit in ("toi-alerts", "toi-portal", "toi-gateway", "toi-core", "toi-sbc-live"):
                    run(client, f"systemctl is-active --quiet {unit}.service")
                for attempt in range(30):
                    try:
                        report = run(client, f"sudo -S -p '' -u toi python3 {ROOT}/deploy/check_sbc_live.py",
                                     password, timeout_s=10)
                        print("Coleta SBC independente:", report.strip())
                        break
                    except RuntimeError:
                        if attempt == 29:
                            raise
                        time.sleep(2)
            history_check = run(
                client, f"sudo -S -p '' -u toi python3 {ROOT}/deploy/check_sbc_traffic.py",
                password, timeout_s=60)
            print("Consulta histórica SBC Norte/Sul:", history_check.strip())
            if args.gateway_telegram:
                telegram_check=json.loads(run(client,"sudo -S -p '' -u toi python3 -c "+
                    shlex.quote(GATEWAY_TELEGRAM_CHECK),password,timeout_s=75))
                save_audit('toi_gateway_telegram_verified.json',telegram_check)
                print('Telegram dos gateways verificado:',json.dumps(telegram_check))
                if not telegram_check.get('delivery_confirmed'):
                    print('Envio ainda sem confirmação. A fila foi preservada; confira os estados acima no Radar.')
            if args.gateway_activity:
                activity_check=json.loads(run(client,"sudo -S -p '' -u toi /var/www/toi/.venv/bin/python -c "+
                    shlex.quote(GATEWAY_ACTIVITY_CHECK),password,timeout_s=75))
                save_audit('toi_gateway_activity_verified.json',activity_check)
                print('Horários dos gateways e identidade V00826 verificados:',json.dumps(activity_check))
            if args.incident_occurrences:
                incident_check=json.loads(run(client,"sudo -S -p '' -u toi /var/www/toi/.venv/bin/python -c "+
                    shlex.quote(INCIDENT_OCCURRENCES_CHECK),password,timeout_s=90))
                save_audit('toi_incident_occurrences_verified.json',incident_check)
                print('Cards e histórico de incidentes verificados:',json.dumps(incident_check))
            if args.gateway_identity:
                identity_check=json.loads(run(client,"sudo -S -p '' -u toi /var/www/toi/.venv/bin/python -c "+
                    shlex.quote(GATEWAY_IDENTITY_CHECK),password,timeout_s=60))
                save_audit('toi_gateway_identity_verified.json',identity_check)
                print('Identidade F00803/V00803 da Instância 6 verificada:',json.dumps(identity_check))
            if args.call_search:
                call_check=read_call_search_verification(run(client,"sudo -S -p '' -u toi /var/www/toi/.venv/bin/python -c "+
                    shlex.quote(CALL_SEARCH_CHECK),password,timeout_s=900))
                save_audit('toi_call_search_verified.json',call_check)
                print('Busca de chamadas Rel21/Rel16 nas duas filas verificada:',json.dumps(call_check))
            if args.sip_inventory:
                for attempt in range(15):
                    try:
                        check = run(client, f"sudo -S -p '' -u toi /var/www/toi/.venv/bin/python {ROOT}/deploy/check_instance_inventory.py",
                                    password, timeout_s=30)
                        save_audit('toi_instance_inventory_audit.json', json.loads(check))
                        print('Gateways e SIP Servers:', check.strip())
                        break
                    except RuntimeError:
                        if attempt == 14:
                            raise
                        time.sleep(3)
            if args.sbc_sessions:
                for attempt in range(24):
                    try:
                        run(client, f"systemctl is-active --quiet {sessions_unit}")
                        check = run(client, f"sudo -S -p '' -u toi python3 {ROOT}/deploy/check_sbc_sessions.py",
                                    password, timeout_s=20)
                        print("Contadores de novas sessões SBC:", check.strip())
                        session_audit = LOCAL.parent / "outputs" / "toi_sbc_sessions_audit.json"
                        session_audit.parent.mkdir(parents=True, exist_ok=True)
                        session_audit.write_text(json.dumps(json.loads(check), ensure_ascii=False, indent=2),
                                                 encoding="utf-8")
                        break
                    except RuntimeError:
                        if attempt == 23:
                            raise
                        time.sleep(3)
        except Exception:
            if gateway_start_attempted:
                try:
                    journal=run(client,"sudo -S -p '' journalctl -u toi-gateway.service -n 45 --no-pager -o short-iso",
                                password,timeout_s=20)
                    print('Registros da coleta técnica antes da reversão:\n'+journal)
                    save_audit('toi_gateway_technical_failure.json',{'journal':journal})
                except Exception:
                    pass
            if gateway_start_attempted and states.get('toi-gateway.service', {}).get('ActiveState') != 'active':
                run(client, "sudo -S -p '' systemctl stop toi-gateway.service", password, timeout_s=45)
            if args.sip_inventory:
                try:
                    journal = run(client, "sudo -S -p '' journalctl -u toi-sip-inventory.service "
                                  "-n 40 --no-pager -o short-iso", password, timeout_s=15)
                    save_audit('toi_sip_inventory_failure.json', {
                        'observed_at': datetime.now().astimezone().isoformat(),
                        'services': service_states(client), 'journal': journal})
                except Exception:
                    pass
                try:
                    run(client, f"sudo -S -p '' systemctl stop {sip_unit}", password)
                    if not had_sip_unit:
                        run(client, f"sudo -S -p '' systemctl disable {sip_unit}", password)
                except Exception:
                    pass
            if args.sbc_sessions:
                try:
                    journal = run(client, "sudo -S -p '' journalctl -u toi-sbc-sessions.service "
                                  "-n 40 --no-pager -o short-iso", password, timeout_s=15)
                    save_audit('toi_sbc_sessions_failure.json', {
                        'observed_at': datetime.now().astimezone().isoformat(),
                        'services': service_states(client), 'journal': journal})
                except Exception:
                    pass
            if args.sbc_sessions:
                try:
                    run(client, f"sudo -S -p '' systemctl stop {sessions_unit}", password)
                    if not had_sessions_unit:
                        run(client, f"sudo -S -p '' systemctl disable {sessions_unit}", password)
                except Exception:
                    pass
            if not args.core_only and not had_sbc_unit:
                try:
                    run(client, f"sudo -S -p '' systemctl disable --now {sbc_unit}", password)
                except Exception:
                    pass
            run(client, f"git -C {ROOT} reset --hard {shlex.quote(previous)}")
            sync_units(client, password)
            if gateway_start_attempted and states.get('toi-gateway.service', {}).get('ActiveState')=='active':
                run(client,"sudo -S -p '' systemctl restart toi-gateway.service",password,timeout_s=45)
            if stopped_services.strip():
                run(client, f"sudo -S -p '' systemctl start {stopped_services}", password)
            if args.dashboard_only:
                if collector_reload_attempted:
                    reload_dashboard(client, password, component='collector')
                reload_dashboard(client, password)
            for attempt in range(15):
                try:
                    run(client, 'curl --fail --silent --show-error --max-time 8 '
                                'http://127.0.0.1:8088/api/health')
                    break
                except RuntimeError:
                    if attempt == 14:
                        raise
                    time.sleep(2)
            restored = run(client, f'git -C {ROOT} rev-parse HEAD').strip()
            states = service_states(client)
            save_audit('toi_sbc_publish_rollback.json', {
                'observed_at': datetime.now().astimezone().isoformat(),
                'expected_commit': previous, 'restored_commit': restored, 'services': states})
            assert restored == previous, 'Rollback commit mismatch'
            for unit in ('toi-core.service', 'toi-sbc-live.service'):
                if states[unit]['ActiveState'] != 'active':
                    raise RuntimeError(f'{unit}: serviço indisponível após reversão')
            print('Reversão confirmada: código anterior, painel e coleta SNMP ativos.')
            raise
        finally:
            run(client, f"rm -f {shlex.quote(remote)}")
        print("Publicação concluída; dados e segredos preservados.")
        if args.audit_sbc:
            try:
                audit = json.loads(run(client, "sudo -S -p '' -u toi python3 "
                                       f"{ROOT}/deploy/audit_sbc_state.py", password))
                output = LOCAL.parent / "outputs" / "toi_sbc_source_audit.json"
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
                print("Auditoria das fontes SBC:", output)
            except Exception as exc:
                print("Auditoria SBC pós-publicação indisponível:", type(exc).__name__)
    finally:
        client.close()


if __name__ == "__main__":
    main()
