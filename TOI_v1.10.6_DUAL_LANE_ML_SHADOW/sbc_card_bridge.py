"""Leitura local das fontes SBC para o TOI antigo (8088).

Sem SNMP, sem chamadas de rede e sem escrita nas bases de produção.
O dashboard consome o MESMO snapshot JSON utilizado pelo bot /sbc.
"""
from __future__ import annotations

import json
import math
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

MEMBROS = {
    'SBCs NORTE': ('10.90.11.132', '10.90.11.133'),
    'SBCs SUL': ('10.90.13.10', '10.90.13.11'),
}

AGENTES_MONITORADOS = {
    'SBCs NORTE': {
        '172.16.10.150': 'URA Orbe 1 (Logistica, Aquasul e PBH)',
        '172.16.11.150': 'URA Orbe 2 (Logistica, Aquasul e PBH)',
        '203.0.113.140': 'Link Alfa Logistica',
        '198.51.100.77': 'SIP TRUNK BLACK ROCK',
        '192.0.2.137': 'SIP TRUNK CLOUDCX TICKET',
        '198.18.1.43': 'SIP TRUNK CLOUDCX TICKET',
        '198.18.2.192': 'SIP TRUNK CLOUDCX TICKET',
        '198.18.2.68': 'SIP TRUNK CLOUDCX TICKET',
    },
    'SBCs SUL': {
        '10.90.15.211': 'SBC Beta Principal',
        '10.90.16.115': 'SBC Beta Backup',
        '198.18.3.69': 'SBC Epsilon Principal',
        '198.18.4.229': 'SBC Epsilon Backup',
    },
}


def _read_json(path):
    try:
        value = json.loads(Path(path).read_text(encoding='utf-8'))
        return (value, None) if isinstance(value, dict) else (None, 'JSON_NAO_OBJETO')
    except (OSError, UnicodeError, ValueError) as exc:
        return None, type(exc).__name__


def _epoch(value):
    if not value:
        return None
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            return time.mktime(stamp.timetuple()) + stamp.microsecond / 1e6
        return stamp.timestamp()
    except (OverflowError, TypeError, ValueError):
        return None


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value) if value is not None else None
        return result if result is not None and math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _events(path, *, limit=30):
    """Consulta apenas o pequeno sidecar de confirmação; nunca o LIVE do TOI."""
    if not path.exists():
        return [], 'BANCO_DE_CONFIRMACAO_AUSENTE'
    try:
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
            db.row_factory = sqlite3.Row
            cols = {row[1] for row in db.execute('PRAGMA table_info(event)')}
            required = {'id', 'entity', 'before_value', 'after_value',
                        'started_at', 'confirmed_at', 'payload'}
            if not required.issubset(cols):
                return [], 'ESQUEMA_DE_EVENTOS_DIFERENTE'
            sbc_rows = db.execute('''SELECT id,entity,before_value,after_value,
                started_at,confirmed_at,payload FROM event
                WHERE entity LIKE 'sbc:%'
                ORDER BY confirmed_at DESC LIMIT ?''', (min(int(limit), 100),)).fetchall()
            other_rows = db.execute('''SELECT id,entity,before_value,after_value,
                started_at,confirmed_at,payload FROM event
                WHERE entity LIKE 'mgw:%' OR entity LIKE 'vpn:%'
                ORDER BY confirmed_at DESC LIMIT ?''', (min(int(limit), 100),)).fetchall()
            events = []
            for row in sorted([*sbc_rows, *other_rows], key=lambda item: item['confirmed_at'], reverse=True):
                item = dict(row)
                try:
                    item['payload'] = json.loads(item['payload'])
                except (TypeError, ValueError):
                    item['payload'] = {}
                item['scope'] = 'SBC' if item['entity'].startswith('sbc:') else 'MONITOR_ALERTAS'
                events.append(item)
            return events, None
    except (OSError, sqlite3.DatabaseError, ValueError) as exc:
        return [], type(exc).__name__


def _active_peer_pairs(path):
    if not path.exists():
        return set()
    try:
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
            rows = db.execute('''SELECT s.entity,e.payload FROM entity_state s
                JOIN event e ON e.id=(SELECT id FROM event WHERE entity=s.entity
                    ORDER BY confirmed_at DESC LIMIT 1)
                WHERE s.entity LIKE 'sbc:peer_timeout:%' AND s.stable='ACTIVE' ''').fetchall()
        return {json.loads(payload).get('par') for _, payload in rows
                if json.loads(payload).get('source') == 'TRAP_REAL+ALARME_ATUAL_ACLI+HEALTH_ACLI'}
    except (OSError, sqlite3.DatabaseError, ValueError, TypeError):
        return set()


def card_data(monitor_root, *, now=None, stale_seconds=180, incident_limit=30):
    """Modelo para /api/sbc-monitor. Falhas da fonte são explícitas.

    O status HA aqui é o observado pelos OIDs da coleta, NÃO a confirmação
    do heartbeat peer do appliance. Essa diferença precisa aparecer no card.
    """
    base = Path(monitor_root).resolve()
    snapshot_path = base / 'dados' / 'status_ha_sbcs.json'
    sidecar_path = base / 'dados' / 'monitor_confirmacao_5min.sqlite'
    traps_path = base / 'dados' / 'status_traps_sbc.json'
    acli_path = base / 'dados' / 'sbc_peer_acli_state.json'
    config_path = base / 'sbc_traps_config.json'
    current = float(now if now is not None else time.time())
    snapshot, snapshot_error = _read_json(snapshot_path)
    traps, traps_error = _read_json(traps_path)
    acli, acli_error = _read_json(acli_path)
    config, config_error = _read_json(config_path)
    events, events_error = _events(sidecar_path, limit=incident_limit)
    active_peer_pairs = _active_peer_pairs(sidecar_path)
    stamp = _epoch((snapshot or {}).get('atualizado_em'))
    age = current - stamp if stamp is not None else None
    fresh = age is not None and -30 <= age <= stale_seconds
    mapping = (config or {}).get('peer_timed_out')
    mapping = mapping if isinstance(mapping, dict) else {}
    active_rule = mapping.get('active') if isinstance(mapping.get('active'), dict) else {}
    clear_rule = mapping.get('clear') if isinstance(mapping.get('clear'), dict) else {}
    mapping_active = (bool(active_rule.get('trap_oids')) and bool(clear_rule.get('trap_oids')) and
                      bool(active_rule.get('peer_varbind_oid')) and bool(clear_rule.get('peer_varbind_oid')))
    stored_pairs = (snapshot or {}).get('pares')
    if not isinstance(stored_pairs, dict):
        stored_pairs = {}
        if snapshot_error is None:
            snapshot_error = 'PARES_INVALIDOS'
    pairs = []
    acli_nodes = (acli or {}).get('nodes', {})
    if not isinstance(acli_nodes, dict):
        acli_nodes = {}
    incomplete = []
    for pair_name, ips in MEMBROS.items():
        slug = 'norte' if pair_name == 'SBCs NORTE' else 'sul'
        live_path = base / 'dados' / 'sbc_live' / (slug + '.json')
        live, live_error = _read_json(live_path)
        live_stamp = _epoch((live or {}).get('timestamp'))
        live_age = current - live_stamp if live_stamp is not None else None
        live_fresh = (live_age is not None and -5 <= live_age <= 45 and
                      bool((live or {}).get('source_ok')))
        live_members = (live or {}).get('nodes')
        live_nodes = {str(item.get('ip')): item for item in (live_members if isinstance(live_members, list) else [])
                      if isinstance(item, dict) and item.get('ip')}
        stored = stored_pairs.get(pair_name) or {}
        if not isinstance(stored, dict):
            stored = {}
        members = stored.get('membros')
        if not isinstance(members, list):
            members = []
            if fresh:
                incomplete.append(pair_name)
        known = {str(item.get('ip')): item for item in members
                 if isinstance(item, dict)}
        nodes = []
        for ip in ips:
            item = (live_nodes.get(ip, {}) if live_fresh else {}) if live is not None else (
                known.get(ip, {}) if fresh else {})
            if (live_fresh if live is not None else fresh) and not item:
                incomplete.append(ip)
            responding = (bool(item) and not item.get('error')) if live is not None else (
                bool(item.get('respondendo_snmp')) if item else False)
            metrics = {key: _number(item.get(key)) if responding else None for key in
                       ('health', 'cpu', 'memoria', 'sip_sessions_ativas', 'cps')}
            complete = responding and bool(item.get('hostname')) and item.get('ha') in ('ACTIVE', 'STANDBY') and all(
                value is not None for value in metrics.values())
            if fresh and responding and not complete:
                incomplete.append(ip)
            raw_agents = item.get('agents') if isinstance(item.get('agents'), dict) else {}
            agents = []
            for agent_ip, description in AGENTES_MONITORADOS[pair_name].items():
                agent = raw_agents.get(agent_ip) if responding else None
                agent = agent if isinstance(agent, dict) else None
                agents.append({
                    'ip': agent_ip, 'name': description,
                    'observed': agent is not None,
                    'status': str(agent.get('status_bruto')) if agent and agent.get('status_bruto') is not None else None,
                    'inbound_active': _number(agent.get('inbound_active')) if agent else None,
                    'outbound_active': _number(agent.get('outbound_active')) if agent else None,
                })
            nodes.append({
                'ip': ip, 'hostname': item.get('hostname') if responding else None,
                'role': item.get('ha') if responding else None,
                'snmp': ('OK' if complete else 'DADOS_INCOMPLETOS' if responding else
                         'SEM_DADOS' if not item else 'SEM_RESPOSTA'),
                'health_pct': metrics['health'],
                'cpu_pct': metrics['cpu'],
                'memory_pct': metrics['memoria'],
                'sip_sessions': metrics['sip_sessions_ativas'],
                'cps': metrics['cps'],
                'error': item.get('erro') if item else None,
                'agents': agents,
            })
        roles = sorted(n['role'] for n in nodes if n['snmp'] == 'OK' and n['role'])
        raw_ha = stored.get('integridade') if fresh else None
        # Recompute the observable status to prevent a green state from a
        # stale/inconsistent JSON file; this is still not peer heartbeat.
        observed = ('NORMAL_OBSERVADO' if roles == ['ACTIVE', 'STANDBY']
                    and len(nodes) == 2 and all(n['snmp'] == 'OK' for n in nodes)
                    else 'DEGRADADO_OBSERVADO' if 'ACTIVE' in roles else 'DESCONHECIDO')
        if not (live_fresh if live is not None else fresh):
            observed = 'SEM_DADOS_RECENTES'
        peer_reads = [acli_nodes.get(ip, {}) if isinstance(acli_nodes.get(ip), dict) else {} for ip in ips]
        readings_fresh = all((_epoch(item.get('collected_utc')) is not None and
                              0 <= current - _epoch(item.get('collected_utc')) <= 120)
                             for item in peer_reads)
        peer_active = pair_name in active_peer_pairs
        source_failed = any(item.get('error') for item in peer_reads) or (acli is not None and not readings_fresh)
        heartbeat = ('SEM_RESPOSTA_DA_FONTE' if source_failed else
                     'ALARME_ATIVO' if peer_active else
                     'VERIFICADO' if readings_fresh and all(
                         item.get('heartbeat_verified') is True and item.get('error') is None
                         and item.get('health_peer_identified') is True and
                         _epoch(item.get('heartbeat_verified_since')) is not None and
                         current - _epoch(item.get('heartbeat_verified_since')) >= 300
                         for item in peer_reads) else 'NAO_VERIFICADO')
        pairs.append({
            'name': pair_name, 'observed_ha': observed, 'source_ha': raw_ha,
            'snapshot_at': (live or {}).get('timestamp') if live is not None else (snapshot or {}).get('atualizado_em'),
            'age_seconds': (round(max(0, live_age), 1) if live_age is not None else None) if live is not None else age,
            'fresh': live_fresh if live is not None else fresh,
            'source': 'SBC SNMP independente' if live is not None else 'SBC monitor de alertas',
            'source_error': ((live or {}).get('error') or live_error) if live is not None else None,
            'peer_heartbeat': heartbeat, 'nodes': nodes,
            'peer_last_reading_utc': max((item.get('collected_utc') for item in peer_reads
                                          if item.get('collected_utc')), default=None),
            'peer_identity': ', '.join(item.get('peer_sysname') for item in peer_reads
                                       if item.get('peer_sysname')) or None,
            'last_switchover': stored.get('ultimo_switchover') if fresh else None,
        })
    errors = {key: err for key, err in (
        ('snapshot', snapshot_error), ('confirmation', events_error),
        ('traps', traps_error), ('trap_config', config_error), ('acli', acli_error)) if err}
    if age is None and snapshot_error is None:
        errors['snapshot_time'] = 'HORARIO_AUSENTE_OU_INVALIDO'
    elif age is not None and age < -30:
        errors['snapshot_time'] = 'HORARIO_NO_FUTURO'
    elif age is not None and age > stale_seconds:
        errors['snapshot_age'] = 'AMOSTRA_DESATUALIZADA'
    if incomplete:
        errors['snapshot_fields'] = 'DADOS_INCOMPLETOS:' + ','.join(incomplete)
    trap_sbcs = (traps or {}).get('sbcs')
    if traps is not None and not isinstance(trap_sbcs, dict):
        errors['traps'] = 'SBCS_INVALIDOS'
    trap_sbcs = trap_sbcs if isinstance(trap_sbcs, dict) else {}
    return {
        'source': 'dados/status_ha_sbcs.json', 'snapshot_at': (snapshot or {}).get('atualizado_em'),
        'age_seconds': round(max(0, age), 1) if age is not None else None,
        'fresh': fresh, 'stale_after_seconds': stale_seconds,
        'pairs': pairs, 'incidents': events,
        'trap_status': {
            'mapping': 'CONFIGURADO' if mapping_active else 'OIDS_NAO_CONFIGURADOS',
            'last_trap_by_sbc': {ip: (trap_sbcs.get(ip) or {}).get('ultimo_trap_em')
                                 if isinstance(trap_sbcs.get(ip), dict) else None
                                 for ips in MEMBROS.values() for ip in ips},
            'last_trap_is_peer_health': False,
        },
        'source_errors': errors,
        'generated_at_utc': datetime.now(timezone.utc).isoformat(),
    }
