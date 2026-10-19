"""Adaptadores para o ciclo do Monitor_alertas; nunca altera o Portal Orbe."""
from datetime import datetime, timezone
import json
from pathlib import Path


def _hora(at):
    if at is None:
        return datetime.now(timezone.utc)
    if at.tzinfo is None:
        return at.astimezone().astimezone(timezone.utc)
    return at.astimezone(timezone.utc)


def observar_mgw(confirmador, status_consulta, rotas_atuais, rotas_anteriores,
                 status_servidores_anterior, *, at=None):
    """Chamar a cada consulta completa; nunca só ao detectar transições.

    Retorna eventos confirmados, mas o envio é recuperável por pending_deliveries.
    Se uma rota desapareceu da coleta ou o servidor caiu, não se infere recuperação.
    """
    stamp = _hora(at)
    eventos = []
    for nome, online in status_consulta.items():
        event = confirmador.observe('mgw:server:' + nome, bool(online), at=stamp,
                                    baseline=True,
                                    payload={'servidor': nome})
        if event:
            eventos.append(event)
    for key in set(rotas_anteriores) - set(rotas_atuais):
        confirmador.observe('mgw:route:' + str(key), None, at=stamp)
    for key, rota in rotas_atuais.items():
        nome = rota.get('servidor')
        if not status_consulta.get(nome):
            confirmador.observe('mgw:route:' + str(key), None, at=stamp)
            continue
        event = confirmador.observe('mgw:route:' + str(key), bool(rota['ativo']),
                                    at=stamp, baseline=True,
                                    payload={k: rota.get(k) for k in
                                             ('servidor', 'nap_id', 'nome', 'endereco')})
        if event:
            eventos.append(event)
    return eventos


def observar_sbcs(confirmador, resultados, pares, *, at=None, ignorar_agents=(),
                  limite_sip=1200, known_agents=None):
    """Adota HA e disponibilidade SNMP; alarmes 'peer timed out' exigem trap/syslog.

    Cada membro é observado mesmo quando o par continua com um ACTIVE.
    Ausência de resultado distingue 'desconhecido' de 'indisponível' apenas se o
    ciclo de coleta foi válido; não chame durante queda da VPN.
    """
    stamp = _hora(at)
    eventos = []
    for nome_par, membros in pares.items():
        active = []
        standby = []
        active_data = None
        all_ok = True
        current_agents = set()
        prior_agents = set((known_agents or {}).get(nome_par) or ())
        pending_new = {s['entity'] for s in confirmador.states('sbc:new_agent:' + nome_par + ':')}
        for ip in membros:
            dados = resultados.get(ip)
            confirmador.record_sbc(ip, nome_par, dados, at=stamp)
            ok = isinstance(dados, dict) and 'erro' not in dados
            all_ok = all_ok and ok
            event = confirmador.observe('sbc:snmp:' + ip, bool(ok), at=stamp,
                                        baseline=True, payload={'par': nome_par, 'ip': ip,
                                                                'erro': (dados or {}).get('erro') if isinstance(dados, dict) else 'Sem resultado'})
            if event:
                eventos.append(event)
            if ok and dados.get('ha') == 'ACTIVE':
                active.append(ip)
                active_data = dados
            if ok and dados.get('ha') == 'STANDBY':
                standby.append(ip)
            if ok:
                for agent_ip, agent in (dados.get('agents') or {}).items():
                    # Apenas o ACTIVE dá estado autoritativo dos agents.
                    if dados.get('ha') != 'ACTIVE':
                        continue
                    if agent_ip in ignorar_agents:
                        continue
                    current_agents.add(agent_ip)
                    raw = str(agent.get('status_bruto', ''))
                    if raw not in ('1', '3'):
                        confirmador.observe('sbc:agent:' + nome_par + ':' + agent_ip, None, at=stamp)
                        continue
                    event = confirmador.observe('sbc:agent:' + nome_par + ':' + agent_ip,
                                                raw, at=stamp, baseline='3',
                                                payload={'par': nome_par, 'agent': agent_ip, 'sbc': ip})
                    if event:
                        eventos.append(event)
                    if known_agents is not None:
                        new_entity = 'sbc:new_agent:' + nome_par + ':' + agent_ip
                        if agent_ip not in prior_agents or new_entity in pending_new:
                            event = confirmador.observe(new_entity, 'PRESENTE', at=stamp,
                                                        baseline='AUSENTE',
                                                        payload={'par': nome_par, 'agent': agent_ip})
                            if event:
                                eventos.append(event)
        for state in confirmador.states('sbc:agent:' + nome_par + ':'):
            if state['entity'].rsplit(':', 1)[-1] not in current_agents:
                confirmador.observe(state['entity'], None, at=stamp)
        for state in confirmador.states('sbc:new_agent:' + nome_par + ':'):
            if state['entity'].rsplit(':', 1)[-1] not in current_agents:
                confirmador.observe(state['entity'], None, at=stamp)
        # O status do par é independente da troca de ACTIVE. Um peer sem SNMP
        # impede declarar redundância preservada, mesmo com o outro ACTIVE.
        roles = [resultados.get(ip, {}).get('ha') for ip in membros
                 if isinstance(resultados.get(ip), dict) and 'erro' not in resultados[ip]]
        integrity = (None if any(role not in ('ACTIVE', 'STANDBY') for role in roles)
                     else 'NORMAL' if all_ok and len(active) == 1 and len(standby) == 1
                     else 'DEGRADADA' if len(active) == 1 else 'INDISPONIVEL')
        event = confirmador.observe('sbc:integrity:' + nome_par, integrity,
                                    at=stamp, baseline='NORMAL',
                                    payload={'par': nome_par, 'active': active[0] if active else '',
                                             'standby': standby[0] if standby else '',
                                             'membros': list(membros)})
        if event:
            eventos.append(event)
        if integrity == 'NORMAL':
            event = confirmador.observe('sbc:active:' + nome_par, active[0], at=stamp,
                                        payload={'par': nome_par, 'active': active[0],
                                                 'membros': list(membros)})
            if event:
                eventos.append(event)
        else:
            confirmador.observe('sbc:active:' + nome_par, None, at=stamp)
        if active_data:
            capacity = active_data.get('sip_sessions_ativas', active_data.get('sessoes'))
            if isinstance(capacity, (float, int)):
                state = 'ALTA' if capacity >= limite_sip else 'NORMAL'
                event = confirmador.observe('sbc:sip:' + nome_par, state,
                                            at=stamp, baseline='NORMAL',
                                            payload={'par': nome_par, 'sessoes': capacity,
                                                     'limite': limite_sip})
                if event:
                    eventos.append(event)
            else:
                confirmador.observe('sbc:sip:' + nome_par, None, at=stamp)
        else:
            confirmador.observe('sbc:sip:' + nome_par, None, at=stamp)
    return eventos


def suspender_amostras(confirmador):
    """Na perda da rede corporativa, interrompe os cinco minutos em andamento."""
    for state in confirmador.states():
        if state['entity'].startswith('vpn:'):
            continue
        confirmador.observe(state['entity'], None)


def observar_vpn(confirmador, rede_ok, *, ip=None, at=None):
    stamp = _hora(at)
    result = confirmador.observe('vpn:connectivity', bool(rede_ok), at=stamp,
                                 baseline=True, payload={'tipo': 'VPN'})
    if rede_ok and ip:
        confirmador.observe('vpn:ip', ip, at=stamp, payload={'ip': ip})
    elif not rede_ok:
        confirmador.observe('vpn:ip', None, at=stamp)
    return result


def observar_peer_timeout(confirmador, path, *, at=None, acli_path=None):
    """Confirm only fresh current alarms and identified peer, never a trap alone."""
    stamp = _hora(at)
    acli_path = Path(acli_path) if acli_path else Path(path).with_name('sbc_peer_acli_state.json')
    try:
        doc = json.loads(Path(path).read_text(encoding='utf-8'))
        alarms = doc.get('peer_timeout', {})
        if not isinstance(alarms, dict):
            alarms = {}
    except (OSError, ValueError):
        alarms = {}
    try:
        current = json.loads(acli_path.read_text(encoding='utf-8')).get('nodes', {})
        if not isinstance(current, dict):
            current = {}
    except (OSError, ValueError):
        current = {}
    events = []
    seen = set()
    states = {s['entity']: s for s in confirmador.states('sbc:peer_timeout:')}
    for key, alarm in alarms.items():
        if not isinstance(alarm, dict) or alarm.get('state') not in ('ACTIVE', 'CLEAR'):
            continue
        parts = key.split('|')
        if len(parts) != 3:
            continue
        pair, ip, peer = parts
        if (not peer or alarm.get('pair') != pair or alarm.get('ip') != ip or
                alarm.get('peer') != peer or alarm.get('source') != 'TRAP_REAL'):
            continue
        entity = 'sbc:peer_timeout:' + key
        seen.add(entity)
        reading = current.get(alarm.get('emitter_ip', ip), current.get(ip, {}))
        if not isinstance(reading, dict):
            reading = {}
        try:
            trap_at = datetime.fromisoformat(alarm['observed_at'])
            if trap_at.tzinfo is None:
                trap_at = trap_at.astimezone()
            trap_at = trap_at.astimezone(timezone.utc)
            observed = datetime.fromisoformat(reading['collected_utc']).astimezone(timezone.utc)
        except (KeyError, ValueError, TypeError):
            trap_at = observed = None
        previous = states.get(entity, {}).get('last_seen')
        identity_ok = (reading.get('ip') == alarm.get('emitter_ip', ip) and
                       reading.get('pair') == alarm.get('pair') and
                       reading.get('peer_ip') == alarm.get('peer_ip') and
                       reading.get('peer_sysname') == peer and
                       reading.get('health_peer_identified') is True and
                       reading.get('error') is None)
        readable = (identity_ok and observed is not None and observed <= stamp and
                    (stamp - observed).total_seconds() <= 120)
        if readable and previous and observed <= datetime.fromisoformat(previous):
            # Monitor cycles are more frequent than ACLI polls. Re-reading the
            # same snapshot is not a new sample, but must not cancel the window.
            continue
        fresh = readable
        prior = states.get(entity, {})
        started = prior.get('candidate') == 'ACTIVE' or prior.get('stable') == 'ACTIVE'
        trap_recent = trap_at is not None and trap_at <= stamp and (stamp - trap_at).total_seconds() <= 600
        if fresh and reading.get('alarm_active') is True and (trap_recent or started):
            value = 'ACTIVE'
        elif fresh and reading.get('alarm_active') is False and reading.get('heartbeat_verified') is True and started:
            value = 'CLEAR'
        else:
            value = None
        event = confirmador.observe(entity, value, at=observed if value else stamp,
                                    baseline='CLEAR', payload={'ip': ip, 'peer': peer,
                                                               'peer_ip': alarm.get('peer_ip'),
                                                               'par': alarm.get('pair'),
                                                               'sbc': alarm.get('sbc'),
                                                               'trap_oid': alarm.get('trap_oid'),
                                                               'acli_at': reading.get('collected_utc'),
                                                               'source': 'TRAP_REAL+ALARME_ATUAL_ACLI+HEALTH_ACLI'})
        if event:
            events.append(event)
    for state in confirmador.states('sbc:peer_timeout:'):
        if state['entity'] not in seen:
            confirmador.observe(state['entity'], None, at=stamp)
    return events


def formatar(event, escapar):
    e = event['entity']
    p = event['payload']
    inicio = datetime.fromisoformat(event['started_at']).astimezone().strftime('%d/%m/%Y %H:%M:%S')
    confirmacao = datetime.fromisoformat(event['confirmed_at']).astimezone().strftime('%d/%m/%Y %H:%M:%S')
    tail = f"\nInício: {inicio}\nConfirmado após 5 minutos: {confirmacao}"
    if e.startswith('sbc:peer_timeout:'):
        label = 'PEER TIMED OUT' if event['after'] == 'ACTIVE' else 'PEER TIMED OUT · CLEAR CONFIRMADO'
        return f"<b>TELECOM MONITOR — {label}</b>\nSBC: {escapar(p.get('ip',''))}\nPeer: {escapar(p.get('peer',''))}\nOID: {escapar(p.get('trap_oid',''))}" + tail
    if e.startswith('mgw:route:'):
        label = 'ROTA INATIVA' if event['after'] == 'False' else 'ROTA RESTABELECIDA'
        return f"<b>MGW MONITOR — {label}</b>\nServidor: {escapar(p.get('servidor',''))}\nRota: {escapar(p.get('nome',''))}\nEndereço: {escapar(p.get('endereco',''))}" + tail
    if e.startswith('mgw:server:'):
        label = 'SERVIDOR INACESSÍVEL' if event['after'] == 'False' else 'SERVIDOR RECUPERADO'
        return f"<b>MGW MONITOR — {label}</b>\nServidor: {escapar(p.get('servidor',''))}" + tail
    if e.startswith('sbc:agent:'):
        label = 'SESSION AGENT INDISPONÍVEL' if event['after'] == '1' else 'SESSION AGENT RESTABELECIDO'
        return f"<b>TELECOM MONITOR — {label}</b>\nPar: {escapar(p.get('par',''))}\nAgent: {escapar(p.get('agent',''))}\nSBC: {escapar(p.get('sbc',''))}" + tail
    if e.startswith('sbc:new_agent:'):
        return f"<b>TELECOM MONITOR — NOVO SESSION AGENT CONFIRMADO</b>\nPar: {escapar(p.get('par',''))}\nAgent: {escapar(p.get('agent',''))}" + tail
    if e.startswith('sbc:snmp:'):
        label = 'SBC SEM RESPOSTA SNMP' if event['after'] == 'False' else 'SBC SNMP RESTABELECIDO'
        return f"<b>TELECOM MONITOR — {label}</b>\nPar: {escapar(p.get('par',''))}\nSBC: {escapar(p.get('ip',''))}\nSNMP: {escapar(p.get('erro',''))}" + tail
    if e.startswith('sbc:active:'):
        return f"<b>TELECOM MONITOR — SWITCHOVER SBC</b>\nPar: {escapar(p.get('par',''))}\nACTIVE anterior: {escapar(event['before'])}\nACTIVE atual: {escapar(event['after'])}" + tail
    if e.startswith('sbc:integrity:'):
        return f"<b>TELECOM MONITOR — REDUNDÂNCIA SBC {escapar(event['after'])}</b>\nPar: {escapar(p.get('par',''))}\nACTIVE observado: {escapar(p.get('active',''))}\nSTANDBY observado: {escapar(p.get('standby',''))}\nSNMP isolado não comprova o estado do peer HA." + tail
    if e.startswith('sbc:sip:'):
        return f"<b>TELECOM MONITOR — CAPACIDADE SIP {escapar(event['after'])}</b>\nPar: {escapar(p.get('par',''))}\nSessões: {escapar(p.get('sessoes',''))} / limite {escapar(p.get('limite',''))}" + tail
    if e.startswith('vpn:connectivity'):
        return f"<b>TELECOM MONITOR — COMUNICAÇÃO {'RESTABELECIDA' if event['after']=='True' else 'INDISPONÍVEL'}</b>" + tail
    if e.startswith('vpn:ip'):
        return f"<b>TELECOM MONITOR — IP VPN ALTERADO</b>\nAnterior: {escapar(event['before'])}\nAtual: {escapar(event['after'])}" + tail
    raise ValueError('entidade sem formato Telegram: ' + e)


def enviar_pendentes(confirmador, telegram, *, ignorar_agents=()):
    """Marca entregue após sucesso; falhas ficam na caixa de saída para nova tentativa."""
    enviados = 0
    for event in confirmador.pending_deliveries():
        if event['entity'].startswith(('sbc:agent:', 'sbc:new_agent:')) and event['payload'].get('agent') in ignorar_agents:
            confirmador.mark_delivered(event['id'])
            continue
        mensagem = formatar(event, telegram.escapar)
        telegram.enviar_mensagem(mensagem)
        confirmador.mark_delivered(event['id'])
        enviados += 1
    return enviados
