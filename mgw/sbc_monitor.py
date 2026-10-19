import asyncio
import ipaddress
import json
import socket
import time
from datetime import datetime
from pathlib import Path

from pysnmp.hlapi.v3arch.asyncio import (
    CommunityData,
    ContextData,
    ObjectIdentity,
    ObjectType,
    SnmpEngine,
    UdpTransportTarget,
    get_cmd,
    walk_cmd,
)


# ============================================================
# TELECOM MONITOR
# Monitoramento Integrado
#
# SBC
# ============================================================


PARES_HA_PADRAO = {
    "SBCs NORTE": [
        "10.90.11.132",
        "10.90.11.133",
    ],
    "SBCs SUL": [
        "10.90.13.10",
        "10.90.13.11",
    ],
}

SBCS = [
    ip
    for membros in PARES_HA_PADRAO.values()
    for ip in membros
]


CAPACIDADE_NOMINAL_POR_PAR = {
    "SBCs NORTE": 1600,
    "SBCs SUL": 8000,
}

LIMITE_OPERACIONAL_SIP = 1200


# ============================================================
# CAMINHOS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

ARQUIVO_EXCLUSOES = (
    BASE_DIR
    / "IPs NAO MONITORADOS.txt"
)

PASTA_DADOS = (
    BASE_DIR
    / "dados"
)

ARQUIVO_ESTADO = (
    PASTA_DADOS
    / "status_sbc.json"
)

ARQUIVO_AGENTS_CONHECIDOS = (
    PASTA_DADOS
    / "sbc_session_agents_conhecidos.json"
)


PASTA_LOGS = (
    BASE_DIR
    / "logs"
)

ARQUIVO_LOG_SNMP = (
    PASTA_LOGS
    / "sbc_monitor.log"
)

ARQUIVO_STATUS_SNMP = (
    PASTA_DADOS
    / "status_snmp_sbcs.json"
)


ARQUIVO_STATUS_HA = (
    PASTA_DADOS
    / "status_ha_sbcs.json"
)


# ============================================================
# OIDs GLOBAIS
# ============================================================

OID_SYSNAME = (
    "1.3.6.1.2.1.1.5.0"
)

OID_CPU = (
    "1.3.6.1.4.1.9148.3.2.1.1.1.0"
)

OID_MEMORIA = (
    "1.3.6.1.4.1.9148.3.2.1.1.2.0"
)

OID_HEALTH = (
    "1.3.6.1.4.1.9148.3.2.1.1.3.0"
)

OID_HA = (
    "1.3.6.1.4.1.9148.3.2.1.1.4.0"
)

OID_SESSOES = (
    "1.3.6.1.4.1.9148.3.2.1.1.5.0"
)

OID_CPS = (
    "1.3.6.1.4.1.9148.3.2.1.1.6.0"
)


# apH323StackCurrentCalls
# Uma entrada por H.323 stack. O total atual de chamadas H.323 é
# descontado de apSysGlobalConSess para obter SIP Sessions ativas.
OID_H323_STACK_CURRENT_CALLS = (
    "1.3.6.1.4.1.9148.3.10.1.1.1.2"
)


# ============================================================
# SESSION AGENTS
# ============================================================

BASE_AGENT = (
    "1.3.6.1.4.1.9148.3.2.1.2.1.1"
)

OID_AGENT_HOSTNAME = (
    f"{BASE_AGENT}.2"
)

OID_AGENT_STATUS = (
    f"{BASE_AGENT}.22"
)

# Agent Individual / show sipd agents
OID_AGENT_INBOUND_ACTIVE = (
    f"{BASE_AGENT}.4"
)

OID_AGENT_OUTBOUND_ACTIVE = (
    f"{BASE_AGENT}.6"
)

ARQUIVO_DIAGNOSTICO_AGENTS = (
    PASTA_DADOS
    / "sbc_session_agents_diagnostico.jsonl"
)


# ============================================================
# STATUS VALIDADO NO AMBIENTE
# ============================================================

STATUS_AGENT = {
    "1": {
        "codigo": "O",
        "descricao": "OUT OF SERVICE",
        "normal": False,
    },
    "3": {
        "codigo": "I",
        "descricao": "IN SERVICE",
        "normal": True,
    },
}


# ============================================================
# HELPERS
# ============================================================

def agora_iso():
    return datetime.now().isoformat(
        timespec="seconds"
    )


def formatar_data_hora(valor):
    if not valor:
        return "-"

    try:
        data = datetime.fromisoformat(
            valor
        )

        return data.strftime(
            "%d/%m/%Y %H:%M:%S"
        )

    except ValueError:
        return str(valor)


def formatar_duracao(segundos):
    segundos = max(
        0,
        int(segundos),
    )

    dias, resto = divmod(
        segundos,
        86400,
    )

    horas, resto = divmod(
        resto,
        3600,
    )

    minutos, segundos = divmod(
        resto,
        60,
    )

    partes = []

    if dias:
        partes.append(
            f"{dias} d"
        )

    if horas:
        partes.append(
            f"{horas} h"
        )

    if minutos:
        partes.append(
            f"{minutos} min"
        )

    partes.append(
        f"{segundos} s"
    )

    return " ".join(
        partes
    )


def _inteiro(valor, padrao=0):
    try:
        return int(str(valor).strip())
    except (TypeError, ValueError):
        return padrao




# ============================================================
# EXCLUSÕES
# ============================================================

def carregar_ips_excluidos():
    if not ARQUIVO_EXCLUSOES.exists():
        # The optional legacy exclusion list was not migrated to the clean
        # Linux installation. Its absence must not disable every SBC poll.
        print("Lista opcional de IPs excluídos ausente; nenhum SBC será excluído.")
        return set()

    ips = set()

    with ARQUIVO_EXCLUSOES.open(
        "r",
        encoding="utf-8",
    ) as arquivo:

        for linha in arquivo:
            texto = linha.strip()

            if not texto:
                continue

            try:
                ip = ipaddress.ip_address(
                    texto
                )

                if ip.version == 4:
                    ips.add(
                        str(ip)
                    )

            except ValueError:
                continue

    return ips


# ============================================================
# ESTADO PERSISTENTE
# ============================================================

def carregar_estado():
    if not ARQUIVO_ESTADO.exists():
        return {
            "inicializado": False,
            "active_anterior": None,
            "sbcs": {},
            "agents": {},
        }

    try:
        with ARQUIVO_ESTADO.open(
            "r",
            encoding="utf-8",
        ) as arquivo:

            dados = json.load(
                arquivo
            )

        dados.setdefault(
            "inicializado",
            False,
        )

        dados.setdefault(
            "active_anterior",
            None,
        )

        dados.setdefault(
            "sbcs",
            {},
        )

        dados.setdefault(
            "agents",
            {},
        )

        return dados

    except (
        OSError,
        json.JSONDecodeError,
    ):
        return {
            "inicializado": False,
            "active_anterior": None,
            "sbcs": {},
            "agents": {},
        }


def salvar_estado(estado):
    PASTA_DADOS.mkdir(
        parents=True,
        exist_ok=True,
    )

    estado["atualizado_em"] = (
        agora_iso()
    )

    temporario = (
        ARQUIVO_ESTADO
        .with_suffix(".tmp")
    )

    with temporario.open(
        "w",
        encoding="utf-8",
    ) as arquivo:

        json.dump(
            estado,
            arquivo,
            ensure_ascii=False,
            indent=4,
        )

    temporario.replace(
        ARQUIVO_ESTADO
    )


# ============================================================
# BASELINE DOS AGENTS
# ============================================================

def carregar_agents_conhecidos():
    if not ARQUIVO_AGENTS_CONHECIDOS.exists():
        return set()

    try:
        with ARQUIVO_AGENTS_CONHECIDOS.open(
            "r",
            encoding="utf-8",
        ) as arquivo:

            dados = json.load(
                arquivo
            )

        return set(
            dados.get(
                "session_agents",
                [],
            )
        )

    except (
        OSError,
        json.JSONDecodeError,
    ):
        return set()


def salvar_agents_conhecidos(
    agents,
):
    PASTA_DADOS.mkdir(
        parents=True,
        exist_ok=True,
    )

    dados = {
        "session_agents": sorted(
            agents
        )
    }

    with ARQUIVO_AGENTS_CONHECIDOS.open(
        "w",
        encoding="utf-8",
    ) as arquivo:

        json.dump(
            dados,
            arquivo,
            ensure_ascii=False,
            indent=4,
        )


# ============================================================
# HA
# ============================================================

def traduzir_ha(valor):
    mapa = {
        "1": "INITIAL",
        "2": "ACTIVE",
        "3": "STANDBY",
        "4": "OUT OF SERVICE",
    }

    valor = str(
        valor
    ).strip()

    return mapa.get(
        valor,
        f"DESCONHECIDO ({valor})",
    )


# ============================================================
# AUDITORIA SNMP
# ============================================================

NOMES_OID = {
    OID_SYSNAME: "SYSNAME",
    OID_CPU: "CPU",
    OID_MEMORIA: "MEMORIA",
    OID_HEALTH: "HEALTH",
    OID_HA: "HA",
    OID_SESSOES: "SESSOES_GLOBAIS",
    OID_CPS: "CPS",
    OID_H323_STACK_CURRENT_CALLS: "H323_CURRENT_CALLS",
    OID_AGENT_HOSTNAME: "SESSION_AGENT_HOSTNAME",
    OID_AGENT_STATUS: "SESSION_AGENT_STATUS",
    OID_AGENT_INBOUND_ACTIVE: "SESSION_AGENT_INBOUND_ACTIVE",
    OID_AGENT_OUTBOUND_ACTIVE: "SESSION_AGENT_OUTBOUND_ACTIVE",
}


def _nome_par_sbc(sbc):
    for nome_par, membros in PARES_HA_PADRAO.items():
        if sbc in membros:
            return nome_par
    return "PAR_DESCONHECIDO"


def _ip_origem_para_sbc(sbc):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect((sbc, 161))
        return sock.getsockname()[0]
    except OSError:
        return "DESCONHECIDO"
    finally:
        sock.close()


def registrar_log_snmp(
    evento,
    sbc,
    operacao="-",
    oid="-",
    erro="-",
    duracao_segundos=None,
    detalhes=None,
):
    PASTA_LOGS.mkdir(parents=True, exist_ok=True)

    partes = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        evento,
        _nome_par_sbc(sbc),
        sbc,
        f"origem={_ip_origem_para_sbc(sbc)}",
        f"operacao={operacao}",
        f"oid={oid}",
    ]

    if duracao_segundos is not None:
        partes.append(f"duracao={duracao_segundos:.3f}s")

    if erro and erro != "-":
        erro_limpo = str(erro).replace("\r", " ").replace("\n", " ").replace("|", "/")
        partes.append(f"erro={erro_limpo}")

    if detalhes:
        detalhes_limpos = str(detalhes).replace("\r", " ").replace("\n", " ").replace("|", "/")
        partes.append(f"detalhes={detalhes_limpos}")

    with ARQUIVO_LOG_SNMP.open("a", encoding="utf-8") as arquivo:
        arquivo.write(" | ".join(partes) + "\n")


def carregar_status_snmp():
    if not ARQUIVO_STATUS_SNMP.exists():
        return {"sbcs": {}}

    try:
        with ARQUIVO_STATUS_SNMP.open("r", encoding="utf-8") as arquivo:
            dados = json.load(arquivo)
        dados.setdefault("sbcs", {})
        return dados
    except (OSError, json.JSONDecodeError):
        return {"sbcs": {}}


def salvar_status_snmp(estado):
    PASTA_DADOS.mkdir(parents=True, exist_ok=True)
    estado["atualizado_em"] = datetime.now().isoformat(timespec="seconds")
    temporario = ARQUIVO_STATUS_SNMP.with_suffix(".tmp")

    with temporario.open("w", encoding="utf-8") as arquivo:
        json.dump(estado, arquivo, ensure_ascii=False, indent=4)

    temporario.replace(ARQUIVO_STATUS_SNMP)


def atualizar_status_snmp(estado, resultados):
    agora = datetime.now()
    agora_iso = agora.isoformat(timespec="seconds")
    sbcs_estado = estado.setdefault("sbcs", {})

    for sbc in SBCS:
        dados = resultados.get(sbc, {"erro": "Sem resultado"})
        anterior = sbcs_estado.get(sbc, {})
        estava_falhando = bool(anterior.get("em_falha", False))

        if "erro" in dados:
            falhas = int(anterior.get("falhas_consecutivas", 0)) + 1
            inicio_falha = anterior.get("inicio_falha") or agora_iso

            sbcs_estado[sbc] = {
                **anterior,
                "em_falha": True,
                "falhas_consecutivas": falhas,
                "inicio_falha": inicio_falha,
                "ultima_falha": agora_iso,
                "ultimo_erro": str(dados.get("erro", "Erro SNMP")),
            }

            if not estava_falhando:
                registrar_log_snmp(
                    evento="SNMP_INDISPONIVEL",
                    sbc=sbc,
                    erro=dados.get("erro", "Erro SNMP"),
                    detalhes="Primeiro ciclo com falha; sem alerta Telegram nesta fase.",
                )
            continue

        if estava_falhando:
            duracao = None
            inicio_texto = anterior.get("inicio_falha")
            if inicio_texto:
                try:
                    inicio = datetime.fromisoformat(inicio_texto)
                    duracao = (agora - inicio).total_seconds()
                except ValueError:
                    pass

            detalhes = "Comunicação SNMP restabelecida"
            if duracao is not None:
                detalhes += f"; indisponibilidade aproximada={duracao:.0f}s"

            registrar_log_snmp(
                evento="SNMP_RESTABELECIDO",
                sbc=sbc,
                detalhes=detalhes,
            )

        sbcs_estado[sbc] = {
            **anterior,
            "em_falha": False,
            "falhas_consecutivas": 0,
            "inicio_falha": None,
            "ultima_resposta_ok": agora_iso,
            "ultimo_erro": None,
        }

    salvar_status_snmp(estado)
    return estado


# ============================================================
# SNMP GET
# ============================================================

async def snmp_get(
    engine,
    community,
    sbc,
    oid,
):
    inicio = time.monotonic()
    nome_oid = NOMES_OID.get(oid, oid)

    try:
        transport = await UdpTransportTarget.create(
            (sbc, 161),
            timeout=4,
            retries=1,
        )

        (
            error_indication,
            error_status,
            error_index,
            var_binds,
        ) = await get_cmd(
            engine,
            CommunityData(community, mpModel=1),
            transport,
            ContextData(),
            ObjectType(ObjectIdentity(oid)),
        )

        if error_indication:
            raise RuntimeError(str(error_indication))

        if error_status:
            raise RuntimeError(error_status.prettyPrint())

        if not var_binds:
            raise RuntimeError("Resposta SNMP vazia.")

        return var_binds[0][1].prettyPrint()

    except Exception as erro:
        registrar_log_snmp(
            evento="SNMP_ERRO_OPERACAO",
            sbc=sbc,
            operacao="GET",
            oid=f"{nome_oid} ({oid})",
            erro=erro,
            duracao_segundos=time.monotonic() - inicio,
            detalhes="timeout=4s; retries=1",
        )
        raise


# ============================================================
# ÍNDICE SNMP
# ============================================================

def extrair_indice(
    oid_objeto,
    oid_base,
):
    try:
        oid_numerico = tuple(
            int(x)
            for x in oid_objeto
        )

        base_numerica = tuple(
            int(x)
            for x
            in oid_base.split(".")
        )

    except Exception:
        return None

    tamanho = len(
        base_numerica
    )

    if (
        oid_numerico[:tamanho]
        != base_numerica
    ):
        return None

    indice = oid_numerico[
        tamanho:
    ]

    if not indice:
        return None

    return ".".join(
        str(x)
        for x in indice
    )


# ============================================================
# SNMP WALK
# ============================================================

async def snmp_walk(
    engine,
    community,
    sbc,
    oid_base,
):
    resultados = {}
    inicio = time.monotonic()
    nome_oid = NOMES_OID.get(oid_base, oid_base)

    try:
        transport = await UdpTransportTarget.create(
            (sbc, 161),
            timeout=4,
            retries=1,
        )

        async for (
            error_indication,
            error_status,
            error_index,
            var_binds,
        ) in walk_cmd(
            engine,
            CommunityData(community, mpModel=1),
            transport,
            ContextData(),
            ObjectType(ObjectIdentity(oid_base)),
            lexicographicMode=False,
        ):
            if error_indication:
                raise RuntimeError(str(error_indication))

            if error_status:
                raise RuntimeError(error_status.prettyPrint())

            for oid, valor in var_binds:
                indice = extrair_indice(oid, oid_base)
                if indice is None:
                    continue
                resultados[indice] = valor.prettyPrint()

        return resultados

    except Exception as erro:
        registrar_log_snmp(
            evento="SNMP_ERRO_OPERACAO",
            sbc=sbc,
            operacao="WALK",
            oid=f"{nome_oid} ({oid_base})",
            erro=erro,
            duracao_segundos=time.monotonic() - inicio,
            detalhes=f"timeout=4s; retries=1; itens_parciais={len(resultados)}",
        )
        raise


# ============================================================
# COLETA DE UM SBC
# ============================================================

async def coletar_sbc(
    engine,
    community,
    sbc,
    ips_excluidos,
):
    hostname = await snmp_get(
        engine,
        community,
        sbc,
        OID_SYSNAME,
    )

    ha_bruto = await snmp_get(
        engine,
        community,
        sbc,
        OID_HA,
    )

    cpu = await snmp_get(
        engine,
        community,
        sbc,
        OID_CPU,
    )

    memoria = await snmp_get(
        engine,
        community,
        sbc,
        OID_MEMORIA,
    )

    health = await snmp_get(
        engine,
        community,
        sbc,
        OID_HEALTH,
    )

    sessoes = await snmp_get(
        engine,
        community,
        sbc,
        OID_SESSOES,
    )

    cps = await snmp_get(
        engine,
        community,
        sbc,
        OID_CPS,
    )

    # O contador global inclui SIP + H.323. O widget Sbc "Sessions"
    # separa esses valores. Somamos as chamadas H.323 ativas e
    # descontamos do total para obter SIP Sessions ativas.
    try:
        h323_por_stack = await snmp_walk(
            engine,
            community,
            sbc,
            OID_H323_STACK_CURRENT_CALLS,
        )
        h323_ativas = sum(
            _inteiro(valor, 0)
            for valor in h323_por_stack.values()
        )
    except Exception:
        # Ambientes sem H.323/MIB correspondente retornam zero.
        h323_ativas = 0

    sessoes_total = _inteiro(sessoes, 0)
    sip_sessions_ativas = max(
        0,
        sessoes_total - h323_ativas,
    )

    hostnames = await snmp_walk(
        engine,
        community,
        sbc,
        OID_AGENT_HOSTNAME,
    )

    status = await snmp_walk(
        engine,
        community,
        sbc,
        OID_AGENT_STATUS,
    )

    # Métricas do widget "Agent Individual". São coletadas também
    # quando o status ainda não está mapeado, para permitir correlação
    # segura do valor bruto antes de habilitar qualquer alerta.
    try:
        inbound_active = await snmp_walk(
            engine,
            community,
            sbc,
            OID_AGENT_INBOUND_ACTIVE,
        )
    except Exception:
        inbound_active = {}

    try:
        outbound_active = await snmp_walk(
            engine,
            community,
            sbc,
            OID_AGENT_OUTBOUND_ACTIVE,
        )
    except Exception:
        outbound_active = {}

    agents = {}

    for indice, agent_ip in (
        hostnames.items()
    ):
        agent_ip = (
            str(agent_ip)
            .strip()
        )

        if agent_ip in ips_excluidos:
            continue

        valor = str(
            status.get(
                indice,
                "SEM_STATUS",
            )
        ).strip()

        agents[
            agent_ip
        ] = {
            "status_bruto": valor,
            "indice": indice,
            "inbound_active": _inteiro(inbound_active[indice]) if indice in inbound_active else None,
            "outbound_active": _inteiro(outbound_active[indice]) if indice in outbound_active else None,
        }

    return {
        "endereco": sbc,
        "hostname": hostname,
        "ha_bruto": ha_bruto,
        "ha": traduzir_ha(
            ha_bruto
        ),
        "cpu": cpu,
        "memoria": memoria,
        "health": health,
        # "sessoes" é mantido para compatibilidade com rotinas antigas.
        "sessoes": sessoes,
        "sessoes_total": sessoes_total,
        "h323_calls_ativas": h323_ativas,
        "sip_sessions_ativas": sip_sessions_ativas,
        "cps": cps,
        "agents": agents,
    }


# ============================================================
# COLETA DOS DOIS SBCs
# ============================================================

async def coletar_todos(
    community,
    ips_excluidos,
    sbcs=None,
):
    engine = SnmpEngine()

    resultados = {}

    try:
        for sbc in (sbcs or SBCS):

            try:
                resultados[
                    sbc
                ] = await coletar_sbc(
                    engine=engine,
                    community=community,
                    sbc=sbc,
                    ips_excluidos=(
                        ips_excluidos
                    ),
                )

            except Exception as erro:
                resultados[
                    sbc
                ] = {
                    "endereco": sbc,
                    "erro": str(
                        erro
                    ),
                }

    finally:
        engine.close_dispatcher()

    return resultados


# ============================================================
# TELEGRAM
# ============================================================

def enviar_agent_indisponivel(
    telegram,
    ip,
    sbc,
    hostname_sbc,
    data,
):
    texto = "\n".join(
        [
            "🚨 <b>TELECOM MONITOR — "
            "SESSION AGENT INDISPONÍVEL</b>",
            "",
            "📡 <b>Monitoramento Integrado</b>",
            "",
            f"🌐 <b>Session Agent</b>",
            telegram.escapar(ip),
            "",
            "🖥 <b>SBC</b>",
            (
                f"{telegram.escapar(hostname_sbc)} "
                f"({telegram.escapar(sbc)})"
            ),
            "",
            "🔴 <b>Status</b>",
            "In Service → Out of Service",
            "",
            "🕐 <b>Detectado em</b>",
            data.strftime(
                "%d/%m/%Y %H:%M:%S"
            ),
        ]
    )

    telegram.enviar_mensagem(
        texto
    )


def enviar_agent_restabelecido(
    telegram,
    ip,
    sbc,
    hostname_sbc,
    inicio,
    fim,
):
    duracao = (
        fim - inicio
    ).total_seconds()

    texto = "\n".join(
        [
            "✅ <b>TELECOM MONITOR — "
            "SESSION AGENT RESTABELECIDO</b>",
            "",
            "📡 <b>Monitoramento Integrado</b>",
            "",
            "🌐 <b>Session Agent</b>",
            telegram.escapar(ip),
            "",
            "🖥 <b>SBC</b>",
            (
                f"{telegram.escapar(hostname_sbc)} "
                f"({telegram.escapar(sbc)})"
            ),
            "",
            "🟢 <b>Status</b>",
            "Out of Service → In Service",
            "",
            "⏱ <b>Tempo indisponível</b>",
            formatar_duracao(
                duracao
            ),
            "",
            "🕐 <b>Restabelecido em</b>",
            fim.strftime(
                "%d/%m/%Y %H:%M:%S"
            ),
        ]
    )

    telegram.enviar_mensagem(
        texto
    )


def enviar_switchover(
    telegram,
    nome_par,
    anterior,
    atual,
):
    agora = datetime.now()

    anterior_nome = (
        anterior.get("hostname")
        or anterior.get("ip")
        or "-"
    )
    atual_nome = (
        atual.get("hostname")
        or atual.get("ip")
        or "-"
    )

    linhas = [
        "🔴 <b>TELECOM MONITOR — SWITCHOVER SBC</b>",
        "",
        "📡 <b>Monitoramento Integrado</b>",
        "",
        "🏢 <b>Ambiente</b>",
        telegram.escapar(nome_par),
        "",
        "🔄 <b>Alteração de redundância detectada</b>",
        "",
        "⬅️ <b>ACTIVE anterior</b>",
        telegram.escapar(anterior_nome),
    ]

    if anterior.get("ip"):
        linhas.append(
            telegram.escapar(anterior["ip"])
        )

    linhas.extend([
        "",
        "➡️ <b>Novo SBC ACTIVE</b>",
        telegram.escapar(atual_nome),
    ])

    if atual.get("ip"):
        linhas.append(
            telegram.escapar(atual["ip"])
        )

    linhas.extend([
        "",
        "📌 <b>Situação</b>",
        "O papel ACTIVE foi transferido entre os membros do par HA.",
        "✅ Redundância HA preservada após a troca.",
        "",
        "🕐 <b>Detectado em</b>",
        agora.strftime("%d/%m/%Y %H:%M:%S"),
    ])

    telegram.enviar_mensagem(
        "\n".join(linhas)
    )


def enviar_redundancia_degradada(
    telegram,
    nome_par,
    active,
    peer,
    motivo,
):
    agora = datetime.now()

    linhas = [
        "🟠 <b>TELECOM MONITOR — REDUNDÂNCIA SBC DEGRADADA</b>",
        "",
        "📡 <b>Monitoramento Integrado</b>",
        "",
        "🏢 <b>Ambiente</b>",
        telegram.escapar(nome_par),
        "",
        "🖥 <b>SBC ACTIVE</b>",
        telegram.escapar(
            active.get("hostname")
            or active.get("ip")
            or "-"
        ),
    ]

    if active.get("ip"):
        linhas.append(
            telegram.escapar(active["ip"])
        )

    linhas.extend([
        "",
        "⚠️ <b>Peer HA</b>",
        telegram.escapar(
            peer.get("hostname")
            or peer.get("ip")
            or "-"
        ),
    ])

    if peer.get("ip") and peer.get("ip") not in linhas:
        linhas.append(
            telegram.escapar(peer["ip"])
        )

    linhas.extend([
        "",
        "📌 <b>Condição identificada</b>",
        telegram.escapar(motivo),
        "",
        "O SBC ACTIVE permanece operacional, porém o ambiente "
        "está temporariamente sem a redundância HA esperada.",
        "",
        "🕐 <b>Detectado em</b>",
        agora.strftime("%d/%m/%Y %H:%M:%S"),
    ])

    telegram.enviar_mensagem(
        "\n".join(linhas)
    )


def enviar_ha_indisponivel(
    telegram,
    nome_par,
    membros,
):
    agora = datetime.now()

    linhas = [
        "⚫ <b>TELECOM MONITOR — HA SBC INDISPONÍVEL</b>",
        "",
        "📡 <b>Monitoramento Integrado</b>",
        "",
        "🏢 <b>Ambiente</b>",
        telegram.escapar(nome_par),
        "",
        "⚠️ Nenhum membro do par pôde ser confirmado como ACTIVE.",
        "",
        "🖥 <b>Membros observados</b>",
    ]

    for membro in membros:
        ip = membro.get("ip", "-")
        status = membro.get("ha", "SEM RESPOSTA")
        hostname = membro.get("hostname") or "-"
        linhas.append(
            "• "
            + telegram.escapar(ip)
            + " — "
            + telegram.escapar(hostname)
            + " — "
            + telegram.escapar(status)
        )

    linhas.extend([
        "",
        "🕐 <b>Detectado em</b>",
        agora.strftime("%d/%m/%Y %H:%M:%S"),
    ])

    telegram.enviar_mensagem(
        "\n".join(linhas)
    )


def enviar_redundancia_restabelecida(
    telegram,
    nome_par,
    active,
    standby,
    inicio_degradacao,
):
    agora = datetime.now()

    duracao = None
    if inicio_degradacao:
        try:
            inicio = datetime.fromisoformat(
                inicio_degradacao
            )
            duracao = (
                agora - inicio
            ).total_seconds()
        except (TypeError, ValueError):
            duracao = None

    linhas = [
        "✅ <b>TELECOM MONITOR — REDUNDÂNCIA SBC RESTABELECIDA</b>",
        "",
        "📡 <b>Monitoramento Integrado</b>",
        "",
        "🏢 <b>Ambiente</b>",
        telegram.escapar(nome_par),
        "",
        "🟢 <b>ACTIVE</b>",
        telegram.escapar(
            active.get("hostname")
            or active.get("ip")
            or "-"
        ),
    ]

    if active.get("ip"):
        linhas.append(
            telegram.escapar(active["ip"])
        )

    linhas.extend([
        "",
        "🟢 <b>STANDBY</b>",
        telegram.escapar(
            standby.get("hostname")
            or standby.get("ip")
            or "-"
        ),
    ])

    if standby.get("ip"):
        linhas.append(
            telegram.escapar(standby["ip"])
        )

    linhas.extend([
        "",
        "✅ Par HA novamente operacional com redundância preservada.",
    ])

    if duracao is not None:
        linhas.extend([
            "",
            "⏱ <b>Tempo em condição degradada</b>",
            formatar_duracao(duracao),
        ])

    linhas.extend([
        "",
        "🕐 <b>Restabelecido em</b>",
        agora.strftime("%d/%m/%Y %H:%M:%S"),
    ])

    telegram.enviar_mensagem(
        "\n".join(linhas)
    )


def salvar_status_ha_publico(
    estado_ha,
    resultados,
    pares_ha,
):
    """
    Snapshot gerencial consumido pelo comando /sbc.
    Não contém credenciais ou community SNMP.
    """
    PASTA_DADOS.mkdir(
        parents=True,
        exist_ok=True,
    )

    payload = {
        "atualizado_em": agora_iso(),
        "pares": {},
    }

    for nome_par, membros in pares_ha.items():
        estado_par = dict(
            estado_ha.get(nome_par, {})
        )

        itens = []
        for ip in membros:
            dados = resultados.get(
                ip,
                {"endereco": ip, "erro": "Sem resultado"},
            )

            if "erro" in dados:
                itens.append({
                    "ip": ip,
                    "hostname": None,
                    "respondendo_snmp": False,
                    "ha": "SEM RESPOSTA",
                    "health": None,
                    "cpu": None,
                    "memoria": None,
                    "sip_sessions_ativas": None,
                    "cps": None,
                    "agents": {},
                    "erro": dados.get("erro"),
                })
            else:
                itens.append({
                    "ip": ip,
                    "hostname": dados.get("hostname"),
                    "respondendo_snmp": True,
                    "ha": dados.get("ha"),
                    "health": _inteiro(dados.get("health"), 0),
                    "cpu": _inteiro(dados.get("cpu"), 0),
                    "memoria": _inteiro(dados.get("memoria"), 0),
                    "sip_sessions_ativas": _inteiro(
                        dados.get(
                            "sip_sessions_ativas",
                            dados.get("sessoes", 0),
                        ),
                        0,
                    ),
                    "cps": _inteiro(dados.get("cps"), 0),
                    "agents": dados.get("agents") if isinstance(dados.get("agents"), dict) else {},
                    "erro": None,
                })

        payload["pares"][nome_par] = {
            "integridade": estado_par.get(
                "integridade_atual",
                "DESCONHECIDA",
            ),
            "inicio_degradacao": estado_par.get(
                "inicio_degradacao"
            ),
            "ultimo_switchover": estado_par.get(
                "ultimo_switchover"
            ),
            "active_anterior": estado_par.get(
                "active_anterior"
            ),
            "active_atual": estado_par.get(
                "active_atual"
            ),
            "ultima_alteracao": estado_par.get(
                "ultima_alteracao"
            ),
            "membros": itens,
        }

    temporario = (
        ARQUIVO_STATUS_HA
        .with_suffix(".tmp")
    )

    with temporario.open(
        "w",
        encoding="utf-8",
    ) as arquivo:
        json.dump(
            payload,
            arquivo,
            ensure_ascii=False,
            indent=4,
        )

    temporario.replace(
        ARQUIVO_STATUS_HA
    )


def processar_integridade_ha(
    resultados,
    estado,
    telegram,
    pares_ha,
):
    """
    Mantém o estado HA por par e gera apenas transições relevantes:
      NORMAL -> DEGRADADA
      NORMAL/DEGRADADA -> INDISPONIVEL
      DEGRADADA/INDISPONIVEL -> NORMAL
      troca do SBC ACTIVE -> SWITCHOVER

    O primeiro ciclo cria baseline silencioso.
    """
    agora = datetime.now()
    agora_txt = agora_iso()

    monitor_ha = estado.setdefault(
        "ha_monitor_por_par",
        {},
    )

    for nome_par, membros in pares_ha.items():
        membros = [
            str(ip).strip()
            for ip in membros
        ]

        estado_par = monitor_ha.setdefault(
            nome_par,
            {
                "inicializado": False,
                "integridade_atual": "DESCONHECIDA",
                "inicio_degradacao": None,
                "ultimo_switchover": None,
                "active_anterior": None,
                "active_atual": None,
                "ultima_alteracao": None,
            },
        )

        observados = []
        for ip in membros:
            dados = resultados.get(
                ip,
                {"endereco": ip, "erro": "Sem resultado"},
            )

            if "erro" in dados:
                observados.append({
                    "ip": ip,
                    "hostname": None,
                    "ha": "SEM RESPOSTA",
                    "respondendo": False,
                    "erro": dados.get("erro"),
                })
            else:
                observados.append({
                    "ip": ip,
                    "hostname": dados.get("hostname"),
                    "ha": dados.get("ha"),
                    "respondendo": True,
                    "erro": None,
                })

        ativos = [
            item
            for item in observados
            if item["respondendo"]
            and item["ha"] == "ACTIVE"
        ]
        standbys = [
            item
            for item in observados
            if item["respondendo"]
            and item["ha"] == "STANDBY"
        ]

        if len(ativos) == 1 and len(standbys) == 1 and len(observados) == 2:
            integridade_nova = "NORMAL"
        elif len(ativos) == 1:
            integridade_nova = "DEGRADADA"
        else:
            integridade_nova = "INDISPONIVEL"

        active_atual = ativos[0] if ativos else None
        active_anterior = estado_par.get(
            "active_atual"
        )

        inicializado = bool(
            estado_par.get(
                "inicializado",
                False,
            )
        )
        integridade_anterior = estado_par.get(
            "integridade_atual",
            "DESCONHECIDA",
        )

        # Switchover é um evento independente da condição atual do par.
        houve_switchover = False

        if (
            inicializado
            and active_anterior
            and active_atual
            and active_anterior.get("ip")
            and active_atual.get("ip")
            and active_anterior.get("ip")
            != active_atual.get("ip")
        ):
            houve_switchover = True
            try:
                enviar_switchover(
                    telegram=telegram,
                    nome_par=nome_par,
                    anterior=active_anterior,
                    atual=active_atual,
                )
            except Exception as erro:
                print(
                    "Aviso: falha ao enviar alerta de switchover "
                    f"do par {nome_par}: {erro}"
                )

            estado_par["ultimo_switchover"] = {
                "detectado_em": agora_txt,
                "active_anterior": active_anterior,
                "active_atual": active_atual,
            }
            estado_par["ultima_alteracao"] = agora_txt

        if inicializado and integridade_nova != integridade_anterior:
            if integridade_nova == "DEGRADADA":
                peer = next(
                    (
                        item
                        for item in observados
                        if not active_atual
                        or item["ip"] != active_atual["ip"]
                    ),
                    {
                        "ip": "-",
                        "hostname": None,
                        "ha": "DESCONHECIDO",
                    },
                )

                motivo = (
                    f"Estado do peer: {peer.get('ha', 'DESCONHECIDO')}."
                )
                if peer.get("erro"):
                    motivo = (
                        "Peer sem resposta SNMP. "
                        f"Motivo: {peer.get('erro')}"
                    )

                try:
                    enviar_redundancia_degradada(
                        telegram=telegram,
                        nome_par=nome_par,
                        active=active_atual or {},
                        peer=peer,
                        motivo=motivo,
                    )
                except Exception as erro:
                    print(
                        "Aviso: falha ao enviar alerta de redundância "
                        f"degradada do par {nome_par}: {erro}"
                    )

                if not estado_par.get("inicio_degradacao"):
                    estado_par["inicio_degradacao"] = agora_txt

            elif integridade_nova == "INDISPONIVEL":
                try:
                    enviar_ha_indisponivel(
                        telegram=telegram,
                        nome_par=nome_par,
                        membros=observados,
                    )
                except Exception as erro:
                    print(
                        "Aviso: falha ao enviar alerta HA indisponível "
                        f"do par {nome_par}: {erro}"
                    )

                if not estado_par.get("inicio_degradacao"):
                    estado_par["inicio_degradacao"] = agora_txt

            elif integridade_nova == "NORMAL":
                inicio_degradacao = estado_par.get(
                    "inicio_degradacao"
                )
                active_normal = ativos[0]
                standby_normal = standbys[0]

                if (
                    integridade_anterior in {
                        "DEGRADADA",
                        "INDISPONIVEL",
                    }
                    and not houve_switchover
                ):
                    try:
                        enviar_redundancia_restabelecida(
                            telegram=telegram,
                            nome_par=nome_par,
                            active=active_normal,
                            standby=standby_normal,
                            inicio_degradacao=inicio_degradacao,
                        )
                    except Exception as erro:
                        print(
                            "Aviso: falha ao enviar restabelecimento HA "
                            f"do par {nome_par}: {erro}"
                        )

                estado_par["inicio_degradacao"] = None

            estado_par["ultima_alteracao"] = agora_txt

        # Baseline silencioso no primeiro ciclo.
        if not inicializado:
            estado_par["inicializado"] = True
            if integridade_nova != "NORMAL":
                estado_par["inicio_degradacao"] = agora_txt

        estado_par["integridade_atual"] = integridade_nova
        estado_par["active_anterior"] = active_anterior
        estado_par["active_atual"] = active_atual
        estado_par["atualizado_em"] = agora_txt
        estado_par["membros_observados"] = observados

    salvar_status_ha_publico(
        estado_ha=monitor_ha,
        resultados=resultados,
        pares_ha=pares_ha,
    )

    return estado


def enviar_alerta_capacidade_sip(
    telegram,
    nome_par,
    sbc_ip,
    hostname_sbc,
    sessoes_sip,
    capacidade_nominal,
    limite_operacional,
):
    excedente = max(0, sessoes_sip - limite_operacional)

    texto = "\n".join(
        [
            "🚨 <b>TELECOM MONITOR — CAPACIDADE SIP</b>",
            "",
            f"Ambiente: <b>{telegram.escapar(nome_par)}</b>",
            (
                "SBC ACTIVE: "
                f"{telegram.escapar(hostname_sbc)} "
                f"({telegram.escapar(sbc_ip)})"
            ),
            "",
            f"Sessões SIP ativas: <b>{sessoes_sip}</b>",
            f"Limite operacional: <b>{limite_operacional}</b>",
            f"Capacidade nominal: {capacidade_nominal}",
            "",
            "⚠️ Limite operacional de sessões SIP atingido.",
            (
                "Limite excedido em "
                f"{excedente} sessão(ões)."
                if excedente > 0
                else "O limite operacional foi atingido."
            ),
        ]
    )

    telegram.enviar_mensagem(texto)


def enviar_normalizacao_capacidade_sip(
    telegram,
    nome_par,
    sbc_ip,
    hostname_sbc,
    sessoes_sip,
    capacidade_nominal,
    limite_operacional,
):
    margem = max(0, limite_operacional - sessoes_sip)

    texto = "\n".join(
        [
            "✅ <b>TELECOM MONITOR — CAPACIDADE SIP NORMALIZADA</b>",
            "",
            f"Ambiente: <b>{telegram.escapar(nome_par)}</b>",
            (
                "SBC ACTIVE: "
                f"{telegram.escapar(hostname_sbc)} "
                f"({telegram.escapar(sbc_ip)})"
            ),
            "",
            f"Sessões SIP ativas: <b>{sessoes_sip}</b>",
            f"Limite operacional: <b>{limite_operacional}</b>",
            f"Capacidade nominal: {capacidade_nominal}",
            "",
            "✅ Utilização novamente abaixo do limite operacional.",
            f"Margem atual: {margem} sessão(ões).",
        ]
    )

    telegram.enviar_mensagem(texto)


def enviar_novo_agent(
    telegram,
    ip,
    nome_par,
    sbc_ip,
    sbc_hostname,
):
    agora = datetime.now()

    linhas = [
        "🆕 <b>TELECOM MONITOR — "
        "NOVO SESSION AGENT</b>",
        "",
        "📡 <b>Monitoramento Integrado</b>",
        "",
        "🌐 <b>Novo Session Agent detectado</b>",
        telegram.escapar(
            ip
        ),
        "",
        "🏢 <b>Ambiente</b>",
        telegram.escapar(
            nome_par
        ),
        "",
        "🖥 <b>Detectado no SBC ACTIVE</b>",
        telegram.escapar(
            sbc_ip
        ),
    ]

    if sbc_hostname:
        linhas.append(
            telegram.escapar(
                sbc_hostname
            )
        )

    linhas.extend(
        [
            "",
            "✅ O endereço foi incorporado "
            "automaticamente à monitoração.",
            "",
            "🕐 <b>Detectado em</b>",
            agora.strftime(
                "%d/%m/%Y %H:%M:%S"
            ),
        ]
    )

    telegram.enviar_mensagem(
        "\n".join(
            linhas
        )
    )


def registrar_diagnostico_agent_nao_mapeado(
    nome_par,
    sbc_ip,
    hostname_sbc,
    agent_ip,
    dados_agent,
):
    """Registra status bruto desconhecido sem gerar alerta Telegram."""
    PASTA_DADOS.mkdir(parents=True, exist_ok=True)

    evento = {
        "timestamp": agora_iso(),
        "par": nome_par,
        "sbc_active": sbc_ip,
        "hostname_sbc": hostname_sbc,
        "session_agent": agent_ip,
        "status_bruto": str(
            dados_agent.get("status_bruto", "")
        ),
        "inbound_active": _inteiro(
            dados_agent.get("inbound_active", 0),
            0,
        ),
        "outbound_active": _inteiro(
            dados_agent.get("outbound_active", 0),
            0,
        ),
    }

    with ARQUIVO_DIAGNOSTICO_AGENTS.open(
        "a",
        encoding="utf-8",
    ) as arquivo:
        arquivo.write(
            json.dumps(
                evento,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        )


# ============================================================
# PROCESSAMENTO DOS ESTADOS
# ============================================================

def processar_resultados(
    resultados,
    estado,
    telegram,
    pares_ha=None,
):
    """Processa cada par HA de forma independente.

    Com mais de um par SBC pode existir mais de um equipamento ACTIVE
    ao mesmo tempo. Por isso switchover, baseline e estado dos Session Agents
    são mantidos separadamente por par.
    """
    agora = datetime.now()
    pares_ha = pares_ha or PARES_HA_PADRAO

    estado = processar_integridade_ha(
        resultados=resultados,
        estado=estado,
        telegram=telegram,
        pares_ha=pares_ha,
    )

    sbcs_ok = {
        ip: dados
        for ip, dados in resultados.items()
        if isinstance(dados, dict) and "erro" not in dados
    }

    ativos_por_par = estado.setdefault("active_anterior_por_par", {})
    inicializado_por_par = estado.setdefault("inicializado_por_par", {})
    agents_por_par = estado.setdefault("agents_por_par", {})
    conhecidos_por_par = estado.setdefault("agents_conhecidos_por_par", {})
    status_nao_mapeado_anterior = estado.setdefault(
        "status_nao_mapeado_anterior_por_par",
        {},
    )
    capacidade_por_par = estado.setdefault(
        "capacidade_sip_por_par",
        {},
    )

    # Compatibilidade com o estado do par histórico (versões anteriores).
    primeiro_par = next(iter(pares_ha), None)
    if primeiro_par:
        if primeiro_par not in ativos_por_par and estado.get("active_anterior"):
            ativos_por_par[primeiro_par] = estado.get("active_anterior")
        if primeiro_par not in inicializado_por_par and estado.get("inicializado"):
            inicializado_por_par[primeiro_par] = True
        if primeiro_par not in agents_por_par and isinstance(estado.get("agents"), dict):
            agents_por_par[primeiro_par] = dict(estado.get("agents") or {})
        if primeiro_par not in conhecidos_por_par:
            antigos = carregar_agents_conhecidos()
            if antigos:
                conhecidos_por_par[primeiro_par] = sorted(antigos)

    for nome_par, membros in pares_ha.items():
        membros = [str(ip).strip() for ip in membros]
        resultados_par = {
            ip: sbcs_ok[ip]
            for ip in membros
            if ip in sbcs_ok
        }

        dados_active = None
        for ip in membros:
            dados = resultados_par.get(ip)
            if dados and dados.get("ha") == "ACTIVE":
                dados_active = dados
                break

        if dados_active is None:
            print(f"⚠ Par HA {nome_par}: nenhum SBC ACTIVE disponível para consolidar Session Agents.")
            continue

        active_atual = dados_active.get("hostname") or dados_active.get("endereco")
        par_inicializado = bool(inicializado_por_par.get(nome_par, False))

        # O switchover agora é tratado pelo monitor de integridade HA,
        # que mantém IP, hostname, histórico e estado por par.
        ativos_por_par[nome_par] = active_atual

        sbc_ip = dados_active["endereco"]
        sbc_hostname = dados_active["hostname"]
        agents_atual = dados_active.get("agents", {})
        ips_atuais = set(agents_atual.keys())

        # --------------------------------------------------------
        # CAPACIDADE SIP POR PAR HA
        # --------------------------------------------------------
        sessoes_sip = _inteiro(
            dados_active.get(
                "sip_sessions_ativas",
                dados_active.get("sessoes", 0),
            ),
            0,
        )
        capacidade_nominal = int(
            CAPACIDADE_NOMINAL_POR_PAR.get(
                nome_par,
                LIMITE_OPERACIONAL_SIP,
            )
        )
        estado_capacidade = capacidade_por_par.setdefault(
            nome_par,
            {
                "em_alerta": False,
                "ultima_medicao": 0,
                "alterado_em": None,
            },
        )
        estava_em_alerta = bool(
            estado_capacidade.get("em_alerta", False)
        )
        esta_em_alerta = (
            sessoes_sip >= LIMITE_OPERACIONAL_SIP
        )

        if esta_em_alerta and not estava_em_alerta:
            try:
                enviar_alerta_capacidade_sip(
                    telegram=telegram,
                    nome_par=nome_par,
                    sbc_ip=sbc_ip,
                    hostname_sbc=sbc_hostname,
                    sessoes_sip=sessoes_sip,
                    capacidade_nominal=capacidade_nominal,
                    limite_operacional=LIMITE_OPERACIONAL_SIP,
                )
            except Exception as erro:
                print(
                    "Aviso: falha ao enviar alerta de capacidade SIP "
                    f"do par {nome_par}: {erro}"
                )

            estado_capacidade["em_alerta"] = True
            estado_capacidade["alterado_em"] = agora_iso()

        elif not esta_em_alerta and estava_em_alerta:
            try:
                enviar_normalizacao_capacidade_sip(
                    telegram=telegram,
                    nome_par=nome_par,
                    sbc_ip=sbc_ip,
                    hostname_sbc=sbc_hostname,
                    sessoes_sip=sessoes_sip,
                    capacidade_nominal=capacidade_nominal,
                    limite_operacional=LIMITE_OPERACIONAL_SIP,
                )
            except Exception as erro:
                print(
                    "Aviso: falha ao enviar normalização de capacidade SIP "
                    f"do par {nome_par}: {erro}"
                )

            estado_capacidade["em_alerta"] = False
            estado_capacidade["alterado_em"] = agora_iso()

        estado_capacidade.update(
            {
                "ultima_medicao": sessoes_sip,
                "sbc_active": sbc_ip,
                "hostname_active": sbc_hostname,
                "capacidade_nominal": capacidade_nominal,
                "limite_operacional": LIMITE_OPERACIONAL_SIP,
                "atualizado_em": agora_iso(),
            }
        )

        # Primeiro ciclo de um novo par cria baseline silencioso para evitar
        # dezenas/centenas de alertas de "novo Session Agent".
        conhecidos = set(conhecidos_por_par.get(nome_par, []))
        if not par_inicializado and not conhecidos:
            conhecidos = set(ips_atuais)

        novos = ips_atuais - conhecidos
        if par_inicializado:
            for ip in sorted(novos):
                try:
                    enviar_novo_agent(
                        telegram=telegram,
                        ip=ip,
                        nome_par=nome_par,
                        sbc_ip=sbc_ip,
                        sbc_hostname=sbc_hostname,
                    )
                except Exception as erro:
                    print(
                        "Aviso: falha ao enviar alerta de novo Session Agent "
                        f"{ip} no par {nome_par}: {erro}"
                    )

        conhecidos_por_par[nome_par] = sorted(conhecidos | ips_atuais)
        estados_agents = agents_por_par.setdefault(nome_par, {})

        for ip, dados in agents_atual.items():
            status_atual = str(dados.get("status_bruto", ""))

            if status_atual not in STATUS_AGENT:
                diagnosticos_par = status_nao_mapeado_anterior.setdefault(
                    nome_par,
                    {},
                )
                assinatura_diag = (
                    f"{status_atual}|"
                    f"{_inteiro(dados.get('inbound_active', 0), 0)}|"
                    f"{_inteiro(dados.get('outbound_active', 0), 0)}"
                )

                # Evita poluir o terminal e o arquivo a cada ciclo quando
                # nada mudou. Um novo registro é criado se status ou
                # contadores Active mudarem.
                if diagnosticos_par.get(ip) != assinatura_diag:
                    print(
                        "⚠ Status SNMP ainda não mapeado para Session Agent "
                        f"{ip} no par {nome_par}: {status_atual} | "
                        f"In={_inteiro(dados.get('inbound_active', 0), 0)} | "
                        f"Out={_inteiro(dados.get('outbound_active', 0), 0)}"
                    )
                    try:
                        registrar_diagnostico_agent_nao_mapeado(
                            nome_par=nome_par,
                            sbc_ip=sbc_ip,
                            hostname_sbc=sbc_hostname,
                            agent_ip=ip,
                            dados_agent=dados,
                        )
                    except Exception as erro:
                        print(
                            "Aviso: não foi possível registrar diagnóstico "
                            f"do Session Agent {ip}: {erro}"
                        )
                    diagnosticos_par[ip] = assinatura_diag

                # Segurança: status não validado nunca dispara OUT/IN.
                continue

            anterior = estados_agents.get(ip)

            if anterior is None:
                estados_agents[ip] = {
                    "status": status_atual,
                    "alterado_em": agora_iso(),
                }
                if par_inicializado and status_atual == "1":
                    try:
                        enviar_agent_indisponivel(
                            telegram=telegram,
                            ip=ip,
                            sbc=sbc_ip,
                            hostname_sbc=sbc_hostname,
                            data=agora,
                        )
                    except Exception as erro:
                        print(f"Aviso: falha ao enviar alerta de Session Agent: {erro}")
                continue

            status_anterior = str(anterior.get("status", ""))
            if status_anterior == status_atual:
                continue

            if status_anterior == "3" and status_atual == "1":
                try:
                    enviar_agent_indisponivel(
                        telegram=telegram,
                        ip=ip,
                        sbc=sbc_ip,
                        hostname_sbc=sbc_hostname,
                        data=agora,
                    )
                except Exception as erro:
                    print(f"Aviso: falha ao enviar alerta Session Agent OUT: {erro}")

            elif status_anterior == "1" and status_atual == "3":
                inicio_texto = anterior.get("alterado_em")
                try:
                    inicio = datetime.fromisoformat(inicio_texto)
                except (TypeError, ValueError):
                    inicio = agora

                try:
                    enviar_agent_restabelecido(
                        telegram=telegram,
                        ip=ip,
                        sbc=sbc_ip,
                        hostname_sbc=sbc_hostname,
                        inicio=inicio,
                        fim=agora,
                    )
                except Exception as erro:
                    print(f"Aviso: falha ao enviar alerta Session Agent restabelecido: {erro}")

            estados_agents[ip] = {
                "status": status_atual,
                "alterado_em": agora_iso(),
            }

        inicializado_por_par[nome_par] = True

    # Mantém também os campos legados do primeiro par para compatibilidade com
    # ferramentas auxiliares que ainda leem status_sbc.json no formato antigo.
    if primeiro_par:
        estado["active_anterior"] = ativos_por_par.get(primeiro_par)
        estado["agents"] = agents_por_par.get(primeiro_par, {})
        estado["inicializado"] = bool(inicializado_por_par.get(primeiro_par, False))
        salvar_agents_conhecidos(set(conhecidos_por_par.get(primeiro_par, [])))

    salvar_estado(estado)
    return estado


# ============================================================
# CLASSE PRINCIPAL
# ============================================================

class SBCMonitor:
    def __init__(
        self,
        community,
        telegram,
        pares_ha=None,
    ):
        if not community:
            raise ValueError(
                "SBC_SNMP_COMMUNITY não foi configurada."
            )

        self.community = community
        self.telegram = telegram
        self.pares_ha = {
            str(nome): [str(ip).strip() for ip in membros]
            for nome, membros in (pares_ha or PARES_HA_PADRAO).items()
        }
        self.sbcs = [
            ip
            for membros in self.pares_ha.values()
            for ip in membros
        ]

        self.ips_excluidos = carregar_ips_excluidos()
        self.estado = carregar_estado()
        self.estado_snmp = carregar_status_snmp()

        print("SBC:")
        print(f"  IPs excluídos : {len(self.ips_excluidos)}")
        print("  SBCs          : " + ", ".join(self.sbcs))
        print("  Pares HA      :")
        for nome, membros in self.pares_ha.items():
            print(f"    {nome}: " + " / ".join(membros))
        print(
            "  Diagnóstico SA : "
            f"{ARQUIVO_DIAGNOSTICO_AGENTS}"
        )
        print(
            "  Log SNMP       : "
            f"{ARQUIVO_LOG_SNMP}"
        )
        print(
            "  Estado SNMP    : "
            f"{ARQUIVO_STATUS_SNMP}"
        )
        print(
            "  Estado HA      : "
            f"{ARQUIVO_STATUS_HA}"
        )

    def executar_ciclo(self):
        resultados = asyncio.run(
            coletar_todos(
                community=self.community,
                ips_excluidos=self.ips_excluidos,
                sbcs=self.sbcs,
            )
        )

        for nome_par, membros in self.pares_ha.items():
            print(f"  [{nome_par}]")
            for sbc in membros:
                dados = resultados.get(sbc, {"erro": "Sem resultado"})
                if "erro" in dados:
                    print(f"SBC {sbc}: ⚠ {dados['erro']}")
                    continue

                print(
                    f"SBC {sbc}: "
                    f"{dados['ha']} | "
                    f"CPU {dados['cpu']}% | "
                    f"MEM {dados['memoria']}% | "
                    f"Health {dados['health']}% | "
                    f"SIP Sessions {dados.get('sip_sessions_ativas', dados['sessoes'])} | "
                    f"CPS {dados['cps']}"
                )

        self.estado_snmp = atualizar_status_snmp(
            estado=self.estado_snmp,
            resultados=resultados,
        )

        self.estado = processar_resultados(
            resultados=resultados,
            estado=self.estado,
            telegram=self.telegram,
            pares_ha=self.pares_ha,
        )

        return resultados
