import csv
import json
import os
import socket
import sys
import subprocess
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

from mgw.alerta import AlertaTelegram
from mgw.banco import BancoDados
from mgw.client import MGWClient
from mgw.monitor import Monitor
from mgw.resumo_diario import ResumoDiario
from mgw.telegram import Telegram
from mgw.sbc_monitor import SBCMonitor
from mgw.sip_capacity import SIPCapacityPilot
from mgw.sbc_monitor import LIMITE_OPERACIONAL_SIP
from confirmacao_monitor import Confirmador
from integracao_alertas import (
    enviar_pendentes, observar_mgw, observar_sbcs, observar_vpn, observar_peer_timeout,
    suspender_amostras,
)


CONFIRMACAO_DB = Path(__file__).resolve().parent / "dados" / "monitor_confirmacao_5min.sqlite"


class AlertasOperacionaisLegadosDesativados:
    """Mantém o histórico legado sem transmitir suas transições imediatas.

    O único emissor operacional é a caixa persistente do Confirmador. O
    resumo diário continua usando a instância Telegram real.
    """

    escapar = staticmethod(Telegram.escapar)

    def enviar_mensagem(self, *_args, **_kwargs):
        return None

    def enviar_documento(self, *_args, **_kwargs):
        return None


SERVIDORES = [
    {
        "nome": "MGW-114",
        "endereco": "10.90.14.114",
        "url": "http://10.90.14.114",
        "verificar_ssl": True,
    },
    {
        "nome": "MGW-115",
        "endereco": "10.90.14.115",
        "url": "http://10.90.14.115",
        "verificar_ssl": True,
    },
    {
        "nome": "MGW-116",
        "endereco": "10.90.14.116",
        "url": "https://10.90.14.116",
        "verificar_ssl": False,
    },
    {
        "nome": "MGW-117",
        "endereco": "10.90.14.117",
        "url": "http://10.90.14.117",
        "verificar_ssl": True,
    },
    {
        "nome": "MGW-118",
        "endereco": "10.90.14.118",
        "url": "http://10.90.14.118",
        "verificar_ssl": True,
    },
    {
        "nome": "MGW-119",
        "endereco": "10.90.14.119",
        "url": "http://10.90.14.119",
        "verificar_ssl": True,
    },
    {
        "nome": "MGW-120",
        "endereco": "10.90.14.120",
        "url": "http://10.90.14.120",
        "verificar_ssl": True,
    },
    {
        "nome": "MGW-121",
        "endereco": "10.90.14.121",
        "url": "http://10.90.14.121",
        "verificar_ssl": True,
    },
]


CAMINHO_STATUS_SERVIDORES = Path(
    "dados/status_servidores.json"
)

CAMINHO_STATUS_COMUNICACAO = Path(
    "dados/status_comunicacao.json"
)

CAMINHO_PERIODOS_INVALIDOS = Path(
    "dados/periodos_invalidos.csv"
)

CABECALHO_PERIODOS_INVALIDOS = [
    "data",
    "inicio",
    "fim",
    "escopo",
    "tipo",
    "motivo",
    "acao",
]

PORTA_INSTANCIA_UNICA = 49621
PORTA_TRAP_RECEIVER = 162

SBCS_SBC = {
    "10.90.11.132": "SBCs NORTE",
    "10.90.11.133": "SBCs NORTE",
    "10.90.13.10": "SBCs SUL",
    "10.90.13.11": "SBCs SUL",
}

CAMINHO_STATUS_TRAPS_SBC = Path(
    "dados/status_traps_sbc.json"
)

CAMINHO_HISTORICO_VPN = Path(
    "dados/vpn_desconexoes.json"
)



def descobrir_ip_vpn():
    """
    Retorna o IPv4 10.90.12.x utilizado pela VPN corporativa.

    A descoberta usa ``ipconfig /all`` para não adicionar dependências
    ao monitor principal. Interfaces/blocos VPN têm prioridade, mas
    existe fallback para qualquer interface ativa que possua um endereço
    10.90.12.x. Isso evita depender do nome atribuído pelo Windows à
    interface que recebeu o IP da VPN.
    """
    if os.name != "nt":
        # No servidor a rede corporativa é direta, sem interface cliente VPN.
        return os.getenv("TOI_SERVER_IP", "10.90.11.20")
    try:
        resultado = subprocess.run(
            ["ipconfig", "/all"],
            capture_output=True,
            text=True,
            encoding="cp850",
            errors="ignore",
            timeout=10,
        )
        texto = resultado.stdout
    except Exception:
        return None

    blocos = re.split(r"\r?\n\r?\n", texto)
    padrao_ip = (
        r"\b10\.201\.84\."
        r"(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\b"
    )

    def ips_do_bloco(bloco):
        bloco_normalizado = bloco.casefold()
        if (
            "mídia desconectada" in bloco_normalizado
            or "media disconnected" in bloco_normalizado
        ):
            return []
        return re.findall(padrao_ip, bloco)

    # 1) Prioriza blocos explicitamente associados ao VPN.
    for bloco in blocos:
        if "vpn" not in bloco.casefold():
            continue
        correspondencias = ips_do_bloco(bloco)
        if correspondencias:
            return correspondencias[0]

    # 2) Fallback: qualquer interface ativa com endereço corporativo VPN.
    for bloco in blocos:
        correspondencias = ips_do_bloco(bloco)
        if correspondencias:
            return correspondencias[0]

    return None


def enviar_alerta_alteracao_ip_vpn(
    telegram,
    ip_anterior,
    ip_atual,
):
    """Envia um alerta operacional quando o cliente VPN muda de IP."""
    sbcs = [
        f"{ip} — {SBCS_SBC[ip]}"
        for ip in sorted(SBCS_SBC)
    ]

    texto = "\n".join(
        [
            "🚨 <b>TELECOM MONITOR — ALTERAÇÃO DE IP VPN DETECTADA</b>",
            "",
            "🌐 <b>IP anterior</b>",
            ip_anterior,
            "",
            "🌐 <b>IP atual</b>",
            ip_atual,
            "",
            "⚠️ <b>O TRAP-RECEIVER DOS SBCs PODE ESTAR DESATUALIZADO.</b>",
            "",
            "<b>Configuração anterior:</b>",
            f"{ip_anterior}:{PORTA_TRAP_RECEIVER}",
            "",
            "<b>Nova configuração necessária:</b>",
            f"{ip_atual}:{PORTA_TRAP_RECEIVER}",
            "",
            "📡 <b>SBCs:</b>",
            *sbcs,
            "",
            "🔧 <b>Ação necessária</b>",
            "Atualizar o trap-receiver nos quatro SBCs e confirmar a autorização do novo IP na SNMP Community.",
        ]
    )

    telegram.enviar_mensagem(texto)


def enviar_alerta_snmp_sbcs_restabelecido(telegram, ip_vpn, sbcs_ok):
    linhas_sbcs = [
        f"{ip} — SNMP OK"
        for ip in sorted(sbcs_ok)
    ]

    texto = "\n".join(
        [
            "✅ <b>TELECOM MONITOR — ACESSO SNMP AOS SBCs RESTABELECIDO</b>",
            "",
            "🌐 <b>IP VPN atual</b>",
            ip_vpn or "Não identificado",
            "",
            "📡 <b>SBCs</b>",
            *linhas_sbcs,
            "",
            "✅ A nova origem está autorizada na SNMP Community.",
            "",
            "⏳ Aguardando confirmação de traps UDP/162 dos quatro SBCs.",
        ]
    )

    telegram.enviar_mensagem(texto)


def enviar_alerta_snmp_traps_normalizados(
    telegram,
    ip_vpn,
    traps_confirmados,
):
    linhas = []
    for ip in sorted(SBCS_SBC):
        horario = traps_confirmados.get(ip)
        linhas.append(
            f"{ip} — SNMP OK / TRAP OK"
            + (f" ({horario})" if horario else "")
        )

    texto = "\n".join(
        [
            "✅ <b>TELECOM MONITOR — SNMP + TRAPS DOS SBCs NORMALIZADOS</b>",
            "",
            "🌐 <b>IP VPN</b>",
            ip_vpn or "Não identificado",
            "",
            "📡 <b>SBCs</b>",
            *linhas,
            "",
            "✅ SNMP Community validada.",
            "✅ Trap Receiver validado pelos traps recebidos.",
            "ℹ Recepção de traps observada; saúde do peer depende do alarme ativo/clear confirmado.",
        ]
    )

    telegram.enviar_mensagem(texto)


def carregar_status_traps_sbc():
    if not CAMINHO_STATUS_TRAPS_SBC.exists():
        return {}

    try:
        dados = json.loads(
            CAMINHO_STATUS_TRAPS_SBC.read_text(encoding="utf-8")
        )
        return dados if isinstance(dados, dict) else {}
    except (OSError, json.JSONDecodeError) as erro:
        print(
            "Aviso: não foi possível ler o estado dos traps SBC: "
            f"{erro}"
        )
        return {}


def _converter_para_dict(valor):
    if isinstance(valor, dict):
        return valor

    try:
        dados = vars(valor)
        return dados if isinstance(dados, dict) else None
    except TypeError:
        return None


def _candidato_snmp_ok(valor):
    if valor is None or valor is False:
        return False

    dados = _converter_para_dict(valor)
    if dados is None:
        return False

    normalizado = {
        str(chave).strip().casefold(): item
        for chave, item in dados.items()
    }

    for chave in (
        "snmp_ok",
        "sucesso",
        "online",
        "acessivel",
        "acessível",
    ):
        if chave in normalizado and isinstance(normalizado[chave], bool):
            return normalizado[chave]

    for chave in ("erro", "error", "falha", "exception"):
        if chave in normalizado and normalizado[chave]:
            return False

    campos_sucesso = (
        "hostname",
        "estado_ha",
        "ha_state",
        "health_score",
        "health",
        "cpu",
        "memoria",
        "memory",
        "sessoes",
        "sessions",
        "cps",
    )

    return any(
        chave in normalizado
        and normalizado[chave] is not None
        and str(normalizado[chave]).strip() != ""
        for chave in campos_sucesso
    )


def identificar_sbcs_snmp_ok(resultados_sbc):
    """Tenta reconhecer, sem depender de um único formato, quais SBCs responderam."""
    encontrados = set()

    def visitar(valor):
        if valor is None:
            return

        if isinstance(valor, dict):
            for ip in SBCS_SBC:
                if ip in valor and _candidato_snmp_ok(valor[ip]):
                    encontrados.add(ip)

            valores_texto = [str(v) for v in valor.values()]
            for ip in SBCS_SBC:
                if ip in valores_texto and _candidato_snmp_ok(valor):
                    encontrados.add(ip)

            for item in valor.values():
                visitar(item)
            return

        if isinstance(valor, (list, tuple, set)):
            for item in valor:
                visitar(item)
            return

        dados = _converter_para_dict(valor)
        if dados is not None:
            visitar(dados)

    visitar(resultados_sbc)
    return encontrados


def _parse_iso(valor):
    if not valor:
        return None
    try:
        return datetime.fromisoformat(str(valor))
    except ValueError:
        return None


def processar_normalizacao_sbc(estado, resultados_sbc, telegram):
    """Fecha o ciclo iniciado por uma mudança de IP da VPN."""
    normalizacao = estado.get("normalizacao_sbc")

    if not isinstance(normalizacao, dict) or not normalizacao.get("pendente"):
        return estado

    snmp_ok = set(normalizacao.get("snmp_ok_ips", []))
    snmp_ok.update(identificar_sbcs_snmp_ok(resultados_sbc))
    normalizacao["snmp_ok_ips"] = sorted(snmp_ok)

    todos_sbcs = set(SBCS_SBC)
    ip_vpn = normalizacao.get("ip_atual") or descobrir_ip_vpn()

    if (
        todos_sbcs.issubset(snmp_ok)
        and not normalizacao.get("alerta_snmp_enviado")
    ):
        agora = datetime.now()
        try:
            enviar_alerta_snmp_sbcs_restabelecido(
                telegram=telegram,
                ip_vpn=ip_vpn,
                sbcs_ok=todos_sbcs,
            )
            normalizacao["alerta_snmp_enviado"] = True
            normalizacao["snmp_restabelecido_em"] = agora.isoformat(
                timespec="seconds"
            )
            print(
                "✅ Acesso SNMP aos quatro SBCs restabelecido após a mudança de IP."
            )
        except Exception as erro:
            print(
                "Aviso: não foi possível enviar o alerta de SNMP SBC "
                f"restabelecido: {erro}"
            )

    referencia = _parse_iso(normalizacao.get("detectado_em"))
    status_traps = carregar_status_traps_sbc()
    traps_confirmados = {}

    for ip in SBCS_SBC:
        item = status_traps.get("sbcs", {}).get(ip, {})
        recebido_em = _parse_iso(item.get("ultimo_trap_em"))
        if referencia and recebido_em and recebido_em > referencia:
            traps_confirmados[ip] = recebido_em.strftime("%H:%M:%S")

    normalizacao["traps_ok_ips"] = sorted(traps_confirmados)

    if (
        todos_sbcs.issubset(snmp_ok)
        and todos_sbcs.issubset(traps_confirmados)
        and not normalizacao.get("alerta_normalizacao_enviado")
    ):
        agora = datetime.now()
        try:
            enviar_alerta_snmp_traps_normalizados(
                telegram=telegram,
                ip_vpn=ip_vpn,
                traps_confirmados=traps_confirmados,
            )
            normalizacao["alerta_normalizacao_enviado"] = True
            normalizacao["pendente"] = False
            normalizacao["concluido_em"] = agora.isoformat(
                timespec="seconds"
            )
            print(
                "✅ SNMP + traps dos quatro SBCs confirmados após a mudança de IP."
            )
        except Exception as erro:
            print(
                "Aviso: não foi possível enviar o alerta final de "
                f"normalização SBC: {erro}"
            )

    estado["normalizacao_sbc"] = normalizacao
    salvar_status_comunicacao(estado)
    return estado


def carregar_historico_vpn():
    padrao = {
        "eventos": [],
        "ultimo_resumo_enviado": None,
    }

    if not CAMINHO_HISTORICO_VPN.exists():
        return padrao

    try:
        dados = json.loads(
            CAMINHO_HISTORICO_VPN.read_text(encoding="utf-8")
        )
        if not isinstance(dados, dict):
            return padrao
        dados.setdefault("eventos", [])
        dados.setdefault("ultimo_resumo_enviado", None)
        return dados
    except (OSError, json.JSONDecodeError) as erro:
        print(
            "Aviso: não foi possível ler o histórico de desconexões VPN: "
            f"{erro}"
        )
        return padrao


def salvar_historico_vpn(dados):
    CAMINHO_HISTORICO_VPN.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    temporario = CAMINHO_HISTORICO_VPN.with_suffix(".tmp")
    temporario.write_text(
        json.dumps(
            dados,
            ensure_ascii=False,
            indent=4,
        ),
        encoding="utf-8",
    )
    temporario.replace(CAMINHO_HISTORICO_VPN)


def registrar_desconexao_vpn(inicio, ip_anterior=None):
    """Registra uma nova indisponibilidade confirmada da VPN/rede corporativa."""
    dados = carregar_historico_vpn()
    eventos = dados.setdefault("eventos", [])
    inicio_iso = inicio.isoformat(timespec="seconds")

    # Evita duplicidade caso o processo seja reiniciado durante a mesma lacuna.
    for evento in reversed(eventos):
        if evento.get("fim") is not None:
            break
        if evento.get("inicio") == inicio_iso:
            return

    eventos.append(
        {
            "inicio": inicio_iso,
            "fim": None,
            "duracao_segundos": None,
            "ip_anterior": ip_anterior,
            "ip_atual": None,
            "ip_alterado": None,
        }
    )
    salvar_historico_vpn(dados)
    print(
        "🧾 Desconexão VPN registrada para o consolidado diário: "
        f"{inicio:%H:%M:%S}."
    )


def finalizar_desconexao_vpn(fim, ip_atual=None):
    """Fecha a última desconexão ainda aberta e calcula sua duração."""
    dados = carregar_historico_vpn()
    eventos = dados.setdefault("eventos", [])

    evento_aberto = None
    for evento in reversed(eventos):
        if evento.get("fim") is None:
            evento_aberto = evento
            break

    if evento_aberto is None:
        return

    inicio = _parse_iso(evento_aberto.get("inicio"))
    evento_aberto["fim"] = fim.isoformat(timespec="seconds")
    if inicio is not None:
        evento_aberto["duracao_segundos"] = max(
            0,
            int((fim - inicio).total_seconds()),
        )

    evento_aberto["ip_atual"] = ip_atual
    ip_anterior = evento_aberto.get("ip_anterior")
    if ip_anterior and ip_atual:
        evento_aberto["ip_alterado"] = ip_anterior != ip_atual

    salvar_historico_vpn(dados)


def _parse_horario_hhmm(valor, padrao="21:10"):
    try:
        return datetime.strptime(
            str(valor).strip(),
            "%H:%M",
        ).time()
    except ValueError:
        print(
            f"Aviso: horário '{valor}' inválido. "
            f"Será utilizado {padrao}."
        )
        return datetime.strptime(padrao, "%H:%M").time()


def enviar_resumo_diario_vpn(telegram, agora, eventos_do_dia):
    linhas_eventos = []

    for indice, evento in enumerate(eventos_do_dia, start=1):
        inicio = _parse_iso(evento.get("inicio"))
        fim = _parse_iso(evento.get("fim"))

        inicio_txt = inicio.strftime("%H:%M:%S") if inicio else "?"
        fim_txt = fim.strftime("%H:%M:%S") if fim else "EM ANDAMENTO"

        if evento.get("duracao_segundos") is not None:
            duracao_txt = formatar_duracao(
                evento["duracao_segundos"]
            )
        elif inicio is not None:
            duracao_txt = formatar_duracao(
                (agora - inicio).total_seconds()
            ) + " (em andamento)"
        else:
            duracao_txt = "indisponível"

        linha = (
            f"{indice}. {inicio_txt} → {fim_txt} "
            f"({duracao_txt})"
        )

        ip_anterior = evento.get("ip_anterior")
        ip_atual = evento.get("ip_atual")
        if ip_anterior or ip_atual:
            linha += (
                "\n   IP: "
                f"{ip_anterior or '?'} → {ip_atual or '?'}"
            )

        linhas_eventos.append(linha)

    if not linhas_eventos:
        linhas_eventos = ["Nenhuma desconexão confirmada no período."]

    texto = "\n".join(
        [
            "📊 <b>TELECOM MONITOR — RESUMO DIÁRIO DA VPN</b>",
            "",
            "📅 <b>Data</b>",
            agora.strftime("%d/%m/%Y"),
            "",
            "🔌 <b>Total de desconexões confirmadas</b>",
            str(len(eventos_do_dia)),
            "",
            "🕐 <b>Horários</b>",
            *linhas_eventos,
            "",
            "ℹ️ Consolidado gerado automaticamente às 21:10.",
        ]
    )
    telegram.enviar_mensagem(texto)


def tentar_enviar_resumo_diario_vpn(
    telegram,
    horario_resumo="21:10",
):
    """Envia uma vez por dia, após o horário configurado, o consolidado da VPN."""
    agora = datetime.now()
    horario = _parse_horario_hhmm(horario_resumo)

    alvo = agora.replace(
        hour=horario.hour,
        minute=horario.minute,
        second=0,
        microsecond=0,
    )

    if agora < alvo:
        return False

    dados = carregar_historico_vpn()
    hoje = agora.date().isoformat()

    if dados.get("ultimo_resumo_enviado") == hoje:
        return False

    eventos_do_dia = []
    for evento in dados.get("eventos", []):
        inicio = _parse_iso(evento.get("inicio"))
        if inicio is not None and inicio.date() == agora.date():
            eventos_do_dia.append(evento)

    try:
        enviar_resumo_diario_vpn(
            telegram=telegram,
            agora=agora,
            eventos_do_dia=eventos_do_dia,
        )
    except Exception as erro:
        print(
            "Aviso: não foi possível enviar o resumo diário da VPN: "
            f"{erro}"
        )
        return False

    dados["ultimo_resumo_enviado"] = hoje
    dados["ultimo_resumo_enviado_em"] = agora.isoformat(
        timespec="seconds"
    )
    salvar_historico_vpn(dados)

    print(
        "📊 Resumo diário da VPN enviado: "
        f"{len(eventos_do_dia)} desconexão(ões)."
    )
    return True


def obter_session_agents_sem_alerta():
    """
    Retorna os Session Agents que continuam sendo monitorados/coletados,
    mas não devem gerar notificações no Telegram pelo monitor SBC.

    A lista pode ser alterada no .env pela variável:
        SBC_SESSION_AGENTS_SEM_ALERTA=10.90.11.23,10.x.x.x
    """
    valor = os.getenv(
        "SBC_SESSION_AGENTS_SEM_ALERTA",
        "10.90.11.23",
    )

    return {
        item.strip()
        for item in valor.split(",")
        if item.strip()
    }


def obter_inteiro_positivo(
    nome_variavel,
    padrao,
):
    valor = os.getenv(
        nome_variavel,
        str(padrao),
    )

    try:
        numero = int(valor)

    except ValueError:
        print(
            f"Aviso: {nome_variavel} inválido. "
            f"Será utilizado {padrao}."
        )
        return padrao

    if numero <= 0:
        print(
            f"Aviso: {nome_variavel} deve ser "
            f"maior que zero. Será utilizado {padrao}."
        )
        return padrao

    return numero



def obter_float_positivo(
    nome_variavel,
    padrao,
):
    valor = os.getenv(
        nome_variavel,
        str(padrao),
    )

    try:
        numero = float(valor)

    except ValueError:
        print(
            f"Aviso: {nome_variavel} inválido. "
            f"Será utilizado {padrao}."
        )
        return float(padrao)

    if numero <= 0:
        print(
            f"Aviso: {nome_variavel} deve ser "
            f"maior que zero. Será utilizado {padrao}."
        )
        return float(padrao)

    return numero


def carregar_status_comunicacao():
    """
    Mantém o estado da comunicação corporativa entre
    reinicializações do monitor/Windows.
    """
    padrao = {
        "suspensa": False,
        "inicio_indisponibilidade": None,
        "primeira_falha_em": None,
        "falhas_consecutivas": 0,
        "alerta_indisponibilidade_enviado": False,
        "alerta_indisponibilidade_pendente": None,
        "alerta_restabelecimento_pendente": None,
        "alerta_ip_vpn_pendente": None,
        "ultimo_ip_vpn": None,
        "normalizacao_sbc": None,
    }

    if not CAMINHO_STATUS_COMUNICACAO.exists():
        return padrao

    try:
        conteudo = CAMINHO_STATUS_COMUNICACAO.read_text(
            encoding="utf-8"
        )
        dados = json.loads(conteudo)

        for chave, valor in padrao.items():
            dados.setdefault(chave, valor)

        return dados

    except (
        OSError,
        json.JSONDecodeError,
    ) as erro:
        print(
            "Aviso: não foi possível ler o estado da "
            f"comunicação corporativa: {erro}"
        )
        return padrao


def salvar_status_comunicacao(estado):
    CAMINHO_STATUS_COMUNICACAO.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    dados = dict(estado)
    dados["atualizado_em"] = datetime.now().isoformat(
        timespec="seconds"
    )

    temporario = CAMINHO_STATUS_COMUNICACAO.with_suffix(
        ".tmp"
    )

    temporario.write_text(
        json.dumps(
            dados,
            ensure_ascii=False,
            indent=4,
        ),
        encoding="utf-8",
    )

    temporario.replace(
        CAMINHO_STATUS_COMUNICACAO
    )


def obter_configuracao_por_nome(nome):
    nome_normalizado = str(nome).strip().casefold()

    for configuracao in SERVIDORES:
        if (
            configuracao["nome"]
            .strip()
            .casefold()
            == nome_normalizado
        ):
            return configuracao

    return None


def obter_alvos_rede_corporativa():
    """
    Os alvos são MGWs normalmente disponíveis.
    MGW-117 não participa do health check porque já é
    conhecido como indisponível.
    """
    valor = os.getenv(
        "ALVOS_REDE_CORPORATIVA",
        "MGW-114,MGW-116,MGW-121",
    )

    nomes = [
        item.strip()
        for item in valor.split(",")
        if item.strip()
    ]

    alvos = []

    for nome in nomes:
        configuracao = obter_configuracao_por_nome(
            nome
        )

        if configuracao is None:
            print(
                "Aviso: alvo de rede corporativa "
                f"desconhecido no .env: {nome}"
            )
            continue

        alvos.append(configuracao)

    if alvos:
        return alvos

    print(
        "Aviso: nenhum ALVOS_REDE_CORPORATIVA válido. "
        "Usando MGW-114, MGW-116 e MGW-121."
    )

    return [
        configuracao
        for configuracao in SERVIDORES
        if configuracao["nome"]
        in {
            "MGW-114",
            "MGW-116",
            "MGW-121",
        }
    ]


def obter_host_porta(configuracao):
    url = urlparse(configuracao["url"])

    host = (
        url.hostname
        or configuracao.get("endereco")
    )

    if url.port is not None:
        porta = url.port

    elif url.scheme.lower() == "https":
        porta = 443

    else:
        porta = 80

    return host, porta


def testar_alvo_rede(
    configuracao,
    timeout,
):
    host, porta = obter_host_porta(configuracao)

    try:
        with socket.create_connection(
            (host, porta),
            timeout=timeout,
        ):
            return True

    except OSError:
        return False


def verificar_rede_corporativa(
    alvos,
    minimo_acessiveis,
    timeout,
):
    """
    Confirma a conectividade por múltiplos pontos.
    Não usa ping e não depende de um único MGW.
    """
    resultados = {}

    for configuracao in alvos:
        nome = configuracao["nome"]

        resultados[nome] = testar_alvo_rede(
            configuracao=configuracao,
            timeout=timeout,
        )

    acessiveis = sum(
        1
        for acessivel in resultados.values()
        if acessivel
    )

    minimo_efetivo = min(
        max(1, minimo_acessiveis),
        len(alvos),
    )

    rede_ok = acessiveis >= minimo_efetivo

    return (
        rede_ok,
        acessiveis,
        minimo_efetivo,
        resultados,
    )


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

    if horas or dias:
        partes.append(
            f"{horas} h"
        )

    if minutos or horas or dias:
        partes.append(
            f"{minutos} min"
        )

    partes.append(
        f"{segundos} s"
    )

    return " ".join(partes)


def enviar_alerta_comunicacao_indisponivel(
    telegram,
    inicio,
    entregue_apos_restabelecimento=False,
):
    horario = inicio.strftime(
        "%d/%m/%Y %H:%M:%S"
    )

    linhas = [
        "⚠️ <b>TELECOM MONITOR — COMUNICAÇÃO "
        "CORPORATIVA INDISPONÍVEL</b>",
        "",
        "🌐 Não foi possível confirmar o acesso "
        "à rede corporativa dos MGWs.",
        "",
        "⏸ <b>Monitoramento temporariamente "
        "suspenso.</b>",
        "",
        "As falhas dos MGWs, IP Peers e Rotas "
        "não serão registradas enquanto a "
        "comunicação estiver indisponível.",
        "",
        "🕐 <b>Detectado em</b>",
        horario,
    ]

    if entregue_apos_restabelecimento:
        linhas.extend(
            [
                "",
                "ℹ️ <b>Notificação entregue após o "
                "restabelecimento da conectividade.</b>",
            ]
        )

    telegram.enviar_mensagem(
        "\n".join(linhas)
    )


def enviar_alerta_comunicacao_restabelecida(
    telegram,
    inicio,
    fim,
):
    duracao = (
        fim - inicio
    ).total_seconds()

    texto = "\n".join(
        [
            "✅ <b>TELECOM MONITOR — COMUNICAÇÃO "
            "RESTABELECIDA</b>",
            "",
            "🌐 A comunicação com a rede corporativa "
            "foi restabelecida.",
            "",
            "▶️ <b>Monitoramento dos MGWs retomado "
            "normalmente.</b>",
            "",
            "⏱ <b>Tempo sem comunicação</b>",
            formatar_duracao(
                duracao
            ),
            "",
            "🕐 <b>Restabelecida em</b>",
            fim.strftime(
                "%d/%m/%Y %H:%M:%S"
            ),
        ]
    )

    telegram.enviar_mensagem(
        texto
    )


def processar_alertas_telegram_pendentes(estado, telegram):
    """
    Tenta entregar, em ordem, alertas operacionais que não puderam
    ser enviados durante a indisponibilidade da VPN/rede.

    A ordem é intencional:
      1) indisponibilidade;
      2) restabelecimento;
      3) alteração de IP VPN.

    Se um envio falhar, os itens permanecem no JSON de estado e
    serão tentados novamente no próximo ciclo com rede disponível.
    """

    pendente_indisponibilidade = estado.get(
        "alerta_indisponibilidade_pendente"
    )

    if isinstance(pendente_indisponibilidade, dict):
        inicio = _parse_iso(
            pendente_indisponibilidade.get("inicio")
        )
        if inicio is not None:
            try:
                enviar_alerta_comunicacao_indisponivel(
                    telegram=telegram,
                    inicio=inicio,
                    entregue_apos_restabelecimento=True,
                )
                estado["alerta_indisponibilidade_pendente"] = None
                estado["alerta_indisponibilidade_enviado"] = True
                print(
                    "📨 Alerta de indisponibilidade pendente enviado "
                    "após o restabelecimento."
                )
            except Exception as erro:
                print(
                    "Aviso: ainda não foi possível entregar o alerta "
                    f"pendente de indisponibilidade: {erro}"
                )
                salvar_status_comunicacao(estado)
                return estado
        else:
            estado["alerta_indisponibilidade_pendente"] = None

    pendente_restabelecimento = estado.get(
        "alerta_restabelecimento_pendente"
    )

    if isinstance(pendente_restabelecimento, dict):
        inicio = _parse_iso(
            pendente_restabelecimento.get("inicio")
        )
        fim = _parse_iso(
            pendente_restabelecimento.get("fim")
        )
        if inicio is not None and fim is not None:
            try:
                enviar_alerta_comunicacao_restabelecida(
                    telegram=telegram,
                    inicio=inicio,
                    fim=fim,
                )
                estado["alerta_restabelecimento_pendente"] = None
                print(
                    "📨 Alerta de restabelecimento pendente enviado."
                )
            except Exception as erro:
                print(
                    "Aviso: ainda não foi possível entregar o alerta "
                    f"pendente de restabelecimento: {erro}"
                )
                salvar_status_comunicacao(estado)
                return estado
        else:
            estado["alerta_restabelecimento_pendente"] = None

    pendente_ip = estado.get("alerta_ip_vpn_pendente")

    if isinstance(pendente_ip, dict):
        ip_anterior = pendente_ip.get("ip_anterior")
        ip_atual = pendente_ip.get("ip_atual")

        if ip_anterior and ip_atual:
            try:
                enviar_alerta_alteracao_ip_vpn(
                    telegram=telegram,
                    ip_anterior=ip_anterior,
                    ip_atual=ip_atual,
                )
                estado["alerta_ip_vpn_pendente"] = None
                print(
                    "📨 Alerta pendente de alteração de IP VPN enviado."
                )
            except Exception as erro:
                print(
                    "Aviso: ainda não foi possível entregar o alerta "
                    f"pendente de alteração de IP VPN: {erro}"
                )
                salvar_status_comunicacao(estado)
                return estado
        else:
            estado["alerta_ip_vpn_pendente"] = None

    salvar_status_comunicacao(estado)
    return estado


def garantir_arquivo_periodos_invalidos():
    """
    Garante que o CSV de períodos inválidos exista com
    o cabeçalho esperado.

    O arquivo também pode conter registros manuais já
    existentes; eles são preservados.
    """
    CAMINHO_PERIODOS_INVALIDOS.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if (
        CAMINHO_PERIODOS_INVALIDOS.exists()
        and CAMINHO_PERIODOS_INVALIDOS.stat().st_size > 0
    ):
        return

    with CAMINHO_PERIODOS_INVALIDOS.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as arquivo:
        writer = csv.writer(arquivo)
        writer.writerow(
            CABECALHO_PERIODOS_INVALIDOS
        )


def registrar_lacuna_rede(
    inicio,
    fim,
):
    """
    Registra no CSV somente a janela efetivamente
    indisponível.

    Se a indisponibilidade atravessar a meia-noite,
    divide o evento em uma linha por data para que os
    campos 'data', 'inicio' e 'fim' continuem coerentes.
    """
    if fim < inicio:
        print(
            "Aviso: lacuna de rede ignorada porque "
            "o horário final é anterior ao inicial."
        )
        return 0

    garantir_arquivo_periodos_invalidos()

    linhas = []
    cursor = inicio

    while cursor.date() < fim.date():
        fim_do_dia = cursor.replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=0,
        )

        linhas.append(
            [
                cursor.strftime("%Y-%m-%d"),
                cursor.strftime("%H:%M:%S"),
                fim_do_dia.strftime("%H:%M:%S"),
                "MONITORAMENTO",
                "LACUNA_REDE",
                (
                    "Indisponibilidade da VPN/rede "
                    "corporativa detectada automaticamente"
                ),
                (
                    "Conectividade restabelecida; "
                    "intervalo excluído das análises"
                ),
            ]
        )

        cursor = (
            fim_do_dia
            + timedelta(seconds=1)
        )

    linhas.append(
        [
            cursor.strftime("%Y-%m-%d"),
            cursor.strftime("%H:%M:%S"),
            fim.strftime("%H:%M:%S"),
            "MONITORAMENTO",
            "LACUNA_REDE",
            (
                "Indisponibilidade da VPN/rede "
                "corporativa detectada automaticamente"
            ),
            (
                "Conectividade restabelecida; "
                "intervalo excluído das análises"
            ),
        ]
    )

    with CAMINHO_PERIODOS_INVALIDOS.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as arquivo:
        writer = csv.writer(arquivo)
        writer.writerows(linhas)

    duracao = max(
        0,
        int(
            (fim - inicio).total_seconds()
        ),
    )

    print(
        "📝 Lacuna de rede registrada em "
        f"{CAMINHO_PERIODOS_INVALIDOS}: "
        f"{formatar_duracao(duracao)}."
    )

    return len(linhas)


def processar_rede_corporativa(
    estado,
    rede_ok,
    limite_falhas,
    telegram,
):
    """
    Retorna (estado_atualizado, pode_monitorar,
    comunicacao_foi_restabelecida).

    Uma única falha de health check já faz o ciclo ser
    pulado por segurança. O alerta de suspensão só é
    considerado após o número configurado de falhas
    consecutivas.

    Se o Telegram não estiver acessível durante a queda, o alerta
    fica persistido em status_comunicacao.json e é entregue, em
    ordem, assim que a rede voltar.
    """
    agora = datetime.now()
    restabelecida = False

    if rede_ok:
        ip_vpn_atual = descobrir_ip_vpn()
        ip_vpn_anterior = estado.get("ultimo_ip_vpn")

        estava_suspensa = bool(
            estado.get("suspensa")
        )

        inicio_texto = estado.get(
            "inicio_indisponibilidade"
        )

        if estava_suspensa:
            inicio = (
                datetime.fromisoformat(
                    inicio_texto
                )
                if inicio_texto
                else agora
            )

            try:
                registrar_lacuna_rede(
                    inicio=inicio,
                    fim=agora,
                )

            except Exception as erro:
                print(
                    "Aviso: não foi possível registrar "
                    "a lacuna de rede no CSV: "
                    f"{erro}"
                )

            try:
                finalizar_desconexao_vpn(
                    fim=agora,
                    ip_atual=ip_vpn_atual,
                )
            except Exception as erro:
                print(
                    "Aviso: não foi possível finalizar o registro "
                    f"da desconexão VPN: {erro}"
                )

            # O restabelecimento deve sempre ser comunicado. Se a
            # notificação de queda falhou durante a indisponibilidade,
            # ela permanece pendente e será enviada antes desta.
            estado["alerta_restabelecimento_pendente"] = {
                "inicio": inicio.isoformat(timespec="seconds"),
                "fim": agora.isoformat(timespec="seconds"),
            }

            if (
                ip_vpn_anterior
                and ip_vpn_atual
                and ip_vpn_anterior != ip_vpn_atual
            ):
                print(
                    "🚨 Alteração de IP VPN detectada: "
                    f"{ip_vpn_anterior} -> {ip_vpn_atual}"
                )

                estado["alerta_ip_vpn_pendente"] = {
                    "ip_anterior": ip_vpn_anterior,
                    "ip_atual": ip_vpn_atual,
                    "detectado_em": agora.isoformat(timespec="seconds"),
                }

                estado["normalizacao_sbc"] = {
                    "pendente": True,
                    "detectado_em": agora.isoformat(timespec="seconds"),
                    "ip_anterior": ip_vpn_anterior,
                    "ip_atual": ip_vpn_atual,
                    "snmp_ok_ips": [],
                    "traps_ok_ips": [],
                    "alerta_snmp_enviado": False,
                    "alerta_normalizacao_enviado": False,
                }

            # Agora que a conectividade corporativa voltou, tentamos
            # entregar os alertas na ordem correta. Se o Telegram ainda
            # estiver indisponível, o JSON mantém tudo para o próximo ciclo.
            estado = processar_alertas_telegram_pendentes(
                estado=estado,
                telegram=telegram,
            )

            print(
                "Comunicação corporativa "
                "restabelecida. Monitoramento retomado."
            )

            restabelecida = True

        else:
            # Pode haver alertas pendentes de um ciclo anterior em que a
            # rede já voltou, mas o Telegram ainda não estava acessível.
            estado = processar_alertas_telegram_pendentes(
                estado=estado,
                telegram=telegram,
            )

        estado.update(
            {
                "suspensa": False,
                "inicio_indisponibilidade": None,
                "primeira_falha_em": None,
                "falhas_consecutivas": 0,
                "alerta_indisponibilidade_enviado": False,
            }
        )

        if ip_vpn_atual:
            estado["ultimo_ip_vpn"] = ip_vpn_atual

        salvar_status_comunicacao(
            estado
        )

        return (
            estado,
            True,
            restabelecida,
        )

    falhas = int(
        estado.get(
            "falhas_consecutivas",
            0,
        )
    ) + 1

    estado["falhas_consecutivas"] = falhas

    if not estado.get("primeira_falha_em"):
        estado["primeira_falha_em"] = (
            agora.isoformat(
                timespec="seconds"
            )
        )

    if (
        falhas >= limite_falhas
        and not estado.get("suspensa")
    ):
        inicio_texto = estado.get(
            "primeira_falha_em"
        )

        estado["suspensa"] = True
        estado["inicio_indisponibilidade"] = (
            inicio_texto
            or agora.isoformat(
                timespec="seconds"
            )
        )

        print(
            "Comunicação corporativa indisponível. "
            "Monitoramento suspenso."
        )

        try:
            registrar_desconexao_vpn(
                inicio=datetime.fromisoformat(
                    estado["inicio_indisponibilidade"]
                ),
                ip_anterior=estado.get("ultimo_ip_vpn"),
            )
        except Exception as erro:
            print(
                "Aviso: não foi possível registrar a desconexão VPN: "
                f"{erro}"
            )

    if (
        estado.get("suspensa")
        and not estado.get("alerta_indisponibilidade_enviado")
        and not estado.get("alerta_indisponibilidade_pendente")
    ):
        inicio = datetime.fromisoformat(
            estado["inicio_indisponibilidade"]
        )

        try:
            enviar_alerta_comunicacao_indisponivel(
                telegram=telegram,
                inicio=inicio,
                entregue_apos_restabelecimento=False,
            )

            estado["alerta_indisponibilidade_enviado"] = True

        except Exception as erro:
            # Não insistimos enquanto a VPN está fora. Persistimos o alerta
            # para entregá-lo assim que a conectividade for restabelecida.
            estado["alerta_indisponibilidade_pendente"] = {
                "inicio": inicio.isoformat(timespec="seconds"),
            }
            print(
                "Aviso: não foi possível enviar o alerta de perda da "
                "comunicação agora; ele ficou pendente para envio após "
                f"o restabelecimento: {erro}"
            )

    salvar_status_comunicacao(
        estado
    )

    return (
        estado,
        False,
        False,
    )


def bloquear_instancia_unica():
    """
    Impede que duas instâncias do monitor sejam
    executadas simultaneamente.
    """
    bloqueio = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    )

    try:
        bloqueio.bind(
            (
                "127.0.0.1",
                PORTA_INSTANCIA_UNICA,
            )
        )

        bloqueio.listen(1)

    except OSError:
        bloqueio.close()

        raise RuntimeError(
            "Já existe uma instância do "
            "MGW Monitor em execução."
        )

    return bloqueio


def consultar_servidor(
    configuracao,
    usuario,
    senha,
):
    nome = configuracao["nome"]

    print(
        f"[{datetime.now():%d/%m/%Y %H:%M:%S}] "
        f"Consultando {nome}..."
    )

    try:
        cliente = MGWClient(
            base_url=configuracao["url"],
            verificar_ssl=configuracao[
                "verificar_ssl"
            ],
        )

        if cliente.conectar() is None:
            print(
                f"  Falha na conexão com {nome}."
            )
            return None

        if cliente.fazer_login(
            usuario,
            senha,
        ) is None:
            print(
                f"  Falha no login de {nome}."
            )
            return None

        monitor = Monitor(
            cliente=cliente,
            servidor=nome,
        )

        naps = monitor.obter_naps()

        if naps is None:
            print(
                f"  Falha na consulta dos NAPs "
                f"de {nome}."
            )
            return None

        print(
            f"  Sucesso: {len(naps)} registros."
        )

        return naps

    except Exception as erro:
        print(
            f"  Erro inesperado em {nome}: "
            f"{erro}"
        )
        return None


def carregar_status_servidores():
    if not CAMINHO_STATUS_SERVIDORES.exists():
        return None

    try:
        conteudo = (
            CAMINHO_STATUS_SERVIDORES.read_text(
                encoding="utf-8"
            )
        )

        dados = json.loads(conteudo)

        return dados.get("servidores", {})

    except (
        OSError,
        json.JSONDecodeError,
    ) as erro:
        print(
            "Aviso: não foi possível ler o estado "
            f"dos servidores: {erro}"
        )

        return None


def salvar_status_servidores(status):
    CAMINHO_STATUS_SERVIDORES.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    dados = {
        "atualizado_em": datetime.now().isoformat(
            timespec="seconds"
        ),
        "servidores": status,
    }

    arquivo_temporario = (
        CAMINHO_STATUS_SERVIDORES.with_suffix(
            ".tmp"
        )
    )

    arquivo_temporario.write_text(
        json.dumps(
            dados,
            ensure_ascii=False,
            indent=4,
        ),
        encoding="utf-8",
    )

    arquivo_temporario.replace(
        CAMINHO_STATUS_SERVIDORES
    )


def criar_chave_rota(
    servidor,
    nome,
    endereco,
):
    return (
        str(servidor or "").strip().casefold(),
        str(nome or "").strip().casefold(),
        str(endereco or "").strip().casefold(),
    )


def criar_estado_rotas(naps):
    estado = {}

    for nap in naps:
        endereco = nap.endereco()

        chave = criar_chave_rota(
            servidor=nap.servidor,
            nome=nap.nome,
            endereco=endereco,
        )

        estado[chave] = {
            "servidor": nap.servidor,
            "nap_id": nap.id,
            "nome": nap.nome,
            "endereco": endereco,
            "ativo": nap.esta_ativo(),
            "tipo": nap.tipo,
            "busy": nap.busy,
            "idle": nap.idle,
            "fail": nap.fail,
        }

    return estado


def comparar_rotas(
    estado_anterior,
    estado_atual,
    servidores_com_evento,
):
    novos_inativos = []
    recuperados = []

    servidores_com_evento = set(
        servidores_com_evento
    )

    for chave, atual in estado_atual.items():
        anterior = estado_anterior.get(chave)

        if anterior is None:
            continue

        if atual["servidor"] in servidores_com_evento:
            continue

        ativo_anterior = anterior["ativo"]
        ativo_atual = atual["ativo"]

        item = {
            "servidor": atual["servidor"],
            "nap_id": atual["nap_id"],
            "nome": atual["nome"],
            "endereco": atual["endereco"],
            "ativo_anterior": ativo_anterior,
            "ativo_atual": ativo_atual,
            "busy_anterior": anterior["busy"],
            "busy_atual": atual["busy"],
            "idle_anterior": anterior["idle"],
            "idle_atual": atual["idle"],
            "fail_anterior": anterior["fail"],
            "fail_atual": atual["fail"],
        }

        if ativo_anterior and not ativo_atual:
            novos_inativos.append(item)

        elif not ativo_anterior and ativo_atual:
            recuperados.append(item)

    return novos_inativos, recuperados


def atualizar_estado_rotas(
    estado_anterior,
    estado_atual,
):
    """
    Atualiza as rotas dos servidores consultados e
    preserva o último estado conhecido das rotas dos
    servidores que ficaram inacessíveis.
    """
    estado_resultante = dict(
        estado_anterior
    )

    for chave, item in estado_atual.items():
        estado_resultante[chave] = item

    return estado_resultante


def processar_status_servidores(
    status_confirmado,
    falhas_consecutivas,
    status_consulta,
    limite_falhas,
):
    offline = []
    recuperados = []

    if status_confirmado is None:
        status_confirmado = {
            nome: online
            for nome, online
            in status_consulta.items()
        }

        falhas_consecutivas = {
            nome: 0
            for nome in status_consulta
        }

        return (
            status_confirmado,
            falhas_consecutivas,
            offline,
            recuperados,
        )

    for nome, online_atual in status_consulta.items():
        online_confirmado = status_confirmado.get(
            nome
        )

        if online_atual:
            falhas_consecutivas[nome] = 0

            if online_confirmado is False:
                status_confirmado[nome] = True
                recuperados.append(nome)

            elif online_confirmado is None:
                status_confirmado[nome] = True

            continue

        falhas_consecutivas[nome] = (
            falhas_consecutivas.get(nome, 0)
            + 1
        )

        if online_confirmado is None:
            status_confirmado[nome] = False
            continue

        if (
            online_confirmado
            and falhas_consecutivas[nome]
            >= limite_falhas
        ):
            status_confirmado[nome] = False
            offline.append(nome)

    return (
        status_confirmado,
        falhas_consecutivas,
        sorted(offline),
        sorted(recuperados),
    )


def obter_endereco_servidor(nome):
    for configuracao in SERVIDORES:
        if configuracao["nome"] == nome:
            return configuracao["endereco"]

    return "Não informado"


def enriquecer_eventos_servidores(
    banco,
    servidores_offline,
    servidores_recuperados,
    falhas_consecutivas,
    horario,
):
    eventos_offline = []
    eventos_recuperados = []

    for nome in servidores_offline:
        dados = banco.registrar_servidor_indisponivel(
            servidor=nome,
            endereco=obter_endereco_servidor(
                nome
            ),
            falhas_consecutivas=(
                falhas_consecutivas.get(
                    nome,
                    0,
                )
            ),
            horario=horario,
        )

        eventos_offline.append(dados)

    for nome in servidores_recuperados:
        dados = banco.registrar_servidor_recuperado(
            servidor=nome,
            endereco=obter_endereco_servidor(
                nome
            ),
            horario=horario,
        )

        eventos_recuperados.append(dados)

    return (
        eventos_offline,
        eventos_recuperados,
    )


def garantir_eventos_servidores_iniciais(
    banco,
    status_confirmado,
):
    """
    Garante um evento aberto para servidores que já
    estavam offline quando o monitor foi iniciado.

    Nenhuma mensagem de queda é enviada neste momento.
    """
    if not status_confirmado:
        return

    horario = datetime.now()

    for nome, online in status_confirmado.items():
        if online:
            continue

        banco.garantir_servidor_indisponivel(
            servidor=nome,
            endereco=obter_endereco_servidor(
                nome
            ),
            horario=horario,
        )


def registrar_eventos_rotas_no_banco(
    banco,
    novos_inativos,
    recuperados,
    horario,
):
    for item in novos_inativos:
        dados_evento = (
            banco.registrar_indisponibilidade(
                item=item,
                horario=horario,
            )
        )

        if dados_evento is not None:
            item.update(dados_evento)

    for item in recuperados:
        dados_evento = (
            banco.registrar_recuperacao(
                item=item,
                horario=horario,
            )
        )

        if dados_evento is not None:
            item.update(dados_evento)


def houve_evento(
    novos_inativos,
    recuperados,
    servidores_offline,
    servidores_recuperados,
):
    return any(
        [
            novos_inativos,
            recuperados,
            servidores_offline,
            servidores_recuperados,
        ]
    )


def validar_configuracao():
    usuario = os.getenv("MGW_USER")
    senha = os.getenv("MGW_PASSWORD")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    ausentes = []

    if not usuario:
        ausentes.append("MGW_USER")

    if not senha:
        ausentes.append("MGW_PASSWORD")

    if not token:
        ausentes.append(
            "TELEGRAM_BOT_TOKEN"
        )

    if not chat_id:
        ausentes.append(
            "TELEGRAM_CHAT_ID"
        )

    if ausentes:
        raise RuntimeError(
            "Variáveis ausentes no .env: "
            + ", ".join(ausentes)
        )

    return (
        usuario,
        senha,
        token,
        chat_id,
    )


def executar_ciclo(
    usuario,
    senha,
    banco,
    estado_rotas_anterior,
    status_confirmado,
    falhas_consecutivas,
    limite_falhas_servidor,
    telegram,
    confirmador=None,
):
    inicio_ciclo = time.monotonic()
    horario_evento = datetime.now()

    todos_os_naps = []
    status_consulta = {}
    status_servidores_anterior = dict(status_confirmado or {})

    print()
    print("=" * 70)
    print(
        "Início da coleta: "
        f"{horario_evento:%d/%m/%Y %H:%M:%S}"
    )
    print("=" * 70)

    for configuracao in SERVIDORES:
        nome = configuracao["nome"]

        naps = consultar_servidor(
            configuracao=configuracao,
            usuario=usuario,
            senha=senha,
        )

        online = naps is not None
        status_consulta[nome] = online

        if online:
            todos_os_naps.extend(naps)

    estado_rotas_atual = criar_estado_rotas(todos_os_naps)
    if confirmador is None:
        (
            status_confirmado, falhas_consecutivas,
            servidores_offline, servidores_recuperados,
        ) = processar_status_servidores(
            status_confirmado=status_confirmado,
            falhas_consecutivas=falhas_consecutivas,
            status_consulta=status_consulta,
            limite_falhas=limite_falhas_servidor,
        )
        servidores_com_evento = servidores_offline + servidores_recuperados
        novos_inativos, recuperados = comparar_rotas(
            estado_anterior=estado_rotas_anterior,
            estado_atual=estado_rotas_atual,
            servidores_com_evento=servidores_com_evento,
        )
    else:
        eventos_confirmados = observar_mgw(
            confirmador, status_consulta, estado_rotas_atual,
            estado_rotas_anterior, status_servidores_anterior,
        )
        servidores_offline = [e["payload"]["servidor"] for e in eventos_confirmados
                             if e["entity"].startswith("mgw:server:") and e["after"] == "False"]
        servidores_recuperados = [e["payload"]["servidor"] for e in eventos_confirmados
                                 if e["entity"].startswith("mgw:server:") and e["after"] == "True"]
        confirmed_states = {s["entity"].removeprefix("mgw:server:"): s["stable"] == "True"
                            for s in confirmador.states("mgw:server:")}
        status_confirmado = {**status_servidores_anterior, **confirmed_states}
        for nome, online in status_consulta.items():
            falhas_consecutivas[nome] = 0 if online else falhas_consecutivas.get(nome, 0) + 1
        novos_inativos, recuperados = [], []
        for event in eventos_confirmados:
            if not event["entity"].startswith("mgw:route:"):
                continue
            item = dict(event["payload"])
            item["ativo_anterior"] = event["before"] == "True"
            item["ativo_atual"] = event["after"] == "True"
            (recuperados if item["ativo_atual"] else novos_inativos).append(item)

    (
        eventos_servidores_offline,
        eventos_servidores_recuperados,
    ) = enriquecer_eventos_servidores(
        banco=banco,
        servidores_offline=servidores_offline,
        servidores_recuperados=(
            servidores_recuperados
        ),
        falhas_consecutivas=falhas_consecutivas,
        horario=horario_evento,
    )

    salvar_status_servidores(
        status_confirmado
    )

    registrar_eventos_rotas_no_banco(
        banco=banco,
        novos_inativos=novos_inativos,
        recuperados=recuperados,
        horario=horario_evento,
    )

    evento_detectado = houve_evento(
        novos_inativos=novos_inativos,
        recuperados=recuperados,
        servidores_offline=(
            eventos_servidores_offline
        ),
        servidores_recuperados=(
            eventos_servidores_recuperados
        ),
    )

    if evento_detectado and confirmador is None:
        print("Evento detectado.")

        alerta = AlertaTelegram(
            telegram=telegram
        )

        quantidade_mensagens = (
            alerta.enviar_eventos(
                novos_inativos=novos_inativos,
                recuperados=recuperados,
                servidores_offline=(
                    eventos_servidores_offline
                ),
                servidores_recuperados=(
                    eventos_servidores_recuperados
                ),
                caminho_relatorio=None,
            )
        )

        print(
            f"{quantidade_mensagens} mensagem(ns) "
            "de alerta enviada(s)."
        )

        print(
            "Evento enviado ao Telegram sem "
            "relatório Excel individual."
        )

    else:
        print(
            "Transições brutas registradas; avisos dependem de 5 minutos contínuos."
        )

    estado_rotas_resultante = (
        atualizar_estado_rotas(
            estado_anterior=estado_rotas_anterior,
            estado_atual=estado_rotas_atual,
        )
    )

    duracao = time.monotonic() - inicio_ciclo

    print(
        f"Coleta concluída em "
        f"{duracao:.1f} segundos."
    )

    return (
        todos_os_naps,
        estado_rotas_resultante,
        status_confirmado,
        falhas_consecutivas,
        evento_detectado,
    )


def tentar_enviar_resumo_diario(
    resumo_diario,
    todos_os_naps,
    nomes_servidores,
):
    if not resumo_diario.deve_enviar():
        return

    if not todos_os_naps:
        print(
            "Horário do resumo diário atingido, "
            "mas nenhum servidor forneceu dados. "
            "O envio será tentado no próximo ciclo."
        )
        return

    print(
        "Horário do resumo diário atingido. "
        "Gerando resumo e relatório diário..."
    )

    try:
        caminho = resumo_diario.enviar(
            naps=todos_os_naps,
            nomes_servidores=nomes_servidores,
        )

        print(
            "Resumo diário enviado com sucesso."
        )

        print(
            f"Relatório diário: {caminho}"
        )

    except Exception as erro:
        print(
            "Não foi possível enviar o resumo "
            f"diário: {erro}"
        )

        print(
            "O sistema tentará novamente no "
            "próximo ciclo."
        )


def main():
    load_dotenv()

    try:
        (
            usuario,
            senha,
            token,
            chat_id,
        ) = validar_configuracao()

        bloqueio_instancia = (
            bloquear_instancia_unica()
        )

    except RuntimeError as erro:
        print(f"Erro: {erro}")
        return

    intervalo_monitoramento = (
        obter_inteiro_positivo(
            "INTERVALO_MONITORAMENTO",
            30,
        )
    )

    intervalo_historico = (
        obter_inteiro_positivo(
            "INTERVALO_HISTORICO",
            300,
        )
    )

    intervalo_monitoramento_sbc = (
        obter_inteiro_positivo(
            "INTERVALO_MONITORAMENTO_SBC",
            60,
        )
    )

    limite_falhas_servidor = (
        obter_inteiro_positivo(
            "FALHAS_SERVIDOR_PARA_ALERTA",
            2,
        )
    )

    limite_falhas_rede = (
        obter_inteiro_positivo(
            "FALHAS_REDE_PARA_SUSPENDER",
            2,
        )
    )

    minimo_alvos_rede = (
        obter_inteiro_positivo(
            "ALVOS_REDE_MINIMOS",
            2,
        )
    )

    timeout_rede = obter_float_positivo(
        "TIMEOUT_REDE_CORPORATIVA",
        2,
    )

    alvos_rede = obter_alvos_rede_corporativa()

    horario_resumo_diario = os.getenv(
        "HORARIO_RESUMO_DIARIO",
        "21:00",
    )

    horario_resumo_vpn = os.getenv(
        "HORARIO_RESUMO_VPN",
        "21:10",
    )

    print("=" * 70)
    print(
        "📡 TELECOM MONITOR — Monitoramento Integrado"
    )
    print("=" * 70)
    print(
        "Intervalo entre consultas       : "
        f"{intervalo_monitoramento} segundos"
    )
    print(
        "Intervalo do histórico completo : "
        f"{intervalo_historico} segundos"
    )
    print(
        "Intervalo SBC             : "
        f"{intervalo_monitoramento_sbc} segundos"
    )
    print(
        "Falhas para servidor offline    : "
        f"{limite_falhas_servidor}"
    )
    print(
        "Falhas de rede para suspender   : "
        f"{limite_falhas_rede}"
    )
    print(
        "Mínimo de alvos corporativos    : "
        f"{minimo_alvos_rede}"
    )
    print(
        "Alvos de rede corporativa       : "
        + ", ".join(
            item["nome"]
            for item in alvos_rede
        )
    )
    print(
        "Horário do resumo diário        : "
        f"{horario_resumo_diario}"
    )
    print(
        "Horário do resumo VPN          : "
        f"{horario_resumo_vpn}"
    )
    print(
        "Histórico de desconexões VPN   : "
        f"{CAMINHO_HISTORICO_VPN}"
    )
    print(
        "Registro automático de lacunas : "
        f"{CAMINHO_PERIODOS_INVALIDOS}"
    )
    print(
        "Estado dos traps SBC     : "
        f"{CAMINHO_STATUS_TRAPS_SBC}"
    )
    print()
    print(
        "Pressione Ctrl+C para encerrar."
    )

    banco = BancoDados()

    estado_rotas = (
        banco.obter_ultimo_estado_rotas()
    )

    status_confirmado = (
        carregar_status_servidores()
    )

    garantir_eventos_servidores_iniciais(
        banco=banco,
        status_confirmado=status_confirmado,
    )

    falhas_consecutivas = {
        configuracao["nome"]: 0
        for configuracao in SERVIDORES
    }

    telegram = Telegram(
        token=token,
        chat_id=chat_id,
    )
    confirmador = Confirmador(CONFIRMACAO_DB)
    trap_listener = None
    trap_listener_enabled = os.getenv('SBC_TRAP_LISTENER_ATIVO', 'true').strip().lower() in {'1', 'true', 'sim', 'yes', 'on'}
    ultimo_inicio_trap = 0.0

    def iniciar_listener_traps():
        try:
            log_path = Path(__file__).resolve().parent / 'logs' / 'snmp_trap_listener.log'
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open('a', encoding='utf-8') as listener_log:
                processo = subprocess.Popen(
                    [sys.executable, '-u', str(Path(__file__).resolve().parent / 'teste_receber_traps_sbc.py')],
                    cwd=Path(__file__).resolve().parent, stdout=listener_log,
                    stderr=subprocess.STDOUT,
                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            print(f'Listener SNMP UDP/162 iniciado (PID {processo.pid}).')
            return processo
        except OSError as erro:
            print(f'Listener SNMP indisponível: {erro}')
            return None

    if trap_listener_enabled:
        trap_listener = iniciar_listener_traps()
        ultimo_inicio_trap = time.monotonic()
    peer_collector = None
    ultimo_inicio_peer = 0.0
    if os.getenv('SBC_ACLI_USER', '').strip():
        def iniciar_coletor_peer():
            try:
                log_path = Path(__file__).resolve().parent / 'logs' / 'sbc_peer_acli.log'
                with log_path.open('a', encoding='utf-8') as collector_log:
                    return subprocess.Popen(
                        [sys.executable, '-u', str(Path(__file__).resolve().parent / 'sbc_peer_acli.py')],
                        cwd=Path(__file__).resolve().parent, stdout=collector_log,
                        stderr=subprocess.STDOUT,
                        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            except OSError as erro:
                print(f'Coleta ACLI peer indisponível: {erro}')
                return None
        peer_collector = iniciar_coletor_peer()
        ultimo_inicio_peer = time.monotonic()
    telegram_legado = AlertasOperacionaisLegadosDesativados()

    session_agents_sem_alerta = (
        obter_session_agents_sem_alerta()
    )

    if session_agents_sem_alerta:
        print(
            "🔕 Session Agent(s) sem alerta Telegram : "
            + ", ".join(
                sorted(session_agents_sem_alerta)
            )
        )

    # ========================================================
    # SBC
    # ========================================================

    sbc_monitor = None

    sbc_monitoramento_ativo = (
        os.getenv(
            "SBC_MONITORAMENTO_ATIVO",
            "true",
        )
        .strip()
        .lower()
        in {
            "1",
            "true",
            "sim",
            "yes",
            "on",
        }
    )

    if sbc_monitoramento_ativo:
        try:
            sbc_monitor = SBCMonitor(
                community=os.getenv(
                    "SBC_SNMP_COMMUNITY",
                    "",
                ),
                telegram=telegram_legado,
            )

            print(
                "✅ Monitoramento SBC habilitado."
            )

        except Exception as erro:
            print(
                "⚠ Não foi possível iniciar "
                f"o monitoramento SBC: {erro}"
            )

            sbc_monitor = None
    else:
        print(
            "ℹ Monitoramento SBC desabilitado."
        )

    # ========================================================
    # PILOTO DE CAPACIDADE SIP
    # ========================================================

    sip_capacity_pilot = None

    # A coleta de realms e seu histórico pertencem ao serviço dedicado
    # toi-sbc-live. Impede duas rotinas de gravarem o mesmo JSONL em cadências
    # diferentes e de fazerem a leitura atual depender deste ciclo de alertas.
    piloto_sip_ativo = False

    if piloto_sip_ativo and sbc_monitor is not None:
        try:
            sip_capacity_pilot = SIPCapacityPilot(
                community=os.getenv(
                    "SBC_SNMP_COMMUNITY",
                    "",
                )
            )

            print(
                "✅ Piloto de capacidade SIP habilitado."
            )

        except Exception as erro:
            print(
                "⚠ Não foi possível iniciar o piloto SIP: "
                f"{erro}"
            )

            sip_capacity_pilot = None

    elif piloto_sip_ativo:
        print(
            "⚠ Piloto SIP não iniciado porque o "
            "monitoramento SBC está indisponível."
        )
    else:
        print(
            "ℹ Piloto de capacidade SIP desabilitado."
        )

    try:
        resumo_diario = ResumoDiario(
            banco=banco,
            telegram=telegram,
            horario=horario_resumo_diario,
            caminho_estado=(
                "dados/resumo_diario.json"
            ),
            pasta_relatorios=(
                "relatorios/diarios"
            ),
        )

    except ValueError as erro:
        print(
            "Erro na configuração do resumo "
            f"diário: {erro}"
        )

        if trap_listener is not None and trap_listener.poll() is None:
            trap_listener.terminate()
            trap_listener.wait(timeout=5)
        bloqueio_instancia.close()
        return

    nomes_servidores = [
        configuracao["nome"]
        for configuracao in SERVIDORES
    ]

    ultima_gravacao_historico = 0.0
    ultima_consulta_sbc = 0.0
    piloto_sip_em_execucao = None

    estado_comunicacao = (
        carregar_status_comunicacao()
    )

    try:
        while True:
            inicio_ciclo = time.monotonic()

            if trap_listener_enabled and (trap_listener is None or trap_listener.poll() is not None):
                if inicio_ciclo - ultimo_inicio_trap >= 60:
                    codigo = trap_listener.returncode if trap_listener is not None else 'sem processo'
                    print(f'Listener SNMP parou ({codigo}); nova tentativa controlada.')
                    trap_listener = iniciar_listener_traps()
                    ultimo_inicio_trap = inicio_ciclo
            if os.getenv('SBC_ACLI_USER', '').strip() and (peer_collector is None or peer_collector.poll() is not None):
                if inicio_ciclo - ultimo_inicio_peer >= 60:
                    peer_collector = iniciar_coletor_peer()
                    ultimo_inicio_peer = inicio_ciclo

            try:
                (
                    rede_ok,
                    alvos_acessiveis,
                    minimo_efetivo,
                    resultados_rede,
                ) = verificar_rede_corporativa(
                    alvos=alvos_rede,
                    minimo_acessiveis=(
                        minimo_alvos_rede
                    ),
                    timeout=timeout_rede,
                )

                resumo_alvos = ", ".join(
                    (
                        f"{nome}="
                        + (
                            "OK"
                            if acessivel
                            else "FALHA"
                        )
                    )
                    for nome, acessivel
                    in resultados_rede.items()
                )

                print()
                print(
                    "Health check corporativo: "
                    f"{alvos_acessiveis}/"
                    f"{len(resultados_rede)} "
                    "alvo(s) acessível(is) "
                    f"(mínimo {minimo_efetivo})."
                )
                print(
                    f"  {resumo_alvos}"
                )

                (
                    estado_comunicacao,
                    pode_monitorar,
                    comunicacao_restabelecida,
                ) = processar_rede_corporativa(
                    estado=estado_comunicacao,
                    rede_ok=rede_ok,
                    limite_falhas=(
                        limite_falhas_rede
                    ),
                    telegram=telegram_legado,
                )

                observar_vpn(
                    confirmador, rede_ok,
                    ip=estado_comunicacao.get("ultimo_ip_vpn") if rede_ok else None,
                )

                if not pode_monitorar:
                    suspender_amostras(confirmador)
                    if estado_comunicacao.get(
                        "suspensa"
                    ):
                        print(
                            "⏸ Coleta dos MGWs ignorada: "
                            "comunicação corporativa "
                            "indisponível."
                        )
                    else:
                        print(
                            "⚠ Falha de conectividade "
                            "ainda não confirmada. "
                            "O ciclo foi ignorado por "
                            "segurança."
                        )

                    duracao_ciclo = (
                        time.monotonic()
                        - inicio_ciclo
                    )

                    espera = max(
                        1,
                        intervalo_monitoramento
                        - duracao_ciclo,
                    )

                    print(
                        "Nova verificação de rede em "
                        f"{espera:.1f} segundos."
                    )

                    tentar_enviar_resumo_diario_vpn(
                        telegram=telegram,
                        horario_resumo=horario_resumo_vpn,
                    )

                    time.sleep(
                        espera
                    )
                    continue

                if comunicacao_restabelecida:
                    # Evita que uma falha de servidor
                    # anterior à queda da VPN complete
                    # indevidamente o contador depois
                    # da retomada.
                    falhas_consecutivas = {
                        configuracao["nome"]: 0
                        for configuracao in SERVIDORES
                    }

                # ================================================
                # SBC
                #
                # Só executa após o health check corporativo ter
                # confirmado que a rede/VPN está disponível.
                # Assim uma queda da VPN não é interpretada como
                # falha dos SBCs ou dos Session Agents.
                # ================================================

                agora_sbc = time.monotonic()
                resultados_sbc_ciclo = None

                deve_consultar_sbc = (
                    sbc_monitor is not None
                    and (
                        ultima_consulta_sbc == 0.0
                        or (
                            agora_sbc
                            - ultima_consulta_sbc
                            >= intervalo_monitoramento_sbc
                        )
                    )
                )

                if deve_consultar_sbc:
                    print()
                    print(
                        "📡 Consultando SBC..."
                    )

                    try:
                        agentes_conhecidos_antes = {
                            nome: tuple(ips)
                            for nome, ips in (
                                sbc_monitor.estado.get("agents_conhecidos_por_par", {})
                                or {}
                            ).items()
                        }
                        resultados_sbc = (
                            sbc_monitor.executar_ciclo()
                        )
                        resultados_sbc_ciclo = resultados_sbc
                        observar_sbcs(
                            confirmador, resultados_sbc, sbc_monitor.pares_ha,
                            ignorar_agents=session_agents_sem_alerta,
                            limite_sip=LIMITE_OPERACIONAL_SIP,
                            known_agents=agentes_conhecidos_antes,
                        )

                        if (
                            sip_capacity_pilot is not None
                            and resultados_sbc
                        ):
                            # O piloto executa vários walks SNMP e pode levar
                            # mais de um minuto. Mantê-lo fora do ciclo de
                            # confirmação preserva o teto de 120 s entre
                            # amostras MGW/SBC sem sobrepor duas coletas SIP.
                            if piloto_sip_em_execucao is None or not piloto_sip_em_execucao.is_alive():
                                def registrar_piloto(snapshot=resultados_sbc):
                                    try:
                                        sip_capacity_pilot.registrar_snapshot(snapshot)
                                    except Exception as erro_piloto:
                                        print(f"⚠ Erro ao registrar piloto de capacidade SIP: {erro_piloto}")
                                piloto_sip_em_execucao = threading.Thread(
                                    target=registrar_piloto,
                                    name="sip-capacity-pilot",
                                    daemon=True,
                                )
                                piloto_sip_em_execucao.start()
                            else:
                                print("ℹ Piloto SIP anterior ainda em coleta; amostra auxiliar adiada.")

                    except Exception as erro:
                        print(
                            "⚠ Erro no monitoramento "
                            f"SBC: {erro}"
                        )
                        for state in confirmador.states("sbc:"):
                            confirmador.observe(state["entity"], None)

                    finally:
                        # Registra a tentativa, mesmo em caso de erro,
                        # para não repetir SNMP a cada ciclo rápido.
                        ultima_consulta_sbc = (
                            time.monotonic()
                        )

                observar_peer_timeout(confirmador, CAMINHO_STATUS_TRAPS_SBC)

                estado_comunicacao = processar_normalizacao_sbc(
                    estado=estado_comunicacao,
                    resultados_sbc=resultados_sbc_ciclo,
                    telegram=telegram_legado,
                )

                (
                    todos_os_naps,
                    estado_rotas,
                    status_confirmado,
                    falhas_consecutivas,
                    evento_detectado,
                ) = executar_ciclo(
                    usuario=usuario,
                    senha=senha,
                    banco=banco,
                    estado_rotas_anterior=(
                        estado_rotas
                    ),
                    status_confirmado=(
                        status_confirmado
                    ),
                    falhas_consecutivas=(
                        falhas_consecutivas
                    ),
                    limite_falhas_servidor=(
                        limite_falhas_servidor
                    ),
                    telegram=telegram,
                    confirmador=confirmador,
                )

                try:
                    enviados = enviar_pendentes(
                        confirmador, telegram,
                        ignorar_agents=session_agents_sem_alerta,
                    )
                    if enviados:
                        print(f"Alertas confirmados entregues: {enviados}")
                except Exception as erro_envio:
                    print(f"Aviso: caixa de alertas confirmados pendente: {erro_envio}")

                agora_monotonic = time.monotonic()

                deve_salvar_historico = (
                    evento_detectado
                    or ultima_gravacao_historico
                    == 0.0
                    or (
                        agora_monotonic
                        - ultima_gravacao_historico
                        >= intervalo_historico
                    )
                )

                if (
                    deve_salvar_historico
                    and todos_os_naps
                ):
                    coleta_id = banco.salvar_coleta(
                        todos_os_naps
                    )

                    ultima_gravacao_historico = (
                        agora_monotonic
                    )

                    print(
                        "Histórico completo salvo. "
                        f"Coleta nº {coleta_id}."
                    )

                tentar_enviar_resumo_diario(
                    resumo_diario=resumo_diario,
                    todos_os_naps=todos_os_naps,
                    nomes_servidores=(
                        nomes_servidores
                    ),
                )

                tentar_enviar_resumo_diario_vpn(
                    telegram=telegram,
                    horario_resumo=horario_resumo_vpn,
                )

            except Exception as erro:
                print(
                    "Erro durante o ciclo de "
                    f"monitoramento: {erro}"
                )
                suspender_amostras(confirmador)

            duracao_ciclo = (
                time.monotonic()
                - inicio_ciclo
            )

            espera = max(
                1,
                intervalo_monitoramento
                - duracao_ciclo,
            )

            print(
                "Próxima consulta em "
                f"{espera:.1f} segundos."
            )

            time.sleep(
                espera
            )
    except KeyboardInterrupt:
        print()
        print(
            "Monitoramento encerrado "
            "pelo usuário."
        )

    finally:
        if peer_collector is not None and peer_collector.poll() is None:
            peer_collector.terminate()
            try:
                peer_collector.wait(timeout=5)
            except subprocess.TimeoutExpired:
                peer_collector.kill()
        if trap_listener is not None and trap_listener.poll() is None:
            trap_listener.terminate()
            try:
                trap_listener.wait(timeout=5)
            except subprocess.TimeoutExpired:
                trap_listener.kill()
        bloqueio_instancia.close()


if __name__ == "__main__":
    main()
