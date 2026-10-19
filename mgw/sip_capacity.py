import asyncio
import json
from datetime import datetime
from pathlib import Path

from pysnmp.hlapi.v3arch.asyncio import (
    CommunityData,
    ContextData,
    ObjectIdentity,
    ObjectType,
    SnmpEngine,
    UdpTransportTarget,
    walk_cmd,
)


# ============================================================
# TELECOM MONITOR
# CAPACIDADE SIP POR AMBIENTE
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
PASTA_DADOS = BASE_DIR / "dados" / "sip_capacity"

LIMITE_OPERACIONAL = 1200

AMBIENTES = {
    "SBCs NORTE": {
        "slug": "norte",
        "sbcs": [
            "10.90.11.132",
            "10.90.11.133",
        ],
        "capacidade_nominal": 1600,
        "realms": [
            "a-alfa-2",
            "a-internet-corpx",
            "a-thetalink-corpx",
            "a-alfa-4",
            "a-alfa-4-stir",
        ],
        "prefixos": {
            "a-alfa-2": "113557",
            "a-internet-corpx": "312509",
        },
    },
    "SBCs SUL": {
        "slug": "sul",
        "sbcs": [
            "10.90.13.10",
            "10.90.13.11",
        ],
        "capacidade_nominal": 8000,
        "realms": [
            "a-beta-link1",
            "a-beta-link2",
            "a-Epsilon",
            "a-Prefeitura",
            "c-agents-ctx",
        ],
        "prefixos": {},
    },
}


# ============================================================
# OIDs - apSigRealmStatsEntry
# ============================================================

BASE_REALM = "1.3.6.1.4.1.9148.3.2.1.2.4.1"

OID_REALM_NAME = f"{BASE_REALM}.2"
OID_INBOUND_ACTIVE = f"{BASE_REALM}.3"
OID_OUTBOUND_ACTIVE = f"{BASE_REALM}.5"
OID_INBOUND_TOTAL = f"{BASE_REALM}.7"
OID_INBOUND_HIGH = f"{BASE_REALM}.9"
OID_OUTBOUND_TOTAL = f"{BASE_REALM}.11"
OID_OUTBOUND_HIGH = f"{BASE_REALM}.13"
OID_REALM_STATUS = f"{BASE_REALM}.30"


def _inteiro(valor, padrao=0):
    try:
        return int(str(valor).strip())
    except (TypeError, ValueError):
        return padrao


def _agora():
    return datetime.now()


def _pasta_ambiente(config):
    slug = config["slug"]

    # NORTE permanece no caminho histórico para não quebrar os
    # relatórios/analisadores já existentes.
    if slug == "norte":
        return PASTA_DADOS

    return PASTA_DADOS / slug


def _arquivo_dia(data, config):
    pasta = _pasta_ambiente(config)
    return pasta / f"sip_capacity_{data:%Y-%m-%d}.jsonl"


def _arquivo_resumo_dia(data, config):
    pasta = _pasta_ambiente(config)
    return pasta / f"sip_capacity_resumo_{data:%Y-%m-%d}.json"


def _extrair_indice(oid_objeto, oid_base):
    try:
        oid_numerico = tuple(int(x) for x in oid_objeto)
        base_numerica = tuple(int(x) for x in oid_base.split("."))
    except Exception:
        return None

    tamanho = len(base_numerica)

    if oid_numerico[:tamanho] != base_numerica:
        return None

    indice = oid_numerico[tamanho:]

    if not indice:
        return None

    return ".".join(str(x) for x in indice)


async def _snmp_walk(engine, community, sbc, oid_base):
    resultados = {}

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
            indice = _extrair_indice(oid, oid_base)

            if indice is None:
                continue

            resultados[indice] = valor.prettyPrint()

    return resultados


async def _coletar_realms(
    community,
    sbc,
    nomes_desejados,
    prefixos,
):
    engine = SnmpEngine()

    try:
        nomes = await _snmp_walk(
            engine,
            community,
            sbc,
            OID_REALM_NAME,
        )
        ativos = await _snmp_walk(
            engine,
            community,
            sbc,
            OID_INBOUND_ACTIVE,
        )
        outbound_ativos = await _snmp_walk(
            engine,
            community,
            sbc,
            OID_OUTBOUND_ACTIVE,
        )
        totais = await _snmp_walk(
            engine,
            community,
            sbc,
            OID_INBOUND_TOTAL,
        )
        highs = await _snmp_walk(
            engine,
            community,
            sbc,
            OID_INBOUND_HIGH,
        )
        outbound_totais = await _snmp_walk(
            engine,
            community,
            sbc,
            OID_OUTBOUND_TOTAL,
        )
        outbound_highs = await _snmp_walk(
            engine,
            community,
            sbc,
            OID_OUTBOUND_HIGH,
        )
        status = await _snmp_walk(
            engine,
            community,
            sbc,
            OID_REALM_STATUS,
        )

    finally:
        engine.close_dispatcher()

    desejados = {nome.casefold(): nome for nome in nomes_desejados}
    por_nome = {}

    for indice, nome in nomes.items():
        nome = str(nome).strip()

        if nome.casefold() not in desejados:
            continue

        nome_configurado = desejados[nome.casefold()]
        por_nome[nome_configurado] = {
            "indice_snmp": indice,
            "prefixo": prefixos.get(nome_configurado),
            "inbound_active": _inteiro(ativos[indice]) if indice in ativos else None,
            "outbound_active": _inteiro(outbound_ativos[indice]) if indice in outbound_ativos else None,
            "inbound_total_100s": _inteiro(
                totais.get(indice, 0)
            ),
            "inbound_period_high_100s": _inteiro(
                highs.get(indice, 0)
            ),
            "outbound_total_100s": _inteiro(
                outbound_totais.get(indice, 0)
            ),
            "outbound_period_high_100s": _inteiro(
                outbound_highs.get(indice, 0)
            ),
            "status_snmp": str(
                status.get(indice, "SEM_STATUS")
            ).strip(),
        }

    return por_nome


def _carregar_resumo(data, config):
    caminho = _arquivo_resumo_dia(data, config)

    padrao = {
        "data": data.strftime("%Y-%m-%d"),
        "ambiente": None,
        "capacidade_nominal": config["capacidade_nominal"],
        "limite_operacional": LIMITE_OPERACIONAL,
        "amostras": 0,
        "pico_sip_sessions": {
            "sessoes": 0,
            "horario": None,
        },
        "realms": {
            nome: {
                "prefixo": config["prefixos"].get(nome),
                "pico_inbound_active": 0,
                "horario_pico_inbound_active": None,
                "pico_period_high_100s": 0,
                "horario_pico_period_high_100s": None,
                "pico_outbound_active": 0,
                "horario_pico_outbound_active": None,
                "pico_outbound_period_high_100s": 0,
                "horario_pico_outbound_period_high_100s": None,
            }
            for nome in config["realms"]
        },
    }

    if not caminho.exists():
        return padrao

    try:
        dados = json.loads(
            caminho.read_text(encoding="utf-8")
        )

        for chave, valor in padrao.items():
            dados.setdefault(chave, valor)

        return dados

    except (OSError, json.JSONDecodeError):
        return padrao


def _salvar_resumo(data, config, resumo):
    pasta = _pasta_ambiente(config)
    pasta.mkdir(parents=True, exist_ok=True)

    caminho = _arquivo_resumo_dia(data, config)
    temporario = caminho.with_suffix(".tmp")

    temporario.write_text(
        json.dumps(
            resumo,
            ensure_ascii=False,
            indent=4,
        ),
        encoding="utf-8",
    )

    temporario.replace(caminho)


class SIPCapacityPilot:
    """
    Mantém o nome histórico da classe para preservar a integração
    existente com monitor_alertas.py.
    """

    def __init__(self, community, sbcs_piloto=None):
        if not community:
            raise ValueError(
                "SBC_SNMP_COMMUNITY não foi configurada."
            )

        self.community = community

        # sbcs_piloto é aceito apenas por compatibilidade.
        # A configuração oficial agora está em AMBIENTES.
        self.sbcs_piloto = sbcs_piloto

        PASTA_DADOS.mkdir(
            parents=True,
            exist_ok=True,
        )

        print("Capacidade SIP:")
        print(
            f"  Limite operacional : {LIMITE_OPERACIONAL} sessões SIP"
        )

        for nome, config in AMBIENTES.items():
            print(f"  {nome}:")
            print(
                "    SBCs             : "
                + ", ".join(config["sbcs"])
            )
            print(
                "    Capacidade       : "
                f"{config['capacidade_nominal']} sessões SIP"
            )
            print(
                "    Realms           : "
                + ", ".join(config["realms"])
            )

        print(
            "  Alertas de limite  : ATIVOS via monitor SBC"
        )

    def _localizar_active(
        self,
        resultados_sbc,
        config,
    ):
        for ip in config["sbcs"]:
            dados = resultados_sbc.get(ip)

            if not isinstance(dados, dict):
                continue

            if "erro" in dados:
                continue

            if dados.get("ha") == "ACTIVE":
                return dados

        return None

    def registrar_snapshot(self, resultados_sbc):
        if not resultados_sbc:
            return None

        snapshots = {}

        for nome_ambiente, config in AMBIENTES.items():
            ativo = self._localizar_active(
                resultados_sbc,
                config,
            )

            if ativo is None:
                print(
                    "⚠ Capacidade SIP: nenhum SBC ACTIVE "
                    f"disponível em {nome_ambiente}."
                )
                continue

            sbc_ip = ativo.get("endereco")
            hostname = ativo.get("hostname")

            sessoes_sip = _inteiro(
                ativo.get(
                    "sip_sessions_ativas",
                    ativo.get("sessoes", 0),
                ),
                0,
            )

            try:
                realms = asyncio.run(
                    _coletar_realms(
                        community=self.community,
                        sbc=sbc_ip,
                        nomes_desejados=config["realms"],
                        prefixos=config["prefixos"],
                    )
                )
            except Exception as erro:
                print(
                    "⚠ Capacidade SIP: falha ao consultar "
                    f"Realms de {nome_ambiente}: {erro}"
                )
                realms = {}

            agora = _agora()
            capacidade = int(
                config["capacidade_nominal"]
            )

            snapshot = {
                "timestamp": agora.isoformat(
                    timespec="seconds"
                ),
                "ambiente": nome_ambiente,
                "sbc_active": {
                    "ip": sbc_ip,
                    "hostname": hostname,
                },
                "global": {
                    "sip_sessions_ativas": sessoes_sip,
                    # Campo legado mantido para analisadores antigos.
                    "sessoes_concorrentes": sessoes_sip,
                    "capacidade_maxima": capacidade,
                    "limite_operacional": LIMITE_OPERACIONAL,
                    "percentual_capacidade": round(
                        (
                            sessoes_sip
                            / capacidade
                            * 100
                        )
                        if capacidade > 0
                        else 0,
                        2,
                    ),
                    "percentual_limite_operacional": round(
                        (
                            sessoes_sip
                            / LIMITE_OPERACIONAL
                            * 100
                        ),
                        2,
                    ),
                    "margem_ate_limite_operacional": (
                        LIMITE_OPERACIONAL
                        - sessoes_sip
                    ),
                },
                "realms": realms,
            }

            pasta = _pasta_ambiente(config)
            pasta.mkdir(
                parents=True,
                exist_ok=True,
            )

            caminho = _arquivo_dia(
                agora,
                config,
            )

            with caminho.open(
                "a",
                encoding="utf-8",
            ) as arquivo:
                arquivo.write(
                    json.dumps(
                        snapshot,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )

            resumo = _carregar_resumo(
                agora,
                config,
            )
            resumo["ambiente"] = nome_ambiente
            resumo["amostras"] = (
                int(resumo.get("amostras", 0))
                + 1
            )

            pico = resumo[
                "pico_sip_sessions"
            ]

            if sessoes_sip > int(
                pico.get("sessoes", 0)
            ):
                pico["sessoes"] = sessoes_sip
                pico["horario"] = agora.isoformat(
                    timespec="seconds"
                )

            for nome, dados_realm in realms.items():
                info = resumo["realms"].setdefault(
                    nome,
                    {
                        "prefixo": (
                            config["prefixos"].get(nome)
                        ),
                        "pico_inbound_active": 0,
                        "horario_pico_inbound_active": None,
                        "pico_period_high_100s": 0,
                        "horario_pico_period_high_100s": None,
                    },
                )

                atual = _inteiro(dados_realm.get("inbound_active"), 0)
                high = _inteiro(
                    dados_realm.get(
                        "inbound_period_high_100s"
                    ),
                    0,
                )
                outbound_atual = _inteiro(dados_realm.get("outbound_active"), 0)
                outbound_high = _inteiro(
                    dados_realm.get(
                        "outbound_period_high_100s"
                    ),
                    0,
                )

                if atual > int(
                    info.get(
                        "pico_inbound_active",
                        0,
                    )
                ):
                    info[
                        "pico_inbound_active"
                    ] = atual
                    info[
                        "horario_pico_inbound_active"
                    ] = agora.isoformat(
                        timespec="seconds"
                    )

                if high > int(
                    info.get(
                        "pico_period_high_100s",
                        0,
                    )
                ):
                    info[
                        "pico_period_high_100s"
                    ] = high
                    info[
                        "horario_pico_period_high_100s"
                    ] = agora.isoformat(
                        timespec="seconds"
                    )

                if outbound_atual > int(
                    info.get(
                        "pico_outbound_active",
                        0,
                    )
                ):
                    info[
                        "pico_outbound_active"
                    ] = outbound_atual
                    info[
                        "horario_pico_outbound_active"
                    ] = agora.isoformat(
                        timespec="seconds"
                    )

                if outbound_high > int(
                    info.get(
                        "pico_outbound_period_high_100s",
                        0,
                    )
                ):
                    info[
                        "pico_outbound_period_high_100s"
                    ] = outbound_high
                    info[
                        "horario_pico_outbound_period_high_100s"
                    ] = agora.isoformat(
                        timespec="seconds"
                    )

            _salvar_resumo(
                agora,
                config,
                resumo,
            )

            margem = (
                LIMITE_OPERACIONAL
                - sessoes_sip
            )

            print(
                f"📊 Capacidade SIP — {nome_ambiente} | "
                f"SIP Ativas={sessoes_sip}/{capacidade} | "
                f"margem p/ {LIMITE_OPERACIONAL}={margem}"
            )

            for realm_nome in config["realms"]:
                dados_realm = realms.get(
                    realm_nome
                )

                if dados_realm is None:
                    print(
                        "  ⚠ Realm não localizado: "
                        f"{realm_nome}"
                    )
                    continue

                if (
                    nome_ambiente == "SBCs SUL"
                    and realm_nome == "c-agents-ctx"
                ):
                    # Em SUL este é o Egress Realm para o ambiente
                    # de agentes/operações. Não deve ser somado aos
                    # Realms de operadora (Ingress).
                    print(
                        f"  {realm_nome} [EGRESS]: "
                        f"OutboundActive={dados_realm['outbound_active']} | "
                        f"Total100s={dados_realm['outbound_total_100s']} | "
                        f"High100s={dados_realm['outbound_period_high_100s']}"
                    )
                elif nome_ambiente == "SBCs SUL":
                    print(
                        f"  {realm_nome} [INGRESS]: "
                        f"InboundActive={dados_realm['inbound_active']} | "
                        f"Total100s={dados_realm['inbound_total_100s']} | "
                        f"High100s={dados_realm['inbound_period_high_100s']}"
                    )
                else:
                    print(
                        f"  {realm_nome}: "
                        f"Active={dados_realm['inbound_active']} | "
                        f"Total100s={dados_realm['inbound_total_100s']} | "
                        f"High100s={dados_realm['inbound_period_high_100s']}"
                    )

            snapshots[nome_ambiente] = snapshot

        return snapshots
