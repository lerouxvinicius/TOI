import asyncio
import csv
import io
import json
import subprocess
import re
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

from pysnmp.carrier.asyncio.dgram import udp
from pysnmp.entity import engine, config
from pysnmp.entity.rfc3413 import ntfrcv
from pysnmp.proto.api import v1, v2c, verdec


# ============================================================
# CONFIGURAÇÃO
# ============================================================

LISTEN_IP = os.getenv("SBC_TRAP_LISTEN_IP", "0.0.0.0")
LISTEN_PORT = int(os.getenv("SBC_TRAP_LISTEN_PORT", "162"))
COMMUNITY = os.getenv("SBC_TRAP_COMMUNITY", "MGW MONITOR")

INTERVALO_VERIFICACAO_VPN = 10  # segundos

SBCS_CONHECIDOS = {
    "10.90.11.132": "SBC112-DC2",
    "10.90.11.133": "SBC113-DC2",
    "10.90.13.10": "SBCSUL001.SBCSUL",
    "10.90.13.11": "SBCSUL002.SBCSUL",
}

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "snmp_traps_sbc.log"
DATA_DIR = BASE_DIR / "dados"
STATUS_TRAPS_FILE = DATA_DIR / "status_traps_sbc.json"
TRAP_EVENTS_FILE = DATA_DIR / "sbc_peer_timeout_events.jsonl"
RAW_TRAPS_FILE = DATA_DIR / "sbc_traps_raw.jsonl"
RAW_ERRORS_FILE = DATA_DIR / "sbc_traps_raw_errors.jsonl"
RAW_MAX_BYTES = 20 * 1024 * 1024
RAW_BACKUPS = 5
_raw_lock = threading.Lock()
_local_sid = None
TRAP_CONFIG_FILE = Path(os.getenv("SBC_TRAP_CONFIG", str(BASE_DIR / "sbc_traps_config.json")))

OID_SYSUPTIME = "1.3.6.1.2.1.1.3.0"
OID_SNMP_TRAP = "1.3.6.1.6.3.1.1.4.1.0"
OID_SBC_ENTERPRISE = "1.3.6.1.4.1.9148"
OID_SYSNAME = "1.3.6.1.2.1.1.5.0"
# Reserved for a local receiver check; it is never an SBC alarm.
OID_TOI_SELFTEST = "1.3.6.1.3.9999.1.0"


# ============================================================
# UTILIDADES
# ============================================================

def linha(caractere="=", tamanho=90):
    return caractere * tamanho


def garantir_log():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    if not LOG_FILE.exists():
        with LOG_FILE.open("w", encoding="utf-8") as f:
            f.write("# MGW Monitor - Captura de SNMP Traps SBC\n")
            f.write(
                f"# Arquivo criado em "
                f"{datetime.now():%d/%m/%Y %H:%M:%S}\n"
            )
            f.write("#" + "=" * 88 + "\n\n")


def escrever_log(texto):
    garantir_log()

    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(texto)




def carregar_status_traps():
    padrao = {
        "atualizado_em": None,
        "listener_iniciado_em": None,
        "listener_ativo": False,
        "ultimo_trap_qualquer": None,
        "sbcs": {},
    }

    if not STATUS_TRAPS_FILE.exists():
        return padrao

    try:
        dados = json.loads(
            STATUS_TRAPS_FILE.read_text(encoding="utf-8")
        )
        if not isinstance(dados, dict):
            return padrao
        dados.setdefault("atualizado_em", None)
        dados.setdefault("listener_iniciado_em", None)
        dados.setdefault("listener_ativo", False)
        dados.setdefault("ultimo_trap_qualquer", None)
        dados.setdefault("sbcs", {})
        return dados
    except (OSError, json.JSONDecodeError):
        return padrao


def salvar_status_traps(dados):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    temporario = STATUS_TRAPS_FILE.with_suffix(".tmp")
    temporario.write_text(
        json.dumps(dados, ensure_ascii=False, indent=4),
        encoding="utf-8",
    )
    temporario.replace(STATUS_TRAPS_FILE)


def registrar_status_trap(
    agora,
    ip_origem,
    porta_origem,
    equipamento,
    trap_oid,
    classificacao,
    sys_uptime,
    var_binds=(),
):
    """Mantém um estado compacto para o monitor principal validar traps."""
    dados = carregar_status_traps()
    horario = agora.isoformat(timespec="seconds")

    evento = {
        "recebido_em": horario,
        "ip_origem": ip_origem,
        "porta_origem": porta_origem,
        "sbc": equipamento,
        "trap_oid": trap_oid,
        "classificacao": classificacao,
        "sys_uptime": sys_uptime,
    }

    dados["atualizado_em"] = horario
    dados["ultimo_trap_qualquer"] = evento

    sbcs = dados.setdefault("sbcs", {})
    sbcs[ip_origem] = {
        "nome": equipamento,
        "ultimo_trap_em": horario,
        "porta_origem": porta_origem,
        "trap_oid": trap_oid,
        "classificacao": classificacao,
        "sys_uptime": sys_uptime,
    }

    alarm = interpretar_peer_timeout(trap_oid, var_binds, ip_origem)
    if alarm:
        key = f"{alarm['pair']}|{alarm['emitter_ip']}|{alarm['peer']}"
        current = dados.setdefault("peer_timeout", {}).get(key, {})
        event_key = f"{trap_oid}|{ip_origem}|{alarm['peer']}|{sys_uptime}|{alarm['state']}"
        if current.get("event_key") == event_key:
            alarm = None
        else:
            current.update({"ip": alarm['emitter_ip'], "src_ip": ip_origem,
                            "sbc": SBCS_CONHECIDOS.get(alarm['emitter_ip'], equipamento), "peer": alarm["peer"],
                            "peer_ip": alarm["peer_ip"], "pair": alarm["pair"],
                            "state": alarm["state"], "observed_at": horario,
                            "source": "TRAP_REAL", "event_key": event_key,
                            "trap_oid": trap_oid, "varbinds": alarm["varbinds"]})
            dados["peer_timeout"][key] = current

    salvar_status_traps(dados)
    if alarm:
        TRAP_EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with TRAP_EVENTS_FILE.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(current, ensure_ascii=False) + '\n')


def oid_texto(oid):
    try:
        return oid.prettyPrint()
    except Exception:
        return str(oid)


def valor_texto(valor):
    try:
        return valor.prettyPrint()
    except Exception:
        return str(valor)


def _secure_local(path):
    """Create a private log before writing any PDU content."""
    global _local_sid
    if not path.exists():
        path.touch(mode=0o600)
    if os.name == 'nt':
        if _local_sid is None:
            account = subprocess.run(['whoami', '/user', '/fo', 'csv', '/nh'],
                                     capture_output=True, text=True, check=True)
            _local_sid = next(csv.reader(io.StringIO(account.stdout)))[1]
        result = subprocess.run(['icacls', str(path), '/inheritance:r',
                                 '/grant:r', f'*{_local_sid}:(F)',
                                 '/grant:r', '*S-1-5-18:(F)'],
                                capture_output=True, text=True)
        if result.returncode:
            raise OSError('Falha ao restringir ACL do arquivo de PDU')
    else:
        os.chmod(path, 0o600)


def _append_rotating(path, record):
    """A single append per PDU; rotated files stay local and never contain community."""
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    encoded = line.encode("utf-8")
    with _raw_lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.stat().st_size + len(encoded) > RAW_MAX_BYTES:
            oldest = path.with_name(path.name + f".{RAW_BACKUPS}")
            oldest.unlink(missing_ok=True)
            for index in range(RAW_BACKUPS - 1, 0, -1):
                source = path.with_name(path.name + f".{index}")
                if source.exists():
                    source.replace(path.with_name(path.name + f".{index + 1}"))
            path.replace(path.with_name(path.name + ".1"))
        _secure_local(path)
        with path.open("ab") as stream:
            stream.write(encoded)
            stream.flush()


def registrar_pdu_bruta(agora, ip, porta, var_binds, metadata=None):
    """Persist every decoded notification before inventory and alarm filters."""
    metadata = metadata or {}
    binds = [{"oid": oid_texto(oid), "type": type(value).__name__,
              "value": valor_texto(value)} for oid, value in var_binds]
    values = {item["oid"]: item["value"] for item in binds}
    record = {
        "received_utc": agora.astimezone(timezone.utc).isoformat(),
        "src_ip": ip, "src_port": porta,
        "snmp_version": metadata.get("snmp_version", "UNKNOWN"),
        "pdu_type": metadata.get("pdu_type"),
        "request_id": metadata.get("request_id"),
        "notification_oid": values.get(OID_SNMP_TRAP),
        "v1_enterprise": metadata.get("v1_enterprise"),
        "v1_agent_address": metadata.get("v1_agent_address"),
        "v1_generic": metadata.get("v1_generic"),
        "v1_specific": metadata.get("v1_specific"),
        "sysUpTime": values.get(OID_SYSUPTIME),
        "sysName": values.get(OID_SYSNAME),
        "context_engine_id": metadata.get("context_engine_id"),
        "varbinds": binds,
    }
    _append_rotating(RAW_TRAPS_FILE, record)
    return record


class RawNotificationReceiver(ntfrcv.NotificationReceiver):
    """Keep the PDU header that the standard callback otherwise discards."""

    def __init__(self, snmp_engine, callback):
        self._metadata = {}
        super().__init__(snmp_engine, callback, self)

    def process_pdu(self, snmp_engine, message_processing_model, security_model,
                    security_name, security_level, context_engine_id, context_name,
                    pdu_version, pdu, max_size_response_scoped_pdu, state_reference):
        metadata = {"snmp_version": {0: "v1", 1: "v2c", 3: "v3"}.get(
            int(message_processing_model), "UNKNOWN"),
            "pdu_type": "TRAP_V1" if int(message_processing_model) == 0 else
                "INFORM" if pdu.tagSet == v2c.InformRequestPDU.tagSet else "TRAP_V2",
            "context_engine_id": context_engine_id.prettyPrint() if context_engine_id else None}
        if int(message_processing_model) == 0:
            metadata.update({
                "v1_enterprise": v1.apiTrapPDU.get_enterprise(pdu).prettyPrint(),
                "v1_agent_address": v1.apiTrapPDU.get_agent_address(pdu).prettyPrint(),
                "v1_generic": int(v1.apiTrapPDU.get_generic_trap(pdu)),
                "v1_specific": int(v1.apiTrapPDU.get_specific_trap(pdu)),
            })
        else:
            metadata['request_id'] = int(v2c.apiPDU.get_request_id(pdu))
        self._metadata = metadata
        try:
            return super().process_pdu(snmp_engine, message_processing_model,
                security_model, security_name, security_level, context_engine_id,
                context_name, pdu_version, pdu, max_size_response_scoped_pdu,
                state_reference)
        finally:
            self._metadata = {}


def localizar_varbind(var_binds, oid_procurado):
    for oid, valor in var_binds:
        atual = oid_texto(oid)

        if atual == oid_procurado:
            return valor_texto(valor)

    return None


def identificar_origem(snmp_engine, state_reference):
    try:
        _, transport_address = (
            snmp_engine.message_dispatcher.get_transport_info(
                state_reference
            )
        )

        ip = str(transport_address[0])
        porta = int(transport_address[1])

        return ip, porta

    except Exception:
        return "DESCONHECIDO", 0


def nome_sbc(ip):
    return SBCS_CONHECIDOS.get(
        ip,
        "SBC_DESCONHECIDO",
    )


def classificar_trap_oid(trap_oid):
    if not trap_oid:
        return "TRAP_OID_NAO_INFORMADO"

    if trap_oid.startswith(OID_SBC_ENTERPRISE):
        return "SBC_VENDOR_TRAP"

    return "SNMP_TRAP"


def interpretar_peer_timeout(trap_oid, var_binds, source_ip=None):
    """Configuração explícita; OIDs desconhecidos nunca viram alarme."""
    try:
        cfg = json.loads(TRAP_CONFIG_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    values = {oid_texto(oid): valor_texto(value) for oid, value in var_binds}
    members = cfg.get("members", {})
    emitter_ip = next((ip for ip, member in members.items()
                       if source_ip in member.get("source_ips", []) or source_ip == ip), None)
    emitter = members.get(emitter_ip, {})
    peer_member = members.get(emitter.get("peer_ip"), {})
    if not emitter or not peer_member or not emitter.get("pair") or emitter.get("pair") != peer_member.get("pair"):
        return None
    sysname = values.get(OID_SYSNAME)
    if sysname and sysname != emitter.get("sysname"):
        return None
    rules = cfg.get("peer_timed_out", {})
    for state, name in (("ACTIVE", "active"), ("CLEAR", "clear")):
        rule = rules.get(name, {})
        if not isinstance(rule, dict) or trap_oid not in rule.get("trap_oids", []):
            continue
        matches = rule.get("varbind_equals", {})
        if not all(values.get(oid) == str(value) for oid, value in matches.items()):
            continue
        peer_oid = rule.get("peer_varbind_oid")
        peer = values.get(peer_oid) if peer_oid else None
        if peer not in {emitter.get("peer_ip"), peer_member.get("sysname")}:
            continue
        return {"state": state, "peer": peer_member["sysname"],
                "peer_ip": emitter["peer_ip"], "emitter_ip": emitter_ip,
                "pair": emitter["pair"], "varbinds": values}
    return None


# ============================================================
# DESCOBERTA DO IP VPN
# ============================================================

def descobrir_ip_vpn():
    """
    Procura um IPv4 10.90.12.x associado a uma
    interface VPN.

    Utiliza ipconfig /all para não depender de
    módulos adicionais.
    """

    if os.name != "nt":
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

    blocos = re.split(
        r"\r?\n\r?\n",
        texto,
    )

    for bloco in blocos:
        bloco_lower = bloco.lower()

        if "vpn" not in bloco_lower:
            continue

        correspondencias = re.findall(
            r"10\.201\.84\.\d{1,3}",
            bloco,
        )

        if correspondencias:
            return correspondencias[0]

    return None


# ============================================================
# MONITORAMENTO DO IP DA VPN
# ============================================================

async def monitorar_ip_vpn():
    """
    Monitora continuamente a interface VPN/VPN.

    Detecta:
    - queda da VPN;
    - retorno com o mesmo IP;
    - retorno com outro IP;
    - alteração de IP sem a interface desaparecer.
    """

    ip_atual = descobrir_ip_vpn()
    ultimo_ip_valido = ip_atual

    print()

    if ip_atual:
        print(
            f"IP VPN atual             : "
            f"{ip_atual}"
        )

        print(
            f"Trap receiver esperado   : "
            f"{ip_atual}:{LISTEN_PORT}"
        )

    else:
        print(
            "⚠ IP VPN VPN não encontrado."
        )

    print()

    while True:
        await asyncio.sleep(
            INTERVALO_VERIFICACAO_VPN
        )

        novo_ip = descobrir_ip_vpn()

        # ====================================================
        # VPN CAIU
        # ====================================================

        if ip_atual and not novo_ip:
            agora = datetime.now()

            ultimo_ip_valido = ip_atual
            ip_atual = None

            print()
            print(linha("!"))

            print(
                f"⚠ VPN/INTERFACE VPN "
                f"INDISPONÍVEL - "
                f"{agora:%d/%m/%Y %H:%M:%S}"
            )

            print(
                f"Último IP conhecido: "
                f"{ultimo_ip_valido}"
            )

            print(
                "Enquanto a VPN estiver fora, "
                "novos traps dos SBCs poderão ser perdidos."
            )

            print(linha("!"))

            escrever_log(
                f"\n[{agora:%Y-%m-%d %H:%M:%S}] "
                f"VPN_INDISPONIVEL | "
                f"ultimo_ip={ultimo_ip_valido}\n"
            )

            continue

        # ====================================================
        # VPN VOLTOU
        # ====================================================

        if not ip_atual and novo_ip:
            agora = datetime.now()

            ip_anterior = ultimo_ip_valido

            ip_atual = novo_ip
            ultimo_ip_valido = novo_ip

            # ------------------------------------------------
            # RETORNOU COM IP DIFERENTE
            # ------------------------------------------------

            if (
                ip_anterior
                and ip_anterior != novo_ip
            ):
                print()
                print(linha("!"))

                print(
                    "🚨 ALTERAÇÃO DE IP VPN DETECTADA"
                )

                print()

                print(
                    f"IP anterior : "
                    f"{ip_anterior}"
                )

                print(
                    f"IP atual    : "
                    f"{novo_ip}"
                )

                print()

                print(
                    "⚠️ O TRAP-RECEIVER DOS SBCs "
                    "PODE ESTAR DESATUALIZADO."
                )

                print()

                print(
                    "Configuração anterior:"
                )

                print(
                    f"{ip_anterior}:{LISTEN_PORT}"
                )

                print()

                print(
                    "Nova configuração necessária:"
                )

                print(
                    f"{novo_ip}:{LISTEN_PORT}"
                )

                print()

                print("SBCs:")

                for ip_sbc in SBCS_CONHECIDOS:
                    print(ip_sbc)

                print(linha("!"))

                escrever_log(
                    f"\n[{agora:%Y-%m-%d %H:%M:%S}] "
                    f"IP_VPN_ALTERADO | "
                    f"anterior={ip_anterior} | "
                    f"atual={novo_ip} | "
                    f"trap_receiver_anterior="
                    f"{ip_anterior}:{LISTEN_PORT} | "
                    f"trap_receiver_novo="
                    f"{novo_ip}:{LISTEN_PORT}\n"
                )

            # ------------------------------------------------
            # RETORNOU COM O MESMO IP
            # ------------------------------------------------

            else:
                print()
                print(linha("!"))

                print(
                    f"✅ VPN/INTERFACE VPN "
                    f"RESTABELECIDA - "
                    f"{agora:%d/%m/%Y %H:%M:%S}"
                )

                print(
                    f"IP mantido: "
                    f"{novo_ip}"
                )

                print(
                    "✅ Trap-receiver continua válido."
                )

                print(
                    f"Destino: "
                    f"{novo_ip}:{LISTEN_PORT}"
                )

                print(linha("!"))

                escrever_log(
                    f"\n[{agora:%Y-%m-%d %H:%M:%S}] "
                    f"VPN_RESTABELECIDA | "
                    f"ip={novo_ip} | "
                    f"ip_alterado=nao\n"
                )

            continue

        # ====================================================
        # IP MUDOU SEM A INTERFACE SUMIR
        # ====================================================

        if (
            ip_atual
            and novo_ip
            and ip_atual != novo_ip
        ):
            agora = datetime.now()

            ip_anterior = ip_atual

            ip_atual = novo_ip
            ultimo_ip_valido = novo_ip

            print()
            print(linha("!"))

            print(
                "🚨 ALTERAÇÃO DE IP VPN DETECTADA"
            )

            print()

            print(
                f"IP anterior : "
                f"{ip_anterior}"
            )

            print(
                f"IP atual    : "
                f"{novo_ip}"
            )

            print()

            print(
                "⚠️ O TRAP-RECEIVER DOS SBCs "
                "PODE ESTAR DESATUALIZADO."
            )

            print()

            print(
                "Configuração anterior:"
            )

            print(
                f"{ip_anterior}:{LISTEN_PORT}"
            )

            print()

            print(
                "Nova configuração necessária:"
            )

            print(
                f"{novo_ip}:{LISTEN_PORT}"
            )

            print()

            print("SBCs:")

            for ip_sbc in SBCS_CONHECIDOS:
                print(ip_sbc)

            print(linha("!"))

            escrever_log(
                f"\n[{agora:%Y-%m-%d %H:%M:%S}] "
                f"IP_VPN_ALTERADO | "
                f"anterior={ip_anterior} | "
                f"atual={novo_ip} | "
                f"trap_receiver_anterior="
                f"{ip_anterior}:{LISTEN_PORT} | "
                f"trap_receiver_novo="
                f"{novo_ip}:{LISTEN_PORT}\n"
            )


# ============================================================
# CALLBACK DE RECEPÇÃO DE TRAPS
# ============================================================

def receber_trap(
    snmp_engine,
    state_reference,
    context_engine_id,
    context_name,
    var_binds,
    cb_ctx,
):
    agora = datetime.now().astimezone()

    ip_origem, porta_origem = identificar_origem(
        snmp_engine,
        state_reference,
    )

    # The receiver supplies version/v1 fields without exposing securityName
    # or the v1/v2c community. Capture comes before all classification.
    try:
        registrar_pdu_bruta(agora, ip_origem, porta_origem, var_binds,
                            getattr(cb_ctx, "_metadata", {}))
    except Exception as erro:
        _append_rotating(RAW_ERRORS_FILE, {
            "received_utc": agora.astimezone(timezone.utc).isoformat(), "src_ip": ip_origem,
            "error": type(erro).__name__, "detail": str(erro)})

    equipamento = nome_sbc(
        ip_origem
    )

    trap_oid = localizar_varbind(
        var_binds,
        OID_SNMP_TRAP,
    )

    # The raw capture above proves reception. Keep loopback test traffic out of
    # the SBC status, alarm stream and operator log.
    if ip_origem in ("127.0.0.1", "::1") and trap_oid == OID_TOI_SELFTEST:
        return

    sys_uptime = localizar_varbind(
        var_binds,
        OID_SYSUPTIME,
    )

    classificacao = classificar_trap_oid(
        trap_oid
    )
    if interpretar_peer_timeout(trap_oid, var_binds, ip_origem):
        classificacao = "PEER_TIMED_OUT"

    try:
        registrar_status_trap(
            agora=agora,
            ip_origem=ip_origem,
            porta_origem=porta_origem,
            equipamento=equipamento,
            trap_oid=trap_oid,
            classificacao=classificacao,
            sys_uptime=sys_uptime,
            var_binds=var_binds,
        )
    except Exception as erro:
        print(f"⚠ Falha ao atualizar {STATUS_TRAPS_FILE}: {erro}")

    # --------------------------------------------------------
    # TERMINAL
    # --------------------------------------------------------

    print()
    print(linha())

    print(
        f"TRAP SNMP RECEBIDO - "
        f"{agora:%d/%m/%Y %H:%M:%S}"
    )

    print(linha())

    print(
        f"Origem        : "
        f"{ip_origem}:{porta_origem}"
    )

    print(
        f"SBC           : "
        f"{equipamento}"
    )

    print(
        f"Trap OID      : "
        f"{trap_oid or 'não informado'}"
    )

    print(
        f"Classificação : "
        f"{classificacao}"
    )

    if sys_uptime is not None:
        print(
            f"SysUpTime     : "
            f"{sys_uptime}"
        )

    print()
    print("VARBINDS:")
    print(linha("-", 90))

    for oid, valor in var_binds:
        print(
            f"{oid_texto(oid)} = "
            f"{valor_texto(valor)}"
        )

    print(linha())

    # --------------------------------------------------------
    # LOG
    # --------------------------------------------------------

    bloco = []

    bloco.append("\n")
    bloco.append(linha() + "\n")

    bloco.append(
        f"TRAP SNMP RECEBIDO - "
        f"{agora:%d/%m/%Y %H:%M:%S}\n"
    )

    bloco.append(linha() + "\n")

    bloco.append(
        f"Data/Hora     : "
        f"{agora:%Y-%m-%d %H:%M:%S}\n"
    )

    bloco.append(
        f"Origem        : "
        f"{ip_origem}:{porta_origem}\n"
    )

    bloco.append(
        f"SBC           : "
        f"{equipamento}\n"
    )

    bloco.append(
        f"Trap OID      : "
        f"{trap_oid or 'não informado'}\n"
    )

    bloco.append(
        f"Classificação : "
        f"{classificacao}\n"
    )

    if sys_uptime is not None:
        bloco.append(
            f"SysUpTime     : "
            f"{sys_uptime}\n"
        )

    bloco.append("\n")
    bloco.append("VARBINDS:\n")
    bloco.append(linha("-", 90) + "\n")

    for oid, valor in var_binds:
        bloco.append(
            f"{oid_texto(oid)} = "
            f"{valor_texto(valor)}\n"
        )

    bloco.append(
        linha() + "\n"
    )

    escrever_log(
        "".join(bloco)
    )


# ============================================================
# LISTENER
# ============================================================

async def main():
    garantir_log()
    for private_log in (RAW_TRAPS_FILE, RAW_ERRORS_FILE):
        private_log.parent.mkdir(parents=True, exist_ok=True)
        _secure_local(private_log)

    # Cria o arquivo de estado imediatamente, mesmo antes do primeiro trap.
    estado_inicial = carregar_status_traps()
    estado_inicial["listener_iniciado_em"] = datetime.now().isoformat(
        timespec="seconds"
    )
    estado_inicial["listener_ativo"] = False
    salvar_status_traps(estado_inicial)

    print(linha())

    print(
        "MGW Monitor - "
        "Captura de SNMP Traps SBC"
    )

    print(linha())

    print(
        f"Escutando em : "
        f"{LISTEN_IP}:{LISTEN_PORT}/UDP"
    )

    print(
        f"Log          : "
        f"{LOG_FILE}"
    )
    print(
        f"Estado traps : "
        f"{STATUS_TRAPS_FILE}"
    )

    print()

    print(
        "SBCs conhecidos:"
    )

    for ip, nome in SBCS_CONHECIDOS.items():
        print(
            f"  {ip} -> {nome}"
        )

    print()

    print(
        "Aguardando traps..."
    )

    print(
        "Pressione Ctrl+C para encerrar."
    )

    print(linha())

    snmp_engine = engine.SnmpEngine()
    original_receive = snmp_engine.message_dispatcher.receive_message

    def receive_with_decode_audit(engine_ref, domain, address, packet):
        try:
            verdec.decode_message_version(packet)
        except Exception as erro:
            _append_rotating(RAW_ERRORS_FILE, {
                'received_utc': datetime.now(timezone.utc).isoformat(),
                'src_ip': str(address[0]), 'src_port': int(address[1]),
                'error': type(erro).__name__, 'packet_bytes': len(packet)})
        return original_receive(engine_ref, domain, address, packet)

    snmp_engine.message_dispatcher.receive_message = receive_with_decode_audit

    # --------------------------------------------------------
    # ABRE UDP/162
    # --------------------------------------------------------

    config.add_transport(
        snmp_engine,
        udp.DOMAIN_NAME,
        udp.UdpTransport().open_server_mode(
            (
                LISTEN_IP,
                LISTEN_PORT,
            )
        ),
    )

    # --------------------------------------------------------
    # COMMUNITY SNMP V1/V2C
    # --------------------------------------------------------

    config.add_v1_system(
        snmp_engine,
        "mgw-monitor-traps",
        COMMUNITY,
    )

    # --------------------------------------------------------
    # CALLBACK DE TRAPS
    # --------------------------------------------------------

    RawNotificationReceiver(
        snmp_engine,
        receber_trap,
    )

    estado_inicial["listener_ativo"] = True
    salvar_status_traps(estado_inicial)

    snmp_engine.transport_dispatcher.job_started(
        1
    )

    # --------------------------------------------------------
    # MONITOR VPN EM PARALELO
    # --------------------------------------------------------

    tarefa_vpn = asyncio.create_task(
        monitorar_ip_vpn()
    )

    try:
        while True:
            await asyncio.sleep(1)

    finally:
        tarefa_vpn.cancel()

        try:
            estado_final = carregar_status_traps()
            estado_final["listener_ativo"] = False
            estado_final["listener_encerrado_em"] = datetime.now().isoformat(
                timespec="seconds"
            )
            salvar_status_traps(estado_final)
        except Exception:
            pass

        snmp_engine.transport_dispatcher.job_finished(
            1
        )

        snmp_engine.close_dispatcher()


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":
    try:
        asyncio.run(
            main()
        )

    except KeyboardInterrupt:
        print()
        print(linha())
        print(
            "Listener de traps encerrado."
        )
        print(linha())
