import json
import os
import re
import time
import threading
import uuid
from html import unescape
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from mgw.telegram import Telegram


LIMITE_P34_PADRAO = 30.0
LIMITE_P135_PADRAO = 40.0
CONFIRMACAO_ALERTA_PADRAO_MIN = 10
CONFIRMACAO_NORMALIZACAO_PADRAO_MIN = 10
REPETICAO_PADRAO_MIN = 60
INTERVALO_PADRAO = 60
HORA_INICIO_PADRAO = "07:00"
HORA_FIM_PADRAO = "21:00"

CANAIS_TRAVADOS_LIMITE_ALERTA_PADRAO = 40
CANAIS_TRAVADOS_LIMITE_CRITICO_PADRAO = 100
CANAIS_TRAVADOS_REPETICAO_PADRAO_MIN = 60
CANAIS_TRAVADOS_AUTO_LIMPEZA_PADRAO = False
CANAIS_TRAVADOS_AUTO_LIMPEZA_COOLDOWN_PADRAO_MIN = 10
CANAIS_TRAVADOS_RECORRENCIA_LIMITE_PADRAO = 200
CANAIS_TRAVADOS_RECORRENCIA_JANELA_PADRAO_MIN = 60
CANAIS_TRAVADOS_RECORRENCIA_ESCALONAMENTO_PADRAO = 4
IPBX_MENU_CANAIS_PADRAO = "8899"  # legado; novo fluxo usa ORBE_IPBX_MENU_<instância>
IPBX_USER_ID_PADRAO = "1504"
IPBX_TIMEOUT_PADRAO = 30
ARQUIVO_AUDITORIA_AUTOLIMPEZA_PADRAO = Path(
    "logs/canais_travados_autolimpeza.log"
)
ARQUIVO_RESUMO_DIARIO_AUTOLIMPEZA_PADRAO = Path(
    "logs/canais_travados_resumo_diario.log"
)

ARQUIVO_LOG_PORTAL_ORBE_PADRAO = Path("logs/portal_orbe.log")
CANAIS_TRAVADOS_RESUMO_DIARIO_PADRAO = True
CANAIS_TRAVADOS_RESUMO_DIARIO_HORA_PADRAO = "21:00"
IPBX_BASE_PADRAO = (
    "http://10.90.10.101:8181/FORNECEDOR/AppATMAACME/"
    "FornecedorPortalExFrontEndWeb/"
)

CAMINHO_ESTADO = Path("dados/status_portal_orbe.json")
BASE_DIR_PORTAL_ORBE = Path(__file__).resolve().parent
CAMINHO_STATUS_HA_SBC = BASE_DIR_PORTAL_ORBE / "dados" / "status_ha_sbcs.json"
PORTAL_ORBE_SBC_PATH_HARDENING_V1 = True
PASTA_HISTORICO = Path("dados/portal_orbe")
VERSAO_ESTADO = 8

STATUS_ALVO = {
    34: "No Circuit Available",
    135: "Internal Congestion",
}


def env_bool(nome, padrao=False):
    valor = os.getenv(nome)
    if valor is None:
        return padrao
    return valor.strip().lower() in {"1", "true", "sim", "yes", "on"}


def parse_hhmm(valor, padrao):
    texto = (valor or padrao).strip()
    try:
        return datetime.strptime(texto, "%H:%M").time()
    except ValueError:
        return datetime.strptime(padrao, "%H:%M").time()


def dentro_janela_operacional(agora, inicio, fim):
    hora = agora.time()
    if inicio <= fim:
        return inicio <= hora <= fim
    return hora >= inicio or hora <= fim


def minutos_decorridos(inicio_iso, agora=None):
    if not inicio_iso:
        return 0.0
    if agora is None:
        agora = datetime.now()
    try:
        inicio = datetime.fromisoformat(inicio_iso)
    except ValueError:
        return 0.0
    return max(0.0, (agora - inicio).total_seconds() / 60.0)


def parse_float(valor, padrao=0.0):
    if valor is None:
        return padrao
    if isinstance(valor, (int, float)):
        return float(valor)

    texto = str(valor).strip().replace("%", "").strip()

    # O Portal Orbe pode devolver percentual como "1,40" ou "1.40".
    # Só tratamos ponto como separador de milhar quando também existe vírgula.
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")

    try:
        return float(texto)
    except (TypeError, ValueError):
        return padrao


def extrair_id_projeto(code_project):
    if code_project is None:
        return None
    m = re.match(r"\s*(\d+)", str(code_project))
    return m.group(1) if m else None


def carregar_estado():
    estado_vazio = {
        "versao": VERSAO_ESTADO,
        "metadados": {},
        "filas": {},
        "canais_travados": {},
    }

    if not CAMINHO_ESTADO.exists():
        return estado_vazio

    try:
        estado = json.loads(CAMINHO_ESTADO.read_text(encoding="utf-8"))
        if not isinstance(estado, dict):
            return estado_vazio

        # Compatibilidade com o arquivo de estado criado pelas versões anteriores.
        estado.setdefault("versao", VERSAO_ESTADO)
        estado.setdefault("metadados", {})
        estado.setdefault("filas", {})
        estado.setdefault("canais_travados", {})
        return estado
    except Exception:
        return estado_vazio


def salvar_estado(estado):
    CAMINHO_ESTADO.parent.mkdir(parents=True, exist_ok=True)
    CAMINHO_ESTADO.write_text(
        json.dumps(estado, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def registrar_log_portal_orbe(evento, detalhe=""):
    """Registra eventos operacionais do Portal Orbe sem incluir credenciais."""
    caminho = Path(
        os.getenv(
            "ORBE_LOG_ARQUIVO",
            str(ARQUIVO_LOG_PORTAL_ORBE_PADRAO),
        )
    )
    try:
        caminho.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        linha = f"{timestamp} | {str(evento).strip()}"
        if detalhe:
            linha += f" | {str(detalhe).strip()}"
        with caminho.open("a", encoding="utf-8") as f:
            f.write(linha + "\n")
    except Exception as e:
        # O log é auxiliar: falha de escrita nunca deve interromper o monitor.
        print(f"⚠ Não foi possível gravar logs/portal_orbe.log: {e}")


def preparar_estado_para_execucao(estado, limite, instancias):
    """
    Evita alertas/normalizações artificiais quando mudamos parâmetros de teste
    para produção. Se o limite ou a lista de instâncias mudou desde a última
    execução, as filas serão rebaselineadas silenciosamente no primeiro ciclo.
    """
    meta = estado.setdefault("metadados", {})
    instancias_norm = sorted(str(x).strip() for x in instancias)

    limite_anterior = meta.get("limite_percentual")
    instancias_anteriores = sorted(str(x).strip() for x in meta.get("instancias", []))

    mudou_config = (
        limite_anterior is None
        or float(limite_anterior) != float(limite)
        or instancias_anteriores != instancias_norm
    )

    if mudou_config:
        estado["filas"] = {}
        meta["rebaseline_pendente"] = True
        meta["motivo_rebaseline"] = "mudanca_de_configuracao"
    else:
        meta.setdefault("rebaseline_pendente", False)

    meta["limite_percentual"] = float(limite)
    meta["instancias"] = instancias_norm
    meta["atualizado_em"] = datetime.now().isoformat(timespec="seconds")

    return mudou_config


def registrar_historico(amostra):
    PASTA_HISTORICO.mkdir(parents=True, exist_ok=True)
    data = datetime.now().strftime("%Y-%m-%d")
    caminho = PASTA_HISTORICO / f"portal_orbe_{data}.jsonl"
    with caminho.open("a", encoding="utf-8") as f:
        f.write(json.dumps(amostra, ensure_ascii=False) + "\n")


class PortalOrbe:
    def __init__(self, instancia, usuario, senha, timeout=30):
        self.instancia = str(instancia).strip()
        self.usuario = usuario
        self.senha = senha
        self.timeout = timeout
        self.session = requests.Session()
        # Uma lista vazia do Portal não é uma leitura operacional válida para
        # as instâncias monitoradas. Mantemos a última lista apenas como
        # telemetria; ela nunca é convertida em estado NORMAL.
        self.ultima_lista_filas = None
        self.ultima_lista_em = None

        # A Instância 1 é a instância principal do Fornecedor Tech Suite e,
        # diferentemente das demais, NÃO possui o sufixo "_inst1" na URL.
        if self.instancia == "1":
            self.base = (
                "http://10.90.10.101:8181/fornecedor/appatmaacme/"
                "fornecedortechsuitefrontendweb/"
            )
        else:
            self.base = (
                f"http://10.90.10.101:8181/fornecedor/appatmaacme/"
                f"fornecedortechsuitefrontendweb_inst{self.instancia}/"
            )

        self.login_url = urljoin(self.base, "start/account/login")
        self.queryqueue_url = urljoin(self.base, "metrics/queryqueue")
        self.getqueues_url = urljoin(self.base, "metrics/queryqueue/getqueuelist")
        self.stats_url = urljoin(
            self.base,
            "metrics/dashboardqueuedetail/getcallstatisticsdata",
        )

    def autenticar(self):
        r = self.session.get(self.login_url, timeout=self.timeout)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        token_input = soup.find("input", {"name": "__RequestVerificationToken"})
        if not token_input or not token_input.get("value"):
            raise RuntimeError(
                f"Instância {self.instancia}: token antiforgery não encontrado."
            )

        token = token_input["value"]
        payload = [
            ("ReturnUrl", "/"),
            ("__RequestVerificationToken", token),
            ("DeLogin", self.usuario),
            ("DePassword", self.senha),
            ("rememberMe", "true"),
            ("rememberMe", "false"),
        ]

        r = self.session.post(
            self.login_url,
            data=payload,
            headers={"Referer": self.login_url},
            allow_redirects=False,
            timeout=self.timeout,
        )

        if r.status_code not in (301, 302, 303):
            raise RuntimeError(
                f"Instância {self.instancia}: login não retornou redirecionamento "
                f"(HTTP {r.status_code})."
            )

        teste = self.session.get(self.queryqueue_url, timeout=self.timeout)
        teste.raise_for_status()
        if "DePassword" in teste.text or "/start/account/login" in teste.url.lower():
            raise RuntimeError(
                f"Instância {self.instancia}: sessão não ficou autenticada."
            )

    def listar_filas(self):
        params = {
            "pMustFilterQueueListHasOmni": "False",
            "pMustFilterAGV": "False",
        }
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.queryqueue_url,
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

        def requisitar():
            resposta = self.session.get(
                self.getqueues_url,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )
            resposta.raise_for_status()
            url_final = resposta.url.lower()
            texto_inicial = resposta.text[:4096]
            if "/start/account/login" in url_final or "DePassword" in texto_inicial:
                raise PermissionError(
                    f"Instância {self.instancia}: sessão expirada durante getqueuelist."
                )
            return resposta.json()

        data = requisitar()

        # Algumas instalações encapsulam a lista em um objeto. Normalize antes
        # de testar vazio; caso contrário {"Data": []} escaparia da proteção e
        # seria convertido silenciosamente em "0 filas".
        def normalizar_lista(valor):
            if isinstance(valor, dict):
                for chave in ("QueueList", "Queues", "Data", "data"):
                    candidato = valor.get(chave)
                    if isinstance(candidato, list):
                        return candidato
            return valor

        data = normalizar_lista(data)

        # O Portal, em alguns ciclos, responde HTTP 200 com uma lista vazia
        # mesmo quando a instância possui filas. Uma única reautenticação e
        # reconsulta evita que essa ausência transitória seja tratada como
        # zero. Se continuar vazia, falhamos de forma explícita e preservamos
        # o último estado válido no chamador.
        if data == []:
            registrar_log_portal_orbe(
                "GETQUEUELIST_VAZIO_REAUTENTICANDO",
                f"instancia={self.instancia}; endpoint={self.getqueues_url}",
            )
            print(
                f"⚠ Instância {self.instancia}: getqueuelist retornou lista vazia; "
                "renovando sessão e repetindo uma vez."
            )
            self.session = requests.Session()
            self.autenticar()
            data = normalizar_lista(requisitar())
            if data == []:
                registrar_log_portal_orbe(
                    "GETQUEUELIST_VAZIO_PERSISTENTE",
                    f"instancia={self.instancia}; leitura descartada após reautenticação",
                )
                raise RuntimeError(
                    f"Instância {self.instancia}: getqueuelist continuou vazio "
                    "após reautenticação; leitura descartada."
                )

        if not isinstance(data, list):
            raise RuntimeError(
                f"Instância {self.instancia}: formato inesperado em getqueuelist: "
                f"{type(data).__name__}"
            )

        filas = []
        ignoradas = []

        for item in data:
            if not isinstance(item, dict):
                ignoradas.append(("registro não é objeto", repr(item)))
                continue

            # Normaliza os nomes das chaves para evitar diferenças de
            # maiúsculas/minúsculas ou espaços vindos do JSON.
            campos = {
                str(k).strip().lower(): v
                for k, v in item.items()
            }

            id_queue = campos.get("idqueue")
            codigo = campos.get("codequeue")
            nome = campos.get("nmqueue")
            # O getqueuelist real usa CodeDeProject (não CodeProject).
            # Mantemos CodeProject como fallback para compatibilidade entre instâncias.
            projeto_desc = campos.get("codedeproject")
            if projeto_desc is None:
                projeto_desc = campos.get("codeproject")

            # CodeProject vem do Portal no formato:
            # "2511 - REGULADOR INST 4" / "2497 - ALFA COBRANCA DS API".
            # Para pIdProject precisamos somente do número inicial.
            projeto_id = None
            if projeto_desc is not None:
                match = re.search(r"\d+", str(projeto_desc))
                if match:
                    projeto_id = match.group(0)

            if codigo is None or str(codigo).strip() == "":
                ignoradas.append(("sem CodeQueue", repr(item)))
                continue
            if nome is None or str(nome).strip() == "":
                ignoradas.append(("sem NmQueue", repr(item)))
                continue
            if projeto_id is None:
                ignoradas.append(("CodeProject sem ID numérico", repr(item)))
                continue

            filas.append({
                "id_queue": id_queue,
                "codigo": str(codigo).strip(),
                "nome": str(nome).strip(),
                "projeto_id": str(projeto_id),
                "projeto": str(projeto_desc).strip(),
            })

        if not filas and data:
            motivo, exemplo = ignoradas[0] if ignoradas else ("desconhecido", repr(data[0]))
            raise RuntimeError(
                f"Instância {self.instancia}: getqueuelist retornou {len(data)} registros, "
                f"mas nenhum pôde ser convertido em fila. "
                f"Motivo do primeiro: {motivo}. Exemplo: {exemplo}"
            )

        if ignoradas:
            print(
                f"ℹ Instância {self.instancia}: {len(ignoradas)} registro(s) "
                f"ignorados; {len(filas)} fila(s) válidas."
            )

        self.ultima_lista_filas = list(filas)
        self.ultima_lista_em = datetime.now().isoformat(timespec="seconds")
        return filas

    def consultar_status(self, fila):
        dashboard = urljoin(
            self.base,
            "metrics/dashboardqueuedetail"
            f"?pCodeQueue={fila['codigo']}&pIdProject={fila['projeto_id']}",
        )
        r = self.session.get(
            self.stats_url,
            params={
                "pCodeQueue": fila["codigo"],
                "pIdProject": fila["projeto_id"],
            },
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Referer": dashboard,
                "Accept": "application/json, text/javascript, */*; q=0.01",
            },
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = r.json()

        # O JSON real do Portal usa callStatisticsList (c minúsculo).
        # Mantemos também as variantes para compatibilidade entre versões/instâncias.
        lista = []
        horario_orbe = None
        if isinstance(data, dict):
            lista = (
                data.get("callStatisticsList")
                or data.get("CallStatisticsList")
                or data.get("callstatisticslist")
                or []
            )
            horario_orbe = (
                data.get("dtCacheAndServerCallStatisticsData")
                or data.get("DtCacheAndServerCallStatisticsData")
            )

        if not isinstance(lista, list):
            raise RuntimeError(
                f"getcallstatisticsdata retornou lista de status em formato inesperado: "
                f"{type(lista).__name__}"
            )

        resultado = {
            34: {"percentual": 0.0, "quantidade": 0, "presente": False},
            135: {"percentual": 0.0, "quantidade": 0, "presente": False},
        }

        for item in lista:
            nome_ipbx = str(item.get("NmStatusIpbx", ""))
            m = re.match(r"\s*(\d+)\s*-", nome_ipbx)
            if not m:
                continue
            codigo = int(m.group(1))
            if codigo not in resultado:
                continue

            percentual = item.get("VlPercCalls")
            if percentual is None:
                percentual = item.get("VlPercCallsToDisplay")

            resultado[codigo] = {
                "percentual": parse_float(percentual),
                "quantidade": int(item.get("QtCalls") or 0),
                "presente": True,
                "nome_ipbx": nome_ipbx,
                "nome_sip": item.get("NmStatusSip"),
                "nome_crm": item.get("NmStatusCRM"),
            }

        if lista and not resultado[34]["presente"] and not resultado[135]["presente"]:
            exemplos = []
            for x in lista[:3]:
                if isinstance(x, dict):
                    exemplos.append({
                        "NmStatusIpbx": x.get("NmStatusIpbx"),
                        "VlPercCalls": x.get("VlPercCalls"),
                        "VlPercCallsToDisplay": x.get("VlPercCallsToDisplay"),
                        "QtCalls": x.get("QtCalls"),
                    })
            print(
                f"    ⚠ Diagnóstico: API retornou {len(lista)} status, "
                f"mas não localizei PABX 34/135. Exemplos: {exemplos}"
            )

        return resultado, horario_orbe



def ipbx_hidden_inputs(html):
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    for inp in soup.find_all("input"):
        nome = inp.get("name")
        if not nome:
            continue
        if (inp.get("type") or "").lower() == "hidden":
            data[nome] = inp.get("value", "")
    return data


def ipbx_guid_valido(valor):
    valor = (valor or "").strip()
    if not valor or valor == "0":
        return False
    try:
        uuid.UUID(valor)
        return True
    except (ValueError, TypeError, AttributeError):
        return False


def parse_ipbx_config(instancias):
    """
    Lê Projeto + Menu individualmente por instância.

    Exemplo:
      ORBE_IPBX_PROJETO_4=828
      ORBE_IPBX_MENU_4=8899

      ORBE_IPBX_PROJETO_6=38
      ORBE_IPBX_MENU_6=9262

    O pNuGuid NÃO é persistido aqui, pois é obtido dinamicamente
    da sessão autenticada do Portal IPBX.
    """
    config = {}

    for instancia in instancias:
        instancia = str(instancia).strip()
        projeto = os.getenv(
            f"ORBE_IPBX_PROJETO_{instancia}",
            "",
        ).strip()
        menu = os.getenv(
            f"ORBE_IPBX_MENU_{instancia}",
            "",
        ).strip()

        if projeto and menu:
            config[instancia] = {
                "projeto": projeto,
                "menu": menu,
            }

    return config


class PortalIPBXCanaisTravados:
    """
    Consulta independente do monitor P34/P135.

    Fluxo validado no Portal:
      login
      -> selecionar projeto em Views/Default.aspx
      -> localizar link ShellIPBX - Remover Canais Travados
      -> GET DynamicQuery.aspx com pNuGuid real
      -> POST btnQuery=Pesquisar
      -> redirect para DynamicQueryResult.aspx
      -> extrair "Registros presos > 30 minutos"
    """

    def __init__(
        self,
        usuario,
        senha,
        config_instancias,
        base=None,
        id_user=None,
        timeout=None,
    ):
        self.usuario = usuario
        self.senha = senha
        self.config_instancias = {
            str(instancia): {
                "projeto": str(dados["projeto"]),
                "menu": str(dados["menu"]),
            }
            for instancia, dados in config_instancias.items()
        }
        self.base = (
            base
            or os.getenv("IPBX_BASE_URL", IPBX_BASE_PADRAO)
        ).rstrip("/") + "/"
        self.id_user = str(
            id_user
            or os.getenv("IPBX_ID_USER", IPBX_USER_ID_PADRAO)
        ).strip()
        self.timeout = int(
            timeout
            or os.getenv("IPBX_TIMEOUT_SEGUNDOS", str(IPBX_TIMEOUT_PADRAO))
        )

        self.return_path = "Views/Default.aspx"
        self.login_path = "Views/Login.aspx"
        self.lock = threading.RLock()
        self.session = None
        self._nova_session()

    def _nova_session(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0 Safari/537.36"
                ),
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )

    def _autenticado_pelos_cookies(self):
        nomes = {c.name.lower() for c in self.session.cookies}
        return ".aspxformsauth" in nomes or "fornecedorauth" in nomes

    @staticmethod
    def _voltou_login(resposta):
        return (
            "Login.aspx" in resposta.url
            or 'id="txtDeLogin"' in resposta.text
            or 'name="txtDeLogin"' in resposta.text
        )

    def autenticar(self):
        return_url = "/" + urlparse(self.base).path.strip("/") + "/" + self.return_path
        login_url = urljoin(self.base, self.login_path)

        r = self.session.get(
            login_url,
            params={"ReturnUrl": return_url},
            timeout=self.timeout,
        )
        r.raise_for_status()

        form = ipbx_hidden_inputs(r.text)
        obrigatorios = [
            "__VIEWSTATE",
            "__VIEWSTATEGENERATOR",
            "__EVENTVALIDATION",
        ]
        faltando = [x for x in obrigatorios if x not in form]
        if faltando:
            raise RuntimeError(
                "Portal IPBX: campos ASP.NET não encontrados no login: "
                + ", ".join(faltando)
            )

        form.update(
            {
                "ScriptManager1": "UpdatePanel1|btnOk",
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "hidScreenWidth": "",
                "hidcaleenHeight": "",
                "textUrl": "",
                "txtDeLogin": self.usuario,
                "txtDePassword": self.senha,
                "__ASYNCPOST": "true",
                "btnOk": "Acessar",
            }
        )

        resp = self.session.post(
            r.url,
            data=form,
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "X-MicrosoftAjax": "Delta=true",
                "Referer": r.url,
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()

        if not self._autenticado_pelos_cookies():
            check = self.session.get(
                urljoin(self.base, self.return_path),
                timeout=self.timeout,
            )
            check.raise_for_status()
            if self._voltou_login(check):
                raise RuntimeError("Portal IPBX não confirmou a autenticação.")

    def _selecionar_projeto(self, instancia):
        instancia = str(instancia)
        cfg = self.config_instancias.get(instancia)
        if not cfg:
            raise RuntimeError(
                f"Instância {instancia}: configuração IPBX incompleta. "
                f"Defina ORBE_IPBX_PROJETO_{instancia} e "
                f"ORBE_IPBX_MENU_{instancia}."
            )

        projeto = cfg["projeto"]

        default_url = urljoin(self.base, self.return_path)
        r = self.session.get(default_url, timeout=self.timeout)
        r.raise_for_status()

        if self._voltou_login(r):
            raise PermissionError("Sessão IPBX expirada.")

        form = ipbx_hidden_inputs(r.text)
        form["__EVENTTARGET"] = "ctl00$cmbProjectList"
        form["__EVENTARGUMENT"] = ""
        form["__LASTFOCUS"] = ""
        form["ctl00$cmbProjectList"] = projeto

        resp = self.session.post(
            default_url,
            data=form,
            headers={
                "Referer": r.url,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()

        if self._voltou_login(resp):
            raise PermissionError("Sessão IPBX expirou ao selecionar o projeto.")

        return projeto, resp

    def _extrair_link_menu(self, html, base_url, menu_esperado):
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a"):
            texto = " ".join(a.stripped_strings).strip()
            href = unescape(a.get("href") or "").strip()

            if not href:
                continue
            if (
                "Remover Canais Travados" not in texto
                and "Remover Canais Travados" not in href
            ):
                continue

            m = re.search(
                r'openReport\s*\(\s*["\'][^"\']*["\']\s*,\s*'
                r'["\'](?P<url>[^"\']+)["\']',
                href,
                re.IGNORECASE,
            )
            if not m:
                continue

            url = m.group("url").replace("&amp;", "&")
            url_abs = urljoin(base_url, url)
            qs = parse_qs(urlparse(url_abs).query)

            menu = (qs.get("pIdMenu") or [""])[0].strip()
            guid = (qs.get("pNuGuid") or [""])[0].strip()

            if menu and menu != str(menu_esperado):
                continue
            if not ipbx_guid_valido(guid):
                continue

            return url_abs, guid

        return None, None

    @staticmethod
    def _extrair_quantidade(html):
        soup = BeautifulSoup(html, "html.parser")
        linha = soup.find("tr", id=re.compile(r"^grdDataSet_DXDataRow\d+$"))

        if linha:
            tds = linha.find_all("td")
            if len(tds) >= 2:
                valor = tds[-1].get_text(" ", strip=True)
                m = re.search(r"\d+", valor)
                if m:
                    return int(m.group())

        # Fallback mais tolerante caso o ID do grid seja alterado.
        texto = soup.get_text(" ", strip=True)
        if "Registros presos > 30 minutos" in texto:
            for tr in soup.find_all("tr"):
                txt = " ".join(tr.stripped_strings)
                nums = re.findall(r"\b\d+\b", txt)
                if nums and "Registros presos" not in txt:
                    try:
                        return int(nums[-1])
                    except ValueError:
                        pass

        return None

    def _abrir_resultado_uma_vez(self, instancia):
        """
        Abre a consulta e devolve também a resposta DynamicQueryResult.aspx.

        Essa resposta contém o estado ASP.NET (__VIEWSTATE,
        __EVENTVALIDATION, grdDataSet$CallbackState etc.) necessário
        para reproduzir com segurança o botão Confirmar.
        """
        instancia = str(instancia)
        cfg = self.config_instancias.get(instancia)
        if not cfg:
            raise RuntimeError(
                f"Instância {instancia}: configuração IPBX incompleta. "
                f"Defina ORBE_IPBX_PROJETO_{instancia} e "
                f"ORBE_IPBX_MENU_{instancia}."
            )

        projeto, resposta_projeto = self._selecionar_projeto(instancia)
        menu_esperado = cfg["menu"]

        dynamic_query_url, guid = self._extrair_link_menu(
            resposta_projeto.text,
            resposta_projeto.url,
            menu_esperado,
        )
        if not dynamic_query_url:
            raise RuntimeError(
                f"Instância {instancia}: menu ShellIPBX - Remover Canais Travados "
                f"(pIdMenu={menu_esperado}) não localizado com pNuGuid válido."
            )

        r = self.session.get(dynamic_query_url, timeout=self.timeout)
        r.raise_for_status()
        if self._voltou_login(r):
            raise PermissionError("Sessão IPBX expirou ao abrir DynamicQuery.")

        form = ipbx_hidden_inputs(r.text)
        obrigatorios = [
            "__VIEWSTATE",
            "__VIEWSTATEGENERATOR",
            "__EVENTVALIDATION",
        ]
        faltando = [x for x in obrigatorios if x not in form]
        if faltando:
            raise RuntimeError(
                f"Instância {instancia}: DynamicQuery sem campos ASP.NET: "
                + ", ".join(faltando)
            )

        form["__EVENTTARGET"] = ""
        form["__EVENTARGUMENT"] = ""
        form["btnQuery"] = "Pesquisar"
        form.setdefault("clientUTCTimeZoneOffsetTime", "")
        form.setdefault("clientLocalTimeZoneOffsetTime", "")

        pesquisa = self.session.post(
            dynamic_query_url,
            data=form,
            headers={
                "Referer": r.url,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=self.timeout,
            allow_redirects=True,
        )
        pesquisa.raise_for_status()

        if self._voltou_login(pesquisa):
            raise PermissionError("Sessão IPBX expirou durante Pesquisar.")

        if "DynamicQueryResult.aspx" not in pesquisa.url:
            raise RuntimeError(
                f"Instância {instancia}: Pesquisar não terminou em "
                f"DynamicQueryResult.aspx. URL final: {pesquisa.url}"
            )

        quantidade = self._extrair_quantidade(pesquisa.text)
        if quantidade is None:
            raise RuntimeError(
                f"Instância {instancia}: não foi possível extrair a quantidade "
                "de canais presos > 30 minutos."
            )

        return {
            "instancia": str(instancia),
            "projeto_id": projeto,
            "quantidade": int(quantidade),
            "guid": guid,
            "consultado_em": datetime.now().isoformat(timespec="seconds"),
            "_resultado_response": pesquisa,
        }

    def _consultar_uma_vez(self, instancia):
        dados = self._abrir_resultado_uma_vez(instancia)
        dados.pop("_resultado_response", None)
        return dados

    def _limpar_uma_vez(self, instancia):
        """
        Reproduz o fluxo manual validado no DevTools:

          marcar Todos
          -> Confirmar
          -> confirmar caixa JavaScript (efeito equivalente no POST)
          -> POST DynamicQueryResult.aspx

        Payload capturado:
          btnContinue=Confirmar
          cbxCheckall=on
          grdDataSet$DXSelInput=T
          + estado ASP.NET da página.

        Depois do POST, uma NOVA consulta é feita para validar quantos
        canais realmente permaneceram travados.
        """
        dados = self._abrir_resultado_uma_vez(instancia)
        antes = int(dados["quantidade"])
        resultado = dados["_resultado_response"]

        if antes <= 0:
            return {
                "instancia": str(instancia),
                "antes": 0,
                "depois": 0,
                "liberados": 0,
                "sucesso": True,
                "mensagem_portal": "Nenhum canal travado para liberar.",
                "executado_em": datetime.now().isoformat(timespec="seconds"),
            }

        form = ipbx_hidden_inputs(resultado.text)

        obrigatorios = [
            "__VIEWSTATE",
            "__VIEWSTATEGENERATOR",
            "__EVENTVALIDATION",
        ]
        faltando = [x for x in obrigatorios if x not in form]
        if faltando:
            raise RuntimeError(
                f"Instância {instancia}: tela de resultado sem campos ASP.NET "
                "necessários para Confirmar: " + ", ".join(faltando)
            )

        # Estado de seleção validado no DevTools ao marcar "Todos".
        form["__EVENTTARGET"] = ""
        form["__EVENTARGUMENT"] = ""
        form["btnContinue"] = "Confirmar"
        form["cbxCheckall"] = "on"
        form["grdDataSet$DXSelInput"] = "T"
        form.setdefault("clientUTCTimeZoneOffsetTime", "")
        form.setdefault("clientLocalTimeZoneOffsetTime", "")

        resposta = self.session.post(
            resultado.url,
            data=form,
            headers={
                "Referer": resultado.url,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=self.timeout,
            allow_redirects=True,
        )
        resposta.raise_for_status()

        if self._voltou_login(resposta):
            raise PermissionError(
                "Sessão IPBX expirou durante a confirmação da limpeza."
            )

        texto_resposta = BeautifulSoup(
            resposta.text,
            "html.parser",
        ).get_text(" ", strip=True)

        sucesso_modal = "Operação realizada com sucesso!" in texto_resposta

        # A validação final não confia somente no modal:
        # consulta novamente o Portal e compara a quantidade real.
        verificacao = self._consultar_uma_vez(instancia)
        depois = int(verificacao["quantidade"])
        liberados = max(0, antes - depois)

        return {
            "instancia": str(instancia),
            "antes": antes,
            "depois": depois,
            "liberados": liberados,
            "sucesso": bool(sucesso_modal or depois < antes),
            "mensagem_portal": (
                "Operação realizada com sucesso!"
                if sucesso_modal
                else "Portal não exibiu a confirmação textual esperada."
            ),
            "executado_em": datetime.now().isoformat(timespec="seconds"),
        }

    def consultar(self, instancia):
        """
        Consulta serializada porque a seleção de projeto altera estado/cookies
        da sessão ASP.NET.
        """
        with self.lock:
            for tentativa in (1, 2):
                try:
                    if not self._autenticado_pelos_cookies():
                        self.autenticar()
                    return self._consultar_uma_vez(instancia)
                except PermissionError:
                    if tentativa == 2:
                        raise
                    self._nova_session()
                    self.autenticar()

    def limpar(self, instancia):
        """
        Executa a limpeza serializada e faz uma nova autenticação se a sessão
        expirar durante o processo.
        """
        with self.lock:
            for tentativa in (1, 2):
                try:
                    if not self._autenticado_pelos_cookies():
                        self.autenticar()
                    return self._limpar_uma_vez(instancia)
                except PermissionError:
                    if tentativa == 2:
                        raise
                    self._nova_session()
                    self.autenticar()


def nivel_canais_travados(quantidade, limite_alerta, limite_critico):
    if quantidade > limite_critico:
        return "critico"
    if quantidade >= limite_alerta:
        return "alerta"
    return "normal"


def formatar_duracao_canais(inicio_iso, agora=None):
    if agora is None:
        agora = datetime.now()
    if not inicio_iso:
        return "0min 00s"
    try:
        inicio = datetime.fromisoformat(inicio_iso)
    except ValueError:
        return "0min 00s"

    total = max(0, int((agora - inicio).total_seconds()))
    horas, resto = divmod(total, 3600)
    minutos, segundos = divmod(resto, 60)

    if horas:
        return f"{horas}h {minutos:02d}min {segundos:02d}s"
    return f"{minutos}min {segundos:02d}s"


def montar_alerta_canais_travados(
    instancia,
    quantidade,
    limite_alerta,
    limite_critico,
    critico=False,
    repeticao=False,
):
    if critico:
        titulo = "🚨🚨 <b>PORTAL ORBE — CANAIS TRAVADOS CRÍTICO</b> 🚨🚨"
        linhas = [
            titulo,
            "",
            f"<b>Instância:</b> {instancia}",
            f"<b>Canais presos > 30 minutos:</b> {quantidade}",
            "",
            "🔴 <b>NÍVEL CRÍTICO</b>",
            f"Mais de {limite_critico} canais travados foram identificados.",
            "",
            "⚠️ <b>REMOVER OS CANAIS TRAVADOS IMEDIATAMENTE.</b>",
            "",
            "<b>Caminho:</b>",
            "ShellIPBX → Remover Canais Travados",
        ]
    else:
        titulo = "⚠️ <b>PORTAL ORBE — CANAIS TRAVADOS</b>"
        linhas = [
            titulo,
            "",
            f"<b>Instância:</b> {instancia}",
            f"<b>Canais presos > 30 minutos:</b> {quantidade}",
            "",
            (
                f"⚠️ Quantidade de canais travados atingiu o limite "
                f"de <b>{limite_alerta}</b>."
            ),
            "",
            "<b>Favor verificar:</b>",
            "ShellIPBX → Remover Canais Travados",
        ]

    if repeticao:
        linhas.extend([
            "",
            "🔁 <b>Lembrete: condição continua ativa.</b>",
        ])

    return "\n".join(linhas)


def montar_normalizacao_canais_travados(
    instancia,
    quantidade,
    limite_alerta,
    inicio_alerta,
):
    return "\n".join([
        "✅ <b>PORTAL ORBE — CANAIS TRAVADOS NORMALIZADOS</b>",
        "",
        f"<b>Instância:</b> {instancia}",
        f"<b>Canais presos > 30 minutos:</b> {quantidade}",
        f"⏱ <b>Tempo total do alerta: {formatar_duracao_canais(inicio_alerta)}</b>",
        "",
        (
            f"✅ Quantidade retornou para abaixo do limite "
            f"de {limite_alerta} canais."
        ),
    ])


def _parse_iso_seguro(valor):
    if not valor:
        return None
    try:
        return datetime.fromisoformat(str(valor))
    except (TypeError, ValueError):
        return None


def _formatar_hora_iso(valor):
    dt = _parse_iso_seguro(valor)
    return dt.strftime("%H:%M") if dt else "-"


def _preservar_metadados_canais(anterior, novo):
    """Preserva dados de autolimpeza/recorrência ao atualizar o estado técnico."""
    prefixos = ("ultima_", "recorrencia_")
    for chave, valor in anterior.items():
        if chave.startswith(prefixos) and chave not in novo:
            novo[chave] = valor
    return novo


def atualizar_recorrencia_gerencial(
    estado,
    instancia,
    quantidade,
    limite_recorrencia=200,
    janela_min=60,
):
    """
    Registra no máximo uma recorrência por limpeza concluída.

    Regra gerencial:
      - quantidade >= 200;
      - retorno em até 60 min após uma limpeza SUCESSO/INCOMPLETA;
      - uma mesma limpeza não pode gerar várias ocorrências a cada ciclo;
      - ao romper 60 min sem nova recorrência, a sequência é zerada.
    """
    chave = str(instancia)
    agora = datetime.now()
    canais_estado = estado.setdefault("canais_travados", {})
    atual = canais_estado.setdefault(chave, {"instancia": chave})

    ultima_limpeza = _parse_iso_seguro(atual.get("ultima_limpeza_concluida_em"))
    if ultima_limpeza is None:
        return int(atual.get("recorrencia_contagem", 0) or 0)

    minutos = max(0.0, (agora - ultima_limpeza).total_seconds() / 60.0)

    # Janela rompida: uma futura sequência começa novamente em 1/4.
    if minutos > janela_min:
        if int(atual.get("recorrencia_contagem", 0) or 0) > 0:
            atual["recorrencia_contagem"] = 0
            atual["recorrencia_historico"] = []
            atual["recorrencia_ultima_limpeza_contabilizada"] = None
            atual["recorrencia_reset_em"] = agora.isoformat(timespec="seconds")
        return 0

    if int(quantidade) < int(limite_recorrencia):
        return int(atual.get("recorrencia_contagem", 0) or 0)

    limpeza_ref = ultima_limpeza.isoformat(timespec="seconds")
    if atual.get("recorrencia_ultima_limpeza_contabilizada") == limpeza_ref:
        return int(atual.get("recorrencia_contagem", 0) or 0)

    contagem = int(atual.get("recorrencia_contagem", 0) or 0) + 1
    historico = list(atual.get("recorrencia_historico") or [])
    historico.append({
        "numero": contagem,
        "data_hora": agora.isoformat(timespec="seconds"),
        "quantidade": int(quantidade),
        "minutos_desde_limpeza": round(minutos, 1),
        "limpeza_referencia": limpeza_ref,
    })

    atual["recorrencia_contagem"] = contagem
    atual["recorrencia_historico"] = historico[-10:]
    atual["recorrencia_ultima_limpeza_contabilizada"] = limpeza_ref
    atual["recorrencia_ultima_ocorrencia"] = agora.isoformat(timespec="seconds")
    return contagem


def _classificacao_gerencial_instancia(atual, quantidade):
    contagem = int(atual.get("recorrencia_contagem", 0) or 0)
    ultimo_resultado = str(atual.get("ultima_limpeza_resultado") or "").upper()

    if ultimo_resultado == "FALHA" and int(quantidade) >= 40:
        return "falha", "⚫", "Falha de tratamento"
    if contagem >= 4:
        return "escalonar", "🔴", "Escalonamento recomendado"
    if contagem == 3:
        return "persistencia", "🟠", "Persistência em observação"
    if contagem in (1, 2):
        return "acompanhar", "🟡", "Em acompanhamento"
    return "controle", "🟢", "Sob controle"


def _faixa_volume_atual(quantidade, auto_limpeza_limite=40, limite_recorrencia=200):
    quantidade = int(quantidade)
    if quantidade < int(auto_limpeza_limite):
        return "Normal"
    if quantidade < int(limite_recorrencia):
        return "Faixa de autolimpeza"
    return "Volume elevado, ocorrência crítica identificada"


def _status_autolimpeza_gerencial(
    atual,
    quantidade,
    auto_limpeza_enabled,
    auto_limpeza_limite,
    cooldown_min,
    existe_limpeza_em_andamento=False,
    mesma_instancia_em_andamento=False,
):
    """Texto gerencial da resposta automática, sem expor o jargão 'cooldown'."""
    quantidade = int(quantidade)
    cooldown_min = int(cooldown_min)

    if not auto_limpeza_enabled:
        return "Autolimpeza desabilitada."
    if quantidade < int(auto_limpeza_limite):
        return "Nenhuma atuação automática necessária neste momento."

    resultado = str(atual.get("ultima_limpeza_resultado") or "").upper()
    if mesma_instancia_em_andamento or resultado == "EM_EXECUCAO":
        return "Limpeza automática em andamento."

    if existe_limpeza_em_andamento:
        return (
            "Tratamento automático será realizado após a conclusão "
            "da limpeza em andamento."
        )

    ultima = _parse_iso_seguro(atual.get("ultima_tentativa_limpeza"))
    if ultima:
        decorridos = max(0.0, (datetime.now() - ultima).total_seconds() / 60.0)
        if decorridos < cooldown_min:
            if resultado == "FALHA":
                return (
                    "A última tentativa de autolimpeza apresentou falha. "
                    f"Nova tentativa automática estará disponível após o intervalo "
                    f"mínimo de {cooldown_min} minutos entre autolimpezas."
                )
            return (
                "Autolimpeza realizada recentemente. "
                f"Nova atuação automática estará disponível após o intervalo "
                f"mínimo de {cooldown_min} minutos entre autolimpezas, caso a "
                f"quantidade permaneça em ou acima de {auto_limpeza_limite} canais."
            )

    return "Autolimpeza será executada automaticamente pelo sistema."

def montar_resposta_travados(
    instancia,
    quantidade,
    limite_alerta,
    limite_critico,
    estado_instancia=None,
    auto_limpeza_enabled=True,
    auto_limpeza_limite=40,
    auto_limpeza_cooldown_min=10,
    limite_recorrencia=200,
    janela_recorrencia_min=60,
    recorrencias_escalonamento=4,
    existe_limpeza_em_andamento=False,
):
    atual = dict(estado_instancia or {})
    quantidade = int(quantidade)
    _, emoji, descricao = _classificacao_gerencial_instancia(atual, quantidade)
    contagem = int(atual.get("recorrencia_contagem", 0) or 0)
    historico = list(atual.get("recorrencia_historico") or [])
    mesma_em_andamento = str(atual.get("ultima_limpeza_resultado") or "").upper() == "EM_EXECUCAO"
    status_auto = _status_autolimpeza_gerencial(
        atual,
        quantidade,
        auto_limpeza_enabled,
        auto_limpeza_limite,
        auto_limpeza_cooldown_min,
        existe_limpeza_em_andamento=existe_limpeza_em_andamento,
        mesma_instancia_em_andamento=mesma_em_andamento,
    )
    faixa_atual = _faixa_volume_atual(
        quantidade,
        auto_limpeza_limite=auto_limpeza_limite,
        limite_recorrencia=limite_recorrencia,
    )

    linhas = [
        f"🔎 <b>PORTAL ORBE — INSTÂNCIA {instancia}</b>",
        "",
        f"🕐 Atualizado às {datetime.now().strftime('%H:%M')}",
        "",
        f"{emoji} <b>SITUAÇÃO GERENCIAL</b>",
        descricao,
        "",
        "────────────────────",
        "📊 <b>POSIÇÃO ATUAL</b>",
        "",
        f"Canais presos &gt; 30 min: <b>{quantidade}</b>",
        f"Classificação do volume: <b>{faixa_atual}</b>",
        f"Limite de autolimpeza: {auto_limpeza_limite}",
        f"Critério de recorrência: ≥ {limite_recorrencia}",
        "",
        "────────────────────",
        "🧹 <b>RESPOSTA AUTOMÁTICA</b>",
        "",
        status_auto,
    ]

    resultado_limpeza = str(atual.get("ultima_limpeza_resultado") or "").upper()

    # Durante uma execução corrente, informa apenas o início da ação atual.
    # Dados da limpeza anterior não são misturados com a execução em andamento.
    if resultado_limpeza == "EM_EXECUCAO":
        inicio_execucao = atual.get("ultima_tentativa_limpeza")
        if inicio_execucao:
            linhas.append("")
            linhas.append(
                f"Execução atual iniciada às {_formatar_hora_iso(inicio_execucao)}."
            )

    # Histórico separado do estado atual: só mostra uma autolimpeza já encerrada.
    elif resultado_limpeza in {"SUCESSO", "INCOMPLETA", "FALHA"}:
        mapa_resultado = {
            "SUCESSO": "✅ Sucesso",
            "INCOMPLETA": "⚠️ Incompleta",
            "FALHA": "❌ Falha",
        }
        horario_ultima = (
            atual.get("ultima_limpeza_resultado_em")
            or atual.get("ultima_limpeza_concluida_em")
            or atual.get("ultima_tentativa_limpeza")
        )

        linhas.extend([
            "",
            "🕘 <b>ÚLTIMA AUTOLIMPEZA</b>",
        ])
        if horario_ultima:
            linhas.append(f"• Horário: {_formatar_hora_iso(horario_ultima)}")
        linhas.append(
            f"• Resultado: {mapa_resultado.get(resultado_limpeza, resultado_limpeza)}"
        )
        if atual.get("ultima_quantidade_antes_limpeza") is not None:
            linhas.append(
                f"• Identificados: {int(atual.get('ultima_quantidade_antes_limpeza', 0))} canais"
            )
        if atual.get("ultima_quantidade_liberada") is not None:
            linhas.append(
                f"• Liberados: {int(atual.get('ultima_quantidade_liberada', 0))} canais"
            )
        if atual.get("ultima_quantidade_depois_limpeza") is not None:
            linhas.append(
                f"• Restantes: {int(atual.get('ultima_quantidade_depois_limpeza', 0))} canais"
            )

    linhas.extend(["", "────────────────────", "📈 <b>RECORRÊNCIA</b>", ""])

    if contagem <= 0:
        linhas.append("Nenhuma recorrência relevante registrada na janela atual.")
        linhas.append(f"Progresso: 0/{recorrencias_escalonamento}")
    else:
        for item in historico[-4:]:
            numero = int(item.get("numero", 0))
            hora = _formatar_hora_iso(item.get("data_hora"))
            qtd = int(item.get("quantidade", 0))
            mins = item.get("minutos_desde_limpeza", 0)
            linhas.append(f"{numero}ª ocorrência • {hora} • {qtd} canais • {mins:g} min após limpeza")
        linhas.append("")
        linhas.append(f"Progresso: <b>{contagem}/{recorrencias_escalonamento}</b>")

    linhas.extend([
        "",
        f"Critério: ≥ {limite_recorrencia} canais novamente em até {janela_recorrencia_min} min após limpeza.",
        "",
        "────────────────────",
        "🎯 <b>DECISÃO</b>",
        "",
    ])
    if str(atual.get("ultima_limpeza_resultado") or "").upper() == "FALHA" and quantidade >= auto_limpeza_limite:
        linhas.append("⚫ Falha de tratamento identificada.")
        linhas.append("Intervenção manual deve ser avaliada.")
    elif contagem >= recorrencias_escalonamento:
        linhas.append("🚨 <b>AÇÃO RECOMENDADA</b>")
        linhas.append(f"Abrir chamado na Orbe para análise do servidor da Instância {instancia}.")
        linhas.append("As autolimpezas tratam o efeito, mas a recorrência indica necessidade de análise da causa.")
    elif contagem == recorrencias_escalonamento - 1:
        linhas.append("Acompanhar com prioridade.")
        linhas.append("Uma nova recorrência dentro do critério atingirá o limiar de escalonamento.")
    elif contagem in (1, 2):
        linhas.append("Manter acompanhamento.")
        linhas.append("Nenhuma abertura de chamado recomendada neste momento.")
    else:
        linhas.append("✅ Nenhuma intervenção ou abertura de chamado necessária.")
        linhas.append("O ambiente permanece em monitoramento automático.")

    linhas.extend([
        "",
        "────────────────────",
        "📘 <b>CLASSIFICAÇÃO GERENCIAL</b>",
        "🟢 Sob controle",
        "🟡 Em acompanhamento — 1ª ou 2ª recorrência",
        "🟠 Persistência em observação — 3ª recorrência",
        "🔴 Escalonamento recomendado — 4ª recorrência",
        "⚫ Falha de tratamento",
    ])
    return "\n".join(linhas).strip()

def montar_resumo_travados_gerencial(
    resultados,
    estado,
    auto_limpeza_enabled=True,
    auto_limpeza_limite=40,
    auto_limpeza_cooldown_min=10,
    limite_recorrencia=200,
    janela_recorrencia_min=60,
    recorrencias_escalonamento=4,
):
    agora = datetime.now()
    canais_estado = estado.setdefault("canais_travados", {})
    itens = []
    total = 0
    acima_auto = 0

    ordem_nivel = {"controle": 0, "acompanhar": 1, "persistencia": 2, "escalonar": 3, "falha": 4}
    pior = ("controle", "🟢", "Ambiente sob controle")

    for instancia in sorted(resultados, key=lambda x: int(x) if str(x).isdigit() else str(x)):
        resultado = resultados[instancia]
        if resultado.get("erro"):
            itens.append({"instancia": instancia, "erro": resultado["erro"]})
            continue
        quantidade = int(resultado["quantidade"])
        total += quantidade
        if quantidade >= auto_limpeza_limite:
            acima_auto += 1
        atual = canais_estado.get(str(instancia), {})
        nivel, emoji, descricao = _classificacao_gerencial_instancia(atual, quantidade)
        if ordem_nivel[nivel] > ordem_nivel[pior[0]]:
            pior = (nivel, emoji, descricao)
        itens.append({
            "instancia": str(instancia),
            "quantidade": quantidade,
            "nivel": nivel,
            "emoji": emoji,
            "descricao": descricao,
            "estado": atual,
        })

    if pior[0] == "controle":
        status_geral = "🟢 <b>STATUS OPERACIONAL</b>\nAmbiente sob controle"
    elif pior[0] == "acompanhar":
        status_geral = "🟡 <b>STATUS OPERACIONAL</b>\nRecorrência em acompanhamento"
    elif pior[0] == "persistencia":
        status_geral = "🟠 <b>STATUS OPERACIONAL</b>\nPersistência em observação"
    elif pior[0] == "escalonar":
        status_geral = "🔴 <b>STATUS OPERACIONAL</b>\nRecorrência persistente — escalonamento recomendado"
    else:
        status_geral = "⚫ <b>STATUS OPERACIONAL</b>\nFalha de tratamento identificada"

    linhas = [
        "📊 <b>PORTAL ORBE — VISÃO GERENCIAL</b>",
        "",
        f"🕐 {agora.strftime('%d/%m/%Y • %H:%M')}",
        "",
        status_geral,
        "",
        "────────────────────",
        "📌 <b>POSIÇÃO ATUAL</b>",
        "",
    ]

    for item in itens:
        if item.get("erro"):
            linhas.append(f"⚪ Instância {item['instancia']} • consulta indisponível")
        else:
            faixa = _faixa_volume_atual(
                item["quantidade"],
                auto_limpeza_limite=auto_limpeza_limite,
                limite_recorrencia=limite_recorrencia,
            )
            linhas.append(
                f"▫️ Instância {item['instancia']}  •  {item['quantidade']} canais  •  {faixa}"
            )

    linhas.extend([
        "",
        f"Total: <b>{total}</b> canais travados",
        f"Instâncias acima de {auto_limpeza_limite}: <b>{acima_auto}</b> de {sum(1 for i in itens if not i.get('erro'))}",
        "",
        "────────────────────",
        "🧹 <b>RESPOSTA AUTOMÁTICA</b>",
        "",
    ])

    relevantes_auto = [i for i in itens if not i.get("erro") and i["quantidade"] >= auto_limpeza_limite]
    em_andamento = [
        i for i in itens
        if not i.get("erro")
        and str(i["estado"].get("ultima_limpeza_resultado") or "").upper() == "EM_EXECUCAO"
    ]
    instancias_em_andamento = {i["instancia"] for i in em_andamento}

    if relevantes_auto:
        relevantes_auto = sorted(
            relevantes_auto,
            key=lambda i: (
                0 if i["instancia"] in instancias_em_andamento else 1,
                int(i["instancia"]) if str(i["instancia"]).isdigit() else str(i["instancia"]),
            ),
        )
        for indice, item in enumerate(relevantes_auto):
            status = _status_autolimpeza_gerencial(
                item["estado"], item["quantidade"], auto_limpeza_enabled,
                auto_limpeza_limite, auto_limpeza_cooldown_min,
                existe_limpeza_em_andamento=bool(em_andamento),
                mesma_instancia_em_andamento=item["instancia"] in instancias_em_andamento,
            )
            if indice:
                linhas.append("")
            linhas.append(f"Instância {item['instancia']}")
            linhas.append(f"• {status}")
    else:
        linhas.append("Nenhuma atuação automática necessária neste momento.")

    linhas.extend(["", "────────────────────", "📈 <b>RECORRÊNCIA</b>", ""])
    recorrentes = [i for i in itens if not i.get("erro") and int(i["estado"].get("recorrencia_contagem", 0) or 0) > 0]
    if not recorrentes:
        linhas.append("Nenhuma instância apresenta recorrência relevante na janela atual.")
    else:
        for item in recorrentes:
            cont = int(item["estado"].get("recorrencia_contagem", 0) or 0)
            ultima = (item["estado"].get("recorrencia_historico") or [{}])[-1]
            qtd = int(ultima.get("quantidade", item["quantidade"]))
            mins = ultima.get("minutos_desde_limpeza", 0)
            linhas.append(f"Instância {item['instancia']} • {cont}ª ocorrência • {qtd} canais • {mins:g} min após limpeza • Progresso {cont}/{recorrencias_escalonamento}")

    linhas.extend([
        "",
        f"Critério: ≥ {limite_recorrencia} canais novamente em até {janela_recorrencia_min} min após limpeza.",
        f"Escalonamento: {recorrencias_escalonamento}ª recorrência → abertura de chamado recomendada.",
        "",
        "────────────────────",
        "🎯 <b>DECISÃO</b>",
        "",
    ])

    falhas = [i for i in itens if not i.get("erro") and i["nivel"] == "falha"]
    escalar = [i for i in itens if not i.get("erro") and i["nivel"] == "escalonar"]
    terceira = [i for i in itens if not i.get("erro") and i["nivel"] == "persistencia"]
    if falhas:
        linhas.append("⚫ Falha de tratamento exige avaliação operacional.")
        linhas.append("Instância(s): " + ", ".join(i["instancia"] for i in falhas))
    elif escalar:
        linhas.append("🚨 <b>AÇÃO RECOMENDADA</b>")
        linhas.append("Abrir chamado na Orbe para análise do servidor da(s) Instância(s): " + ", ".join(i["instancia"] for i in escalar) + ".")
        linhas.append("A autolimpeza trata o efeito, mas a recorrência persistente indica necessidade de análise da causa.")
    elif terceira:
        linhas.append("Acompanhar com prioridade a(s) Instância(s): " + ", ".join(i["instancia"] for i in terceira) + ".")
        linhas.append("Uma nova recorrência válida atingirá o limiar de escalonamento.")
    elif recorrentes:
        linhas.append("🔎 Manter acompanhamento das recorrências registradas.")
        linhas.append("Nenhuma abertura de chamado recomendada neste momento.")
    else:
        linhas.append("✅ Nenhuma intervenção manual necessária.")
        linhas.append("✅ Nenhuma abertura de chamado recomendada.")
        linhas.append("O ambiente permanece em monitoramento automático.")

    linhas.extend([
        "",
        "────────────────────",
        "📘 <b>LEGENDA GERENCIAL</b>",
        "",
        "🟢 Sob controle",
        "🟡 Em acompanhamento — 1ª ou 2ª recorrência",
        "🟠 Persistência em observação — 3ª recorrência",
        "🔴 Escalonamento recomendado — 4ª recorrência",
        "⚫ Falha de tratamento",
    ])
    return "\n".join(linhas).strip()

def enviar_telegram_ou_console(telegram, telegram_enabled, mensagem):
    if telegram_enabled and telegram is not None:
        telegram.enviar_mensagem(mensagem)
    else:
        print("\n--- MENSAGEM QUE SERIA ENVIADA AO TELEGRAM ---")
        print(re.sub(r"</?b>", "", mensagem))
        print("--- FIM DA MENSAGEM ---\n")


def processar_canais_travados(
    instancia,
    quantidade,
    estado,
    telegram,
    telegram_enabled,
    limite_alerta,
    limite_critico,
    repeticao_min,
    dentro_horario,
):
    agora = datetime.now()
    chave = str(instancia)
    canais_estado = estado.setdefault("canais_travados", {})
    atual = canais_estado.get(chave, {})

    nivel_novo = nivel_canais_travados(
        quantidade,
        limite_alerta,
        limite_critico,
    )
    nivel_anterior = atual.get("nivel", "normal")
    alerta_enviado = bool(atual.get("alerta_enviado"))
    inicio_alerta = atual.get("inicio_alerta")
    ultima_notificacao = atual.get("ultima_notificacao")

    # NORMALIZAÇÃO: somente se um alerta realmente chegou a ser enviado.
    if nivel_novo == "normal":
        if alerta_enviado:
            mensagem = montar_normalizacao_canais_travados(
                instancia=instancia,
                quantidade=quantidade,
                limite_alerta=limite_alerta,
                inicio_alerta=inicio_alerta,
            )
            enviar_telegram_ou_console(
                telegram,
                telegram_enabled,
                mensagem,
            )

        novo_estado = {
            "instancia": chave,
            "quantidade": int(quantidade),
            "nivel": "normal",
            "alerta_enviado": False,
            "inicio_alerta": None,
            "ultima_notificacao": None,
            "ultima_consulta": agora.isoformat(timespec="seconds"),
        }
        canais_estado[chave] = _preservar_metadados_canais(atual, novo_estado)
        return

    # Começou condição acima do limite.
    if not inicio_alerta:
        inicio_alerta = agora.isoformat(timespec="seconds")

    deve_enviar = False
    repeticao = False

    # Primeiro alerta.
    if not alerta_enviado:
        deve_enviar = dentro_horario

    # Escalada ALERTA -> CRÍTICO é imediata e ignora janela de repetição.
    elif nivel_anterior != "critico" and nivel_novo == "critico":
        deve_enviar = dentro_horario

    # Repetição enquanto permanece em alerta.
    elif dentro_horario and ultima_notificacao:
        try:
            ultima_dt = datetime.fromisoformat(ultima_notificacao)
            if agora - ultima_dt >= timedelta(minutes=repeticao_min):
                deve_enviar = True
                repeticao = True
        except ValueError:
            deve_enviar = True

    if deve_enviar:
        mensagem = montar_alerta_canais_travados(
            instancia=instancia,
            quantidade=quantidade,
            limite_alerta=limite_alerta,
            limite_critico=limite_critico,
            critico=(nivel_novo == "critico"),
            repeticao=repeticao,
        )
        enviar_telegram_ou_console(
            telegram,
            telegram_enabled,
            mensagem,
        )
        alerta_enviado = True
        ultima_notificacao = agora.isoformat(timespec="seconds")

    novo_estado = {
        "instancia": chave,
        "quantidade": int(quantidade),
        "nivel": nivel_novo,
        "alerta_enviado": bool(alerta_enviado),
        "inicio_alerta": inicio_alerta,
        "ultima_notificacao": ultima_notificacao,
        "ultima_consulta": agora.isoformat(timespec="seconds"),
    }
    canais_estado[chave] = _preservar_metadados_canais(atual, novo_estado)



def montar_sucesso_limpeza_automatica(
    instancia,
    antes,
    depois,
    liberados,
    limite_critico,
):
    critico = antes > limite_critico

    if critico:
        titulo = "🚨 <b>PORTAL ORBE — LIMPEZA AUTOMÁTICA CRÍTICA CONCLUÍDA</b>"
    else:
        titulo = "✅ <b>PORTAL ORBE — LIMPEZA AUTOMÁTICA CONCLUÍDA</b>"

    linhas = [
        titulo,
        "",
        f"<b>Instância:</b> {instancia}",
        f"<b>Canais travados identificados:</b> {antes}",
        f"<b>Canais liberados:</b> {liberados}",
        f"<b>Canais restantes:</b> {depois}",
        "",
        "✅ Limpeza automática realizada com sucesso.",
    ]

    if critico:
        linhas.extend([
            "",
            "🔴 A quantidade estava em nível crítico (> "
            f"{limite_critico}) e foi tratada automaticamente.",
        ])

    return "\n".join(linhas)


def montar_limpeza_automatica_incompleta(
    instancia,
    antes,
    depois,
    liberados,
    limite,
):
    return "\n".join([
        "⚠️ <b>PORTAL ORBE — LIMPEZA AUTOMÁTICA INCOMPLETA</b>",
        "",
        f"<b>Instância:</b> {instancia}",
        f"<b>Canais antes da limpeza:</b> {antes}",
        f"<b>Canais liberados:</b> {liberados}",
        f"<b>Canais restantes:</b> {depois}",
        "",
        (
            f"⚠️ A limpeza foi executada, porém a quantidade permanece "
            f"em ou acima do limite de {limite} canais."
        ),
    ])


def montar_falha_limpeza_automatica(
    instancia,
    quantidade,
    motivo,
    limite_critico,
):
    critico = quantidade > limite_critico
    titulo = (
        "🚨🚨 <b>PORTAL ORBE — FALHA NA LIMPEZA AUTOMÁTICA CRÍTICA</b> 🚨🚨"
        if critico
        else "🚨 <b>PORTAL ORBE — FALHA NA LIMPEZA AUTOMÁTICA</b>"
    )

    linhas = [
        titulo,
        "",
        f"<b>Instância:</b> {instancia}",
        f"<b>Canais travados detectados:</b> {quantidade}",
        "",
        "❌ Não foi possível concluir a limpeza automática.",
        f"<b>Motivo:</b> {motivo}",
    ]

    if critico:
        linhas.extend([
            "",
            "⚠️ <b>INTERVENÇÃO MANUAL IMEDIATA RECOMENDADA.</b>",
        ])

    return "\n".join(linhas)


def pode_executar_autolimpeza(estado, instancia, cooldown_min):
    atual = estado.setdefault("canais_travados", {}).get(str(instancia), {})
    ultima = atual.get("ultima_tentativa_limpeza")

    if not ultima:
        return True

    try:
        ultima_dt = datetime.fromisoformat(ultima)
    except ValueError:
        return True

    return datetime.now() - ultima_dt >= timedelta(minutes=cooldown_min)


def marcar_tentativa_autolimpeza(estado, instancia, quantidade):
    chave = str(instancia)
    canais_estado = estado.setdefault("canais_travados", {})
    atual = canais_estado.get(chave, {})
    agora = datetime.now().isoformat(timespec="seconds")

    atual.update({
        "instancia": chave,
        "quantidade": int(quantidade),
        "ultima_tentativa_limpeza": agora,
        "ultima_limpeza_resultado": "EM_EXECUCAO",
        "ultima_consulta": agora,
    })
    canais_estado[chave] = atual


def registrar_resultado_autolimpeza(
    estado,
    instancia,
    antes,
    depois,
    liberados,
    limite_alerta,
    limite_critico,
    notificacao_enviada,
    sucesso,
    status_resultado=None,
):
    chave = str(instancia)
    agora = datetime.now()
    agora_iso = agora.isoformat(timespec="seconds")
    canais_estado = estado.setdefault("canais_travados", {})
    anterior = canais_estado.get(chave, {})

    nivel = nivel_canais_travados(
        depois,
        limite_alerta,
        limite_critico,
    )

    status_resultado = str(
        status_resultado
        or ("SUCESSO" if sucesso and int(depois) < int(limite_alerta) else ("INCOMPLETA" if sucesso else "FALHA"))
    ).upper()
    limpeza_concluida = status_resultado in {"SUCESSO", "INCOMPLETA"}

    if nivel == "normal":
        novo_estado = {
            "instancia": chave,
            "quantidade": int(depois),
            "nivel": "normal",
            "alerta_enviado": False,
            "inicio_alerta": None,
            "ultima_notificacao": (
                agora_iso if notificacao_enviada else anterior.get("ultima_notificacao")
            ),
            "ultima_consulta": agora_iso,
            "ultima_limpeza_resultado": status_resultado,
            "ultima_limpeza_resultado_em": agora_iso,
            "ultima_limpeza_concluida_em": (
                agora_iso if limpeza_concluida else anterior.get("ultima_limpeza_concluida_em")
            ),
            "ultima_quantidade_depois_limpeza": int(depois),
            "ultima_tentativa_limpeza": anterior.get(
                "ultima_tentativa_limpeza",
                agora_iso,
            ),
            "ultima_limpeza_sucesso": agora_iso if sucesso else None,
            "ultima_quantidade_antes_limpeza": int(antes),
            "ultima_quantidade_liberada": int(liberados),
        }
        canais_estado[chave] = _preservar_metadados_canais(anterior, novo_estado)
        return

    inicio_alerta = anterior.get("inicio_alerta") or agora_iso
    novo_estado = {
        "instancia": chave,
        "quantidade": int(depois),
        "nivel": nivel,
        # A mensagem de falha/incompleta já cumpre o papel de alerta.
        "alerta_enviado": bool(notificacao_enviada),
        "inicio_alerta": inicio_alerta,
        "ultima_notificacao": (
            agora_iso if notificacao_enviada else anterior.get("ultima_notificacao")
        ),
        "ultima_consulta": agora_iso,
        "ultima_limpeza_resultado": status_resultado,
        "ultima_limpeza_resultado_em": agora_iso,
        "ultima_limpeza_concluida_em": (
            agora_iso if limpeza_concluida else anterior.get("ultima_limpeza_concluida_em")
        ),
        "ultima_quantidade_depois_limpeza": int(depois),
        "ultima_tentativa_limpeza": anterior.get(
            "ultima_tentativa_limpeza",
            agora_iso,
        ),
        "ultima_limpeza_sucesso": agora_iso if sucesso else None,
        "ultima_quantidade_antes_limpeza": int(antes),
        "ultima_quantidade_liberada": int(liberados),
    }
    canais_estado[chave] = _preservar_metadados_canais(anterior, novo_estado)


def registrar_auditoria_autolimpeza(
    instancia,
    status,
    antes=None,
    depois=None,
    liberados=None,
    telegram_status=None,
    detalhe=None,
):
    """
    Auditoria persistente e independente do Telegram.

    Exemplo:
    19/08/2026 14:12:03 | Instância 4 | SUCESSO |
    antes=42 | depois=0 | liberados=42 | telegram=OK
    """
    caminho = Path(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_AUDITORIA_ARQUIVO",
            str(ARQUIVO_AUDITORIA_AUTOLIMPEZA_PADRAO),
        )
    )
    caminho.parent.mkdir(parents=True, exist_ok=True)

    partes = [
        datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        f"Instância {instancia}",
        str(status).upper(),
    ]

    if antes is not None:
        partes.append(f"antes={antes}")
    if depois is not None:
        partes.append(f"depois={depois}")
    if liberados is not None:
        partes.append(f"liberados={liberados}")
    if telegram_status is not None:
        partes.append(f"telegram={telegram_status}")
    if detalhe:
        detalhe_limpo = str(detalhe).replace("\n", " ").replace("\r", " ").strip()
        partes.append(f"detalhe={detalhe_limpo}")

    with caminho.open("a", encoding="utf-8") as f:
        f.write(" | ".join(partes) + "\n")


def enviar_telegram_com_status(telegram, telegram_enabled, mensagem):
    """
    Envia mensagem e devolve um status simples para auditoria.
    Não impede a continuidade da rotina se o Telegram falhar.
    """
    if not telegram_enabled or telegram is None:
        print("\n--- MENSAGEM QUE SERIA ENVIADA AO TELEGRAM ---")
        print(re.sub(r"</?b>", "", mensagem))
        print("--- FIM DA MENSAGEM ---\n")
        return "DESABILITADO"

    try:
        telegram.enviar_mensagem(mensagem)
        return "OK"
    except Exception as e:
        print(f"⚠ Telegram: falha ao enviar mensagem de autolimpeza: {e}")
        return f"FALHA:{type(e).__name__}"




def caminho_auditoria_autolimpeza():
    return Path(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_AUDITORIA_ARQUIVO",
            str(ARQUIVO_AUDITORIA_AUTOLIMPEZA_PADRAO),
        )
    )


def ler_eventos_autolimpeza_do_dia(data_referencia):
    """
    Lê apenas os resultados finais do dia:
      SUCESSO, INCOMPLETA e FALHA.

    As linhas INICIADA são ignoradas para não duplicar a contagem.
    """
    caminho = caminho_auditoria_autolimpeza()
    eventos = []

    if not caminho.exists():
        return eventos

    prefixo_data = data_referencia.strftime("%d/%m/%Y")

    try:
        linhas = caminho.read_text(encoding="utf-8").splitlines()
    except Exception as e:
        print(f"⚠ Não foi possível ler auditoria para resumo diário: {e}")
        return eventos

    for linha in linhas:
        if not linha.startswith(prefixo_data + " "):
            continue

        partes = [p.strip() for p in linha.split(" | ")]
        if len(partes) < 3:
            continue

        status = partes[2].upper()
        if status not in {"SUCESSO", "INCOMPLETA", "FALHA"}:
            continue

        m_inst = re.search(r"Instância\s+(\d+)", partes[1], re.IGNORECASE)
        if not m_inst:
            continue

        evento = {
            "data_hora": partes[0],
            "instancia": m_inst.group(1),
            "status": status,
            "antes": 0,
            "depois": 0,
            "liberados": 0,
            "telegram": "",
        }

        for parte in partes[3:]:
            if "=" not in parte:
                continue
            chave, valor = parte.split("=", 1)
            chave = chave.strip().lower()
            valor = valor.strip()

            if chave in {"antes", "depois", "liberados"}:
                try:
                    evento[chave] = int(valor)
                except ValueError:
                    pass
            elif chave == "telegram":
                evento["telegram"] = valor

        eventos.append(evento)

    return eventos


def estatisticas_autolimpeza_do_dia(data_referencia, limite_critico):
    eventos = ler_eventos_autolimpeza_do_dia(data_referencia)

    resultados = {
        "eventos": eventos,
        "sucessos": 0,
        "incompletas": 0,
        "falhas": 0,
        "criticas": 0,
        "canais_liberados": 0,
        "maior_evento": None,
        "por_instancia": {},
    }

    for evento in eventos:
        status = evento["status"]
        antes = int(evento.get("antes", 0))
        liberados = int(evento.get("liberados", 0))
        instancia = str(evento["instancia"])

        if status == "SUCESSO":
            resultados["sucessos"] += 1
        elif status == "INCOMPLETA":
            resultados["incompletas"] += 1
        elif status == "FALHA":
            resultados["falhas"] += 1

        if antes > limite_critico:
            resultados["criticas"] += 1

        resultados["canais_liberados"] += liberados

        if (
            resultados["maior_evento"] is None
            or antes > resultados["maior_evento"]["antes"]
        ):
            resultados["maior_evento"] = {
                "instancia": instancia,
                "antes": antes,
                "status": status,
            }

        por_inst = resultados["por_instancia"].setdefault(
            instancia,
            {
                "eventos": 0,
                "sucessos": 0,
                "incompletas": 0,
                "falhas": 0,
                "liberados": 0,
                "pico": 0,
            },
        )

        por_inst["eventos"] += 1
        por_inst["liberados"] += liberados
        por_inst["pico"] = max(por_inst["pico"], antes)

        if status == "SUCESSO":
            por_inst["sucessos"] += 1
        elif status == "INCOMPLETA":
            por_inst["incompletas"] += 1
        elif status == "FALHA":
            por_inst["falhas"] += 1

    total_resultados = (
        resultados["sucessos"]
        + resultados["incompletas"]
        + resultados["falhas"]
    )
    resultados["total_resultados"] = total_resultados
    resultados["taxa_sucesso"] = (
        (resultados["sucessos"] / total_resultados) * 100.0
        if total_resultados
        else 100.0
    )
    resultados["media_liberados"] = (
        resultados["canais_liberados"] / resultados["sucessos"]
        if resultados["sucessos"]
        else 0.0
    )

    return resultados


def montar_resumo_diario_autolimpeza(
    data_referencia,
    estado,
    instancias_monitoradas,
    limite_alerta,
    limite_critico,
):
    stats = estatisticas_autolimpeza_do_dia(
        data_referencia,
        limite_critico,
    )

    total = stats["total_resultados"]
    taxa_txt = f'{stats["taxa_sucesso"]:.1f}%'.replace(".", ",")
    media_txt = f'{stats["media_liberados"]:.1f}'.replace(".", ",")

    linhas = [
        "📊 <b>PORTAL ORBE — RESUMO DIÁRIO DE AUTOLIMPEZA</b>",
        "",
        f"<b>Data:</b> {data_referencia:%d/%m/%Y}",
        "",
        "<b>Visão geral</b>",
    ]

    if total == 0:
        linhas.extend([
            "✅ Nenhuma autolimpeza foi necessária no dia.",
            "• Limpezas executadas: 0",
            "• Canais liberados: 0",
        ])
    else:
        linhas.extend([
            f"• Limpezas concluídas: <b>{stats['sucessos']}</b>",
            f"• Canais liberados: <b>{stats['canais_liberados']}</b>",
            f"• Eventos críticos (> {limite_critico}): <b>{stats['criticas']}</b>",
            f"• Limpezas incompletas: <b>{stats['incompletas']}</b>",
            f"• Falhas: <b>{stats['falhas']}</b>",
            f"• Taxa de sucesso: <b>{taxa_txt}</b>",
            f"• Média liberada por limpeza: <b>{media_txt}</b>",
        ])

        if stats["por_instancia"]:
            linhas.extend(["", "<b>Por instância</b>"])
            for instancia in sorted(
                stats["por_instancia"],
                key=lambda x: int(x),
            ):
                item = stats["por_instancia"][instancia]
                linhas.append(
                    f"• Instância {instancia}: "
                    f"{item['sucessos']} limpeza(s) | "
                    f"{item['liberados']} liberados | "
                    f"pico {item['pico']}"
                )
                if item["incompletas"] or item["falhas"]:
                    linhas.append(
                        f"  ↳ incompletas: {item['incompletas']} | "
                        f"falhas: {item['falhas']}"
                    )

        maior = stats["maior_evento"]
        if maior:
            linhas.extend([
                "",
                "<b>Maior evento do dia</b>",
                (
                    f"• Instância {maior['instancia']}: "
                    f"<b>{maior['antes']}</b> canais travados "
                    f"({maior['status']})"
                ),
            ])

    linhas.extend(["", "<b>Situação no fechamento</b>"])

    canais_estado = estado.get("canais_travados", {})
    for instancia in sorted(
        [str(x) for x in instancias_monitoradas],
        key=lambda x: int(x),
    ):
        atual = canais_estado.get(instancia, {})
        quantidade = atual.get("quantidade")
        nivel = str(atual.get("nivel", "sem dado")).upper()

        if quantidade is None:
            linhas.append(f"• Instância {instancia}: sem leitura disponível")
            continue

        if nivel == "CRITICO":
            emoji = "🔴"
        elif nivel == "ALERTA":
            emoji = "⚠️"
        else:
            emoji = "✅"

        linhas.append(
            f"• Instância {instancia}: "
            f"<b>{quantidade}</b> canais | {emoji} {nivel}"
        )

    if stats["falhas"] or stats["incompletas"]:
        linhas.extend([
            "",
            "⚠️ <b>Atenção gerencial:</b> houve limpeza incompleta "
            "ou falha no período. Recomenda-se revisar a auditoria.",
        ])
    elif stats["criticas"]:
        linhas.extend([
            "",
            "📌 <b>Insight:</b> houve ocorrência crítica no período, "
            "mas o tratamento automático concluiu sem pendências registradas.",
        ])

    return "\n".join(linhas), stats


def registrar_resumo_diario_autolimpeza(data_referencia, stats):
    caminho = Path(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_RESUMO_DIARIO_ARQUIVO",
            str(ARQUIVO_RESUMO_DIARIO_AUTOLIMPEZA_PADRAO),
        )
    )
    caminho.parent.mkdir(parents=True, exist_ok=True)

    resumo = {
        "data": data_referencia.strftime("%Y-%m-%d"),
        "limpezas_concluidas": stats["sucessos"],
        "incompletas": stats["incompletas"],
        "falhas": stats["falhas"],
        "eventos_criticos": stats["criticas"],
        "canais_liberados": stats["canais_liberados"],
        "taxa_sucesso": round(stats["taxa_sucesso"], 2),
        "media_liberados_por_limpeza": round(stats["media_liberados"], 2),
        "por_instancia": stats["por_instancia"],
        "maior_evento": stats["maior_evento"],
    }

    with caminho.open("a", encoding="utf-8") as f:
        f.write(json.dumps(resumo, ensure_ascii=False) + "\n")


def horario_resumo_atingido(agora, hora_resumo):
    return agora.time() >= hora_resumo


def tentar_enviar_resumo_diario_autolimpeza(
    agora,
    estado,
    telegram,
    telegram_enabled,
    enabled,
    hora_resumo,
    instancias_monitoradas,
    limite_alerta,
    limite_critico,
):
    if not enabled:
        return

    meta = estado.setdefault("metadados", {})
    hoje_iso = agora.strftime("%Y-%m-%d")

    if meta.get("ultimo_resumo_autolimpeza_data") == hoje_iso:
        return

    if not horario_resumo_atingido(agora, hora_resumo):
        return

    mensagem, stats = montar_resumo_diario_autolimpeza(
        data_referencia=agora,
        estado=estado,
        instancias_monitoradas=instancias_monitoradas,
        limite_alerta=limite_alerta,
        limite_critico=limite_critico,
    )

    status = enviar_telegram_com_status(
        telegram,
        telegram_enabled,
        mensagem,
    )

    # Se Telegram estiver desabilitado, o resumo é exibido no console
    # e considerado processado. Em falha real, tentaremos novamente
    # no próximo ciclo.
    if status == "OK" or status == "DESABILITADO":
        registrar_resumo_diario_autolimpeza(agora, stats)
        meta["ultimo_resumo_autolimpeza_data"] = hoje_iso
        meta["ultimo_resumo_autolimpeza_status"] = status
        meta["ultimo_resumo_autolimpeza_em"] = datetime.now().isoformat(
            timespec="seconds"
        )
        salvar_estado(estado)

        print(
            f"📊 Resumo diário de autolimpeza enviado/processado "
            f"para {agora:%d/%m/%Y}."
        )
    else:
        print(
            "⚠ Resumo diário de autolimpeza não foi entregue; "
            "nova tentativa ocorrerá no próximo ciclo."
        )



def formatar_percentual(valor):
    return f"{valor:.2f}%".replace(".", ",")


def chave_fila(instancia, fila):
    return f"inst{instancia}|{fila['projeto_id']}|{fila['codigo']}"


def limites_ultrapassados(status, limite_p34, limite_p135):
    return {
        34: status[34]["percentual"] > limite_p34,
        135: status[135]["percentual"] > limite_p135,
    }


def contar_incidentes_ativos(estado):
    return sum(
        1
        for item in estado.get("filas", {}).values()
        if item.get("incidente_confirmado") is True
    )


def formatar_duracao_incidente(inicio_incidente, agora=None):
    """Formata duração contínua de incidente para leitura gerencial."""
    agora = agora or datetime.now()
    try:
        inicio = datetime.fromisoformat(str(inicio_incidente))
    except (TypeError, ValueError):
        return "indisponível"

    total_min = max(0, int((agora - inicio).total_seconds() // 60))
    dias, resto_min = divmod(total_min, 1440)
    horas, minutos = divmod(resto_min, 60)

    partes = []
    if dias:
        partes.append(f"{dias}d")
    if horas or dias:
        partes.append(f"{horas:02d}h" if dias else f"{horas}h")
    partes.append(f"{minutos:02d}min" if (horas or dias) else f"{minutos} min")
    return " ".join(partes)


def formatar_inicio_incidente(inicio_incidente):
    try:
        return datetime.fromisoformat(str(inicio_incidente)).strftime("%d/%m/%Y %H:%M")
    except (TypeError, ValueError):
        return "indisponível"


def montar_alerta(
    instancia,
    fila,
    status,
    limite_p34,
    limite_p135,
    confirmacao_min,
    incidentes_ativos,
    persistencia=False,
    inicio_incidente=None,
    atualizado_em=None,
):
    acima = limites_ultrapassados(status, limite_p34, limite_p135)
    atualizado_em = atualizado_em or datetime.now()

    if persistencia:
        linhas = [
            "🔴 <b>PORTAL ORBE — CONGESTIONAMENTO PERSISTENTE</b>",
            "",
            f"<b>Instância:</b> {instancia}",
            f"<b>Projeto:</b> {fila['projeto']}",
            f"<b>Fila:</b> {fila['nome']} ({fila['codigo']})",
            "",
            f"🕐 <b>Atualizado em:</b> {atualizado_em:%d/%m/%Y %H:%M}",
            "",
            (
                f"Status PABX 34 — No Circuit Available: "
                f"<b>{formatar_percentual(status[34]['percentual'])}</b> "
                f"({status[34]['quantidade']} chamadas)"
            ),
            (
                f"Status PABX 135 — Internal Congestion: "
                f"<b>{formatar_percentual(status[135]['percentual'])}</b> "
                f"({status[135]['quantidade']} chamadas)"
            ),
            "",
            "────────────────────",
            "📌 <b>HISTÓRICO DO INCIDENTE</b>",
            "",
            f"Início: <b>{formatar_inicio_incidente(inicio_incidente)}</b>",
            f"Duração contínua: <b>{formatar_duracao_incidente(inicio_incidente, atualizado_em)}</b>",
            "Situação: permanece acima do limite",
            "",
            "⚠️ <b>Este não é um novo incidente.</b>",
            "A condição permanece ativa de forma ininterrupta.",
            "",
            f"🔴 <b>Total de congestionamentos ativos no Portal Orbe: {incidentes_ativos}</b>",
        ]
        return "\n".join(linhas)

    linhas = [
        "🚨 <b>PORTAL ORBE — NOVO CONGESTIONAMENTO</b>",
        "",
        f"<b>Instância:</b> {instancia}",
        f"<b>Projeto:</b> {fila['projeto']}",
        f"<b>Fila:</b> {fila['nome']} ({fila['codigo']})",
        "",
        "<b>Telefonia - Classificação por status - 5'</b>",
        "",
        (
            f"Status PABX 34 — No Circuit Available: "
            f"<b>{formatar_percentual(status[34]['percentual'])}</b> "
            f"({status[34]['quantidade']} chamadas)"
        ),
        (
            f"Status PABX 135 — Internal Congestion: "
            f"<b>{formatar_percentual(status[135]['percentual'])}</b> "
            f"({status[135]['quantidade']} chamadas)"
        ),
        "",
        (
            f"⚠️ Limites: P34 &gt; <b>{formatar_percentual(limite_p34)}</b> | "
            f"P135 &gt; <b>{formatar_percentual(limite_p135)}</b>"
        ),
        f"⏱ Confirmação: <b>{confirmacao_min} minutos contínuos</b>",
        "",
        f"🔴 Incidente confirmado. Início da condição: <b>{formatar_inicio_incidente(inicio_incidente)}</b>",
        "",
    ]

    if acima[34] and acima[135]:
        linhas.append(
            "Favor verificar, pois esta fila está acima do limite aceitável "
            "tanto para o Status PABX 34 quanto para o Status PABX 135."
        )
    elif acima[34]:
        linhas.append(
            "Favor verificar, pois esta fila está acima do limite aceitável "
            "para o Status PABX 34."
        )
    elif acima[135]:
        linhas.append(
            "Favor verificar, pois esta fila está acima do limite aceitável "
            "para o Status PABX 135."
        )

    linhas.extend([
        "",
        f"🔴 <b>Total de congestionamentos ativos no Portal Orbe: {incidentes_ativos}</b>",
    ])

    return "\n".join(linhas)


def montar_normalizacao(
    instancia,
    fila,
    status,
    limite_p34,
    limite_p135,
    inicio_incidente,
    fim_congestionamento,
    confirmacao_normalizacao_min,
    incidentes_ativos,
):
    duracao_min = minutos_decorridos(
        inicio_incidente,
        agora=(
            datetime.fromisoformat(fim_congestionamento)
            if fim_congestionamento
            else datetime.now()
        ),
    )
    duracao_total_seg = int(round(duracao_min * 60))
    horas, resto = divmod(duracao_total_seg, 3600)
    minutos, segundos = divmod(resto, 60)

    if horas:
        duracao_txt = f"{horas}h {minutos:02d}min {segundos:02d}s"
    else:
        duracao_txt = f"{minutos}min {segundos:02d}s"

    return "\n".join([
        "✅ <b>PORTAL ORBE — FILA NORMALIZADA</b>",
        "",
        f"<b>Instância:</b> {instancia}",
        f"<b>Projeto:</b> {fila['projeto']}",
        f"<b>Fila:</b> {fila['nome']} ({fila['codigo']})",
        "",
        (
            f"Status PABX 34 — No Circuit Available: "
            f"<b>{formatar_percentual(status[34]['percentual'])}</b>"
        ),
        (
            f"Status PABX 135 — Internal Congestion: "
            f"<b>{formatar_percentual(status[135]['percentual'])}</b>"
        ),
        "",
        (
            f"✅ Limites atuais: P34 ≤ {formatar_percentual(limite_p34)} | "
            f"P135 ≤ {formatar_percentual(limite_p135)}"
        ),
        (
            f"🛡 Normalização confirmada após "
            f"<b>{confirmacao_normalizacao_min} minutos contínuos</b> "
            "abaixo dos limites."
        ),
        f"⏱ <b>Tempo total do congestionamento: {duracao_txt}</b>",
        "",
        f"🟢 <b>Congestionamentos ativos restantes: {incidentes_ativos}</b>",
    ])


def montar_resumo_ativos(estado):
    agora = datetime.now()
    ativos = []

    for item in estado.get("filas", {}).values():
        if item.get("incidente_confirmado") is not True:
            continue
        ativos.append(item)

    ativos.sort(
        key=lambda x: (
            str(x.get("instancia", "")),
            str(x.get("projeto", "")),
            str(x.get("fila", "")),
        )
    )

    if not ativos:
        return "\n".join([
            "🟢 <b>PORTAL ORBE — CONGESTIONAMENTOS ATIVOS</b>",
            "",
            "Nenhum congestionamento ativo no momento.",
        ])

    linhas = [
        "🔴 <b>PORTAL ORBE — CONGESTIONAMENTOS ATIVOS</b>",
        "",
        f"<b>Atualizado:</b> {agora:%d/%m/%Y %H:%M:%S}",
        f"<b>Total:</b> {len(ativos)}",
        "",
    ]

    for idx, item in enumerate(ativos, start=1):
        duracao = minutos_decorridos(item.get("inicio_incidente"), agora=agora)
        linhas.extend([
            f"<b>{idx}. Instância {item.get('instancia')}</b>",
            f"Projeto: {item.get('projeto')}",
            f"Fila: {item.get('fila')} ({item.get('codigo')})",
            (
                f"P34: <b>{formatar_percentual(float(item.get('status34_percentual', 0.0)))}</b> | "
                f"P135: <b>{formatar_percentual(float(item.get('status135_percentual', 0.0)))}</b>"
            ),
            f"<b>Em alerta há: {formatar_duracao_incidente(item.get('inicio_incidente'), agora)}</b>",
            "",
        ])

    return "\n".join(linhas).strip()


def _datas_no_periodo(inicio, fim):
    data = inicio.date()
    while data <= fim.date():
        yield data
        data += timedelta(days=1)


def _formatar_minutos_gerencial(minutos):
    minutos = max(0, int(round(float(minutos))))
    horas, mins = divmod(minutos, 60)
    if horas and mins:
        return f"{horas}h {mins:02d}min"
    if horas:
        return f"{horas}h"
    return f"{mins} min"


def _periodo_comando_instancia(argumento, estado, agora=None):
    """Resolve /instanciaN [minutos|hoje]. Retorna inicio, fim e rótulo."""
    agora = agora or datetime.now()
    argumento = str(argumento or "").strip().lower()

    if not argumento:
        minutos = 60
        return agora - timedelta(minutes=minutos), agora, "últimos 60 minutos"

    if argumento == "hoje":
        assinatura = estado.get("metadados", {}).get("assinatura_config", {})
        hora_inicio_txt = str(assinatura.get("hora_inicio") or HORA_INICIO_PADRAO)
        hora_inicio = parse_hhmm(hora_inicio_txt, HORA_INICIO_PADRAO)
        inicio = datetime.combine(agora.date(), hora_inicio)
        if agora < inicio:
            inicio = datetime.combine(agora.date(), datetime.min.time())
        return inicio, agora, f"hoje, desde {inicio:%H:%M}"

    try:
        minutos = int(argumento)
    except ValueError:
        raise ValueError("Use um número de minutos entre 15 e 1440, ou 'hoje'.")

    if minutos < 15 or minutos > 1440:
        raise ValueError("O intervalo deve estar entre 15 e 1440 minutos.")

    return agora - timedelta(minutes=minutos), agora, f"últimos {minutos} minutos"


def _ler_historico_instancia_periodo(instancia, inicio, fim):
    """
    Analisa o JSONL do Portal Orbe para uma instância.

    Mantém a última amostra anterior ao início para distinguir corretamente
    um incidente já aberto de uma nova confirmação dentro do período.
    """
    instancia = str(instancia)
    needle = f'"instancia": "{instancia}"'
    anteriores = {}
    amostras = []

    for data in _datas_no_periodo(inicio, fim):
        caminho = PASTA_HISTORICO / f"portal_orbe_{data:%Y-%m-%d}.jsonl"
        if not caminho.exists():
            continue
        try:
            with caminho.open("r", encoding="utf-8") as f:
                for linha in f:
                    if needle not in linha:
                        continue
                    try:
                        item = json.loads(linha)
                        ts = datetime.fromisoformat(str(item.get("timestamp") or ""))
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
                    if str(item.get("instancia")) != instancia:
                        continue
                    key = f"{item.get('projeto_id')}|{item.get('codigo')}"
                    if ts < inicio:
                        anteriores[key] = item
                    elif ts <= fim:
                        item["_dt"] = ts
                        item["_key"] = key
                        amostras.append(item)
        except Exception as e:
            registrar_log_portal_orbe(
                "HISTORICO_INSTANCIA_FALHA",
                f"instancia={instancia}; arquivo={caminho}; erro={type(e).__name__}:{e}",
            )

    amostras.sort(key=lambda x: x["_dt"])
    return anteriores, amostras


def _eventos_autolimpeza_periodo(instancia, inicio, fim):
    caminho = caminho_auditoria_autolimpeza()
    eventos = []
    if not caminho.exists():
        return eventos

    try:
        with caminho.open("r", encoding="utf-8") as f:
            for linha in f:
                partes = [p.strip() for p in linha.strip().split(" | ")]
                if len(partes) < 3:
                    continue
                try:
                    dt = datetime.strptime(partes[0], "%d/%m/%Y %H:%M:%S")
                except ValueError:
                    continue
                if dt < inicio or dt > fim:
                    continue
                m_inst = re.search(r"Instância\s+(\d+)", partes[1], re.IGNORECASE)
                if not m_inst or m_inst.group(1) != str(instancia):
                    continue
                status = partes[2].upper()
                if status not in {"SUCESSO", "INCOMPLETA", "FALHA"}:
                    continue
                evento = {"data_hora": dt, "status": status, "antes": 0, "depois": 0, "liberados": 0}
                for parte in partes[3:]:
                    if "=" not in parte:
                        continue
                    chave, valor = parte.split("=", 1)
                    chave = chave.strip().lower()
                    if chave in {"antes", "depois", "liberados"}:
                        try:
                            evento[chave] = int(valor.strip())
                        except ValueError:
                            pass
                eventos.append(evento)
    except Exception as e:
        registrar_log_portal_orbe(
            "AUDITORIA_INSTANCIA_FALHA",
            f"instancia={instancia}; erro={type(e).__name__}:{e}",
        )
    return eventos


def analisar_instancia_periodo(instancia, inicio, fim, estado):
    anteriores, amostras = _ler_historico_instancia_periodo(instancia, inicio, fim)
    por_fila = {}
    ultimo = dict(anteriores)
    ocorrencias = []
    normalizacoes = []
    filas_monitoradas = set()
    filas_acima_34 = set()
    filas_acima_135 = set()

    # Incidentes que já estavam confirmados imediatamente antes do início
    # da janela analisada. Esse indicador é diferente de "incidentes ativos agora".
    ativos_inicio_janela = {
        key
        for key, item in anteriores.items()
        if bool(item.get("incidente_confirmado"))
    }

    pico34 = {"valor": 0.0, "fila": "-", "codigo": "-"}
    pico135 = {"valor": 0.0, "fila": "-", "codigo": "-"}

    def garantir(item):
        key = item["_key"]
        agg = por_fila.setdefault(key, {
            "key": key,
            "projeto": item.get("projeto", ""),
            "fila": item.get("fila", ""),
            "codigo": item.get("codigo", ""),
            "max34": 0.0,
            "max135": 0.0,
            "dur34_s": 0.0,
            "dur135_s": 0.0,
            "dur_any_s": 0.0,
            "max_ratio": 0.0,
            "last_dt": None,
            "last_acima34": False,
            "last_acima135": False,
            "last_acima_any": False,
        })
        return agg

    for item in amostras:
        key = item["_key"]
        dt = item["_dt"]
        filas_monitoradas.add(key)
        agg = garantir(item)

        s34 = item.get("status34") or {}
        s135 = item.get("status135") or {}
        p34 = float(s34.get("percentual") or 0.0)
        p135 = float(s135.get("percentual") or 0.0)
        lim34 = float(item.get("limite_p34") or 0.0)
        lim135 = float(item.get("limite_p135") or 0.0)
        acima34 = p34 > lim34 if lim34 >= 0 else False
        acima135 = p135 > lim135 if lim135 >= 0 else False
        acima_any = acima34 or acima135

        if acima34:
            filas_acima_34.add(key)
        if acima135:
            filas_acima_135.add(key)

        if p34 > agg["max34"]:
            agg["max34"] = p34
        if p135 > agg["max135"]:
            agg["max135"] = p135
        if lim34 > 0:
            agg["max_ratio"] = max(agg["max_ratio"], p34 / lim34)
        if lim135 > 0:
            agg["max_ratio"] = max(agg["max_ratio"], p135 / lim135)

        if p34 > pico34["valor"]:
            pico34 = {"valor": p34, "fila": item.get("fila", "-"), "codigo": item.get("codigo", "-")}
        if p135 > pico135["valor"]:
            pico135 = {"valor": p135, "fila": item.get("fila", "-"), "codigo": item.get("codigo", "-")}

        prev = ultimo.get(key)
        if prev is not None:
            try:
                prev_dt = prev.get("_dt") or datetime.fromisoformat(str(prev.get("timestamp") or ""))
            except (TypeError, ValueError):
                prev_dt = None
            if prev_dt is not None:
                delta = max(0.0, (dt - max(prev_dt, inicio)).total_seconds())
                delta = min(delta, 180.0)  # não transforma indisponibilidade longa em tempo de congestionamento
                ps34 = prev.get("status34") or {}
                ps135 = prev.get("status135") or {}
                pl34 = float(prev.get("limite_p34") or 0.0)
                pl135 = float(prev.get("limite_p135") or 0.0)
                prev34 = float(ps34.get("percentual") or 0.0) > pl34
                prev135 = float(ps135.get("percentual") or 0.0) > pl135
                if prev34:
                    agg["dur34_s"] += delta
                if prev135:
                    agg["dur135_s"] += delta
                if prev34 or prev135:
                    agg["dur_any_s"] += delta

            prev_confirmado = bool(prev.get("incidente_confirmado"))
            cur_confirmado = bool(item.get("incidente_confirmado"))
            if not prev_confirmado and cur_confirmado:
                ocorrencias.append({
                    "data_hora": dt,
                    "key": key,
                    "fila": item.get("fila", ""),
                    "codigo": item.get("codigo", ""),
                    "p34": p34,
                    "p135": p135,
                    "p34_acionou": acima34,
                    "p135_acionou": acima135,
                })
            elif prev_confirmado and not cur_confirmado and prev.get("normalizacao_inicio"):
                normalizacoes.append({
                    "data_hora": dt,
                    "key": key,
                    "fila": item.get("fila", ""),
                    "codigo": item.get("codigo", ""),
                })

        agg["last_dt"] = dt
        agg["last_acima34"] = acima34
        agg["last_acima135"] = acima135
        agg["last_acima_any"] = acima_any
        ultimo[key] = item

    # Completa no máximo 3 minutos após a última amostra, evitando superestimar lacunas.
    for agg in por_fila.values():
        if agg["last_dt"] is None:
            continue
        delta = min(max(0.0, (fim - agg["last_dt"]).total_seconds()), 180.0)
        if agg["last_acima34"]:
            agg["dur34_s"] += delta
        if agg["last_acima135"]:
            agg["dur135_s"] += delta
        if agg["last_acima_any"]:
            agg["dur_any_s"] += delta

    ativos = []
    observacao_agora = []
    for item in estado.get("filas", {}).values():
        if str(item.get("instancia")) != str(instancia):
            continue
        if item.get("incidente_confirmado") is True:
            ativos.append(dict(item))
        elif item.get("observacao_inicio"):
            observacao_agora.append(dict(item))

    ativos.sort(key=lambda x: minutos_decorridos(x.get("inicio_incidente"), agora=fim), reverse=True)

    # Reforço de consistência: se um incidente ativo agora informa início anterior
    # à janela, ele também é classificado como pré-existente, mesmo que não exista
    # amostra histórica imediatamente anterior disponível no JSONL.
    for item in ativos:
        try:
            dt_inicio = datetime.fromisoformat(str(item.get("inicio_incidente") or ""))
        except (TypeError, ValueError):
            continue
        if dt_inicio < inicio:
            ativos_inicio_janela.add(
                f"{item.get('projeto_id', '')}|{item.get('codigo', '')}"
            )

    eventos_auto = _eventos_autolimpeza_periodo(instancia, inicio, fim)
    canais_estado = estado.get("canais_travados", {}).get(str(instancia), {}) or {}
    recorrencias_periodo = []
    for rec in canais_estado.get("recorrencia_historico") or []:
        try:
            dt = datetime.fromisoformat(str(rec.get("data_hora") or ""))
        except (TypeError, ValueError):
            continue
        if inicio <= dt <= fim:
            recorrencias_periodo.append(rec)

    top = sorted(
        [x for x in por_fila.values() if x["dur_any_s"] > 0 or x["max_ratio"] > 1.0],
        key=lambda x: (x["dur_any_s"], x["max_ratio"], max(x["max34"], x["max135"])),
        reverse=True,
    )[:3]

    return {
        "amostras": len(amostras),
        "filas_monitoradas": len(filas_monitoradas),
        "ocorrencias": ocorrencias,
        "normalizacoes": normalizacoes,
        "ativos": ativos,
        "ativos_inicio_janela": len(ativos_inicio_janela),
        "ativos_inicio_janela_chaves": sorted(ativos_inicio_janela),
        "observacao_agora": observacao_agora,
        "pico34": pico34,
        "pico135": pico135,
        "filas_acima_34": len(filas_acima_34),
        "filas_acima_135": len(filas_acima_135),
        "ocorrencias_p34": sum(1 for x in ocorrencias if x["p34_acionou"]),
        "ocorrencias_p135": sum(1 for x in ocorrencias if x["p135_acionou"]),
        "maior_dur34": max(por_fila.values(), key=lambda x: x["dur34_s"], default=None),
        "maior_dur135": max(por_fila.values(), key=lambda x: x["dur135_s"], default=None),
        "por_fila": por_fila,
        "top": top,
        "eventos_auto": eventos_auto,
        "canais_estado": dict(canais_estado),
        "recorrencias_periodo": recorrencias_periodo,
    }


def _descricao_janela_gerencial(inicio, fim, rotulo_periodo):
    rotulo = str(rotulo_periodo or "").strip()
    if rotulo.lower().startswith("hoje"):
        return rotulo

    minutos = max(1, int(round((fim - inicio).total_seconds() / 60.0)))
    if minutos > 60 and minutos % 60 == 0:
        horas = minutos // 60
        return f"últimas {horas} horas"
    return f"últimos {minutos} minutos"


def _intervalo_exato_gerencial(inicio, fim):
    if inicio.date() == fim.date():
        return f"{inicio:%d/%m/%Y %H:%M} → {fim:%H:%M}"
    return f"{inicio:%d/%m/%Y %H:%M} → {fim:%d/%m/%Y %H:%M}"


def _nome_congestionamento(codigo):
    if int(codigo) == 34:
        return "Congestionamento na Companhia (P34)"
    return "Congestionamento Interno (P135)"


def _gatilho_gerencial(oc):
    nomes = []
    if oc.get("p34_acionou"):
        nomes.append(_nome_congestionamento(34))
    if oc.get("p135_acionou"):
        nomes.append(_nome_congestionamento(135))
    return " + ".join(nomes) or "Congestionamento P34/P135"


def montar_resumo_instancia_gerencial(instancia, inicio, fim, rotulo_periodo, estado):
    dados = analisar_instancia_periodo(instancia, inicio, fim, estado)
    confirmadas = len(dados["ocorrencias"])
    normalizadas = len(dados["normalizacoes"])
    ativos = dados["ativos"]
    preexistentes = int(dados.get("ativos_inicio_janela", 0) or 0)
    observacao = dados["observacao_agora"]
    acima_transitorio = int(dados["filas_acima_34"]) + int(dados["filas_acima_135"])

    assinatura = estado.get("metadados", {}).get("assinatura_config", {}) or {}
    limite_p34 = float(assinatura.get("limite_p34", LIMITE_P34_PADRAO))
    limite_p135 = float(assinatura.get("limite_p135", LIMITE_P135_PADRAO))
    confirmacao_min = int(assinatura.get("confirmacao_minutos", CONFIRMACAO_ALERTA_PADRAO_MIN))
    normalizacao_min = int(
        assinatura.get(
            "confirmacao_normalizacao_minutos",
            CONFIRMACAO_NORMALIZACAO_PADRAO_MIN,
        )
    )

    janela = _descricao_janela_gerencial(inicio, fim, rotulo_periodo)
    intervalo_exato = _intervalo_exato_gerencial(inicio, fim)

    if ativos and preexistentes and confirmadas:
        situacao = "🔴 Situação: novos eventos e incidente(s) persistente(s)"
    elif ativos and preexistentes:
        situacao = "🔴 Situação: incidente persistente ativo"
    elif ativos:
        situacao = "🔴 Situação: novo incidente ativo"
    elif observacao:
        situacao = "🟡 Situação: condição em observação"
    elif confirmadas:
        situacao = f"🟡 Situação: ocorrência registrada em {janela}"
    elif acima_transitorio:
        situacao = f"🟡 Situação: oscilações acima dos limites observadas em {janela}"
    else:
        situacao = "🟢 Situação: operação estável na janela analisada"

    linhas = [
        f"📊 <b>PORTAL ORBE — INSTÂNCIA {instancia}</b>",
        "",
        "🕐 <b>JANELA ANALISADA</b>",
        f"<b>{janela.capitalize()}</b>",
        f"Período: {intervalo_exato}",
        f"Atualizado às {fim:%H:%M}",
        "",
        "────────────────────",
        "📌 <b>VISÃO EXECUTIVA</b>",
        "",
        situacao,
        "",
        f"Filas monitoradas: <b>{dados['filas_monitoradas']}</b>",
        f"Novas ocorrências confirmadas em {janela}: <b>{confirmadas}</b>",
        f"Normalizações registradas em {janela}: <b>{normalizadas}</b>",
        f"Incidentes já ativos antes do início da janela ({janela}): <b>{preexistentes}</b>",
        f"Incidentes ativos agora: <b>{len(ativos)}</b>",
        "",
        "────────────────────",
        "⚙️ <b>CRITÉRIOS DE MONITORAMENTO</b>",
        "",
        "<b>Congestionamento na Companhia (P34)</b>",
        f"• Limite operacional: &gt; <b>{formatar_percentual(limite_p34)}</b>",
        f"• Confirmação de novo incidente: <b>{confirmacao_min} minutos contínuos</b>",
        "",
        "<b>Congestionamento Interno (P135)</b>",
        f"• Limite operacional: &gt; <b>{formatar_percentual(limite_p135)}</b>",
        f"• Confirmação de novo incidente: <b>{confirmacao_min} minutos contínuos</b>",
        "",
        f"Normalização: <b>{normalizacao_min} minutos contínuos</b> abaixo dos limites.",
    ]

    if ativos:
        linhas.extend(["", "────────────────────", "🔴 <b>INCIDENTES ATIVOS AGORA</b>", ""])
        for item in ativos[:5]:
            duracao_total = formatar_duracao_incidente(item.get("inicio_incidente"), fim)
            p34_atual = float(item.get("status34_percentual", 0.0) or 0.0)
            p135_atual = float(item.get("status135_percentual", 0.0) or 0.0)
            linhas.append(f"• <b>{item.get('fila')}</b> ({item.get('codigo')})")
            linhas.append(
                f"  Congestionamento na Companhia (P34): {formatar_percentual(p34_atual)}"
            )
            linhas.append(
                f"  Congestionamento Interno (P135): {formatar_percentual(p135_atual)}"
            )
            linhas.append(f"  Início do incidente: {formatar_inicio_incidente(item.get('inicio_incidente'))}")
            linhas.append(f"  Duração total contínua: <b>{duracao_total}</b>")

            try:
                dt_inicio = datetime.fromisoformat(str(item.get("inicio_incidente") or ""))
            except (TypeError, ValueError):
                dt_inicio = None
            if dt_inicio and dt_inicio < inicio:
                linhas.append(f"  Situação na janela: já estava ativo antes de {janela} e permanece ativo agora.")
            else:
                linhas.append(f"  Situação na janela: confirmado durante {janela} e permanece ativo agora.")
        if len(ativos) > 5:
            linhas.append(f"• + {len(ativos) - 5} incidente(s) ativo(s)")

    if dados["ocorrencias"]:
        linhas.extend(["", "────────────────────", f"📋 <b>NOVAS OCORRÊNCIAS — {janela.upper()}</b>", ""])
        estado_filas = estado.get("filas", {})
        for oc in dados["ocorrencias"][-5:]:
            chave_estado = f"inst{instancia}|{oc['key']}"
            atual_oc = estado_filas.get(chave_estado, {})
            status_oc = "🔴 Ativo agora" if atual_oc.get("incidente_confirmado") is True else "✅ Não ativo agora"
            linhas.append(
                f"• {oc['data_hora']:%H:%M} • <b>{oc['fila']}</b> ({oc['codigo']})"
            )
            linhas.append(f"  {_gatilho_gerencial(oc)} • {status_oc}")
        if len(dados["ocorrencias"]) > 5:
            linhas.append(f"• + {len(dados['ocorrencias']) - 5} nova(s) ocorrência(s) em {janela}")

    linhas.extend(["", "────────────────────", "📈 <b>CONGESTIONAMENTOS</b>", ""])

    maior34 = dados["maior_dur34"]
    linhas.append("<b>P34 — Congestionamento na Companhia</b>")
    linhas.append(f"• Limite operacional considerado: &gt; {formatar_percentual(limite_p34)}")
    linhas.append(f"• Novas ocorrências confirmadas em {janela}: {dados['ocorrencias_p34']}")
    linhas.append(
        f"• Maior percentual observado em {janela}: "
        f"{formatar_percentual(dados['pico34']['valor'])} — "
        f"{dados['pico34']['fila']} ({dados['pico34']['codigo']})"
    )
    linhas.append(
        f"• Filas com pelo menos uma amostra acima do limite em {janela}: "
        f"{dados['filas_acima_34']}"
    )
    if maior34 and maior34["dur34_s"] > 0:
        linhas.append(
            f"• Maior permanência estimada acima do limite dentro de {janela}: "
            f"{_formatar_minutos_gerencial(maior34['dur34_s'] / 60)} — {maior34['fila']}"
        )

    maior135 = dados["maior_dur135"]
    linhas.extend(["", "<b>P135 — Congestionamento Interno</b>"])
    linhas.append(f"• Limite operacional considerado: &gt; {formatar_percentual(limite_p135)}")
    linhas.append(f"• Novas ocorrências confirmadas em {janela}: {dados['ocorrencias_p135']}")
    linhas.append(
        f"• Maior percentual observado em {janela}: "
        f"{formatar_percentual(dados['pico135']['valor'])} — "
        f"{dados['pico135']['fila']} ({dados['pico135']['codigo']})"
    )
    linhas.append(
        f"• Filas com pelo menos uma amostra acima do limite em {janela}: "
        f"{dados['filas_acima_135']}"
    )
    if maior135 and maior135["dur135_s"] > 0:
        linhas.append(
            f"• Maior permanência estimada acima do limite dentro de {janela}: "
            f"{_formatar_minutos_gerencial(maior135['dur135_s'] / 60)} — {maior135['fila']}"
        )

    if dados["top"]:
        linhas.extend(["", "────────────────────", f"🎯 <b>FILAS DE MAIOR IMPACTO — {janela.upper()}</b>", ""])
        estado_filas = estado.get("filas", {})
        for idx, item in enumerate(dados["top"], start=1):
            chave_estado = f"inst{instancia}|{item['key']}"
            atual = estado_filas.get(chave_estado, {})
            if atual.get("incidente_confirmado") is True:
                status_txt = "🔴 Ativo"
            elif atual.get("observacao_inicio"):
                status_txt = "🟡 Em observação"
            else:
                status_txt = "✅ Sem incidente ativo"
            linhas.append(f"{idx}. <b>{item['fila']}</b> ({item['codigo']})")
            linhas.append(
                f"   • P34 máx em {janela}: {formatar_percentual(item['max34'])} | "
                f"P135 máx em {janela}: {formatar_percentual(item['max135'])}"
            )
            linhas.append(
                f"   • Permanência estimada acima de algum limite dentro de {janela}: "
                f"{_formatar_minutos_gerencial(item['dur_any_s'] / 60)}"
            )
            linhas.append(f"   • Status atual: {status_txt}")

    eventos_auto = dados["eventos_auto"]
    canais = dados["canais_estado"]
    qtd_atual = int(canais.get("quantidade", 0) or 0)
    liberados = sum(int(x.get("liberados", 0) or 0) for x in eventos_auto)
    maior_tratado = max((int(x.get("antes", 0) or 0) for x in eventos_auto), default=0)
    linhas.extend(["", "────────────────────", "🧹 <b>CANAIS TRAVADOS</b>", ""])
    linhas.append(f"Canais travados agora: <b>{qtd_atual}</b>")
    linhas.append(f"Autolimpezas concluídas em {janela}: <b>{len(eventos_auto)}</b>")
    linhas.append(f"Canais liberados em {janela}: <b>{liberados}</b>")
    if eventos_auto:
        linhas.append(f"Maior volume tratado em uma autolimpeza de {janela}: <b>{maior_tratado}</b> canais")
    limite_rec = int(os.getenv("ORBE_CANAIS_TRAVADOS_RECORRENCIA_LIMITE", "200"))
    linhas.append(
        f"Recorrências ≥ {limite_rec} canais registradas em {janela}: "
        f"<b>{len(dados['recorrencias_periodo'])}</b>"
    )

    linhas.extend(["", "────────────────────", "🎯 <b>LEITURA GERENCIAL</b>", ""])

    if ativos:
        if confirmadas == 0:
            linhas.append(
                f"A Instância {instancia} não apresentou novos congestionamentos confirmados em {janela}."
            )
        else:
            linhas.append(
                f"A Instância {instancia} apresentou <b>{confirmadas}</b> nova(s) ocorrência(s) confirmada(s) em {janela}."
            )

        if preexistentes:
            linhas.append("")
            linhas.append(
                f"Entretanto, <b>{preexistentes}</b> incidente(s) iniciado(s) antes da janela analisada permanece(m) ativo(s)."
            )

        # Leitura individual dos incidentes ativos, limitada para não tornar a mensagem excessiva.
        for item in ativos[:3]:
            p34_atual = float(item.get("status34_percentual", 0.0) or 0.0)
            p135_atual = float(item.get("status135_percentual", 0.0) or 0.0)
            tipos = []
            if p34_atual > limite_p34:
                tipos.append(f"Congestionamento na Companhia (P34), limite &gt; {formatar_percentual(limite_p34)}")
            if p135_atual > limite_p135:
                tipos.append(f"Congestionamento Interno (P135), limite &gt; {formatar_percentual(limite_p135)}")
            tipo_txt = " e ".join(tipos) if tipos else "condição de congestionamento ainda confirmada"
            duracao_total = formatar_duracao_incidente(item.get("inicio_incidente"), fim)
            linhas.append("")
            linhas.append(
                f"A fila <b>{item.get('fila')}</b> apresenta {tipo_txt} e acumula "
                f"<b>{duracao_total}</b> de duração contínua."
            )

            key = f"{item.get('projeto_id', '')}|{item.get('codigo', '')}"
            agg = dados.get("por_fila", {}).get(key)
            if agg:
                trechos = []
                if p34_atual > limite_p34 and agg.get("dur34_s", 0) > 0:
                    trechos.append(
                        f"{_formatar_minutos_gerencial(agg['dur34_s'] / 60)} acima do limite de "
                        f"Congestionamento na Companhia (P34)"
                    )
                if p135_atual > limite_p135 and agg.get("dur135_s", 0) > 0:
                    trechos.append(
                        f"{_formatar_minutos_gerencial(agg['dur135_s'] / 60)} acima do limite de "
                        f"Congestionamento Interno (P135)"
                    )
                if trechos:
                    linhas.append(
                        f"Dentro de {janela}, essa fila permaneceu aproximadamente " + " e ".join(trechos) + "."
                    )

        if preexistentes:
            linhas.append("")
            linhas.append(
                "O cenário caracteriza persistência de incidente(s) existente(s), "
                "e não apenas abertura de nova ocorrência."
            )
        else:
            linhas.append("")
            linhas.append("Há incidente(s) novo(s) ainda ativo(s) que requer(em) acompanhamento.")

    elif confirmadas:
        linhas.append(
            f"A Instância {instancia} registrou <b>{confirmadas}</b> nova(s) ocorrência(s) confirmada(s) em {janela}."
        )
        linhas.append(
            f"Nenhuma dessas ocorrências permanece ativa no momento da consulta; "
            f"foram registradas <b>{normalizadas}</b> normalização(ões) em {janela}."
        )
    elif observacao:
        linhas.append(
            f"A Instância {instancia} não possui incidente confirmado ativo, mas há "
            f"<b>{len(observacao)}</b> condição(ões) em observação neste momento."
        )
        linhas.append(
            f"A confirmação de novo incidente exige {confirmacao_min} minutos contínuos acima do limite operacional."
        )
    elif acima_transitorio:
        linhas.append(
            f"A Instância {instancia} não apresentou novos congestionamentos confirmados em {janela}."
        )
        linhas.append(
            f"Foram observadas amostras transitórias acima dos limites em "
            f"{dados['filas_acima_34']} fila(s) para P34 e {dados['filas_acima_135']} fila(s) para P135."
        )
        linhas.append(
            f"Nenhuma dessas condições completou os {confirmacao_min} minutos contínuos necessários para confirmação de novo incidente."
        )
    else:
        linhas.append(
            f"A Instância {instancia} permaneceu sem novas ocorrências confirmadas e sem amostras acima dos limites durante {janela}."
        )
        linhas.append("Não há incidente de congestionamento ativo neste momento.")

    contagem_rec = int(canais.get("recorrencia_contagem", 0) or 0)
    escala = int(os.getenv("ORBE_CANAIS_TRAVADOS_RECORRENCIA_ESCALONAMENTO", "4"))
    linhas.append("")
    if contagem_rec >= escala:
        linhas.append("🚨 A recorrência persistente de canais travados atingiu o critério de escalonamento.")
    elif dados["recorrencias_periodo"]:
        linhas.append(
            f"Canais travados registraram {len(dados['recorrencias_periodo'])} recorrência(s) relevante(s) em {janela}."
        )
    else:
        linhas.append(f"Nenhuma condição de escalonamento de canais travados foi identificada em {janela}.")

    linhas.extend([
        "",
        f"ℹ️ Tempos de permanência dentro de {janela} são estimados a partir das amostras gravadas pelo monitor.",
        "A duração total de incidentes ativos é calculada a partir do horário original de início do incidente.",
    ])
    return "\n".join(linhas).strip()



def _carregar_status_ha_sbc_contrato():
    """Lê exclusivamente o snapshot HA oficial e valida seu contrato mínimo."""
    caminho = CAMINHO_STATUS_HA_SBC
    if not caminho.exists():
        raise FileNotFoundError(
            f"snapshot HA oficial não encontrado: {caminho}"
        )

    with caminho.open("r", encoding="utf-8") as arquivo:
        dados = json.load(arquivo)

    if not isinstance(dados, dict):
        raise ValueError("raiz do snapshot HA deve ser um objeto JSON")

    atualizado_em = dados.get("atualizado_em")
    if not isinstance(atualizado_em, str) or not atualizado_em.strip():
        raise ValueError("campo obrigatório 'atualizado_em' ausente ou inválido")
    try:
        datetime.fromisoformat(atualizado_em)
    except ValueError as erro:
        raise ValueError("campo 'atualizado_em' não está em formato ISO válido") from erro

    pares = dados.get("pares")
    if not isinstance(pares, dict) or not pares:
        raise ValueError("campo obrigatório 'pares' deve conter ao menos um ambiente HA")

    integridades_validas = {
        "NORMAL",
        "DEGRADADA",
        "INDISPONIVEL",
        "DESCONHECIDA",
    }
    for nome_par, item in pares.items():
        if not isinstance(nome_par, str) or not nome_par.strip():
            raise ValueError("nome de ambiente HA inválido")
        if not isinstance(item, dict):
            raise ValueError(f"ambiente HA '{nome_par}' deve ser um objeto")

        integridade = str(item.get("integridade", "")).upper().strip()
        if integridade not in integridades_validas:
            raise ValueError(
                f"integridade inválida em '{nome_par}': {integridade or '<vazia>'}"
            )

        membros = item.get("membros")
        if not isinstance(membros, list) or not membros:
            raise ValueError(f"ambiente HA '{nome_par}' não possui membros válidos")

        for indice, membro in enumerate(membros, start=1):
            if not isinstance(membro, dict):
                raise ValueError(
                    f"membro {indice} de '{nome_par}' deve ser um objeto"
                )
            ip = membro.get("ip")
            if not isinstance(ip, str) or not ip.strip():
                raise ValueError(
                    f"membro {indice} de '{nome_par}' sem campo 'ip' válido"
                )
            if not isinstance(membro.get("respondendo_snmp"), bool):
                raise ValueError(
                    f"membro {ip} de '{nome_par}' sem booleano 'respondendo_snmp'"
                )
            ha = membro.get("ha")
            if not isinstance(ha, str) or not ha.strip():
                raise ValueError(
                    f"membro {ip} de '{nome_par}' sem campo 'ha' válido"
                )

    return dados

def _fmt_data_ha(valor):
    if not valor:
        return "-"
    try:
        return datetime.fromisoformat(
            valor
        ).strftime("%d/%m/%Y %H:%M:%S")
    except (TypeError, ValueError):
        return str(valor)


def montar_resumo_sbc_gerencial():
    """
    Lê o snapshot produzido pelo sbc_monitor.py.
    O comando não dispara uma nova coleta SNMP; mostra o último ciclo
    consolidado do monitor integrado.
    """
    try:
        dados = _carregar_status_ha_sbc_contrato()
    except FileNotFoundError:
        return (
            "⚠️ <b>TELECOM MONITOR — INTEGRIDADE DOS SBCs</b>\n\n"
            "Leitura consolidada de HA indisponível no caminho oficial.\n"
            f"Arquivo esperado: <code>{CAMINHO_STATUS_HA_SBC}</code>\n"
            "Nenhum caminho alternativo será usado."
        )
    except (OSError, json.JSONDecodeError, ValueError) as erro:
        return (
            "⚠️ <b>TELECOM MONITOR — INTEGRIDADE DOS SBCs</b>\n\n"
            "O snapshot HA oficial existe, mas não passou na validação de contrato.\n"
            f"Motivo: {erro}\n"
            "Nenhum fallback será usado."
        )

    pares = dados.get("pares", {}) or {}
    atualizado_em = dados.get("atualizado_em")

    total = len(pares)
    normais = 0
    degradados = 0
    indisponiveis = 0
    health_atencao = 0

    for item in pares.values():
        integridade = str(
            item.get("integridade", "DESCONHECIDA")
        ).upper()
        if integridade == "NORMAL":
            normais += 1
        elif integridade == "DEGRADADA":
            degradados += 1
        else:
            indisponiveis += 1

        for membro in item.get("membros", []):
            health = membro.get("health")
            if (
                membro.get("respondendo_snmp")
                and health is not None
                and int(health) < 100
            ):
                health_atencao += 1

    if indisponiveis:
        geral_emoji = "⚫"
        geral_texto = (
            f"HA indisponível em {indisponiveis} ambiente(s)."
        )
    elif degradados:
        geral_emoji = "🟠"
        geral_texto = (
            f"Redundância degradada em {degradados} ambiente(s)."
        )
    elif health_atencao:
        geral_emoji = "🟡"
        geral_texto = (
            "Redundância preservada, com Health abaixo de 100% "
            f"em {health_atencao} SBC(s)."
        )
    else:
        geral_emoji = "🟢"
        geral_texto = (
            f"Redundância operacional nos {total} ambientes."
        )

    linhas = [
        "📡 <b>TELECOM MONITOR — INTEGRIDADE DOS SBCs</b>",
        "",
        f"🕐 Atualizado em: <b>{_fmt_data_ha(atualizado_em)}</b>",
        "",
        f"{geral_emoji} <b>SITUAÇÃO GERAL</b>",
        geral_texto,
    ]

    for nome_par, item in pares.items():
        integridade = str(
            item.get("integridade", "DESCONHECIDA")
        ).upper()

        if integridade == "NORMAL":
            emoji = "🟢"
            descricao = "Normal"
        elif integridade == "DEGRADADA":
            emoji = "🟠"
            descricao = "Redundância degradada"
        else:
            emoji = "⚫"
            descricao = "HA indisponível"

        linhas.extend([
            "",
            "────────────────────",
            f"🏢 <b>{nome_par}</b>",
            "",
            f"{emoji} <b>Integridade HA:</b> {descricao}",
        ])

        membros = item.get("membros", []) or []
        ordem = {
            "ACTIVE": 0,
            "STANDBY": 1,
            "SEM RESPOSTA": 2,
        }

        membros = sorted(
            membros,
            key=lambda x: (
                ordem.get(
                    str(x.get("ha", "")).upper(),
                    9,
                ),
                str(x.get("ip", "")),
            ),
        )

        for membro in membros:
            ha = str(
                membro.get("ha") or "DESCONHECIDO"
            )
            respondendo = bool(
                membro.get("respondendo_snmp")
            )
            ip = str(
                membro.get("ip") or "-"
            )
            hostname = (
                membro.get("hostname")
                or "-"
            )

            linhas.extend([
                "",
                f"<b>{ha}</b>",
                f"• {ip} — {hostname}",
            ])

            if not respondendo:
                linhas.append(
                    "• SNMP: sem resposta"
                )
                continue

            health = membro.get("health")
            health_emoji = (
                "✅"
                if health is not None
                and int(health) == 100
                else "⚠️"
            )

            linhas.append(
                f"• Health: {health}% {health_emoji}"
            )
            linhas.append(
                "• CPU: "
                f"{membro.get('cpu')}% | "
                "Memória: "
                f"{membro.get('memoria')}%"
            )
            linhas.append(
                "• SIP Sessions: "
                f"{membro.get('sip_sessions_ativas')} | "
                "CPS: "
                f"{membro.get('cps')}"
            )

        ultimo = item.get(
            "ultimo_switchover"
        )
        if isinstance(ultimo, dict):
            anterior = (
                ultimo.get("active_anterior")
                or {}
            )
            atual = (
                ultimo.get("active_atual")
                or {}
            )

            linhas.extend([
                "",
                "🔄 <b>ÚLTIMA ALTERAÇÃO HA</b>",
                "Switchover detectado em: "
                f"<b>{_fmt_data_ha(ultimo.get('detectado_em'))}</b>",
                "ACTIVE anterior: "
                f"{anterior.get('hostname') or anterior.get('ip') or '-'}",
                "ACTIVE atual: "
                f"{atual.get('hostname') or atual.get('ip') or '-'}",
            ])

        inicio_deg = item.get(
            "inicio_degradacao"
        )
        if integridade != "NORMAL" and inicio_deg:
            linhas.extend([
                "",
                "⏱ Condição degradada desde: "
                f"<b>{_fmt_data_ha(inicio_deg)}</b>",
            ])

    linhas.extend([
        "",
        "────────────────────",
        "🎯 <b>LEITURA GERENCIAL</b>",
        "",
    ])

    if indisponiveis:
        linhas.append(
            "Há ambiente sem SBC ACTIVE confirmado. "
            "A condição requer verificação prioritária."
        )
    elif degradados:
        linhas.append(
            "Há ambiente operando com o SBC ACTIVE disponível, "
            "porém sem a redundância HA esperada."
        )
    elif health_atencao:
        linhas.append(
            "Os pares HA estão estruturalmente normais, "
            "mas existe SBC com Health abaixo de 100%."
        )
    else:
        linhas.append(
            "Os pares SBC encontram-se com redundância "
            "preservada e Health normal."
        )
        linhas.append(
            "Nenhuma condição de degradação HA identificada."
        )

    return "\n".join(linhas)



class TelegramCommandListener(threading.Thread):
    """
    Receptor resiliente de comandos do Telegram via getUpdates.

    Comandos:
      /travados  -> visão gerencial consolidada das instâncias
      /ativos    -> lista congestionamentos confirmados ainda ativos
      /sbc       -> integridade HA e Health dos SBCs
      /travadosN -> detalhe gerencial de canais travados da instância N
      /instanciaN -> resumo histórico/gerencial da instância N
      /ajuda     -> explica os comandos disponíveis
      /ping      -> teste rápido do receptor

    Estratégia:
      - não depende de getMe no startup;
      - não desabilita o receptor por uma falha inicial;
      - tenta remover webhook, mas segue tentando mesmo se der timeout;
      - reconecta automaticamente após timeout/erro;
      - preserva offset para não reprocessar updates.
    """

    def __init__(
        self,
        token,
        chat_id,
        estado,
        lock_estado,
        stop_event,
        consultar_travados=None,
        travados_instancias=None,
        travados_limite_alerta=40,
        travados_limite_critico=100,
        auto_limpeza_enabled=True,
        auto_limpeza_limite=40,
        auto_limpeza_cooldown_min=10,
        recorrencia_limite=200,
        recorrencia_janela_min=60,
        recorrencia_escalonamento=4,
    ):
        super().__init__(daemon=True)
        self.token = token.strip()
        self.chat_id = str(chat_id).strip()
        self.estado = estado
        self.lock_estado = lock_estado
        self.stop_event = stop_event
        self.offset = None
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.conectado = False
        self.ultima_falha = None
        self.consultar_travados = consultar_travados
        self.travados_instancias = {
            str(x) for x in (travados_instancias or [])
        }
        self.travados_limite_alerta = int(travados_limite_alerta)
        self.travados_limite_critico = int(travados_limite_critico)
        self.auto_limpeza_enabled = bool(auto_limpeza_enabled)
        self.auto_limpeza_limite = int(auto_limpeza_limite)
        self.auto_limpeza_cooldown_min = int(auto_limpeza_cooldown_min)
        self.recorrencia_limite = int(recorrencia_limite)
        self.recorrencia_janela_min = int(recorrencia_janela_min)
        self.recorrencia_escalonamento = int(recorrencia_escalonamento)

    def _get(self, metodo, params=None, timeout=35):
        r = requests.get(
            f"{self.base_url}/{metodo}",
            params=params or {},
            timeout=timeout,
        )
        r.raise_for_status()
        payload = r.json()
        if not payload.get("ok", False):
            raise RuntimeError(
                f"Telegram {metodo} retornou ok=false: {payload}"
            )
        return payload

    def _post(self, metodo, data=None, timeout=20):
        r = requests.post(
            f"{self.base_url}/{metodo}",
            data=data or {},
            timeout=timeout,
        )
        r.raise_for_status()
        payload = r.json()
        if not payload.get("ok", False):
            raise RuntimeError(
                f"Telegram {metodo} retornou ok=false: {payload}"
            )
        return payload

    def registrar_comandos(self):
        """
        Registra o menu nativo de comandos do Telegram.

        A lista de /travadosN é criada dinamicamente a partir das
        instâncias configuradas no monitor. Assim, uma alteração futura
        das instâncias não exige manutenção manual do menu.
        """
        comandos = [
            {
                "command": "travados",
                "description": "Visão gerencial dos canais travados",
            },
            {
                "command": "ativos",
                "description": "Congestionamentos ativos no Portal Orbe",
            },
            {
                "command": "sbc",
                "description": "Integridade HA e Health dos SBCs",
            },
        ]

        instancias_ordenadas = sorted(
            self.travados_instancias,
            key=lambda x: int(x) if str(x).isdigit() else str(x),
        )
        for instancia in instancias_ordenadas:
            comandos.append(
                {
                    "command": f"instancia{instancia}",
                    "description": f"Resumo gerencial — Instância {instancia}",
                }
            )
            comandos.append(
                {
                    "command": f"travados{instancia}",
                    "description": f"Canais travados — Instância {instancia}",
                }
            )

        comandos.extend(
            [
                {
                    "command": "ajuda",
                    "description": "Explicar os comandos disponíveis",
                },
                {
                    "command": "ping",
                    "description": "Verificar se o bot está online",
                },
            ]
        )

        try:
            self._post(
                "setMyCommands",
                data={
                    "commands": json.dumps(
                        comandos,
                        ensure_ascii=False,
                    )
                },
                timeout=20,
            )
            print(
                "✅ Telegram: menu de comandos registrado: "
                + ", ".join("/" + item["command"] for item in comandos)
            )
            return True
        except Exception as e:
            # Falha no menu não interrompe o receptor de comandos.
            print(
                "⚠ Telegram: não foi possível registrar o menu de comandos; "
                f"o bot continuará operando normalmente. Motivo: {e}"
            )
            return False

    def montar_ajuda(self):
        """Monta uma ajuda curta, coerente com as instâncias configuradas."""
        linhas = [
            "ℹ️ <b>PORTAL ORBE — COMANDOS DISPONÍVEIS</b>",
            "",
            "<b>/travados</b>",
            "Visão gerencial consolidada dos canais travados.",
            "",
            "<b>/ativos</b>",
            "Congestionamentos confirmados ainda ativos no Portal Orbe.",
            "",
            "<b>/sbc</b>",
            "Integridade HA, Health e métricas atuais dos SBCs.",
            "",
            "<b>Detalhe por instância</b>",
        ]

        for instancia in sorted(
            self.travados_instancias,
            key=lambda x: int(x) if str(x).isdigit() else str(x),
        ):
            linhas.append(
                f"/instancia{instancia} — resumo gerencial dos últimos 60 min"
            )
        linhas.extend([
            "",
            "Use <b>/instanciaN 30</b>, <b>/instanciaN 120</b> ou <b>/instanciaN hoje</b> para alterar o período.",
            "",
            "<b>Canais travados por instância</b>",
        ])
        for instancia in sorted(
            self.travados_instancias,
            key=lambda x: int(x) if str(x).isdigit() else str(x),
        ):
            linhas.append(
                f"/travados{instancia} — canais travados da Instância {instancia}"
            )

        linhas.extend(
            [
                "",
                "<b>/ping</b>",
                "Verifica se o receptor de comandos está online.",
                "",
                "💡 Digite <b>/</b> no Telegram para abrir o menu de comandos.",
            ]
        )
        return "\n".join(linhas)

    def tentar_remover_webhook(self):
        """
        getUpdates não funciona quando existe webhook configurado.
        A remoção é tentada em segundo plano. Timeout aqui NÃO desabilita
        o receptor; ele continuará tentando getUpdates e reconectando.
        """
        try:
            self._post(
                "deleteWebhook",
                data={"drop_pending_updates": "false"},
                timeout=20,
            )
            print("ℹ Telegram: webhook removido/verificado para uso de getUpdates.")
            return True
        except Exception as e:
            print(
                "⚠ Telegram: não foi possível remover/verificar webhook agora; "
                f"o receptor continuará tentando. Motivo: {e}"
            )
            return False

    def enviar(self, texto):
        try:
            self._post(
                "sendMessage",
                data={
                    "chat_id": self.chat_id,
                    "text": texto,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true",
                },
                timeout=20,
            )
        except Exception as e:
            print(f"⚠ Falha ao responder comando Telegram: {e}")

    def processar_update(self, update):
        mensagem = (
            update.get("message")
            or update.get("channel_post")
            or update.get("edited_message")
            or update.get("edited_channel_post")
        )
        if not isinstance(mensagem, dict):
            return

        chat = mensagem.get("chat") or {}
        chat_id_recebido = str(chat.get("id", "")).strip()

        texto = str(mensagem.get("text") or "").strip()
        if not texto:
            return

        primeiro_token = texto.split()[0]
        comando = primeiro_token.split("@")[0].lower()

        comando_travados = re.fullmatch(r"/travados(\d+)", comando)
        comando_instancia = re.fullmatch(r"/instancia(\d+)", comando)
        if (
            comando not in {"/ativos", "/sbc", "/ping", "/travados", "/ajuda"}
            and not comando_travados
            and not comando_instancia
        ):
            return

        print(
            f"📩 Telegram: comando {comando} recebido "
            f"do chat {chat_id_recebido}."
        )

        if chat_id_recebido != self.chat_id:
            print(
                "⚠ Telegram: comando ignorado porque o chat_id recebido "
                f"({chat_id_recebido}) é diferente do TELEGRAM_CHAT_ID "
                f"configurado ({self.chat_id})."
            )
            return

        if comando == "/ping":
            self.enviar(
                "✅ <b>PORTAL ORBE — BOT ONLINE</b>\n\n"
                "Receptor de comandos funcionando."
            )
            return

        if comando == "/sbc":
            self.enviar(
                montar_resumo_sbc_gerencial()
            )
            return

        if comando == "/ajuda":
            self.enviar(self.montar_ajuda())
            return

        if comando_instancia:
            instancia = comando_instancia.group(1)
            if instancia not in self.travados_instancias:
                self.enviar(
                    "⚠️ <b>PORTAL ORBE — INSTÂNCIA</b>\n\n"
                    f"Instância {instancia} não está configurada no monitor."
                )
                return
            argumentos = texto.split()[1:]
            argumento = argumentos[0] if argumentos else ""
            try:
                inicio, fim, rotulo = _periodo_comando_instancia(
                    argumento, self.estado, agora=datetime.now()
                )
                with self.lock_estado:
                    resposta = montar_resumo_instancia_gerencial(
                        instancia=instancia,
                        inicio=inicio,
                        fim=fim,
                        rotulo_periodo=rotulo,
                        estado=self.estado,
                    )
                self.enviar(resposta)
            except ValueError as e:
                self.enviar(
                    "ℹ️ <b>PORTAL ORBE — USO DO COMANDO</b>\n\n"
                    f"{e}\n\n"
                    f"Exemplos:\n/instancia{instancia}\n"
                    f"/instancia{instancia} 30\n"
                    f"/instancia{instancia} 120\n"
                    f"/instancia{instancia} hoje"
                )
            except Exception as e:
                registrar_log_portal_orbe(
                    "COMANDO_INSTANCIA_FALHA",
                    f"instancia={instancia}; erro={type(e).__name__}:{e}",
                )
                self.enviar(
                    "⚠️ <b>PORTAL ORBE — FALHA NO RESUMO DA INSTÂNCIA</b>\n\n"
                    f"Instância: {instancia}\nMotivo: {e}"
                )
            return

        if comando == "/travados":
            if self.consultar_travados is None:
                self.enviar(
                    "⚠️ <b>PORTAL ORBE — CANAIS TRAVADOS</b>\n\n"
                    "Consulta sob demanda indisponível nesta execução."
                )
                return

            resultados = {}
            for instancia in sorted(
                self.travados_instancias,
                key=lambda x: int(x) if str(x).isdigit() else str(x),
            ):
                try:
                    resultado = self.consultar_travados(instancia)
                    quantidade = int(resultado["quantidade"])
                    resultados[instancia] = {"quantidade": quantidade}
                    with self.lock_estado:
                        atual = self.estado.setdefault("canais_travados", {}).setdefault(
                            str(instancia), {"instancia": str(instancia)}
                        )
                        atual["quantidade"] = quantidade
                        atual["ultima_consulta"] = datetime.now().isoformat(timespec="seconds")
                        atualizar_recorrencia_gerencial(
                            self.estado, instancia, quantidade,
                            self.recorrencia_limite, self.recorrencia_janela_min,
                        )
                        salvar_estado(self.estado)
                except Exception as e:
                    resultados[instancia] = {"erro": str(e)}

            with self.lock_estado:
                resposta = montar_resumo_travados_gerencial(
                    resultados=resultados,
                    estado=self.estado,
                    auto_limpeza_enabled=self.auto_limpeza_enabled,
                    auto_limpeza_limite=self.auto_limpeza_limite,
                    auto_limpeza_cooldown_min=self.auto_limpeza_cooldown_min,
                    limite_recorrencia=self.recorrencia_limite,
                    janela_recorrencia_min=self.recorrencia_janela_min,
                    recorrencias_escalonamento=self.recorrencia_escalonamento,
                )
            self.enviar(resposta)
            return

        if comando_travados:
            instancia = comando_travados.group(1)

            if instancia not in self.travados_instancias:
                self.enviar(
                    "⚠️ <b>PORTAL ORBE — CANAIS TRAVADOS</b>\n\n"
                    f"Instância {instancia} ainda não está configurada "
                    "para consulta de canais travados."
                )
                return

            if self.consultar_travados is None:
                self.enviar(
                    "⚠️ <b>PORTAL ORBE — CANAIS TRAVADOS</b>\n\n"
                    "Consulta sob demanda indisponível nesta execução."
                )
                return

            try:
                resultado = self.consultar_travados(instancia)
                quantidade = int(resultado["quantidade"])
                with self.lock_estado:
                    atual = self.estado.setdefault("canais_travados", {}).setdefault(
                        str(instancia), {"instancia": str(instancia)}
                    )
                    atual["quantidade"] = quantidade
                    atual["ultima_consulta"] = datetime.now().isoformat(timespec="seconds")
                    atualizar_recorrencia_gerencial(
                        self.estado, instancia, quantidade,
                        self.recorrencia_limite, self.recorrencia_janela_min,
                    )
                    salvar_estado(self.estado)
                    canais_estado_atual = self.estado.setdefault("canais_travados", {})
                    estado_instancia = dict(canais_estado_atual.get(str(instancia), {}))
                    existe_limpeza_em_andamento = any(
                        str(v.get("ultima_limpeza_resultado") or "").upper() == "EM_EXECUCAO"
                        for k, v in canais_estado_atual.items()
                        if str(k) != str(instancia) and isinstance(v, dict)
                    )

                resposta = montar_resposta_travados(
                    instancia=instancia,
                    quantidade=quantidade,
                    limite_alerta=self.travados_limite_alerta,
                    limite_critico=self.travados_limite_critico,
                    estado_instancia=estado_instancia,
                    auto_limpeza_enabled=self.auto_limpeza_enabled,
                    auto_limpeza_limite=self.auto_limpeza_limite,
                    auto_limpeza_cooldown_min=self.auto_limpeza_cooldown_min,
                    limite_recorrencia=self.recorrencia_limite,
                    janela_recorrencia_min=self.recorrencia_janela_min,
                    recorrencias_escalonamento=self.recorrencia_escalonamento,
                )
            except Exception as e:
                resposta = (
                    "⚠️ <b>PORTAL ORBE — FALHA NA CONSULTA</b>\n\n"
                    f"Instância: {instancia}\n"
                    f"Motivo: {e}"
                )

            self.enviar(resposta)
            return

        if comando == "/ativos":
            with self.lock_estado:
                resposta = montar_resumo_ativos(self.estado)
            self.enviar(resposta)

    def run(self):
        print(
            "🤖 Telegram: receptor /travados, /ativos, /sbc, /travadosN, /instanciaN, "
            "/ajuda e /ping iniciado em modo reconexão automática."
        )

        # Tentativa inicial, mas sem bloquear o restante do monitor.
        self.tentar_remover_webhook()

        # Registra o menu que o Telegram exibe quando o usuário digita "/".
        # Se houver falha de rede, o receptor continua operando normalmente.
        self.registrar_comandos()

        falhas_consecutivas = 0

        while not self.stop_event.is_set():
            try:
                params = {
                    # Long polling do Telegram.
                    "timeout": 20,
                    "allowed_updates": json.dumps(
                        [
                            "message",
                            "channel_post",
                            "edited_message",
                            "edited_channel_post",
                        ]
                    ),
                }

                if self.offset is not None:
                    params["offset"] = self.offset

                # Timeout do requests deve ser MAIOR que o timeout do long polling.
                payload = self._get(
                    "getUpdates",
                    params=params,
                    timeout=35,
                )

                if not self.conectado:
                    self.conectado = True
                    print("✅ Telegram: receptor de comandos conectado.")

                falhas_consecutivas = 0
                self.ultima_falha = None

                updates = payload.get("result", [])

                for update in updates:
                    update_id = update.get("update_id")
                    if isinstance(update_id, int):
                        self.offset = update_id + 1

                    self.processar_update(update)

            except requests.HTTPError as e:
                self.conectado = False
                falhas_consecutivas += 1
                status = e.response.status_code if e.response is not None else "?"
                corpo = ""
                if e.response is not None:
                    try:
                        corpo = e.response.text[:300]
                    except Exception:
                        pass

                self.ultima_falha = f"HTTP {status}: {corpo}"
                print(
                    f"⚠ Telegram /ativos: HTTP {status}. "
                    f"Nova tentativa automática em 10s."
                )

                # HTTP 409 costuma indicar outro getUpdates ativo.
                if str(status) == "409":
                    print(
                        "⚠ Telegram: possível outro processo consumindo getUpdates "
                        "com o mesmo bot."
                    )

                if self.stop_event.wait(10):
                    break

            except Exception as e:
                self.conectado = False
                falhas_consecutivas += 1
                self.ultima_falha = str(e)

                # Evita poluir o terminal a cada tentativa idêntica.
                if falhas_consecutivas == 1 or falhas_consecutivas % 6 == 0:
                    print(
                        "⚠ Telegram comandos: conexão indisponível; "
                        f"tentativa automática continuará. Motivo: {e}"
                    )

                # A cada ~1 minuto de falhas, tenta também remover webhook novamente.
                if falhas_consecutivas % 6 == 0:
                    self.tentar_remover_webhook()

                if self.stop_event.wait(10):
                    break



def processar_fila(
    instancia,
    fila,
    status,
    horario_orbe,
    estado,
    telegram,
    telegram_enabled,
    limite_p34,
    limite_p135,
    confirmacao_min,
    confirmacao_normalizacao_min,
    repeticao_min,
    primeira_execucao_silenciosa,
    dentro_horario,
):
    agora = datetime.now()
    key = chave_fila(instancia, fila)
    atual = estado["filas"].get(key, {})

    acima_dict = limites_ultrapassados(status, limite_p34, limite_p135)
    acima = acima_dict[34] or acima_dict[135]

    fila_nova_no_estado = key not in estado["filas"]
    incidente_confirmado = bool(atual.get("incidente_confirmado"))
    observacao_inicio = atual.get("observacao_inicio")
    inicio_incidente = atual.get("inicio_incidente")
    normalizacao_inicio = atual.get("normalizacao_inicio")

    ultima_notificacao = None
    if atual.get("ultima_notificacao"):
        try:
            ultima_notificacao = datetime.fromisoformat(atual["ultima_notificacao"])
        except ValueError:
            pass

    deve_enviar = False
    persistencia = False
    normalizacao = False

    # Fora da janela operacional continuamos coletando histórico, mas não
    # criamos, confirmamos, repetimos ou normalizamos incidentes.
    # Também zeramos uma eventual janela de normalização para que ela não
    # atravesse a noite e gere confirmação imediata na manhã seguinte.
    if not dentro_horario and incidente_confirmado:
        atual["normalizacao_inicio"] = None
        normalizacao_inicio = None

    if dentro_horario:
        if fila_nova_no_estado and primeira_execucao_silenciosa:
            if acima:
                atual["observacao_inicio"] = agora.isoformat(timespec="seconds")
            else:
                atual["observacao_inicio"] = None

        elif incidente_confirmado:
            if acima:
                # Se a fila voltou a ultrapassar qualquer limite durante
                # a janela de confirmação de normalização, a contagem é
                # cancelada e deverá começar novamente no próximo período
                # contínuo abaixo dos limites.
                atual["normalizacao_inicio"] = None
                normalizacao_inicio = None

                if (
                    ultima_notificacao is None
                    or agora - ultima_notificacao >= timedelta(minutes=repeticao_min)
                ):
                    deve_enviar = True
                    persistencia = True
            else:
                # Incidente já confirmado, porém a saída também precisa ser
                # consistente. Só enviamos FILA NORMALIZADA se P34 e P135
                # permanecerem dentro dos limites durante toda a janela.
                if not normalizacao_inicio:
                    atual["normalizacao_inicio"] = agora.isoformat(
                        timespec="seconds"
                    )
                    normalizacao_inicio = atual["normalizacao_inicio"]

                tempo_normalizacao = minutos_decorridos(
                    normalizacao_inicio,
                    agora=agora,
                )

                if tempo_normalizacao >= confirmacao_normalizacao_min:
                    deve_enviar = True
                    normalizacao = True

        else:
            if acima:
                if not observacao_inicio:
                    atual["observacao_inicio"] = agora.isoformat(timespec="seconds")
                    observacao_inicio = atual["observacao_inicio"]

                tempo_observacao = minutos_decorridos(observacao_inicio, agora=agora)

                if tempo_observacao >= confirmacao_min:
                    atual["incidente_confirmado"] = True
                    atual["inicio_incidente"] = observacao_inicio
                    inicio_incidente = observacao_inicio
                    deve_enviar = True
            else:
                atual["observacao_inicio"] = None

    if deve_enviar:
        if normalizacao:
            atual["incidente_confirmado"] = False
            atual["observacao_inicio"] = None
            # O próprio objeto "atual" já foi alterado para False e pertence ao
            # dicionário de estado; portanto a contagem já exclui esta fila.
            incidentes_ativos = contar_incidentes_ativos(estado)

            mensagem = montar_normalizacao(
                instancia=instancia,
                fila=fila,
                status=status,
                limite_p34=limite_p34,
                limite_p135=limite_p135,
                inicio_incidente=inicio_incidente,
                fim_congestionamento=normalizacao_inicio,
                confirmacao_normalizacao_min=confirmacao_normalizacao_min,
                incidentes_ativos=incidentes_ativos,
            )
            atual["inicio_incidente"] = None
            atual["normalizacao_inicio"] = None
            registrar_log_portal_orbe(
                "INCIDENTE_NORMALIZADO",
                (
                    f"instancia={instancia}; fila={fila['codigo']}; "
                    f"P34={status[34]['percentual']:.2f}%; "
                    f"P135={status[135]['percentual']:.2f}%; "
                    f"ativos_restantes={incidentes_ativos}"
                ),
            )
        else:
            atual["incidente_confirmado"] = True
            # Como "atual" pode ainda não estar gravado no dicionário para uma
            # fila nova, calculamos explicitamente sem duplicar incidentes.
            if key in estado["filas"]:
                estado["filas"][key] = atual
                incidentes_ativos = contar_incidentes_ativos(estado)
            else:
                incidentes_ativos = contar_incidentes_ativos(estado) + 1

            mensagem = montar_alerta(
                instancia=instancia,
                fila=fila,
                status=status,
                limite_p34=limite_p34,
                limite_p135=limite_p135,
                confirmacao_min=confirmacao_min,
                incidentes_ativos=incidentes_ativos,
                persistencia=persistencia,
                inicio_incidente=inicio_incidente,
                atualizado_em=agora,
            )
            registrar_log_portal_orbe(
                "INCIDENTE_REPETICAO" if persistencia else "INCIDENTE_CONFIRMADO",
                (
                    f"instancia={instancia}; fila={fila['codigo']}; "
                    f"P34={status[34]['percentual']:.2f}%; "
                    f"P135={status[135]['percentual']:.2f}%; "
                    f"limites=P34>{limite_p34:.2f}%/P135>{limite_p135:.2f}%; "
                    f"ativos={incidentes_ativos}"
                ),
            )

        if telegram_enabled and telegram is not None:
            telegram.enviar_mensagem(mensagem)
        else:
            print("\n--- MENSAGEM QUE SERIA ENVIADA AO TELEGRAM ---")
            print(re.sub(r"</?b>", "", mensagem))
            print("--- FIM DA MENSAGEM ---\n")

        atual["ultima_notificacao"] = agora.isoformat(timespec="seconds")

    atual.update({
        "em_alerta": bool(atual.get("incidente_confirmado")),
        "ultima_amostra": agora.isoformat(timespec="seconds"),
        "horario_orbe": horario_orbe,
        "status34_percentual": status[34]["percentual"],
        "status34_quantidade": status[34]["quantidade"],
        "status135_percentual": status[135]["percentual"],
        "status135_quantidade": status[135]["quantidade"],
        "instancia": str(instancia),
        "projeto": fila["projeto"],
        "fila": fila["nome"],
        "codigo": fila["codigo"],
    })
    estado["filas"][key] = atual

    registrar_historico({
        "timestamp": agora.isoformat(timespec="seconds"),
        "horario_orbe": horario_orbe,
        "instancia": str(instancia),
        "projeto_id": fila["projeto_id"],
        "projeto": fila["projeto"],
        "id_queue": fila["id_queue"],
        "fila": fila["nome"],
        "codigo": fila["codigo"],
        "status34": status[34],
        "status135": status[135],
        "acima_limite": acima,
        "limite_p34": limite_p34,
        "limite_p135": limite_p135,
        "confirmacao_minutos": confirmacao_min,
        "confirmacao_normalizacao_minutos": confirmacao_normalizacao_min,
        "normalizacao_inicio": atual.get("normalizacao_inicio"),
        "incidente_confirmado": bool(atual.get("incidente_confirmado")),
        "dentro_horario_operacional": dentro_horario,
    })


def main():
    load_dotenv()

    usuario = os.getenv("ORBE_LOGIN", "").strip()
    senha = os.getenv("ORBE_PASSWORD", "")
    instancias = [
        x.strip()
        for x in os.getenv("ORBE_INSTANCIAS", "1,2,4,5,6").split(",")
        if x.strip()
    ]

    if not usuario or not senha:
        raise SystemExit(
            "Configure ORBE_LOGIN e ORBE_PASSWORD no .env antes de executar."
        )

    intervalo = int(os.getenv("ORBE_INTERVALO_SEGUNDOS", str(INTERVALO_PADRAO)))
    limite_p34 = float(os.getenv("ORBE_LIMITE_P34", str(LIMITE_P34_PADRAO)))
    limite_p135 = float(os.getenv("ORBE_LIMITE_P135", str(LIMITE_P135_PADRAO)))
    confirmacao_min = int(
        os.getenv(
            "ORBE_CONFIRMACAO_ALERTA_MINUTOS",
            str(CONFIRMACAO_ALERTA_PADRAO_MIN),
        )
    )
    confirmacao_normalizacao_min = int(
        os.getenv(
            "ORBE_CONFIRMACAO_NORMALIZACAO_MINUTOS",
            str(CONFIRMACAO_NORMALIZACAO_PADRAO_MIN),
        )
    )
    repeticao_min = int(
        os.getenv("ORBE_REPETICAO_ALERTA_MINUTOS", str(REPETICAO_PADRAO_MIN))
    )

    hora_inicio_txt = os.getenv("ORBE_HORA_INICIO_OPERACAO", HORA_INICIO_PADRAO)
    hora_fim_txt = os.getenv("ORBE_HORA_FIM_OPERACAO", HORA_FIM_PADRAO)
    hora_inicio = parse_hhmm(hora_inicio_txt, HORA_INICIO_PADRAO)
    hora_fim = parse_hhmm(hora_fim_txt, HORA_FIM_PADRAO)

    telegram_enabled = env_bool("ORBE_TELEGRAM_ENABLED", False)
    comando_ativos_enabled = env_bool("ORBE_COMANDO_ATIVOS_ENABLED", True)
    primeira_execucao_silenciosa = env_bool(
        "ORBE_PRIMEIRA_EXECUCAO_SILENCIOSA",
        True,
    )

    canais_travados_enabled = env_bool(
        "ORBE_CANAIS_TRAVADOS_ENABLED",
        True,
    )
    canais_travados_instancias = [
        x.strip()
        for x in os.getenv("ORBE_CANAIS_TRAVADOS_INSTANCIAS", "4").split(",")
        if x.strip()
    ]
    canais_travados_limite_alerta = int(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_LIMITE_ALERTA",
            str(CANAIS_TRAVADOS_LIMITE_ALERTA_PADRAO),
        )
    )
    canais_travados_limite_critico = int(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_LIMITE_CRITICO",
            str(CANAIS_TRAVADOS_LIMITE_CRITICO_PADRAO),
        )
    )
    canais_travados_repeticao_min = int(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_REPETICAO_MINUTOS",
            str(CANAIS_TRAVADOS_REPETICAO_PADRAO_MIN),
        )
    )

    canais_travados_auto_limpeza = env_bool(
        "ORBE_CANAIS_TRAVADOS_AUTO_LIMPEZA_ENABLED",
        CANAIS_TRAVADOS_AUTO_LIMPEZA_PADRAO,
    )
    canais_travados_auto_limpeza_limite = int(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_AUTO_LIMPEZA_LIMITE",
            str(canais_travados_limite_alerta),
        )
    )
    canais_travados_auto_limpeza_cooldown_min = int(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_AUTO_LIMPEZA_COOLDOWN_MINUTOS",
            str(CANAIS_TRAVADOS_AUTO_LIMPEZA_COOLDOWN_PADRAO_MIN),
        )
    )

    canais_travados_recorrencia_limite = int(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_RECORRENCIA_LIMITE",
            str(CANAIS_TRAVADOS_RECORRENCIA_LIMITE_PADRAO),
        )
    )
    canais_travados_recorrencia_janela_min = int(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_RECORRENCIA_JANELA_MINUTOS",
            str(CANAIS_TRAVADOS_RECORRENCIA_JANELA_PADRAO_MIN),
        )
    )
    canais_travados_recorrencia_escalonamento = int(
        os.getenv(
            "ORBE_CANAIS_TRAVADOS_RECORRENCIA_ESCALONAMENTO",
            str(CANAIS_TRAVADOS_RECORRENCIA_ESCALONAMENTO_PADRAO),
        )
    )

    canais_travados_resumo_diario = env_bool(
        "ORBE_CANAIS_TRAVADOS_RESUMO_DIARIO_ENABLED",
        CANAIS_TRAVADOS_RESUMO_DIARIO_PADRAO,
    )
    canais_travados_resumo_diario_hora_txt = os.getenv(
        "ORBE_CANAIS_TRAVADOS_RESUMO_DIARIO_HORA",
        CANAIS_TRAVADOS_RESUMO_DIARIO_HORA_PADRAO,
    )
    canais_travados_resumo_diario_hora = parse_hhmm(
        canais_travados_resumo_diario_hora_txt,
        CANAIS_TRAVADOS_RESUMO_DIARIO_HORA_PADRAO,
    )

    ipbx_config = parse_ipbx_config(canais_travados_instancias)

    telegram = None
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if telegram_enabled:
        if not tg_token or not tg_chat_id:
            raise SystemExit(
                "ORBE_TELEGRAM_ENABLED=true, mas TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID "
                "não estão configurados."
            )

        telegram = Telegram(
            token=tg_token,
            chat_id=tg_chat_id,
        )

    estado = carregar_estado()

    # Mudanças de regra de negócio (limites, tempos e janela operacional)
    # NÃO apagam incidentes já confirmados. O estado existente é preservado e
    # passa a ser reavaliado normalmente com os novos parâmetros. Isso evita
    # que /ativos perca um congestionamento real após simples ajuste no .env.
    meta = estado.setdefault("metadados", {})
    assinatura_config = {
        "instancias": sorted(instancias),
        "limite_p34": limite_p34,
        "limite_p135": limite_p135,
        "confirmacao_minutos": confirmacao_min,
        "confirmacao_normalizacao_minutos": confirmacao_normalizacao_min,
        "hora_inicio": hora_inicio_txt,
        "hora_fim": hora_fim_txt,
    }
    assinatura_anterior = meta.get("assinatura_config") or {}
    mudou_config = assinatura_anterior != assinatura_config

    if mudou_config:
        alteracoes = []
        for campo, novo_valor in assinatura_config.items():
            antigo_valor = assinatura_anterior.get(campo)
            if antigo_valor != novo_valor:
                alteracoes.append(f"{campo}: {antigo_valor!r} -> {novo_valor!r}")

        # Até mesmo inclusão/remoção de instâncias preserva as filas das
        # instâncias que continuam configuradas. Estados de instâncias removidas
        # são descartados para não permanecerem eternamente no /ativos.
        instancias_atuais = {str(x) for x in assinatura_config["instancias"]}
        instancias_anteriores = {str(x) for x in assinatura_anterior.get("instancias", [])}
        removidas = instancias_anteriores - instancias_atuais
        if removidas:
            estado["filas"] = {
                chave: item
                for chave, item in estado.get("filas", {}).items()
                if str(item.get("instancia", "")) not in removidas
            }
            alteracoes.append(
                "instâncias removidas do estado: " + ",".join(sorted(removidas))
            )

        meta["assinatura_config"] = assinatura_config
        meta["rebaseline_pendente"] = False
        meta["ultima_alteracao_config_em"] = datetime.now().isoformat(timespec="seconds")
        registrar_log_portal_orbe(
            "CONFIGURACAO_ALTERADA",
            "; ".join(alteracoes) + "; estado de incidentes preservado",
        )
    else:
        meta.setdefault("rebaseline_pendente", False)

    assinatura_canais = {
        "enabled": canais_travados_enabled,
        "instancias": sorted(canais_travados_instancias),
        "limite_alerta": canais_travados_limite_alerta,
        "limite_critico": canais_travados_limite_critico,
        "repeticao_minutos": canais_travados_repeticao_min,
        "auto_limpeza": canais_travados_auto_limpeza,
        "auto_limpeza_limite": canais_travados_auto_limpeza_limite,
        "auto_limpeza_cooldown_minutos": (
            canais_travados_auto_limpeza_cooldown_min
        ),
        "config_instancias": {
            k: dict(v)
            for k, v in sorted(ipbx_config.items())
        },
    }
    mudou_config_canais = (
        meta.get("assinatura_canais_travados") != assinatura_canais
    )
    if mudou_config_canais:
        estado["canais_travados"] = {}
        meta["assinatura_canais_travados"] = assinatura_canais
    estado.setdefault("canais_travados", {})

    salvar_estado(estado)
    registrar_log_portal_orbe(
        "MONITOR_INICIADO",
        (
            f"pid={os.getpid()}; instancias={','.join(instancias)}; "
            f"P34={limite_p34:.2f}%; P135={limite_p135:.2f}%; "
            f"confirmacao={confirmacao_min}min; "
            f"normalizacao={confirmacao_normalizacao_min}min; "
            f"janela={hora_inicio_txt}-{hora_fim_txt}"
        ),
    )

    lock_estado = threading.RLock()
    stop_event = threading.Event()
    listener = None

    ipbx_canais = None
    if canais_travados_enabled or ipbx_config:
        ipbx_canais = PortalIPBXCanaisTravados(
            usuario=usuario,
            senha=senha,
            config_instancias=ipbx_config,
        )

    def consultar_travados_telegram(instancia):
        if ipbx_canais is None:
            raise RuntimeError("Monitor de canais travados não inicializado.")
        return ipbx_canais.consultar(instancia)

    if telegram_enabled and comando_ativos_enabled:
        listener = TelegramCommandListener(
            token=tg_token,
            chat_id=tg_chat_id,
            estado=estado,
            lock_estado=lock_estado,
            stop_event=stop_event,
            consultar_travados=consultar_travados_telegram,
            travados_instancias=ipbx_config.keys(),
            travados_limite_alerta=canais_travados_limite_alerta,
            travados_limite_critico=canais_travados_limite_critico,
            auto_limpeza_enabled=canais_travados_auto_limpeza,
            auto_limpeza_limite=canais_travados_auto_limpeza_limite,
            auto_limpeza_cooldown_min=canais_travados_auto_limpeza_cooldown_min,
            recorrencia_limite=canais_travados_recorrencia_limite,
            recorrencia_janela_min=canais_travados_recorrencia_janela_min,
            recorrencia_escalonamento=canais_travados_recorrencia_escalonamento,
        )
        listener.start()

    print("=" * 72)
    print("📞 PORTAL ORBE — Monitor PABX 34 / 135")
    print("=" * 72)
    print(f"Instâncias configuradas : {', '.join(instancias)}")
    print(f"Limite P34             : {limite_p34:.2f}%")
    print(f"Limite P135            : {limite_p135:.2f}%")
    print(f"Confirmação alerta     : {confirmacao_min} min contínuos")
    print(
        f"Confirmação normaliz.  : "
        f"{confirmacao_normalizacao_min} min contínuos"
    )
    print(f"Intervalo              : {intervalo}s")
    print(f"Repetição de alerta    : {repeticao_min} min")
    print(f"Janela operacional     : {hora_inicio_txt} às {hora_fim_txt}")
    print(f"Telegram habilitado    : {'SIM' if telegram_enabled else 'NÃO (modo teste)'}")
    print(
        f"Comando /ativos        : "
        f"{'RECONECTANDO/ATIVO' if listener is not None else 'NÃO'}"
    )
    print(
        f"Baseline inicial       : "
        f"{'SILENCIOSO' if primeira_execucao_silenciosa else 'ALERTA IMEDIATO'}"
    )
    print(
        "Log Portal Orbe        : "
        + os.getenv("ORBE_LOG_ARQUIVO", str(ARQUIVO_LOG_PORTAL_ORBE_PADRAO))
    )
    print(
        f"Canais travados        : "
        f"{'SIM' if canais_travados_enabled else 'NÃO'}"
    )
    if canais_travados_enabled:
        print(
            f"Limite canais alerta   : ≥ {canais_travados_limite_alerta}"
        )
        print(
            f"Limite canais crítico  : > {canais_travados_limite_critico}"
        )
        print(
            f"Repetição canais       : {canais_travados_repeticao_min} min"
        )
        print(
            f"Instâncias canais      : "
            f"{', '.join(canais_travados_instancias) or '-'}"
        )
        print(
            f"Comandos travados      : /travados | "
            f"{', '.join('/travados' + x for x in sorted(ipbx_config)) or 'NENHUM'}"
        )
        print(
            f"Resumos por instância  : "
            f"{', '.join('/instancia' + x for x in sorted(ipbx_config)) or 'NENHUM'}"
        )
        print(
            f"Auto limpeza canais    : "
            f"{'SIM' if canais_travados_auto_limpeza else 'NÃO'}"
        )
        if canais_travados_auto_limpeza:
            print(
                f"Limite auto limpeza    : ≥ "
                f"{canais_travados_auto_limpeza_limite}"
            )
            print(
                f"Cooldown auto limpeza  : "
                f"{canais_travados_auto_limpeza_cooldown_min} min"
            )
            print(
                f"Recorrência gerencial  : ≥ {canais_travados_recorrencia_limite} canais "
                f"em até {canais_travados_recorrencia_janela_min} min | "
                f"escalona na {canais_travados_recorrencia_escalonamento}ª"
            )
            print(
                "Auditoria auto limpeza : "
                + os.getenv(
                    "ORBE_CANAIS_TRAVADOS_AUDITORIA_ARQUIVO",
                    str(ARQUIVO_AUDITORIA_AUTOLIMPEZA_PADRAO),
                )
            )
            print(
                f"Resumo diário canais   : "
                f"{'SIM' if canais_travados_resumo_diario else 'NÃO'}"
            )
            if canais_travados_resumo_diario:
                print(
                    f"Hora resumo diário     : "
                    f"{canais_travados_resumo_diario_hora_txt}"
                )
    if mudou_config:
        print("Proteção de estado     : REBASELINE por mudança de configuração")
    print()

    clientes = {}

    try:
        while True:
            inicio_ciclo = time.monotonic()
            agora_ciclo = datetime.now()
            dentro_horario = dentro_janela_operacional(
                agora_ciclo,
                hora_inicio,
                hora_fim,
            )

            if not dentro_horario:
                print(
                    f"\n[{agora_ciclo:%H:%M:%S}] Fora da janela operacional "
                    f"({hora_inicio_txt}–{hora_fim_txt}): coleta continua, alertas suspensos."
                )

            for instancia in instancias:
                try:
                    cliente = clientes.get(instancia)
                    if cliente is None:
                        cliente = PortalOrbe(instancia, usuario, senha)
                        cliente.autenticar()
                        clientes[instancia] = cliente
                        print(f"✅ Instância {instancia}: autenticação OK.")

                    filas = cliente.listar_filas()
                    print(
                        f"\n[{datetime.now():%H:%M:%S}] Instância {instancia}: "
                        f"{len(filas)} filas encontradas."
                    )

                    for fila in filas:
                        try:
                            status, horario_orbe = cliente.consultar_status(fila)

                            s34 = status[34]
                            s135 = status[135]
                            acima = (
                                s34["percentual"] > limite_p34
                                or s135["percentual"] > limite_p135
                            )

                            with lock_estado:
                                key = chave_fila(instancia, fila)
                                atual = estado.get("filas", {}).get(key, {})
                                confirmado = bool(atual.get("incidente_confirmado"))
                                em_observacao = bool(atual.get("observacao_inicio"))

                            normalizacao_em_observacao = bool(
                                atual.get("normalizacao_inicio")
                            )

                            if confirmado and not acima and normalizacao_em_observacao:
                                situacao = "NORMALIZAÇÃO EM OBSERVAÇÃO"
                            elif confirmado:
                                situacao = "ALERTA CONFIRMADO"
                            elif acima and em_observacao:
                                situacao = "OBSERVAÇÃO"
                            elif acima:
                                situacao = "ACIMA DO LIMITE"
                            else:
                                situacao = "NORMAL"

                            print(
                                f"  {fila['nome']} ({fila['codigo']}) | "
                                f"P34={formatar_percentual(s34['percentual'])} "
                                f"[{s34['quantidade']}] | "
                                f"P135={formatar_percentual(s135['percentual'])} "
                                f"[{s135['quantidade']}] | {situacao}"
                            )

                            with lock_estado:
                                processar_fila(
                                    instancia=instancia,
                                    fila=fila,
                                    status=status,
                                    horario_orbe=horario_orbe,
                                    estado=estado,
                                    telegram=telegram,
                                    telegram_enabled=telegram_enabled,
                                    limite_p34=limite_p34,
                                    limite_p135=limite_p135,
                                    confirmacao_min=confirmacao_min,
                                    confirmacao_normalizacao_min=(
                                        confirmacao_normalizacao_min
                                    ),
                                    repeticao_min=repeticao_min,
                                    primeira_execucao_silenciosa=primeira_execucao_silenciosa,
                                    dentro_horario=dentro_horario,
                                )

                        except requests.HTTPError as e:
                            if (
                                e.response is not None
                                and e.response.status_code in (401, 403)
                            ):
                                print(
                                    f"⚠ Instância {instancia}: sessão expirou; "
                                    "nova autenticação será feita no próximo ciclo."
                                )
                                clientes.pop(instancia, None)
                                break
                            print(f"⚠ Erro HTTP em {fila['nome']}: {e}")
                        except Exception as e:
                            print(f"⚠ Erro na fila {fila['nome']}: {e}")

                    with lock_estado:
                        meta = estado.setdefault("metadados", {})
                        meta["rebaseline_pendente"] = False
                        meta["ultimo_ciclo_completo"] = datetime.now().isoformat(
                            timespec="seconds"
                        )
                        salvar_estado(estado)

                except Exception as e:
                    clientes.pop(instancia, None)
                    print(f"⚠ Instância {instancia}: {e}")

            # ------------------------------------------------------
            # MONITOR INDEPENDENTE: CANAIS TRAVADOS
            # ------------------------------------------------------
            if canais_travados_enabled and ipbx_canais is not None:
                for instancia_canais in canais_travados_instancias:
                    if instancia_canais not in ipbx_config:
                        print(
                            f"⚠ Canais travados Instância {instancia_canais}: "
                            f"configure ORBE_IPBX_PROJETO_{instancia_canais} e "
                            f"ORBE_IPBX_MENU_{instancia_canais} no .env."
                        )
                        continue

                    try:
                        resultado_canais = ipbx_canais.consultar(
                            instancia_canais
                        )
                        qtd_canais = int(resultado_canais["quantidade"])
                        nivel = nivel_canais_travados(
                            qtd_canais,
                            canais_travados_limite_alerta,
                            canais_travados_limite_critico,
                        )

                        with lock_estado:
                            atual_canais = estado.setdefault("canais_travados", {}).setdefault(
                                str(instancia_canais), {"instancia": str(instancia_canais)}
                            )
                            atual_canais["quantidade"] = qtd_canais
                            atual_canais["ultima_consulta"] = datetime.now().isoformat(timespec="seconds")
                            atualizar_recorrencia_gerencial(
                                estado=estado,
                                instancia=instancia_canais,
                                quantidade=qtd_canais,
                                limite_recorrencia=canais_travados_recorrencia_limite,
                                janela_min=canais_travados_recorrencia_janela_min,
                            )
                            salvar_estado(estado)

                        print(
                            f"🔒 Canais travados Instância {instancia_canais}: "
                            f"{qtd_canais} | {nivel.upper()}"
                        )

                        autolimpeza_executada = False

                        if (
                            canais_travados_auto_limpeza
                            and dentro_horario
                            and qtd_canais >= canais_travados_auto_limpeza_limite
                        ):
                            with lock_estado:
                                pode_limpar = pode_executar_autolimpeza(
                                    estado,
                                    instancia_canais,
                                    canais_travados_auto_limpeza_cooldown_min,
                                )

                                if pode_limpar:
                                    marcar_tentativa_autolimpeza(
                                        estado,
                                        instancia_canais,
                                        qtd_canais,
                                    )
                                    salvar_estado(estado)

                            if pode_limpar:
                                autolimpeza_executada = True
                                print(
                                    f"🧹 Auto limpeza Instância {instancia_canais}: "
                                    f"iniciando com {qtd_canais} canais travados..."
                                )
                                registrar_auditoria_autolimpeza(
                                    instancia=instancia_canais,
                                    status="INICIADA",
                                    antes=qtd_canais,
                                    detalhe="Auto limpeza acionada pelo limite configurado.",
                                )

                                try:
                                    limpeza = ipbx_canais.limpar(
                                        instancia_canais
                                    )
                                    antes = int(limpeza["antes"])
                                    depois = int(limpeza["depois"])
                                    liberados = int(limpeza["liberados"])

                                    print(
                                        f"✅ Auto limpeza Instância {instancia_canais}: "
                                        f"antes={antes} | liberados={liberados} | "
                                        f"restantes={depois}"
                                    )

                                    if depois < canais_travados_auto_limpeza_limite:
                                        mensagem_limpeza = (
                                            montar_sucesso_limpeza_automatica(
                                                instancia=instancia_canais,
                                                antes=antes,
                                                depois=depois,
                                                liberados=liberados,
                                                limite_critico=(
                                                    canais_travados_limite_critico
                                                ),
                                            )
                                        )
                                    else:
                                        mensagem_limpeza = (
                                            montar_limpeza_automatica_incompleta(
                                                instancia=instancia_canais,
                                                antes=antes,
                                                depois=depois,
                                                liberados=liberados,
                                                limite=(
                                                    canais_travados_auto_limpeza_limite
                                                ),
                                            )
                                        )

                                    telegram_status = enviar_telegram_com_status(
                                        telegram,
                                        telegram_enabled,
                                        mensagem_limpeza,
                                    )

                                    status_auditoria = (
                                        "SUCESSO"
                                        if depois < canais_travados_auto_limpeza_limite
                                        else "INCOMPLETA"
                                    )
                                    detalhe_auditoria = (
                                        "Validação pós-limpeza confirmada."
                                        if status_auditoria == "SUCESSO"
                                        else (
                                            "Validação pós-limpeza confirmou "
                                            f"{depois} canais ainda travados."
                                        )
                                    )

                                    registrar_auditoria_autolimpeza(
                                        instancia=instancia_canais,
                                        status=status_auditoria,
                                        antes=antes,
                                        depois=depois,
                                        liberados=liberados,
                                        telegram_status=telegram_status,
                                        detalhe=detalhe_auditoria,
                                    )

                                    with lock_estado:
                                        registrar_resultado_autolimpeza(
                                            estado=estado,
                                            instancia=instancia_canais,
                                            antes=antes,
                                            depois=depois,
                                            liberados=liberados,
                                            limite_alerta=(
                                                canais_travados_limite_alerta
                                            ),
                                            limite_critico=(
                                                canais_travados_limite_critico
                                            ),
                                            notificacao_enviada=True,
                                            sucesso=bool(limpeza["sucesso"]),
                                            status_resultado=status_auditoria,
                                        )
                                        salvar_estado(estado)

                                except Exception as e:
                                    mensagem_falha = (
                                        montar_falha_limpeza_automatica(
                                            instancia=instancia_canais,
                                            quantidade=qtd_canais,
                                            motivo=str(e),
                                            limite_critico=(
                                                canais_travados_limite_critico
                                            ),
                                        )
                                    )
                                    telegram_status = enviar_telegram_com_status(
                                        telegram,
                                        telegram_enabled,
                                        mensagem_falha,
                                    )

                                    registrar_auditoria_autolimpeza(
                                        instancia=instancia_canais,
                                        status="FALHA",
                                        antes=qtd_canais,
                                        depois=qtd_canais,
                                        liberados=0,
                                        telegram_status=telegram_status,
                                        detalhe=str(e),
                                    )

                                    with lock_estado:
                                        registrar_resultado_autolimpeza(
                                            estado=estado,
                                            instancia=instancia_canais,
                                            antes=qtd_canais,
                                            depois=qtd_canais,
                                            liberados=0,
                                            limite_alerta=(
                                                canais_travados_limite_alerta
                                            ),
                                            limite_critico=(
                                                canais_travados_limite_critico
                                            ),
                                            notificacao_enviada=True,
                                            sucesso=False,
                                            status_resultado="FALHA",
                                        )
                                        salvar_estado(estado)

                        # Quando a autolimpeza está habilitada e pode tratar
                        # essa faixa, o alerta convencional é suprimido.
                        #
                        # Isso vale inclusive durante o cooldown: não enviamos
                        # "CANAIS TRAVADOS" e depois "LIMPEZA..." em sequência.
                        # A próxima tentativa automática acontecerá quando o
                        # cooldown vencer.
                        autolimpeza_responsavel = (
                            canais_travados_auto_limpeza
                            and dentro_horario
                            and qtd_canais >= canais_travados_auto_limpeza_limite
                        )

                        if not autolimpeza_executada and not autolimpeza_responsavel:
                            with lock_estado:
                                processar_canais_travados(
                                    instancia=instancia_canais,
                                    quantidade=qtd_canais,
                                    estado=estado,
                                    telegram=telegram,
                                    telegram_enabled=telegram_enabled,
                                    limite_alerta=(
                                        canais_travados_limite_alerta
                                    ),
                                    limite_critico=(
                                        canais_travados_limite_critico
                                    ),
                                    repeticao_min=(
                                        canais_travados_repeticao_min
                                    ),
                                    dentro_horario=dentro_horario,
                                )
                                salvar_estado(estado)

                    except Exception as e:
                        print(
                            f"⚠ Canais travados Instância {instancia_canais}: {e}"
                        )

            # ------------------------------------------------------
            # RESUMO GERENCIAL DIÁRIO DA AUTOLIMPEZA
            # ------------------------------------------------------
            with lock_estado:
                tentar_enviar_resumo_diario_autolimpeza(
                    agora=datetime.now(),
                    estado=estado,
                    telegram=telegram,
                    telegram_enabled=telegram_enabled,
                    enabled=canais_travados_resumo_diario,
                    hora_resumo=canais_travados_resumo_diario_hora,
                    instancias_monitoradas=canais_travados_instancias,
                    limite_alerta=canais_travados_limite_alerta,
                    limite_critico=canais_travados_limite_critico,
                )

            duracao = time.monotonic() - inicio_ciclo
            espera = max(1, intervalo - duracao)
            print(f"\nPróximo ciclo em {espera:.1f}s. Ctrl+C para encerrar.")
            time.sleep(espera)

    except KeyboardInterrupt:
        print("\nMonitor Portal Orbe encerrado.")
    finally:
        stop_event.set()
        if listener is not None and listener.is_alive():
            listener.join(timeout=2)




if __name__ == "__main__":
    main()
