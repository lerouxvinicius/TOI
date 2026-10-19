import requests
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MGWClient:
    def __init__(self, base_url, verificar_ssl=True):
        self.base_url = base_url.rstrip("/")
        self.verificar_ssl = verificar_ssl
        self.session = requests.Session()

    def conectar(self):
        try:
            resposta = self.session.get(
                self.base_url,
                timeout=10,
                verify=self.verificar_ssl,
            )

            resposta.raise_for_status()
            return resposta

        except requests.exceptions.RequestException as erro:
            print()
            print(f"Erro ao conectar em {self.base_url}.")
            print(erro)
            return None

    def fazer_login(self, usuario, senha):
        url = f"{self.base_url}/checkLogin.php"

        dados = {
            "timezone": "America/Sao_Paulo",
            "login": usuario,
            "password": senha,
        }

        try:
            resposta = self.session.post(
                url,
                data=dados,
                timeout=10,
                allow_redirects=True,
                verify=self.verificar_ssl,
            )

            resposta.raise_for_status()
            return resposta

        except requests.exceptions.RequestException as erro:
            print()
            print(f"Erro durante o login em {self.base_url}.")
            print(erro)
            return None

    def obter_naps(self):
        url = f"{self.base_url}/rpc/json.fcgi/kgateway"

        payload = {
            "id": 9,
            "method": "StatusNaps",
            "params": [],
            "jsonrpc": "2.0",
        }

        try:
            resposta = self.session.post(
                url,
                json=payload,
                timeout=15,
                verify=self.verificar_ssl,
            )

            resposta.raise_for_status()

            dados = resposta.json()

            if "error" in dados:
                print(f"Erro JSON-RPC em {self.base_url}:")
                print(dados["error"])
                return None

            return dados.get("result", [])

        except requests.exceptions.RequestException as erro:
            print()
            print(f"Erro ao consultar os NAPs em {self.base_url}.")
            print(erro)
            return None

        except ValueError as erro:
            print()
            print(f"Resposta JSON inválida recebida de {self.base_url}.")
            print(erro)
            return None