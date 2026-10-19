import html
from pathlib import Path

import requests


class Telegram:
    def __init__(
        self,
        token,
        chat_id,
        timeout=30,
    ):
        if not token:
            raise ValueError(
                "TELEGRAM_BOT_TOKEN não foi configurado."
            )

        if not chat_id:
            raise ValueError(
                "TELEGRAM_CHAT_ID não foi configurado."
            )

        self.token = token
        self.chat_id = str(chat_id)
        self.timeout = timeout
        self.base_url = (
            f"https://api.telegram.org/bot{self.token}"
        )

    def enviar_mensagem(self, texto):
        """
        Envia texto ao Telegram.

        Mensagens extensas são divididas automaticamente
        em blocos menores.
        """
        blocos = self._dividir_texto(
            texto=texto,
            limite=3800,
        )

        resultados = []

        for bloco in blocos:
            resposta = requests.post(
                f"{self.base_url}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": bloco,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=self.timeout,
            )

            self._validar_resposta(resposta)
            resultados.append(resposta.json())

        return resultados

    def enviar_documento(
        self,
        caminho,
        legenda=None,
    ):
        """
        Envia um arquivo ao grupo do Telegram.
        """
        caminho = Path(caminho)

        if not caminho.exists():
            raise FileNotFoundError(
                f"Arquivo não encontrado: {caminho}"
            )

        dados = {
            "chat_id": self.chat_id,
        }

        if legenda:
            dados["caption"] = legenda
            dados["parse_mode"] = "HTML"

        with caminho.open("rb") as arquivo:
            resposta = requests.post(
                f"{self.base_url}/sendDocument",
                data=dados,
                files={
                    "document": (
                        caminho.name,
                        arquivo,
                        (
                            "application/"
                            "vnd.openxmlformats-officedocument."
                            "spreadsheetml.sheet"
                        ),
                    )
                },
                timeout=120,
            )

        self._validar_resposta(resposta)

        return resposta.json()

    @staticmethod
    def escapar(texto):
        """
        Protege caracteres especiais usados pelo HTML
        do Telegram.
        """
        if texto is None:
            return "Não informado"

        return html.escape(
            str(texto),
            quote=False,
        )

    @staticmethod
    def _dividir_texto(
        texto,
        limite,
    ):
        """
        Divide a mensagem sem cortar linhas no meio.
        """
        if len(texto) <= limite:
            return [texto]

        blocos = []
        bloco_atual = ""

        for linha in texto.splitlines(keepends=True):
            if len(bloco_atual) + len(linha) > limite:
                if bloco_atual:
                    blocos.append(bloco_atual.rstrip())

                bloco_atual = linha

            else:
                bloco_atual += linha

        if bloco_atual:
            blocos.append(bloco_atual.rstrip())

        return blocos

    @staticmethod
    def _validar_resposta(resposta):
        try:
            conteudo = resposta.json()
        except ValueError:
            conteudo = {}

        if resposta.ok and conteudo.get("ok", False):
            return

        descricao = conteudo.get(
            "description",
            resposta.text,
        )

        raise RuntimeError(
            "Erro ao comunicar com o Telegram: "
            f"{descricao}"
        )