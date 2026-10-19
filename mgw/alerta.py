from datetime import datetime

from mgw.telegram import Telegram


class AlertaTelegram:
    def __init__(self, telegram):
        if not isinstance(telegram, Telegram):
            raise TypeError(
                "telegram deve ser uma instância de Telegram."
            )

        self.telegram = telegram

    def enviar_eventos(
        self,
        novos_inativos,
        recuperados,
        servidores_offline,
        servidores_recuperados,
        caminho_relatorio=None,
    ):
        mensagens_enviadas = 0

        for item in novos_inativos:
            texto = self._mensagem_rota_inativa(item)
            self.telegram.enviar_mensagem(texto)
            mensagens_enviadas += 1

        for item in recuperados:
            texto = self._mensagem_rota_recuperada(item)
            self.telegram.enviar_mensagem(texto)
            mensagens_enviadas += 1

        for item in servidores_offline:
            texto = self._mensagem_servidor_offline(item)
            self.telegram.enviar_mensagem(texto)
            mensagens_enviadas += 1

        for item in servidores_recuperados:
            texto = self._mensagem_servidor_recuperado(item)
            self.telegram.enviar_mensagem(texto)
            mensagens_enviadas += 1

        if (
            mensagens_enviadas > 0
            and caminho_relatorio is not None
        ):
            self.telegram.enviar_documento(
                caminho=caminho_relatorio,
                legenda=(
                    "📎 <b>Relatório atualizado — "
                    "MGW Monitor</b>"
                ),
            )

        return mensagens_enviadas

    def _mensagem_rota_inativa(self, item):
        servidor = self._texto(
            item.get("servidor")
        )

        rota = self._texto(
            item.get("nome")
        )

        endereco = self._texto(
            item.get("endereco")
            or "Não informado"
        )

        caiu_em = self._converter_data(
            item.get("caiu_em")
        ) or datetime.now()

        partes = [
            "🚨 <b>MGW MONITOR — ROTA INATIVA</b>",
            "",
            "<b>Servidor</b>",
            servidor,
            "",
            "<b>Rota</b>",
            rota,
            "",
            "<b>Endereço</b>",
            endereco,
            "",
            "<b>Status</b>",
            "🔴 Ativo → Inativo",
            "",
            "<b>Detectado em</b>",
            f"{caiu_em:%d/%m/%Y %H:%M:%S}",
        ]

        return "\n".join(partes)

    def _mensagem_rota_recuperada(self, item):
        servidor = self._texto(
            item.get("servidor")
        )

        rota = self._texto(
            item.get("nome")
        )

        endereco = self._texto(
            item.get("endereco")
            or "Não informado"
        )

        caiu_em = self._converter_data(
            item.get("caiu_em")
        )

        voltou_em = self._converter_data(
            item.get("voltou_em")
        ) or datetime.now()

        duracao_segundos = item.get(
            "duracao_segundos"
        )

        if (
            duracao_segundos is None
            and caiu_em is not None
        ):
            duracao_segundos = max(
                0,
                int(
                    (
                        voltou_em - caiu_em
                    ).total_seconds()
                ),
            )

        partes = [
            "✅ <b>MGW MONITOR — ROTA RESTABELECIDA</b>",
            "",
            "<b>Servidor</b>",
            servidor,
            "",
            "<b>Rota</b>",
            rota,
            "",
            "<b>Endereço</b>",
            endereco,
            "",
            "<b>Status</b>",
            "🟢 Inativo → Ativo",
        ]

        if caiu_em is not None:
            partes.extend(
                [
                    "",
                    "<b>Caiu em</b>",
                    f"{caiu_em:%d/%m/%Y %H:%M:%S}",
                ]
            )

        partes.extend(
            [
                "",
                "<b>Restabelecida em</b>",
                f"{voltou_em:%d/%m/%Y %H:%M:%S}",
            ]
        )

        if duracao_segundos is not None:
            partes.extend(
                [
                    "",
                    "<b>Tempo indisponível</b>",
                    self._formatar_duracao(
                        duracao_segundos
                    ),
                ]
            )

        return "\n".join(partes)

    def _mensagem_servidor_offline(self, item):
        servidor = self._texto(
            item.get("servidor")
        )

        endereco = self._texto(
            item.get("endereco")
            or "Não informado"
        )

        falhas = item.get(
            "falhas_consecutivas",
            0,
        )

        caiu_em = self._converter_data(
            item.get("caiu_em")
        ) or datetime.now()

        partes = [
            "🚨 <b>MGW MONITOR — SERVIDOR INACESSÍVEL</b>",
            "",
            "<b>Servidor</b>",
            servidor,
            "",
            "<b>Endereço</b>",
            endereco,
            "",
            "<b>Status</b>",
            "🔴 Online → Inacessível",
            "",
            "<b>Falhas consecutivas</b>",
            str(falhas),
            "",
            "<b>Detectado em</b>",
            f"{caiu_em:%d/%m/%Y %H:%M:%S}",
        ]

        return "\n".join(partes)

    def _mensagem_servidor_recuperado(self, item):
        servidor = self._texto(
            item.get("servidor")
        )

        endereco = self._texto(
            item.get("endereco")
            or "Não informado"
        )

        caiu_em = self._converter_data(
            item.get("caiu_em")
        )

        voltou_em = self._converter_data(
            item.get("voltou_em")
        ) or datetime.now()

        duracao_segundos = item.get(
            "duracao_segundos"
        )

        partes = [
            "✅ <b>MGW MONITOR — SERVIDOR RESTABELECIDO</b>",
            "",
            "<b>Servidor</b>",
            servidor,
            "",
            "<b>Endereço</b>",
            endereco,
            "",
            "<b>Status</b>",
            "🟢 Inacessível → Online",
        ]

        if caiu_em is not None:
            partes.extend(
                [
                    "",
                    "<b>Ficou inacessível em</b>",
                    f"{caiu_em:%d/%m/%Y %H:%M:%S}",
                ]
            )

        partes.extend(
            [
                "",
                "<b>Restabelecido em</b>",
                f"{voltou_em:%d/%m/%Y %H:%M:%S}",
            ]
        )

        if duracao_segundos is not None:
            partes.extend(
                [
                    "",
                    "<b>Tempo indisponível</b>",
                    self._formatar_duracao(
                        duracao_segundos
                    ),
                ]
            )

        return "\n".join(partes)

    def _texto(self, valor):
        if valor is None:
            valor = "Não informado"

        return self.telegram.escapar(valor)

    @staticmethod
    def _converter_data(valor):
        if valor is None:
            return None

        if isinstance(valor, datetime):
            return valor

        try:
            return datetime.fromisoformat(
                str(valor)
            )

        except ValueError:
            return None

    @staticmethod
    def _formatar_duracao(segundos):
        segundos = max(
            0,
            int(segundos or 0),
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
                f"{dias} dia"
                if dias == 1
                else f"{dias} dias"
            )

        if horas:
            partes.append(f"{horas} h")

        if minutos:
            partes.append(f"{minutos} min")

        if segundos or not partes:
            partes.append(f"{segundos} s")

        return " ".join(partes)