import json
from datetime import datetime, time
from pathlib import Path

from mgw.dashboard_inteligente import DashboardInteligente
from mgw.relatorio_diario import RelatorioDiario


class ResumoDiario:
    def __init__(
        self,
        banco,
        telegram,
        horario="21:00",
        caminho_estado="dados/resumo_diario.json",
        pasta_relatorios="relatorios/diarios",
    ):
        self.banco = banco
        self.telegram = telegram

        self.horario = self._converter_horario(
            horario
        )

        self.caminho_estado = Path(
            caminho_estado
        )

        self.relatorio = RelatorioDiario(
            banco=banco,
            pasta_saida=pasta_relatorios,
        )

    def deve_enviar(self, agora=None):
        agora = agora or datetime.now()

        horario_programado = datetime.combine(
            agora.date(),
            self.horario,
        )

        if agora < horario_programado:
            return False

        ultima_data = self._obter_ultima_data()

        return (
            ultima_data
            != agora.date().isoformat()
        )

    def enviar(
        self,
        naps,
        nomes_servidores,
        agora=None,
    ):
        agora = agora or datetime.now()

        inicio_periodo = datetime.combine(
            agora.date(),
            time.min,
        )

        fim_periodo = agora

        todos_eventos_rotas = (
            self.banco.listar_eventos(
                limite=100000
            )
        )

        todos_eventos_servidores = (
            self.banco.listar_eventos_servidores(
                limite=100000
            )
        )

        eventos_rotas_dia = self._eventos_do_periodo(
            eventos=todos_eventos_rotas,
            inicio_periodo=inicio_periodo,
            fim_periodo=fim_periodo,
        )

        eventos_servidores_dia = (
            self._eventos_do_periodo(
                eventos=todos_eventos_servidores,
                inicio_periodo=inicio_periodo,
                fim_periodo=fim_periodo,
            )
        )

        resumo = self._calcular_resumo(
            naps=naps,
            nomes_servidores=nomes_servidores,
            eventos_rotas=eventos_rotas_dia,
            todos_eventos_rotas=(
                todos_eventos_rotas
            ),
            eventos_servidores=(
                eventos_servidores_dia
            ),
            inicio_periodo=inicio_periodo,
            fim_periodo=fim_periodo,
        )

        texto = self._montar_mensagem(
            resumo=resumo,
            data=agora,
        )

        dashboard = DashboardInteligente()

        dados_dashboard = dashboard.calcular(
            naps=naps,
            nomes_servidores=nomes_servidores,
            eventos_rotas=todos_eventos_rotas,
            eventos_servidores=(
                todos_eventos_servidores
            ),
            inicio_periodo=inicio_periodo,
            fim_periodo=fim_periodo,
            banco=self.banco,
        )

        texto_dashboard = (
            dashboard.montar_mensagem_telegram(
                telegram=self.telegram,
                dados=dados_dashboard,
            )
        )

        caminho_relatorio = (
            self.relatorio.gerar(
                naps=naps,
                nomes_servidores=(
                    nomes_servidores
                ),
                inicio_periodo=(
                    inicio_periodo
                ),
                fim_periodo=fim_periodo,
            )
        )

        # 1. Resumo operacional do dia.
        self.telegram.enviar_mensagem(
            texto
        )

        # 2. Dashboard Inteligente.
        self.telegram.enviar_mensagem(
            texto_dashboard
        )

        # 3. Excel completo.
        self.telegram.enviar_documento(
            caminho=caminho_relatorio,
            legenda=(
                "📎 <b>Relatório diário — "
                f"{agora:%d/%m/%Y}</b>"
            ),
        )

        # O controle só é salvo depois dos três
        # envios terminarem com sucesso.
        self._salvar_envio(
            data=agora.date().isoformat(),
            horario_envio=datetime.now(),
            caminho_relatorio=caminho_relatorio,
        )

        return caminho_relatorio

    def _calcular_resumo(
        self,
        naps,
        nomes_servidores,
        eventos_rotas,
        todos_eventos_rotas,
        eventos_servidores,
        inicio_periodo,
        fim_periodo,
    ):
        servidores_com_dados = {
            str(nap.servidor)
            for nap in naps
        }

        servidores_inacessiveis = sorted(
            set(nomes_servidores)
            - servidores_com_dados
        )

        quedas_detectadas = sum(
            1
            for evento in eventos_rotas
            if self._esta_no_periodo(
                evento.get("caiu_em"),
                inicio_periodo,
                fim_periodo,
            )
        )

        recuperacoes = sum(
            1
            for evento in eventos_rotas
            if self._esta_no_periodo(
                evento.get("voltou_em"),
                inicio_periodo,
                fim_periodo,
            )
        )

        enderecos_rotas_indisponiveis = (
            self._agrupar_enderecos_indisponiveis(
                naps=naps,
                todos_eventos_rotas=(
                    todos_eventos_rotas
                ),
                fim_periodo=fim_periodo,
            )
        )

        return {
            "servidores_monitorados": len(
                nomes_servidores
            ),
            "rotas_monitoradas": len(naps),
            "quedas_detectadas": (
                quedas_detectadas
            ),
            "recuperacoes": recuperacoes,
            "enderecos_rotas_indisponiveis": (
                enderecos_rotas_indisponiveis
            ),
            "servidores_inacessiveis": (
                servidores_inacessiveis
            ),
            "eventos_servidores": (
                eventos_servidores
            ),
        }

    def _agrupar_enderecos_indisponiveis(
        self,
        naps,
        todos_eventos_rotas,
        fim_periodo,
    ):
        agrupados = {}

        for nap in naps:
            if not nap.esta_inativo():
                continue

            endereco = str(
                nap.endereco()
                or "Não informado"
            ).strip()

            rota = str(
                nap.nome
                or "Rota não informada"
            ).strip()

            servidor = str(
                nap.servidor
                or "Não informado"
            ).strip()

            chave = endereco.casefold()

            if chave not in agrupados:
                agrupados[chave] = {
                    "endereco": endereco,
                    "rotas": set(),
                    "servidores": set(),
                    "detalhes_rotas": [],
                    "indisponivel_desde": None,
                    "tempo_indisponivel_segundos": 0,
                }

            agrupados[chave]["rotas"].add(rota)
            agrupados[chave]["servidores"].add(servidor)

            inicio = self.banco.obter_inicio_indisponibilidade_atual(
                servidor=servidor,
                rota=rota,
                endereco=(
                    ""
                    if endereco == "Não informado"
                    else endereco
                ),
            )

            inicio = self._converter_data(inicio)

            agrupados[chave]["detalhes_rotas"].append(
                {
                    "rota": rota,
                    "servidor": servidor,
                    "inicio": inicio,
                }
            )

        # Compatibilidade com eventos abertos já existentes.
        # Caso o histórico de coletas não tenha sido suficiente,
        # aproveitamos o evento aberto registrado no banco.
        for item in agrupados.values():
            inicios = [
                detalhe["inicio"]
                for detalhe in item["detalhes_rotas"]
                if detalhe["inicio"] is not None
            ]

            if not inicios:
                for evento in todos_eventos_rotas:
                    evento_endereco = str(
                        evento.get("endereco")
                        or "Não informado"
                    ).strip()

                    if (
                        evento_endereco.casefold()
                        != item["endereco"].casefold()
                    ):
                        continue

                    voltou = self._converter_data(
                        evento.get("voltou_em")
                    )

                    status = str(
                        evento.get("status") or ""
                    ).upper()

                    if (
                        voltou is not None
                        and status != "ABERTO"
                    ):
                        continue

                    caiu = self._converter_data(
                        evento.get("caiu_em")
                    )

                    if caiu is not None:
                        inicios.append(caiu)

            if inicios:
                indisponivel_desde = min(inicios)

                item["indisponivel_desde"] = (
                    indisponivel_desde
                )

                item[
                    "tempo_indisponivel_segundos"
                ] = max(
                    0,
                    int(
                        (
                            fim_periodo
                            - indisponivel_desde
                        ).total_seconds()
                    ),
                )

            item["rotas"] = sorted(
                item["rotas"],
                key=str.casefold,
            )
            item["servidores"] = sorted(
                item["servidores"],
                key=str.casefold,
            )

        return sorted(
            agrupados.values(),
            key=lambda item: (
                item["endereco"].casefold()
            ),
        )

    def _montar_mensagem(
        self,
        resumo,
        data,
    ):
        linhas = [
            "📊 <b>MGW MONITOR — RESUMO DIÁRIO</b>",
            "",
            "📅 <b>Data</b>",
            f"{data:%d/%m/%Y}",
            "",
            (
                "🖥 <b>Servidores monitorados:</b> "
                f"{resumo['servidores_monitorados']}"
            ),
            "",
            (
                "📡 <b>Rotas monitoradas:</b> "
                f"{resumo['rotas_monitoradas']}"
            ),
            "",
            (
                "🔴 <b>Quedas detectadas:</b> "
                f"{resumo['quedas_detectadas']}"
            ),
            "",
            (
                "🟢 <b>Recuperações:</b> "
                f"{resumo['recuperacoes']}"
            ),
            "",
            "⚠️ <b>ENDEREÇOS E ROTAS INDISPONÍVEIS</b>",
            "",
        ]

        indisponiveis = resumo[
            "enderecos_rotas_indisponiveis"
        ]

        if not indisponiveis:
            linhas.append(
                "Nenhum endereço indisponível."
            )
        else:
            for indice, item in enumerate(
                indisponiveis
            ):
                quantidade_rotas = len(
                    item["rotas"]
                )

                linhas.extend(
                    [
                        "🌐 <b>"
                        + self._texto(
                            item["endereco"]
                        )
                        + "</b>",
                        (
                            "🖥 <b>Servidores afetados:</b> "
                            + self._texto(
                                ", ".join(item["servidores"])
                                if item["servidores"]
                                else "Não informado"
                            )
                        ),
                        (
                            "📡 "
                            f"{quantidade_rotas} "
                            + (
                                "rota afetada:"
                                if quantidade_rotas == 1
                                else "rotas afetadas:"
                            )
                        ),
                    ]
                )

                for rota in item["rotas"]:
                    linhas.append(
                        "• " + self._texto(rota)
                    )

                linhas.extend(
                    [
                        "",
                        (
                            "⏱ <b>Indisponível há:</b> "
                            + self._formatar_duracao_horas(
                                item[
                                    "tempo_indisponivel_segundos"
                                ]
                            )
                        ),
                    ]
                )

                if indice < len(indisponiveis) - 1:
                    linhas.extend(["", ""])

        linhas.extend(
            [
                "",
                "",
                (
                    "🚫 <b>SERVIDORES INACESSÍVEIS: "
                    f"{len(resumo['servidores_inacessiveis'])}"
                    "</b>"
                ),
            ]
        )

        linhas.extend(
            self._formatar_lista(
                resumo["servidores_inacessiveis"]
            )
        )

        # O caractere invisível preserva uma linha extra no fim
        # do balão do Telegram, separando visualmente o Resumo
        # Diário da mensagem seguinte do Dashboard.
        linhas.extend(["", "\u200b"])

        return "\n".join(linhas)

    def _formatar_lista(self, valores):
        if not valores:
            return ["• Nenhum"]

        return [
            "• " + self._texto(valor)
            for valor in valores
        ]

    def _texto(self, valor):
        return self.telegram.escapar(
            valor
            if valor is not None
            else "Não informado"
        )

    @staticmethod
    def _eventos_do_periodo(
        eventos,
        inicio_periodo,
        fim_periodo,
    ):
        resultado = []

        for evento in eventos:
            caiu_em = (
                ResumoDiario._converter_data(
                    evento.get("caiu_em")
                )
            )

            voltou_em = (
                ResumoDiario._converter_data(
                    evento.get("voltou_em")
                )
            )

            if caiu_em is None:
                continue

            if caiu_em > fim_periodo:
                continue

            if (
                voltou_em is not None
                and voltou_em < inicio_periodo
            ):
                continue

            resultado.append(evento)

        return resultado

    @staticmethod
    def _duracao_no_periodo(
        evento,
        inicio_periodo,
        fim_periodo,
    ):
        caiu_em = (
            ResumoDiario._converter_data(
                evento.get("caiu_em")
            )
        )

        voltou_em = (
            ResumoDiario._converter_data(
                evento.get("voltou_em")
            )
        )

        if caiu_em is None:
            return 0

        inicio = max(
            caiu_em,
            inicio_periodo,
        )

        fim = min(
            voltou_em or fim_periodo,
            fim_periodo,
        )

        return max(
            0,
            int((fim - inicio).total_seconds()),
        )

    @staticmethod
    def _esta_no_periodo(
        valor,
        inicio_periodo,
        fim_periodo,
    ):
        data = ResumoDiario._converter_data(
            valor
        )

        if data is None:
            return False

        return (
            inicio_periodo
            <= data
            <= fim_periodo
        )

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

        except (
            ValueError,
            TypeError,
        ):
            return None

    @staticmethod
    def _converter_horario(valor):
        try:
            return datetime.strptime(
                valor,
                "%H:%M",
            ).time()

        except ValueError as erro:
            raise ValueError(
                "HORARIO_RESUMO_DIARIO deve "
                "estar no formato HH:MM."
            ) from erro

    def _obter_ultima_data(self):
        dados = self._ler_estado()

        if not dados:
            return None

        return (
            dados.get("ultima_data_enviada")
            or dados.get("ultima_data")
        )

    def _ler_estado(self):
        if not self.caminho_estado.exists():
            return {}

        try:
            return json.loads(
                self.caminho_estado.read_text(
                    encoding="utf-8"
                )
            )

        except (
            OSError,
            json.JSONDecodeError,
        ):
            return {}

    def _salvar_envio(
        self,
        data,
        horario_envio,
        caminho_relatorio,
    ):
        self.caminho_estado.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        caminho = Path(caminho_relatorio)

        dados = {
            "ultima_data_enviada": data,
            "horario_envio": (
                horario_envio.isoformat(
                    timespec="seconds"
                )
            ),
            "arquivo": caminho.name,
            "caminho_relatorio": str(caminho),
        }

        temporario = (
            self.caminho_estado.with_suffix(
                ".tmp"
            )
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
            self.caminho_estado
        )

    @staticmethod
    def _formatar_duracao_horas(segundos):
        """
        Formata duração contínua sem converter horas em dias.
        Ex.: 30 h 12 min, em vez de 1 dia 6 h 12 min.
        """
        segundos = max(
            0,
            int(segundos or 0),
        )

        horas, resto = divmod(
            segundos,
            3600,
        )

        minutos, segundos = divmod(
            resto,
            60,
        )

        partes = []

        if horas:
            partes.append(f"{horas} h")

        if minutos:
            partes.append(f"{minutos} min")

        if segundos or not partes:
            partes.append(f"{segundos} s")

        return " ".join(partes)

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
            partes.append(
                f"{horas} h"
            )

        if minutos:
            partes.append(
                f"{minutos} min"
            )

        if segundos or not partes:
            partes.append(
                f"{segundos} s"
            )

        return " ".join(partes)
