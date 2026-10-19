"""
Integração Ticketdesk/Redmine para o MGWMonitor.

V2.2 - modo seguro / padronização Discadores:
- consultas GET reais;
- nenhuma escrita quando dry_run=True;
- chave diária: DATA + FILA (um chamado por fila por dia);
- P34 e P135 compartilham o mesmo chamado diário da fila;
- identifica ação: CRIAR, REUTILIZAR ou REABRIR;
- payload de criação/atualização é montado para auditoria.

IDs confirmados em chamados reais do projeto Discadores:
Projeto 321 / Tracker 1 / Assigned 1945.\nCategorias: P135=1318 / P34=1447.
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


PROJECT_ID = 321
TRACKER_ID = 1
ASSIGNED_TO_ID = 1945
CATEGORY_P135_ID = 1318  # Fornecedor - Congest. Interno
CATEGORY_P34_ID = 1447   # Fornecedor - Congestionamento ALFA

STATUS_NOVA_ID = 1
STATUS_REABERTA_ID = 11

PRIORITY_URGENTE_ID = 4
PRIORITY_ALTO_IMPACTO_ID = 10

CF_IMPACTO = 125
CF_EMPRESA = 6
CF_EMAIL = 2
CF_TELEFONE = 1
CF_SITE = 18
CF_CLIENTE = 3
CF_CELULA_PRODUTO = 184
CF_CAMPANHA_FALHA = 180
CF_RAMAL = 186
CF_CLIENTES = 743

IMPACTO_CRITICO = "Acima de 90% Prioridade (Crítica)"

# Padrão operacional definido para abertura automática em Discadores.
EMPRESA_FIXA = "Acme"
SITE_FIXO_ID = "125"
SITE_FIXO_NOME = "Data Center - Norte"
CLIENTE_FIXO = "TI"
CLIENTES_FIXO_ID = "228"
CLIENTES_FIXO_NOME = "ALFA"
EMAIL_FIXO = "operacao@example.com"
TELEFONE_FIXO = "11900000000"
RAMAL_FIXO = "00000"

TZ_BR = timezone(timedelta(hours=-3))


def _norm(txt: Any) -> str:
    txt = str(txt or "").strip()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    txt = re.sub(r"\s+", " ", txt)
    return txt.upper()


def _bool_env(nome: str, padrao: bool) -> bool:
    valor = os.getenv(nome)
    if valor is None:
        return padrao
    return valor.strip().lower() in {"1", "true", "yes", "sim", "on"}


def _parse_dt_redmine(valor: Optional[str]) -> Optional[datetime]:
    if not valor:
        return None
    try:
        dt = datetime.fromisoformat(valor.replace("Z", "+00:00"))
        return dt.astimezone(TZ_BR)
    except ValueError:
        return None


def _dia_local(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(TZ_BR).strftime("%Y-%m-%d")


@dataclass
class OperacaoTicketdesk:
    empresa: str
    site: str
    cliente: str
    clientes_id: str
    celula_produto: str
    campanha_falha: str
    email: str
    telefone: str
    ramal: str
    origem: str = "MAPEAMENTO_EXPLICITO"
    issue_referencia_id: Optional[int] = None


@dataclass
class ResultadoTicket:
    acao: str
    issue_id: Optional[int]
    motivo: str
    marcador: str
    issue_existente: Optional[Dict[str, Any]]
    payload: Optional[Dict[str, Any]]
    campos_faltantes: List[str]
    dry_run: bool
    operacao_origem: Optional[str] = None
    operacao_issue_referencia_id: Optional[int] = None


class TicketdeskClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        dry_run: Optional[bool] = None,
        timeout: int = 20,
    ):
        self.base_url = (
            base_url
            or os.getenv(
                "TICKETDESK_BASE_URL",
                "https://tickets.example.com",
            )
        ).strip().rstrip("/")

        self.api_key = (
            api_key
            or os.getenv("TICKETDESK_API_KEY", "")
        ).strip()

        if not self.api_key:
            raise RuntimeError(
                "TICKETDESK_API_KEY não configurada."
            )

        self.dry_run = (
            _bool_env("TICKETDESK_DRY_RUN", True)
            if dry_run is None
            else bool(dry_run)
        )
        self.timeout = int(timeout)

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Redmine-API-Key": self.api_key,
            "User-Agent": "MGWMonitor-Ticketdesk/2.2",
        })

    # ---------------------------------------------------------
    # HTTP
    # ---------------------------------------------------------
    def _get(self, caminho: str, params=None) -> Dict[str, Any]:
        r = self.session.get(
            self.base_url + caminho,
            params=params,
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def _post(self, caminho: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.dry_run:
            raise RuntimeError(
                "POST bloqueado: Ticketdesk está em DRY-RUN."
            )
        r = self.session.post(
            self.base_url + caminho,
            json=payload,
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json() if r.content else {}

    def _put(self, caminho: str, payload: Dict[str, Any]) -> None:
        if self.dry_run:
            raise RuntimeError(
                "PUT bloqueado: Ticketdesk está em DRY-RUN."
            )
        r = self.session.put(
            self.base_url + caminho,
            json=payload,
            timeout=self.timeout,
        )
        r.raise_for_status()

    # ---------------------------------------------------------
    # Mapeamento operacional
    # ---------------------------------------------------------
    @staticmethod
    def carregar_mapeamento(
        caminho: str | Path = "dados/ticketdesk_operacoes.json",
    ) -> Dict[str, Any]:
        p = Path(caminho)
        if not p.exists():
            return {}
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def localizar_operacao(
        mapeamento: Dict[str, Any],
        *,
        projeto_id_orbe: Any,
        projeto_nome: str,
        fila_nome: str,
        fila_codigo: str,
    ) -> Optional[OperacaoTicketdesk]:
        regras = mapeamento.get("regras", []) or []

        candidatos = [
            str(projeto_id_orbe or ""),
            projeto_nome,
            fila_nome,
            fila_codigo,
        ]
        alvo = " | ".join(_norm(x) for x in candidatos)

        for regra in regras:
            termos = regra.get("contem", []) or []
            if termos and not all(_norm(t) in alvo for t in termos):
                continue

            dados = regra.get("campos", {}) or {}
            return OperacaoTicketdesk(
                empresa=str(dados.get("empresa", "")),
                site=str(dados.get("site", "")),
                cliente=str(dados.get("cliente", "")),
                clientes_id=str(dados.get("clientes_id", "")),
                celula_produto=str(
                    dados.get("celula_produto", projeto_nome)
                ),
                campanha_falha=str(
                    dados.get("campanha_falha", projeto_nome)
                ),
                email=str(dados.get("email", "")),
                telefone=str(dados.get("telefone", "")),
                ramal=str(dados.get("ramal", "")),
                origem="MAPEAMENTO_EXPLICITO",
                issue_referencia_id=None,
            )
        return None

    # ---------------------------------------------------------
    # Descoberta segura de operação por histórico
    # ---------------------------------------------------------
    @staticmethod
    def _custom_fields_dict(issue: Dict[str, Any]) -> Dict[int, Any]:
        saida: Dict[int, Any] = {}
        for campo in issue.get("custom_fields", []) or []:
            try:
                campo_id = int(campo.get("id"))
            except (TypeError, ValueError):
                continue
            saida[campo_id] = campo.get("value")
        return saida

    @staticmethod
    def _valor_cf(
        campos: Dict[int, Any],
        campo_id: int,
    ) -> str:
        valor = campos.get(campo_id, "")
        if isinstance(valor, list):
            return ",".join(
                str(x) for x in valor if str(x).strip()
            )
        if valor is None:
            return ""
        return str(valor).strip()

    @classmethod
    def operacao_por_issue(
        cls,
        issue: Dict[str, Any],
    ) -> OperacaoTicketdesk:
        campos = cls._custom_fields_dict(issue)

        return OperacaoTicketdesk(
            empresa=cls._valor_cf(campos, CF_EMPRESA),
            site=cls._valor_cf(campos, CF_SITE),
            cliente=cls._valor_cf(campos, CF_CLIENTE),
            clientes_id=cls._valor_cf(campos, CF_CLIENTES),
            celula_produto=cls._valor_cf(
                campos,
                CF_CELULA_PRODUTO,
            ),
            campanha_falha=cls._valor_cf(
                campos,
                CF_CAMPANHA_FALHA,
            ),
            email=cls._valor_cf(campos, CF_EMAIL),
            telefone=cls._valor_cf(campos, CF_TELEFONE),
            ramal=cls._valor_cf(campos, CF_RAMAL),
            origem="CHAMADO_HISTORICO",
            issue_referencia_id=(
                int(issue["id"])
                if issue.get("id") is not None
                else None
            ),
        )

    @staticmethod
    def campos_operacao_faltantes(
        operacao: Optional[OperacaoTicketdesk],
    ) -> List[str]:
        if operacao is None:
            return ["mapeamento da operação"]

        obrigatorios = {
            "empresa": operacao.empresa,
            "site": operacao.site,
            "cliente": operacao.cliente,
            "clientes_id": operacao.clientes_id,
            "celula_produto": operacao.celula_produto,
            "campanha_falha": operacao.campanha_falha,
            "email": operacao.email,
            "telefone": operacao.telefone,
            "ramal": operacao.ramal,
        }

        return [
            nome
            for nome, valor in obrigatorios.items()
            if not str(valor or "").strip()
        ]

    def descobrir_operacao_por_historico(
        self,
        *,
        fila_codigo: str,
        fila_nome: str,
        project_id: int = PROJECT_ID,
        limite_total: int = 500,
    ) -> Optional[OperacaoTicketdesk]:
        """
        Procura um chamado anterior da MESMA fila no projeto Discadores.

        Segurança:
        - exige o código exato da fila no chamado;
        - exige todos os campos operacionais obrigatórios;
        - escolhe o chamado completo mais recente.
        """
        codigo_n = _norm(fila_codigo)
        candidatos: List[
            Tuple[datetime, OperacaoTicketdesk]
        ] = []

        for resumo in self.listar_issues_recentes(
            project_id=project_id,
            limite_total=limite_total,
        ):
            issue_id = resumo.get("id")
            if not issue_id:
                continue

            issue = self.obter_issue(int(issue_id))

            projeto = issue.get("project") or {}
            if int(projeto.get("id") or 0) != int(project_id):
                continue

            blob = "\n".join([
                str(issue.get("subject") or ""),
                str(issue.get("description") or ""),
                "\n".join(
                    str(j.get("notes") or "")
                    for j in (issue.get("journals", []) or [])
                ),
            ])

            if not codigo_n or codigo_n not in _norm(blob):
                continue

            operacao = self.operacao_por_issue(issue)
            if self.campos_operacao_faltantes(operacao):
                continue

            criado = _parse_dt_redmine(issue.get("created_on"))
            if criado is None:
                criado = datetime.min.replace(tzinfo=TZ_BR)

            candidatos.append((criado, operacao))

        if not candidatos:
            return None

        candidatos.sort(
            key=lambda item: item[0],
            reverse=True,
        )
        return candidatos[0][1]

    def resolver_operacao(
        self,
        *,
        mapeamento: Dict[str, Any],
        projeto_id_orbe: Any,
        projeto_nome: str,
        fila_nome: str,
        fila_codigo: str,
    ) -> Optional[OperacaoTicketdesk]:
        operacao = self.localizar_operacao(
            mapeamento,
            projeto_id_orbe=projeto_id_orbe,
            projeto_nome=projeto_nome,
            fila_nome=fila_nome,
            fila_codigo=fila_codigo,
        )
        if operacao is not None:
            return operacao

        return self.descobrir_operacao_por_historico(
            fila_codigo=fila_codigo,
            fila_nome=fila_nome,
        )

    # ---------------------------------------------------------
    # Regra diária
    # ---------------------------------------------------------
    @staticmethod
    def marcador_diario(
        data_local: str,
        instancia: Any,
        fila_codigo: str,
    ) -> str:
        # Deliberadamente SEM P34/P135:
        # um único chamado por dia para a fila.
        return (
            f"MGW-ORBE-ID: {data_local}|"
            f"{str(instancia)}|{str(fila_codigo)}"
        )

    def listar_issues_recentes(
        self,
        *,
        project_id: int = PROJECT_ID,
        limite_total: int = 300,
    ) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        offset = 0
        page = 100

        while len(issues) < limite_total:
            payload = self._get(
                "/issues.json",
                params={
                    "project_id": project_id,
                    "status_id": "*",
                    "sort": "created_on:desc",
                    "limit": page,
                    "offset": offset,
                },
            )

            lote = payload.get("issues", []) or []
            issues.extend(lote)

            total = int(
                payload.get("total_count", len(issues)) or 0
            )
            if not lote or len(issues) >= total:
                break

            offset += page

        return issues[:limite_total]

    def obter_issue(self, issue_id: int) -> Dict[str, Any]:
        return (
            self._get(
                f"/issues/{int(issue_id)}.json",
                params={"include": "journals"},
            )
            .get("issue", {})
        )

    def localizar_chamado_do_dia(
        self,
        *,
        data_local: str,
        instancia: Any,
        fila_codigo: str,
        fila_nome: str,
    ) -> Optional[Dict[str, Any]]:
        marcador = self.marcador_diario(
            data_local,
            instancia,
            fila_codigo,
        )
        codigo_n = _norm(fila_codigo)
        nome_n = _norm(fila_nome)

        for resumo in self.listar_issues_recentes():
            criado = _parse_dt_redmine(resumo.get("created_on"))
            if _dia_local(criado) != data_local:
                continue

            # Filtro rápido pelo resumo.
            assunto = _norm(resumo.get("subject"))
            if codigo_n not in assunto and nome_n not in assunto:
                # Mesmo que o código não esteja no título, ele pode estar
                # na descrição; busca o detalhe apenas para candidatos do dia.
                pass

            issue = self.obter_issue(int(resumo["id"]))
            blob = "\n".join([
                str(issue.get("subject") or ""),
                str(issue.get("description") or ""),
                "\n".join(
                    str(j.get("notes") or "")
                    for j in (issue.get("journals", []) or [])
                ),
            ])

            if marcador in blob:
                return issue

            # Compatibilidade com chamados manuais do mesmo dia:
            # se o código exato da fila estiver no conteúdo, reutilizamos.
            if codigo_n and codigo_n in _norm(blob):
                return issue

        return None

    # ---------------------------------------------------------
    # Tipo de congestionamento / prioridade
    # ---------------------------------------------------------
    @staticmethod
    def tipo_congestionamento(
        p34_acima: bool,
        p135_acima: bool,
    ) -> str:
        if p34_acima and p135_acima:
            return (
                "Congestionamento na Companhia (P34) e "
                "Congestionamento Interno (P135)"
            )
        if p34_acima:
            return "Congestionamento na Companhia (P34)"
        if p135_acima:
            return "Congestionamento Interno (P135)"
        raise ValueError(
            "Ticketdesk só deve ser acionado para P34/P135 acima do limite."
        )

    @staticmethod
    def prioridade_id(
        p34_acima: bool,
        p135_acima: bool,
    ) -> int:
        # Regra operacional V2.2:
        # todos os chamados automáticos são Alto Impacto.
        return PRIORITY_ALTO_IMPACTO_ID

    @staticmethod
    def categoria_id(
        p34_acima: bool,
        p135_acima: bool,
    ) -> int:
        # Se as duas condições estiverem acima simultaneamente,
        # P135 prevalece como categoria inicial; a descrição registra ambas.
        if p135_acima:
            return CATEGORY_P135_ID
        if p34_acima:
            return CATEGORY_P34_ID
        raise ValueError(
            "Nenhuma categoria aplicável: P34/P135 abaixo do limite."
        )

    @staticmethod
    def categoria_nome(
        p34_acima: bool,
        p135_acima: bool,
    ) -> str:
        if p135_acima:
            return "Fornecedor - Congest. Interno"
        if p34_acima:
            return "Fornecedor - Congestionamento ALFA"
        raise ValueError(
            "Nenhuma categoria aplicável: P34/P135 abaixo do limite."
        )

    # ---------------------------------------------------------
    # Payload
    # ---------------------------------------------------------
    def montar_payload_criacao(
        self,
        *,
        data_local: str,
        instancia: Any,
        projeto_id_orbe: Any,
        projeto_nome: str,
        fila_nome: str,
        fila_codigo: str,
        p34_percentual: float,
        p34_quantidade: int,
        p135_percentual: float,
        p135_quantidade: int,
        limite_p34: float,
        limite_p135: float,
        confirmacao_min: int,
        inicio_incidente: str,
        operacao: Optional[OperacaoTicketdesk],
    ) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        # Em V2.2, os dados do formulário são padronizados.
        # 'operacao' permanece no contrato por compatibilidade,
        # mas não é mais necessário para a criação.
        p34_acima = float(p34_percentual) > float(limite_p34)
        p135_acima = float(p135_percentual) > float(limite_p135)

        tipo = self.tipo_congestionamento(
            p34_acima,
            p135_acima,
        )

        marcador = self.marcador_diario(
            data_local,
            instancia,
            fila_codigo,
        )

        if p34_acima and p135_acima:
            titulo_tipo = "CONGESTIONAMENTO P34 E P135"
        elif p34_acima:
            titulo_tipo = "CONGESTIONAMENTO NA COMPANHIA"
        else:
            titulo_tipo = "CONGESTIONAMENTO INTERNO"

        subject = (
            f"{titulo_tipo} - {fila_nome} ({fila_codigo})"
        )

        celula_produto = (
            f"{projeto_id_orbe} - {projeto_nome}"
        )
        campanha_falha = (
            f"{fila_nome} ({fila_codigo})"
        )

        categoria_id = self.categoria_id(
            p34_acima,
            p135_acima,
        )
        categoria_nome = self.categoria_nome(
            p34_acima,
            p135_acima,
        )

        descricao = "\n".join([
            "Incidente identificado automaticamente pelo MGW Monitor.",
            "",
            f"Instância: {instancia}",
            f"Projeto: {celula_produto}",
            f"Fila: {campanha_falha}",
            f"Classificação: {tipo}",
            f"Categoria Ticketdesk: {categoria_nome}",
            "",
            (
                "Congestionamento na Companhia (P34): "
                f"{float(p34_percentual):.2f}% "
                f"({int(p34_quantidade)} chamadas)"
            ),
            (
                "Congestionamento Interno (P135): "
                f"{float(p135_percentual):.2f}% "
                f"({int(p135_quantidade)} chamadas)"
            ),
            "",
            (
                "Limite Congestionamento na Companhia (P34): "
                f"{float(limite_p34):.2f}%"
            ),
            (
                "Limite Congestionamento Interno (P135): "
                f"{float(limite_p135):.2f}%"
            ),
            (
                "Critério de confirmação: "
                f"{int(confirmacao_min)} minutos contínuos."
            ),
            f"Início observado do incidente: {inicio_incidente}",
            "",
            marcador,
        ])

        custom_fields = [
            {"id": CF_IMPACTO, "value": IMPACTO_CRITICO},
            {"id": CF_EMPRESA, "value": EMPRESA_FIXA},
            {"id": CF_EMAIL, "value": EMAIL_FIXO},
            {"id": CF_TELEFONE, "value": TELEFONE_FIXO},
            {"id": CF_SITE, "value": SITE_FIXO_ID},
            {"id": CF_CLIENTE, "value": CLIENTE_FIXO},
            {
                "id": CF_CELULA_PRODUTO,
                "value": celula_produto,
            },
            {
                "id": CF_CAMPANHA_FALHA,
                "value": campanha_falha,
            },
            {"id": CF_RAMAL, "value": RAMAL_FIXO},
            {"id": CF_CLIENTES, "value": CLIENTES_FIXO_ID},
        ]

        payload = {
            "issue": {
                "project_id": PROJECT_ID,
                "tracker_id": TRACKER_ID,
                "status_id": STATUS_NOVA_ID,
                "priority_id": PRIORITY_ALTO_IMPACTO_ID,
                "assigned_to_id": ASSIGNED_TO_ID,
                "category_id": categoria_id,
                "subject": subject,
                "description": descricao,
                "custom_fields": custom_fields,
            }
        }

        return payload, []

    # ---------------------------------------------------------
    # Resolver / executar
    # ---------------------------------------------------------
    def resolver_incidente(
        self,
        *,
        data_local: str,
        instancia: Any,
        projeto_id_orbe: Any,
        projeto_nome: str,
        fila_nome: str,
        fila_codigo: str,
        p34_percentual: float,
        p34_quantidade: int,
        p135_percentual: float,
        p135_quantidade: int,
        limite_p34: float,
        limite_p135: float,
        confirmacao_min: int,
        inicio_incidente: str,
        mapeamento_operacoes: Dict[str, Any],
    ) -> ResultadoTicket:
        marcador = self.marcador_diario(
            data_local,
            instancia,
            fila_codigo,
        )

        existente = self.localizar_chamado_do_dia(
            data_local=data_local,
            instancia=instancia,
            fila_codigo=fila_codigo,
            fila_nome=fila_nome,
        )

        operacao = self.resolver_operacao(
            mapeamento=mapeamento_operacoes,
            projeto_id_orbe=projeto_id_orbe,
            projeto_nome=projeto_nome,
            fila_nome=fila_nome,
            fila_codigo=fila_codigo,
        )

        payload, faltantes = self.montar_payload_criacao(
            data_local=data_local,
            instancia=instancia,
            projeto_id_orbe=projeto_id_orbe,
            projeto_nome=projeto_nome,
            fila_nome=fila_nome,
            fila_codigo=fila_codigo,
            p34_percentual=p34_percentual,
            p34_quantidade=p34_quantidade,
            p135_percentual=p135_percentual,
            p135_quantidade=p135_quantidade,
            limite_p34=limite_p34,
            limite_p135=limite_p135,
            confirmacao_min=confirmacao_min,
            inicio_incidente=inicio_incidente,
            operacao=operacao,
        )

        if existente:
            status = existente.get("status") or {}
            issue_id = int(existente["id"])
            is_closed = bool(status.get("is_closed"))

            if is_closed:
                return ResultadoTicket(
                    acao="REABRIR",
                    issue_id=issue_id,
                    motivo=(
                        "Já existe chamado da fila neste dia, "
                        "mas ele está fechado."
                    ),
                    marcador=marcador,
                    issue_existente=existente,
                    payload={
                        "issue": {
                            "status_id": STATUS_REABERTA_ID,
                            "notes": (
                                "MGW Monitor: nova ocorrência confirmada "
                                "para a mesma fila no mesmo dia.\n"
                                f"{marcador}"
                            ),
                        }
                    },
                    campos_faltantes=[],
                    dry_run=self.dry_run,
                    operacao_origem=(
                        operacao.origem if operacao else None
                    ),
                    operacao_issue_referencia_id=(
                        operacao.issue_referencia_id
                        if operacao else None
                    ),
                )

            return ResultadoTicket(
                acao="REUTILIZAR",
                issue_id=issue_id,
                motivo=(
                    "Já existe chamado aberto da fila neste dia."
                ),
                marcador=marcador,
                issue_existente=existente,
                payload={
                    "issue": {
                        "notes": (
                            "MGW Monitor: nova ocorrência confirmada "
                            "associada ao chamado diário existente.\n"
                            f"{marcador}"
                        )
                    }
                },
                campos_faltantes=[],
                dry_run=self.dry_run,
                operacao_origem=(
                    operacao.origem if operacao else None
                ),
                operacao_issue_referencia_id=(
                    operacao.issue_referencia_id
                    if operacao else None
                ),
            )

        if faltantes:
            return ResultadoTicket(
                acao="BLOQUEADO",
                issue_id=None,
                motivo=(
                    "Nenhum chamado existente foi encontrado, mas faltam "
                    "campos obrigatórios para criar o chamado."
                ),
                marcador=marcador,
                issue_existente=None,
                payload=payload,
                campos_faltantes=faltantes,
                dry_run=self.dry_run,
                operacao_origem=(
                    operacao.origem if operacao else None
                ),
                operacao_issue_referencia_id=(
                    operacao.issue_referencia_id
                    if operacao else None
                ),
            )

        return ResultadoTicket(
            acao="CRIAR",
            issue_id=None,
            motivo=(
                "Nenhum chamado da fila foi encontrado neste dia."
            ),
            marcador=marcador,
            issue_existente=None,
            payload=payload,
            campos_faltantes=[],
            dry_run=self.dry_run,
            operacao_origem=(
                operacao.origem if operacao else None
            ),
            operacao_issue_referencia_id=(
                operacao.issue_referencia_id
                if operacao else None
            ),
        )

    def aplicar_resultado(
        self,
        resultado: ResultadoTicket,
    ) -> Optional[int]:
        if self.dry_run:
            return resultado.issue_id

        if resultado.acao == "CRIAR":
            resposta = self._post(
                "/issues.json",
                resultado.payload or {},
            )
            issue = resposta.get("issue", {}) or {}
            return int(issue["id"])

        if resultado.acao in {"REABRIR", "REUTILIZAR"}:
            if not resultado.issue_id:
                raise RuntimeError(
                    "Resultado sem issue_id para atualização."
                )
            self._put(
                f"/issues/{resultado.issue_id}.json",
                resultado.payload or {},
            )
            return int(resultado.issue_id)

        if resultado.acao == "BLOQUEADO":
            raise RuntimeError(
                "Criação bloqueada por campos obrigatórios ausentes: "
                + ", ".join(resultado.campos_faltantes)
            )

        raise RuntimeError(
            f"Ação Ticketdesk desconhecida: {resultado.acao}"
        )
