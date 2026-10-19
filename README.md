# TOI — Telecom Operational Intelligence

Plataforma de observabilidade para operações de contact center e telecom: coleta em tempo real as filas dos discadores, a ocupação de gateways e troncos SIP e o estado dos SBCs, correlaciona tudo numa linha do tempo única e transforma o resultado em diagnóstico, alertas e recomendações para a operação.

> **Projeto de portfólio.** Todos os nomes de empresas, clientes, operadoras, hostnames, IPs, filas e números deste repositório são **fictícios**. A pasta [`demo/`](demo/) gera dados sintéticos para rodar o painel sem nenhuma integração externa.

![Painel — visão geral](docs/visao-geral.png)

## O que o TOI faz

- **Visão executiva** — pressão operacional e capacidade por instância, com score, faixa de risco e cobertura das fontes.
- **Filas e discador** — P34 (sem circuito), P135 (congestionamento), hit rate, chamadas em discagem e em atendimento, amostradas a cada 10 s nas filas prioritárias.
- **Infraestrutura** — ocupação de gateways e grupos de troncos, canais travados, inventário de SIP servers e contadores de sessões por realm nos SBCs (SNMP e ACLI, somente leitura).
- **Linha do tempo sincronizada** — todas as métricas no mesmo eixo temporal, com marcação de eventos e zoom.
- **Inteligência** — diagnóstico automático, correlações, padrões e recorrências, análise de capacidade e recomendações.
- **ML em modo shadow** — modelo (scikit-learn) que prevê degradações em observação, gravando previsões sem alterar alertas; só é liberado após homologação de frescor e fidelidade com manifesto de hashes da aquisição.
- **Alertas** — confirmação de incidentes com janela de normalização, notificações via Telegram, receptor de traps SNMP e abertura de chamados (integração com Redmine).
- **Assistente local** — perguntas em linguagem natural sobre os dados do painel, respondidas por um LLM local (Ollama).
- **Gestão de acesso** — usuários com Argon2id, papéis (Administrador, Gestor, Operador, Consulta), sessões com CSRF e trilha de auditoria.

## Arquitetura

```
 Portal do discador ─┐                       ┌──────────────────────────┐
 (HTTP/JSON)         │   coletor dual-lane   │ SQLite (WAL)             │
 SBCs (SNMP / ACLI) ─┼──► EDF + deadlines ──►│ queue / infra / dialer   │──► painel HTTP ──► Nginx /toi/
 Gateways (ICMP)    ─┘   supervisor + guard  │ ml_shadow_prediction     │    + API JSON
                                             └──────────────────────────┘
                              │                         │
                       alertas / traps            ML shadow, health,
                       Telegram / Redmine         backup e relatórios
```

- **Coletor dual-lane** — agendador *earliest-deadline-first* com deadlines absolutos (sem deriva nem *catch-up storm*), lanes independentes para filas críticas, política adaptativa por instância, *circuit breaker* e cooldown por endpoint.
- **Supervisor** — reinicia componentes travados com limite de três reinícios em dez minutos e acompanha o avanço real de `queue_snapshot`, não apenas o processo.
- **Painel** — servidor HTTP em Python puro + front-end em JavaScript sem framework.
- **Operação** — unidades systemd com *hardening* (`ProtectSystem=strict`, `NoNewPrivileges`, capabilities mínimas), timers de health check e backup diário com retenção.

<p>
  <img src="docs/login.png" width="49%" alt="Tela de login">
  <img src="docs/gateways.png" width="49%" alt="Ocupação dos gateways">
</p>

## Stack

Python 3.12 · SQLite · pandas · scikit-learn · pysnmp · requests · Playwright · Argon2 · JavaScript · HTML/CSS · Nginx · systemd · Ollama

## Rodando a demonstração

```bash
python -m venv .venv
.venv/Scripts/activate        # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
python demo/run_demo.py
```

Acesse **http://127.0.0.1:8099/toi/login** com o usuário `demo`. A senha de demonstração está em [`demo/seed_demo.py`](demo/seed_demo.py).

O script gera três horas de histórico fictício, continua gravando novas amostras a cada 10 segundos e publica o painel sob o prefixo `/toi/`, como o Nginx faz em produção.

## Testes

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Estrutura

| Caminho | Conteúdo |
|---|---|
| `TOI_v1.10.6_DUAL_LANE_ML_SHADOW/` | Supervisor, coletor, painel, ML shadow e front-end (`static/`) |
| `TOI_v1.10.6.3.7.4.1_CONTINUOUS_ACTIVATION_R2/` | Inteligência de gateways em modo shadow |
| `TOI_INTELLIGENCE_READONLY_v1_2/` | Snapshot de saúde do sistema |
| `mgw/` | Media gateways e SBCs: SNMP, ACLI, capacidade SIP, relatórios e chamados |
| `monitor_alertas.py`, `integracao_alertas.py` | Motor de alertas e confirmação de incidentes |
| `monitor_portal_orbe_v1_gerencial.py` | Cliente do portal do discador e relatórios gerenciais |
| `deploy/` | Unidades systemd, Nginx, publicação, rollback e smoke tests |
| `demo/` | Gerador de dados fictícios e execução local |
| `tests/` | Testes unitários |

## Configuração

As credenciais ficam fora do repositório, num arquivo de ambiente lido pelas unidades systemd. Veja [`.env.example`](.env.example).
