import sqlite3
from datetime import datetime
from pathlib import Path


class BancoDados:
    def __init__(self, caminho="dados/mgw_monitor.db"):
        self.caminho = Path(caminho)

        self.caminho.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._criar_tabelas()

    def _conectar(self):
        conexao = sqlite3.connect(self.caminho)
        conexao.row_factory = sqlite3.Row

        return conexao

    def _criar_tabelas(self):
        with self._conectar() as conexao:
            conexao.execute(
                """
                CREATE TABLE IF NOT EXISTS coletas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_hora TEXT NOT NULL
                )
                """
            )

            conexao.execute(
                """
                CREATE TABLE IF NOT EXISTS registros_naps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coleta_id INTEGER NOT NULL,
                    servidor TEXT NOT NULL,
                    nap_id INTEGER,
                    nome TEXT NOT NULL,
                    endereco TEXT,
                    ativo INTEGER NOT NULL,
                    tipo INTEGER,
                    busy INTEGER NOT NULL,
                    idle INTEGER NOT NULL,
                    fail INTEGER NOT NULL,
                    FOREIGN KEY (coleta_id)
                        REFERENCES coletas(id)
                )
                """
            )

            conexao.execute(
                """
                CREATE TABLE IF NOT EXISTS
                eventos_indisponibilidade (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    servidor TEXT NOT NULL,
                    nap_id INTEGER,
                    rota TEXT NOT NULL,
                    endereco TEXT,
                    caiu_em TEXT NOT NULL,
                    voltou_em TEXT,
                    duracao_segundos INTEGER,
                    status TEXT NOT NULL DEFAULT 'ABERTO'
                )
                """
            )

            conexao.execute(
                """
                CREATE TABLE IF NOT EXISTS
                eventos_servidores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    servidor TEXT NOT NULL,
                    endereco TEXT NOT NULL,
                    caiu_em TEXT NOT NULL,
                    voltou_em TEXT,
                    duracao_segundos INTEGER,
                    falhas_consecutivas INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ABERTO'
                )
                """
            )

            conexao.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_registros_coleta
                ON registros_naps(coleta_id)
                """
            )

            conexao.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_registros_identidade
                ON registros_naps(
                    servidor,
                    nome,
                    endereco
                )
                """
            )

            conexao.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_eventos_identidade
                ON eventos_indisponibilidade(
                    servidor,
                    rota,
                    endereco,
                    status
                )
                """
            )

            conexao.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_eventos_servidores
                ON eventos_servidores(
                    servidor,
                    status
                )
                """
            )

    def salvar_coleta(self, naps):
        data_hora = datetime.now().isoformat(
            timespec="seconds"
        )

        with self._conectar() as conexao:
            cursor = conexao.execute(
                """
                INSERT INTO coletas (data_hora)
                VALUES (?)
                """,
                (data_hora,),
            )

            coleta_id = cursor.lastrowid

            registros = []

            for nap in naps:
                registros.append(
                    (
                        coleta_id,
                        nap.servidor,
                        nap.id,
                        nap.nome,
                        nap.endereco(),
                        1 if nap.esta_ativo() else 0,
                        nap.tipo,
                        nap.busy,
                        nap.idle,
                        nap.fail,
                    )
                )

            conexao.executemany(
                """
                INSERT INTO registros_naps (
                    coleta_id,
                    servidor,
                    nap_id,
                    nome,
                    endereco,
                    ativo,
                    tipo,
                    busy,
                    idle,
                    fail
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                registros,
            )

        return coleta_id

    def obter_coleta_anterior(self, coleta_atual_id):
        with self._conectar() as conexao:
            resultado = conexao.execute(
                """
                SELECT id
                FROM coletas
                WHERE id < ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (coleta_atual_id,),
            ).fetchone()

        if resultado is None:
            return None

        return resultado["id"]

    def comparar_coletas(
        self,
        coleta_anterior_id,
        coleta_atual_id,
    ):
        with self._conectar() as conexao:
            resultados = conexao.execute(
                """
                SELECT
                    atual.servidor,
                    atual.nap_id,
                    atual.nome,
                    atual.endereco,
                    anterior.ativo AS ativo_anterior,
                    atual.ativo AS ativo_atual,
                    anterior.busy AS busy_anterior,
                    atual.busy AS busy_atual,
                    anterior.idle AS idle_anterior,
                    atual.idle AS idle_atual,
                    anterior.fail AS fail_anterior,
                    atual.fail AS fail_atual
                FROM registros_naps AS atual
                INNER JOIN registros_naps AS anterior
                    ON anterior.servidor = atual.servidor
                    AND anterior.nome = atual.nome
                    AND COALESCE(anterior.endereco, '')
                        = COALESCE(atual.endereco, '')
                WHERE atual.coleta_id = ?
                    AND anterior.coleta_id = ?
                    AND anterior.ativo != atual.ativo
                ORDER BY
                    atual.servidor,
                    atual.nome
                """,
                (
                    coleta_atual_id,
                    coleta_anterior_id,
                ),
            ).fetchall()

        novos_inativos = []
        recuperados = []

        for registro in resultados:
            item = {
                "servidor": registro["servidor"],
                "nap_id": registro["nap_id"],
                "nome": registro["nome"],
                "endereco": registro["endereco"],
                "ativo_anterior": bool(
                    registro["ativo_anterior"]
                ),
                "ativo_atual": bool(
                    registro["ativo_atual"]
                ),
                "busy_anterior": registro[
                    "busy_anterior"
                ],
                "busy_atual": registro[
                    "busy_atual"
                ],
                "idle_anterior": registro[
                    "idle_anterior"
                ],
                "idle_atual": registro[
                    "idle_atual"
                ],
                "fail_anterior": registro[
                    "fail_anterior"
                ],
                "fail_atual": registro[
                    "fail_atual"
                ],
            }

            if (
                item["ativo_anterior"]
                and not item["ativo_atual"]
            ):
                novos_inativos.append(item)

            elif (
                not item["ativo_anterior"]
                and item["ativo_atual"]
            ):
                recuperados.append(item)

        return {
            "novos_inativos": novos_inativos,
            "recuperados": recuperados,
        }

    def obter_ultimo_estado_rotas(self):
        """
        Obtém a última situação conhecida de cada rota.

        Isso permite reiniciar o monitor sem produzir
        falsos alertas para todas as rotas.
        """
        with self._conectar() as conexao:
            ultima_coleta = conexao.execute(
                """
                SELECT id
                FROM coletas
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()

            if ultima_coleta is None:
                return {}

            registros = conexao.execute(
                """
                SELECT
                    servidor,
                    nap_id,
                    nome,
                    endereco,
                    ativo,
                    tipo,
                    busy,
                    idle,
                    fail
                FROM registros_naps
                WHERE coleta_id = ?
                """,
                (ultima_coleta["id"],),
            ).fetchall()

        estado = {}

        for registro in registros:
            chave = self._criar_chave_rota(
                servidor=registro["servidor"],
                nome=registro["nome"],
                endereco=registro["endereco"],
            )

            estado[chave] = {
                "servidor": registro["servidor"],
                "nap_id": registro["nap_id"],
                "nome": registro["nome"],
                "endereco": registro["endereco"],
                "ativo": bool(registro["ativo"]),
                "tipo": registro["tipo"],
                "busy": registro["busy"],
                "idle": registro["idle"],
                "fail": registro["fail"],
            }

        return estado

    def _obter_inicio_indisponibilidade_historico(
        self,
        conexao,
        servidor,
        rota,
        endereco,
    ):
        """
        Retorna o início mais antigo da indisponibilidade
        atual usando o histórico de coletas.

        A regra é: localizar a última coleta em que a rota
        esteve ativa e, depois dela, pegar a primeira coleta
        em que a rota apareceu inativa. Se a rota nunca esteve
        ativa no banco, usa a primeira coleta inativa conhecida.
        """
        endereco = endereco or ""

        ultima_ativa = conexao.execute(
            """
            SELECT MAX(c.id) AS coleta_id
            FROM registros_naps AS r
            INNER JOIN coletas AS c
                ON c.id = r.coleta_id
            WHERE r.servidor = ?
                AND r.nome = ?
                AND COALESCE(r.endereco, '') = ?
                AND r.ativo = 1
            """,
            (servidor, rota, endereco),
        ).fetchone()

        ultima_ativa_id = (
            ultima_ativa["coleta_id"]
            if ultima_ativa is not None
            else None
        )

        if ultima_ativa_id is None:
            registro = conexao.execute(
                """
                SELECT c.data_hora
                FROM registros_naps AS r
                INNER JOIN coletas AS c
                    ON c.id = r.coleta_id
                WHERE r.servidor = ?
                    AND r.nome = ?
                    AND COALESCE(r.endereco, '') = ?
                    AND r.ativo = 0
                ORDER BY c.id ASC
                LIMIT 1
                """,
                (servidor, rota, endereco),
            ).fetchone()
        else:
            registro = conexao.execute(
                """
                SELECT c.data_hora
                FROM registros_naps AS r
                INNER JOIN coletas AS c
                    ON c.id = r.coleta_id
                WHERE r.servidor = ?
                    AND r.nome = ?
                    AND COALESCE(r.endereco, '') = ?
                    AND r.ativo = 0
                    AND c.id > ?
                ORDER BY c.id ASC
                LIMIT 1
                """,
                (
                    servidor,
                    rota,
                    endereco,
                    ultima_ativa_id,
                ),
            ).fetchone()

        if registro is None:
            return None

        return registro["data_hora"]

    def obter_inicio_indisponibilidade_atual(
        self,
        servidor,
        rota,
        endereco,
    ):
        """
        Obtém o início real conhecido da indisponibilidade
        atual. Prioriza um evento aberto e usa o histórico de
        coletas como fallback para rotas que já estavam fora
        antes da criação do controle de eventos.
        """
        endereco = endereco or ""

        with self._conectar() as conexao:
            evento = conexao.execute(
                """
                SELECT caiu_em
                FROM eventos_indisponibilidade
                WHERE servidor = ?
                    AND rota = ?
                    AND COALESCE(endereco, '') = ?
                    AND status = 'ABERTO'
                ORDER BY id DESC
                LIMIT 1
                """,
                (servidor, rota, endereco),
            ).fetchone()

            if evento is not None:
                return evento["caiu_em"]

            return self._obter_inicio_indisponibilidade_historico(
                conexao=conexao,
                servidor=servidor,
                rota=rota,
                endereco=endereco,
            )

    def registrar_indisponibilidade(
        self,
        item,
        horario=None,
    ):
        horario = horario or datetime.now()

        caiu_em = horario.isoformat(
            timespec="seconds"
        )

        servidor = item["servidor"]
        rota = item["nome"]
        endereco = item.get("endereco") or ""

        with self._conectar() as conexao:
            evento_aberto = conexao.execute(
                """
                SELECT
                    id,
                    caiu_em
                FROM eventos_indisponibilidade
                WHERE servidor = ?
                    AND rota = ?
                    AND COALESCE(endereco, '') = ?
                    AND status = 'ABERTO'
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    servidor,
                    rota,
                    endereco,
                ),
            ).fetchone()

            if evento_aberto is not None:
                return {
                    "evento_id": evento_aberto["id"],
                    "caiu_em": evento_aberto[
                        "caiu_em"
                    ],
                }

            cursor = conexao.execute(
                """
                INSERT INTO eventos_indisponibilidade (
                    servidor,
                    nap_id,
                    rota,
                    endereco,
                    caiu_em,
                    status
                )
                VALUES (?, ?, ?, ?, ?, 'ABERTO')
                """,
                (
                    servidor,
                    item.get("nap_id"),
                    rota,
                    endereco,
                    caiu_em,
                ),
            )

            return {
                "evento_id": cursor.lastrowid,
                "caiu_em": caiu_em,
            }

    def registrar_recuperacao(
        self,
        item,
        horario=None,
    ):
        horario = horario or datetime.now()

        servidor = item["servidor"]
        rota = item["nome"]
        endereco = item.get("endereco") or ""

        with self._conectar() as conexao:
            evento = conexao.execute(
                """
                SELECT
                    id,
                    caiu_em
                FROM eventos_indisponibilidade
                WHERE servidor = ?
                    AND rota = ?
                    AND COALESCE(endereco, '') = ?
                    AND status = 'ABERTO'
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    servidor,
                    rota,
                    endereco,
                ),
            ).fetchone()

            voltou_em = horario.isoformat(
                timespec="seconds"
            )

            if evento is None:
                caiu_em_historico = (
                    self._obter_inicio_indisponibilidade_historico(
                        conexao=conexao,
                        servidor=servidor,
                        rota=rota,
                        endereco=endereco,
                    )
                )

                if caiu_em_historico is None:
                    return {
                        "evento_id": None,
                        "caiu_em": None,
                        "voltou_em": voltou_em,
                        "duracao_segundos": None,
                    }

                caiu_em_data = datetime.fromisoformat(
                    caiu_em_historico
                )

                duracao_segundos = max(
                    0,
                    int(
                        (
                            horario - caiu_em_data
                        ).total_seconds()
                    ),
                )

                cursor = conexao.execute(
                    """
                    INSERT INTO eventos_indisponibilidade (
                        servidor,
                        nap_id,
                        rota,
                        endereco,
                        caiu_em,
                        voltou_em,
                        duracao_segundos,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'ENCERRADO')
                    """,
                    (
                        servidor,
                        item.get("nap_id"),
                        rota,
                        endereco,
                        caiu_em_historico,
                        voltou_em,
                        duracao_segundos,
                    ),
                )

                return {
                    "evento_id": cursor.lastrowid,
                    "caiu_em": caiu_em_historico,
                    "voltou_em": voltou_em,
                    "duracao_segundos": duracao_segundos,
                }

            caiu_em_data = datetime.fromisoformat(
                evento["caiu_em"]
            )

            duracao_segundos = max(
                0,
                int(
                    (
                        horario - caiu_em_data
                    ).total_seconds()
                ),
            )

            conexao.execute(
                """
                UPDATE eventos_indisponibilidade
                SET
                    voltou_em = ?,
                    duracao_segundos = ?,
                    status = 'ENCERRADO'
                WHERE id = ?
                """,
                (
                    voltou_em,
                    duracao_segundos,
                    evento["id"],
                ),
            )

            return {
                "evento_id": evento["id"],
                "caiu_em": evento["caiu_em"],
                "voltou_em": voltou_em,
                "duracao_segundos": duracao_segundos,
            }

    def registrar_servidor_indisponivel(
        self,
        servidor,
        endereco,
        falhas_consecutivas,
        horario=None,
    ):
        horario = horario or datetime.now()

        caiu_em = horario.isoformat(
            timespec="seconds"
        )

        with self._conectar() as conexao:
            evento_aberto = conexao.execute(
                """
                SELECT
                    id,
                    endereco,
                    caiu_em,
                    falhas_consecutivas
                FROM eventos_servidores
                WHERE servidor = ?
                    AND status = 'ABERTO'
                ORDER BY id DESC
                LIMIT 1
                """,
                (servidor,),
            ).fetchone()

            if evento_aberto is not None:
                return {
                    "evento_id": evento_aberto["id"],
                    "servidor": servidor,
                    "endereco": evento_aberto[
                        "endereco"
                    ],
                    "caiu_em": evento_aberto[
                        "caiu_em"
                    ],
                    "falhas_consecutivas": (
                        evento_aberto[
                            "falhas_consecutivas"
                        ]
                    ),
                }

            cursor = conexao.execute(
                """
                INSERT INTO eventos_servidores (
                    servidor,
                    endereco,
                    caiu_em,
                    falhas_consecutivas,
                    status
                )
                VALUES (?, ?, ?, ?, 'ABERTO')
                """,
                (
                    servidor,
                    endereco,
                    caiu_em,
                    falhas_consecutivas,
                ),
            )

            return {
                "evento_id": cursor.lastrowid,
                "servidor": servidor,
                "endereco": endereco,
                "caiu_em": caiu_em,
                "falhas_consecutivas": (
                    falhas_consecutivas
                ),
            }

    def registrar_servidor_recuperado(
        self,
        servidor,
        endereco,
        horario=None,
    ):
        horario = horario or datetime.now()

        voltou_em = horario.isoformat(
            timespec="seconds"
        )

        with self._conectar() as conexao:
            evento = conexao.execute(
                """
                SELECT
                    id,
                    endereco,
                    caiu_em,
                    falhas_consecutivas
                FROM eventos_servidores
                WHERE servidor = ?
                    AND status = 'ABERTO'
                ORDER BY id DESC
                LIMIT 1
                """,
                (servidor,),
            ).fetchone()

            if evento is None:
                return {
                    "evento_id": None,
                    "servidor": servidor,
                    "endereco": endereco,
                    "caiu_em": None,
                    "voltou_em": voltou_em,
                    "duracao_segundos": None,
                }

            caiu_em_data = datetime.fromisoformat(
                evento["caiu_em"]
            )

            duracao_segundos = max(
                0,
                int(
                    (
                        horario - caiu_em_data
                    ).total_seconds()
                ),
            )

            conexao.execute(
                """
                UPDATE eventos_servidores
                SET
                    voltou_em = ?,
                    duracao_segundos = ?,
                    status = 'ENCERRADO'
                WHERE id = ?
                """,
                (
                    voltou_em,
                    duracao_segundos,
                    evento["id"],
                ),
            )

            return {
                "evento_id": evento["id"],
                "servidor": servidor,
                "endereco": (
                    evento["endereco"]
                    or endereco
                ),
                "caiu_em": evento["caiu_em"],
                "voltou_em": voltou_em,
                "duracao_segundos": duracao_segundos,
                "falhas_consecutivas": evento[
                    "falhas_consecutivas"
                ],
            }

    def garantir_servidor_indisponivel(
        self,
        servidor,
        endereco,
        horario=None,
    ):
        """
        Cria silenciosamente um evento aberto para um
        servidor que já estava offline quando esta versão
        do monitor foi iniciada.

        Caso já exista um evento aberto, ele será mantido.
        """
        horario = horario or datetime.now()

        return self.registrar_servidor_indisponivel(
            servidor=servidor,
            endereco=endereco,
            falhas_consecutivas=0,
            horario=horario,
        )

    def listar_eventos(self, limite=500):
        with self._conectar() as conexao:
            registros = conexao.execute(
                """
                SELECT
                    id,
                    servidor,
                    nap_id,
                    rota,
                    endereco,
                    caiu_em,
                    voltou_em,
                    duracao_segundos,
                    status
                FROM eventos_indisponibilidade
                ORDER BY caiu_em DESC
                LIMIT ?
                """,
                (limite,),
            ).fetchall()

        return [
            dict(registro)
            for registro in registros
        ]

    def listar_eventos_servidores(
        self,
        limite=500,
    ):
        with self._conectar() as conexao:
            registros = conexao.execute(
                """
                SELECT
                    id,
                    servidor,
                    endereco,
                    caiu_em,
                    voltou_em,
                    duracao_segundos,
                    falhas_consecutivas,
                    status
                FROM eventos_servidores
                ORDER BY caiu_em DESC
                LIMIT ?
                """,
                (limite,),
            ).fetchall()

        return [
            dict(registro)
            for registro in registros
        ]

    @staticmethod
    def _criar_chave_rota(
        servidor,
        nome,
        endereco,
    ):
        return (
            str(servidor or "").strip().casefold(),
            str(nome or "").strip().casefold(),
            str(endereco or "").strip().casefold(),
        )