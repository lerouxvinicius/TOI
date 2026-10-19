from pathlib import Path
from datetime import datetime
import csv
import json


BASE_DIR = Path(__file__).resolve().parent.parent
DADOS_DIR = BASE_DIR / "dados"

CSV_INVALIDOS = DADOS_DIR / "periodos_invalidos.csv"
ESTADO_ARQUIVO = DADOS_DIR / "estado_lacuna_rede.json"

CABECALHO = [
    "data",
    "inicio",
    "fim",
    "escopo",
    "tipo",
    "motivo",
    "acao",
]


def _agora():
    return datetime.now()


def _carregar_estado():
    if not ESTADO_ARQUIVO.exists():
        return {
            "lacuna_aberta": False,
            "inicio": None,
        }

    try:
        with ESTADO_ARQUIVO.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "lacuna_aberta": False,
            "inicio": None,
        }


def _salvar_estado(estado):
    DADOS_DIR.mkdir(parents=True, exist_ok=True)

    with ESTADO_ARQUIVO.open("w", encoding="utf-8") as f:
        json.dump(
            estado,
            f,
            ensure_ascii=False,
            indent=2,
        )


def _garantir_csv():
    DADOS_DIR.mkdir(parents=True, exist_ok=True)

    if CSV_INVALIDOS.exists() and CSV_INVALIDOS.stat().st_size > 0:
        return

    with CSV_INVALIDOS.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.writer(f)
        writer.writerow(CABECALHO)


def _registrar_lacuna(inicio, fim):
    _garantir_csv()

    # Se atravessar meia-noite, por enquanto registramos
    # usando a data em que a indisponibilidade começou.
    data = inicio.strftime("%Y-%m-%d")

    with CSV_INVALIDOS.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.writer(f)

        writer.writerow([
            data,
            inicio.strftime("%H:%M:%S"),
            fim.strftime("%H:%M:%S"),
            "MONITORAMENTO",
            "LACUNA_REDE",
            "Indisponibilidade da VPN/rede corporativa detectada automaticamente",
            "Conectividade restabelecida automaticamente; intervalo excluído das análises",
        ])


def atualizar_estado_rede(
    alvos_acessiveis: int,
    minimo_necessario: int,
):
    """
    Deve ser chamado após cada health check corporativo.

    Retorna:
        "OK"
        "LACUNA_ABERTA"
        "LACUNA_EM_ANDAMENTO"
        "LACUNA_FECHADA"
    """

    estado = _carregar_estado()
    agora = _agora()

    rede_ok = alvos_acessiveis >= minimo_necessario

    # ------------------------------------------------------
    # REDE NORMAL
    # ------------------------------------------------------

    if rede_ok and not estado.get("lacuna_aberta"):
        return "OK"

    # ------------------------------------------------------
    # COMEÇO DA INDISPONIBILIDADE
    # ------------------------------------------------------

    if not rede_ok and not estado.get("lacuna_aberta"):
        estado = {
            "lacuna_aberta": True,
            "inicio": agora.isoformat(),
        }

        _salvar_estado(estado)

        print(
            f"⚠ Lacuna de rede iniciada em "
            f"{agora.strftime('%d/%m/%Y %H:%M:%S')}"
        )

        return "LACUNA_ABERTA"

    # ------------------------------------------------------
    # INDISPONIBILIDADE CONTINUA
    # ------------------------------------------------------

    if not rede_ok and estado.get("lacuna_aberta"):
        return "LACUNA_EM_ANDAMENTO"

    # ------------------------------------------------------
    # REDE VOLTOU
    # ------------------------------------------------------

    if rede_ok and estado.get("lacuna_aberta"):

        inicio_txt = estado.get("inicio")

        try:
            inicio = datetime.fromisoformat(inicio_txt)
        except Exception:
            inicio = agora

        fim = agora

        _registrar_lacuna(
            inicio=inicio,
            fim=fim,
        )

        _salvar_estado({
            "lacuna_aberta": False,
            "inicio": None,
        })

        duracao = fim - inicio
        segundos = int(duracao.total_seconds())

        print(
            f"✅ Conectividade restabelecida. "
            f"Lacuna encerrada após {segundos} segundo(s)."
        )

        return "LACUNA_FECHADA"

    return "OK"