"""Popula uma base SQLite com dados 100% fictícios para rodar o painel do TOI localmente.

Uso:
    python demo/seed_demo.py            # gera 3 horas de histórico e encerra
    python demo/seed_demo.py --follow   # gera o histórico e continua gravando a cada 10 s

Depois, em outro terminal:
    python TOI_v1.10.6_DUAL_LANE_ML_SHADOW/toi_dashboard_server_v1_10_6.py --db dados_toi/live/toi_demo.sqlite
e acesse http://127.0.0.1:8088/login com o usuário de demonstração abaixo.
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"
sys.path.insert(0, str(APP))

import toi_collector_core_v3 as core  # noqa: E402
import toi_ml_shadow_v1_10_6 as shadow  # noqa: E402
import toi_shared_admin_v1 as admin  # noqa: E402

DEMO_LOGIN = "demo"
DEMO_PASSWORD = "Demo#TOI-2026"
DEFAULT_DB = ROOT / "dados_toi" / "live" / "toi_demo.sqlite"
STEP_S = 60
FOLLOW_S = 10


def load_queues():
    scope = json.loads((APP / "toi_demo_scope_v1.json").read_text(encoding="utf-8"))
    return scope["queues"]


def load_gateways():
    ref = json.loads((ROOT / "TOI_v1.10.6.3.7.4.1_CONTINUOUS_ACTIVATION_R2"
                      / "gateway_identity_reference_v4.json").read_text(encoding="utf-8"))
    by_inst: dict[str, list[str]] = {}
    for name, info in ref["gateways"].items():
        by_inst.setdefault(str(info.get("reference_instance") or "1"), []).append(name)
    return by_inst


def iso(ts: datetime) -> str:
    return ts.astimezone().isoformat(timespec="milliseconds")


def daily_wave(ts: datetime) -> float:
    """Carga entre 0.15 e 1.0, com pico no meio da tarde (horário de Brasília)."""
    hour = (ts.hour - 3) % 24 + ts.minute / 60
    return 0.15 + 0.85 * max(0.0, math.sin(math.pi * (hour - 8) / 13)) if 8 <= hour <= 21 else 0.15


def queue_row(q, ts: datetime, rng: random.Random):
    load = daily_wave(ts) * rng.uniform(0.8, 1.2)
    agents = max(1, int(40 * load * rng.uniform(0.6, 1.4)))
    talking = int(agents * rng.uniform(0.4, 0.8))
    paused = rng.randint(0, max(1, agents // 8))
    acw = rng.randint(0, max(1, agents // 6))
    available = max(0, agents - talking - paused - acw)
    calls = int(talking * rng.uniform(2.5, 4.5))
    p34 = round(min(35.0, max(0.0, rng.gauss(4, 3) * (2.5 if rng.random() < 0.04 else 1))), 2)
    p135 = round(min(20.0, max(0.0, rng.gauss(2, 1.5))), 2)
    hit5 = round(rng.uniform(18, 42), 2)
    d1 = {"QtAgentsAvailable": available, "QtAgentsTalking": talking, "QtAgentsAcw": acw,
          "QtAgentsPaused": paused, "QtAgentsOther": 0, "QtAgentsDialing": 0,
          "QtCallsDialing": int(calls * 0.3), "QtCallsInAttend": talking, "QtCallsLast5": calls,
          "PercHitRateLast5": hit5, "PercHitRateLast15": round(hit5 * rng.uniform(0.9, 1.1), 2),
          "VlRatioCalls": round(rng.uniform(1.2, 3.5), 2), "QtMailingOnScreen": rng.randint(200, 5000),
          "DeDsState": "Running", "DeIsActiveDs": "Sim", "DeIsPausedDs": "Não"}
    d2 = {"callStatisticsList": [
        {"NmStatusIpbx": "34 - No Circuit Available", "QtCalls": int(calls * p34 / 100), "VlPercCalls": p34},
        {"NmStatusIpbx": "135 - Congestionamento", "QtCalls": int(calls * p135 / 100), "VlPercCalls": p135}]}
    lat_q, lat_s = round(rng.uniform(0.2, 1.4), 3), round(rng.uniform(0.2, 1.4), 3)
    return (iso(ts), q["instancia"], q["projeto_id"], q["codigo"], q["nome"],
            q.get("motivo_prioridade", "demo"), p34, int(calls * p34 / 100), p135,
            int(calls * p135 / 100), available, talking, acw, paused, 0, 0,
            d1["QtCallsDialing"], talking, calls, hit5, d1["PercHitRateLast15"],
            d1["VlRatioCalls"], d1["QtMailingOnScreen"], "Running", "Sim", "Não",
            lat_q, lat_s, 0.0, json.dumps(d1, ensure_ascii=False), json.dumps(d2, ensure_ascii=False))


def infra_rows(inst: str, gateways: list[str], ts: datetime, rng: random.Random):
    load = daily_wave(ts)
    rows = []
    gw_list, trunk_list = [], []
    for name in gateways:
        total = 240
        busy = int(total * min(0.98, load * rng.uniform(0.5, 1.1)))
        gw_list.append({"NmGateway": name, "QtValidsTotal": total, "QtValidsBusy": busy,
                        "QtValidsIdle": total - busy, "VlPercValidsBusy": round(100 * busy / total, 2),
                        "QtInvalidsTotal": rng.randint(0, 3)})
    for i in range(1, 4):
        total = 120
        busy = int(total * min(0.99, load * rng.uniform(0.4, 1.0)))
        trunk_list.append({"DeNmTrunkGroup": f"TRONCO_INST{inst}_{i:02d}", "QtValidsTotal": total,
                           "QtValidsBusy": busy, "QtValidsIdle": total - busy,
                           "VlPercValidsBusy": round(100 * busy / total, 2), "QtInvalidsTotal": 0})
    lat = round(rng.uniform(0.2, 0.9), 3)
    for source, key, items, name_key in (("gateway", "gatewayList", gw_list, "NmGateway"),
                                         ("trunk", "trunkGroupList", trunk_list, "DeNmTrunkGroup")):
        doc = json.dumps({key: items}, ensure_ascii=False)
        for x in items:
            rows.append((iso(ts), inst, source, x[name_key], x["QtValidsTotal"], x["QtValidsBusy"],
                         x["QtValidsIdle"], x["VlPercValidsBusy"], x["QtInvalidsTotal"], lat, doc))
    return rows


def write_tick(store, queues, gateways, ts, rng):
    store.many("INSERT INTO queue_snapshot VALUES(" + ",".join(["?"] * 31) + ")",
               [queue_row(q, ts, rng) for q in queues])
    insts = sorted({q["instancia"] for q in queues})
    store.many("INSERT INTO infra_snapshot VALUES(?,?,?,?,?,?,?,?,?,?,?)",
               [r for inst in insts for r in infra_rows(inst, gateways.get(inst, []), ts, rng)])


def ensure_demo_user():
    admin.setup()
    with admin.db(admin.AUTH_DB) as con:
        if con.execute("SELECT 1 FROM users WHERE login=?", (DEMO_LOGIN,)).fetchone():
            return
        con.execute("INSERT INTO users(login,name,hash,role,instances,active,must_change) "
                    "VALUES(?,?,?,?,?,1,0)",
                    (DEMO_LOGIN, "Usuário Demo", admin.HASHER.hash(DEMO_PASSWORD),
                     "Administrador", json.dumps(["*"])))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--hours", type=float, default=3.0)
    ap.add_argument("--follow", action="store_true")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    store = core.Store(args.db)
    shadow.ensure(store.conn)
    rng = random.Random(args.seed)
    queues, gateways = load_queues(), load_gateways()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    ts = now - timedelta(hours=args.hours)
    while ts <= now:
        write_tick(store, queues, gateways, ts, rng)
        ts += timedelta(seconds=STEP_S)
    ensure_demo_user()
    print(f"Base demo pronta: {args.db} ({len(queues)} filas fictícias). Login: {DEMO_LOGIN}")
    while args.follow:
        time.sleep(FOLLOW_S)
        write_tick(store, queues, gateways, datetime.now(timezone.utc).replace(microsecond=0), rng)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
