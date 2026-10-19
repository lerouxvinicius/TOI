"""Estudo de caso reproduzível do pipeline de ML do TOI, com dados 100% sintéticos.

Executa, de ponta a ponta, as mesmas peças usadas em produção:
  1. gera 14 dias de séries por minuto para 3 filas, com episódios de congestionamento
     precedidos por sinais (e também "falsos precursores" que não viram incidente);
  2. calcula o score do ML Shadow (baseline robusto mediana + MAD) minuto a minuto,
     usando a mesma função de produção (`score_from_baseline`);
  3. roda a auditoria retrospectiva de produção (`audit_queue`): rótulo proxy com
     horizonte de 15 min, split cronológico 70/30 com embargo de 15 min, e compara
     regra atual x ML Shadow x candidato HistGradientBoosting;
  4. salva métricas em docs/ml/case_study_results.json e gráficos em docs/ml/.

Os números refletem os dados sintéticos gerados aqui, não o desempenho em produção.

    python demo/ml_case_study.py
"""
from __future__ import annotations

import json
import random
import sqlite3
import sys
import tempfile
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"
OUT = ROOT / "docs" / "ml"
sys.path.insert(0, str(APP))

import toi_collector_core_v3 as core  # noqa: E402
import toi_ml_homologation_audit_v1 as audit  # noqa: E402
import toi_ml_shadow_v1_10_6 as shadow  # noqa: E402

BRT = timezone(timedelta(hours=-3))
DAYS = 14
HISTORY_MIN = 360          # baseline de 6 h, como no ML Shadow em produção
MIN_SAMPLES = 30
EPISODES_PER_DAY = 3.0
FALSE_PRECURSORS_PER_DAY = 2.0


def simulate_queue(rng: random.Random, start: datetime):
    """Retorna lista de minutos (ts, valores, trunk) e os inícios reais de congestionamento."""
    rows, onsets = [], []
    for day in range(DAYS):
        d0 = start + timedelta(days=day)
        if d0.weekday() == 6:
            continue  # domingo sem operação
        minutes = 13 * 60
        effect = [0.0] * minutes          # intensidade do precursor (0..1)
        congest = [0.0] * minutes         # nível de congestionamento acima do limite
        for _ in range(int(rng.expovariate(1 / EPISODES_PER_DAY) + 0.5)):
            t = rng.randint(40, minutes - 60)
            ramp, hold = rng.randint(10, 25), rng.randint(10, 30)
            for i in range(ramp):
                effect[t + i] = max(effect[t + i], (i + 1) / ramp)
            for i in range(hold):
                effect[t + ramp + i] = 1.0
                congest[t + ramp + i] = rng.uniform(21, 34)
            onsets.append(d0 + timedelta(hours=8, minutes=t + ramp))
        for _ in range(int(rng.expovariate(1 / FALSE_PRECURSORS_PER_DAY) + 0.5)):
            t, ramp = rng.randint(40, minutes - 60), rng.randint(10, 25)
            peak = rng.uniform(0.4, 0.8)
            for i in range(ramp * 2):
                lvl = peak * (i + 1) / ramp if i < ramp else peak * (2 - (i + 1) / ramp)
                effect[t + i] = max(effect[t + i], lvl)
        for m in range(minutes):
            ts = d0 + timedelta(hours=8, minutes=m)
            hour = 8 + m / 60
            load = 0.35 + 0.65 * max(0.0, 1 - abs(hour - 15) / 7)
            e = effect[m]
            p34 = max(0.0, rng.gauss(4, 2) + 7 * e)
            p135 = max(0.0, rng.gauss(2, 1.2) + 4 * e)
            if congest[m]:
                if rng.random() < 0.6:
                    p34 = congest[m]
                else:
                    p135 = congest[m] * 0.85
            vals = {
                "p34_pct": round(min(p34, 19.5) if not congest[m] else p34, 2),
                "p135_pct": round(min(p135, 19.5) if not congest[m] else p135, 2),
                "hit_rate15": round(max(5.0, rng.gauss(31, 3) - 9 * e), 2),
                "calls_dialing": max(0, int(rng.gauss(40 * load, 5) * (1 + 0.7 * e))),
                "dialing_rate": round(max(0.5, rng.gauss(2.0, 0.3) + 1.1 * e), 2),
                "agents_available": max(0, int(rng.gauss(8 * load, 2) - 5 * e)),
                "calls_in_attendance": max(0, int(rng.gauss(30 * load, 4))),
            }
            trunk = min(99.0, max(10.0, rng.gauss(45 + 25 * load, 5) + 28 * e))
            rows.append((ts, vals, trunk))
    return rows, onsets


def write_queue(con, inst, code, rows):
    con.executemany(
        "INSERT INTO queue_snapshot(ts,instancia,projeto_id,codigo,nome,p34_pct,p135_pct,hit_rate15,"
        "calls_dialing,dialing_rate,agents_available,calls_in_attendance) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        [(ts.isoformat(), inst, code[1:5], code, f"FILA_SINTETICA_{inst}", v["p34_pct"], v["p135_pct"],
          v["hit_rate15"], v["calls_dialing"], v["dialing_rate"], v["agents_available"],
          v["calls_in_attendance"]) for ts, v, _ in rows])


def replay_shadow(con, inst, code, rows):
    """Recalcula o ML Shadow minuto a minuto com a função de produção."""
    window = {f: deque(maxlen=HISTORY_MIN) for f in shadow.FIELDS}
    out = []
    for ts, vals, trunk in rows:
        if len(window["p34_pct"]) >= MIN_SAMPLES - 1:
            s = shadow.score_from_baseline(vals, {f: list(window[f]) for f in shadow.FIELDS}, trunk)
            out.append((ts.isoformat(), inst, code, "robust_mad_shadow_v0", s["risk"], s["anomaly"],
                        s["risk_class"], s["p34_signal"], s["p135_signal"], s["capacity_signal"],
                        "; ".join(s["reasons"]), "{}"))
        for f in shadow.FIELDS:
            window[f].append(float(vals[f]))
    con.executemany("INSERT INTO ml_shadow_prediction VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", out)
    return {ts: r[4] for ts, r in zip([o[0] for o in out], out)}


def plot(results, example):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    models = [("rule_current_congestion", "Regra atual\n(congestionamento agora)"),
              ("existing_shadow", "ML Shadow v0\n(mediana + MAD)"),
              ("candidate_hist_gradient_boosting", "Candidato\nHistGradientBoosting")]
    metrics = [("average_precision", "PR-AUC"), ("roc_auc", "ROC-AUC"),
               ("precision_top_5pct", "Precisão @5%"), ("recall_top_5pct", "Recall @5%")]
    fig, axes = plt.subplots(1, len(metrics), figsize=(14, 3.8), sharey=True)
    colors = ["#9aa5b1", "#36b5cc", "#1f6feb"]
    for ax, (key, title) in zip(axes, metrics):
        vals = [sum(q.get(m, {}).get(key, 0) for q in results) / len(results) for m, _ in models]
        bars = ax.bar(range(len(models)), vals, color=colors)
        ax.set_title(title)
        ax.set_xticks(range(len(models)), [label for _, label in models], fontsize=8)
        ax.set_ylim(0, 1.05)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v + 0.02, f"{v:.2f}", ha="center", fontsize=9)
    fig.suptitle("Alerta antecipado de congestionamento (horizonte 15 min) — média das 3 filas, conjunto de teste",
                 fontsize=11)
    fig.tight_layout()
    fig.savefig(OUT / "model_comparison.png", dpi=130)
    plt.close(fig)

    ts, cong, risk, onset = example
    fig, ax1 = plt.subplots(figsize=(12, 3.8))
    ax1.plot(ts, cong, color="#e5534b", label="max(P34, P135) %")
    ax1.axhline(20, color="#e5534b", ls="--", lw=1)
    ax1.text(ts[0], 21, "limite de congestionamento (20%)", color="#e5534b", fontsize=8)
    ax1.set_ylabel("congestionamento %")
    ax2 = ax1.twinx()
    ax2.plot(ts, risk, color="#1f6feb", label="risco ML Shadow (0-100)")
    ax2.set_ylabel("risco shadow")
    ax2.set_ylim(0, 100)
    ax1.axvspan(onset - timedelta(minutes=15), onset, color="#f2cc60", alpha=0.25,
                label="janela de alerta antecipado (15 min)")
    lines = ax1.get_legend_handles_labels()
    lines2 = ax2.get_legend_handles_labels()
    ax1.legend(lines[0] + lines2[0], lines[1] + lines2[1], loc="upper left", fontsize=8, framealpha=0.95)
    ax1.set_title("Exemplo sintético: o risco shadow sobe antes de a fila cruzar o limite", fontsize=11)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(OUT / "early_warning_example.png", dpi=130)
    plt.close(fig)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    rng = random.Random(7)
    start = datetime.now(BRT).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=DAYS)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        db = Path(tmp) / "case_study.sqlite"
        core.Store(str(db)).conn.close()
        con = sqlite3.connect(db)
        shadow.ensure(con)
        example = None
        for inst, code in audit.QUEUES:
            rows, onsets = simulate_queue(rng, start)
            write_queue(con, inst, code, rows)
            risk_by_ts = replay_shadow(con, inst, code, rows)
            if example is None and onsets:
                onset = onsets[int(len(onsets) * 0.85)]
                win = [(t, max(v["p34_pct"], v["p135_pct"]), risk_by_ts.get(t.isoformat()))
                       for t, v, _ in rows if onset - timedelta(minutes=35) <= t <= onset + timedelta(minutes=20)]
                example = ([w[0] for w in win], [w[1] for w in win], [w[2] for w in win], onset)
            print(f"fila {inst}/{code}: {len(rows)} minutos, {len(onsets)} episódios de congestionamento")
        con.commit()
        con.close()
        since = (start - timedelta(days=1)).isoformat()
        ro = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
        try:
            results = [audit.audit_queue(ro, inst, code, since) for inst, code in audit.QUEUES]
        finally:
            ro.close()
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "data": "SINTÉTICA — gerada por demo/ml_case_study.py (seed 7); não representa produção",
        "goal": "Alerta antecipado: P34 ou P135 >= 20% nos próximos 15 minutos, com a fila ainda abaixo do limite",
        "evaluation": "Split cronológico 70/30, embargo de 15 min entre treino e teste, amostragem a cada 5 min",
        "queues": results,
    }
    (OUT / "case_study_results.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    plot(results, example)
    for q in results:
        print(q["instancia"], q["decision"])
        for m in ("rule_current_congestion", "existing_shadow", "candidate_hist_gradient_boosting"):
            print(f"   {m:34s}", {k: q.get(m, {}).get(k) for k in ("average_precision", "roc_auc", "precision_top_5pct", "recall_top_5pct")})
    print("Resultados em", OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
