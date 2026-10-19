#!/usr/bin/env python3
"""Auditoria retrospectiva e candidato supervisionado, sem publicar decisões no TOI.

Alvo de estudo: início de P34 ou P135 >= 20% nos próximos 15 minutos,
quando a fila ainda está abaixo desse limite. É um proxy observável, não
um rótulo de incidente confirmado. Nenhum modelo é salvo ou ativado.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score


APP = Path(__file__).resolve().parent
QUEUES = (("4", "0249700009"), ("5", "0247400046"), ("6", "0255800003"))
FEATURES = (
    "p34_pct", "p135_pct", "hit_rate15", "calls_dialing", "dialing_rate",
    "agents_available", "calls_in_attendance", "p34_5m", "p135_5m",
    "calls_5m", "hour_sin", "hour_cos", "weekday",
)


def _metrics(y: np.ndarray, score: np.ndarray) -> dict:
    valid = np.isfinite(score)
    y, score = y[valid], score[valid]
    if len(y) == 0:
        return {"samples": 0}
    result = {"samples": int(len(y)), "positives": int(y.sum()),
              "prevalence": round(float(y.mean()), 5)}
    if y.min() == y.max():
        return result
    budget = max(1, int(np.ceil(len(y) * .05)))
    top = np.argsort(score, kind="stable")[-budget:]
    result.update({
        "average_precision": round(float(average_precision_score(y, score)), 5),
        "roc_auc": round(float(roc_auc_score(y, score)), 5),
        "precision_top_5pct": round(float(y[top].mean()), 5),
        "recall_top_5pct": round(float(y[top].sum() / y.sum()), 5),
    })
    return result


def _episode_count(times: pd.DatetimeIndex, target: np.ndarray) -> int:
    positives = times[np.asarray(target, dtype=bool)]
    if len(positives) == 0:
        return 0
    return 1 + int((positives[1:] - positives[:-1] > pd.Timedelta(minutes=15)).sum())


def _queue_frame(conn: sqlite3.Connection, inst: str, code: str, since: str) -> pd.DataFrame:
    rows = pd.read_sql_query(
        """SELECT ts,p34_pct,p135_pct,hit_rate15,calls_dialing,dialing_rate,
                  agents_available,calls_in_attendance
           FROM queue_snapshot WHERE instancia=? AND codigo=? AND ts>=? ORDER BY ts""",
        conn, params=(inst, code, since),
    )
    if rows.empty:
        return rows
    rows["ts"] = pd.to_datetime(rows.pop("ts"), utc=True)
    rows = rows.set_index("ts").resample("1min").last()
    valid = rows["p34_pct"].notna() & rows["p135_pct"].notna()
    bad = ((rows["p34_pct"] >= 20) | (rows["p135_pct"] >= 20)).where(valid)
    # A inversão calcula uma janela futura sem incluir o minuto corrente.
    future = bad.iloc[::-1].rolling(15, min_periods=1).max().iloc[::-1].shift(-1)
    coverage = valid.iloc[::-1].rolling(15, min_periods=1).sum().iloc[::-1].shift(-1)
    rows["target"] = future.where(coverage >= 10)
    rows["currently_below_threshold"] = valid & ~bad.eq(True)
    for col in ("p34_pct", "p135_pct", "calls_dialing"):
        rows[{"p34_pct": "p34_5m", "p135_pct": "p135_5m",
              "calls_dialing": "calls_5m"}[col]] = rows[col].rolling(5, min_periods=2).mean()
    hour = rows.index.tz_convert("America/Sao_Paulo").hour
    rows["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    rows["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    rows["weekday"] = rows.index.tz_convert("America/Sao_Paulo").dayofweek

    shadow = pd.read_sql_query(
        """SELECT ts,risk_score FROM ml_shadow_prediction
           WHERE instancia=? AND codigo=? AND ts>=? ORDER BY ts""",
        conn, params=(inst, code, since),
    )
    if not shadow.empty:
        shadow["ts"] = pd.to_datetime(shadow.pop("ts"), utc=True)
        series = shadow.set_index("ts")["risk_score"].resample("1min").last()
        rows["shadow_risk"] = series.reindex(rows.index).ffill(limit=2)
    else:
        rows["shadow_risk"] = np.nan
    rows["current_congestion"] = rows[["p34_pct", "p135_pct"]].max(axis=1)
    return rows.loc[rows["currently_below_threshold"] & rows["target"].notna()].iloc[::5].copy()


def audit_queue(conn: sqlite3.Connection, inst: str, code: str, since: str) -> dict:
    frame = _queue_frame(conn, inst, code, since)
    result = {"instancia": inst, "fila": code, "observations": len(frame)}
    if len(frame) < 500:
        result["decision"] = "DADOS_INSUFICIENTES"
        return result
    split = int(len(frame) * .7)
    cutoff = frame.index[split]
    train = frame.iloc[:split].loc[lambda x: x.index < cutoff - pd.Timedelta(minutes=15)]
    test = frame.iloc[split:]
    y_train = train["target"].astype(int).to_numpy()
    y_test = test["target"].astype(int).to_numpy()
    result.update({"train_end": train.index.max().isoformat(),
                   "test_start": test.index.min().isoformat(),
                   "train_positives": int(y_train.sum()),
                   "test_positives": int(y_test.sum()),
                   "test_positive_episodes_approx": _episode_count(test.index, y_test),
                   "rule_current_congestion": _metrics(y_test, test["current_congestion"].to_numpy()),
                   "existing_shadow": _metrics(y_test, test["shadow_risk"].to_numpy())})
    if y_train.sum() < 30 or y_test.sum() < 10:
        result["decision"] = "JANELAS_POSITIVAS_INSUFICIENTES_PARA_VALIDAR"
        return result
    model = HistGradientBoostingClassifier(
        max_iter=100, learning_rate=.05, max_leaf_nodes=15,
        min_samples_leaf=30, l2_regularization=1.0,
        early_stopping=False, random_state=42,
    )
    model.fit(train[list(FEATURES)], y_train)
    score = model.predict_proba(test[list(FEATURES)])[:, 1]
    result["candidate_hist_gradient_boosting"] = _metrics(y_test, score)
    result["decision"] = "CANDIDATO_APENAS_SHADOW; FALTAM_ROTULOS_DE_INCIDENTE_E_VALIDACAO_PROSPECTIVA"
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=(APP / "../dados_toi/live/toi_live.sqlite").resolve())
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--output", type=Path, default=APP / "homologacao/ml_model_audit_latest.json")
    args = parser.parse_args()
    since = (datetime.now().astimezone() - timedelta(days=args.days)).isoformat()
    with sqlite3.connect(f"file:{args.db.resolve().as_posix()}?mode=ro", uri=True) as conn:
        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "goal": "Alerta antecipado de congestionamento em até 15 minutos",
            "proxy_label": "P34 ou P135 atinge 20%; estado atual ainda abaixo de 20%",
            "horizon_minutes": 15, "sampling_minutes": 5,
            "count_unit": "Janelas de cinco minutos; janelas adjacentes podem pertencer ao mesmo incidente",
            "evaluation": "70% cronológico para treino; 15 minutos de lacuna; 30% final para teste",
            "operational_approval": False,
            "limitation": "O alvo é um proxy das métricas do TOI, não incidente confirmado. Não houve validação prospectiva.",
            "queues": [audit_queue(conn, inst, code, since) for inst, code in QUEUES],
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    public = {key: result[key] for key in ("generated_at", "goal", "operational_approval", "limitation")}
    public["next_review_at"] = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    public["model_review"] = (
        "Nenhum modelo homologado: faltam rótulos de incidentes confirmados e validação prospectiva."
    )
    public["new_model_needed"] = None
    public["new_model_assessment"] = (
        "Necessidade de novo modelo indeterminada: faltam incidentes confirmados e avaliação prospectiva. "
        "Comparar Shadow, regra atual e candidato quando houver casos suficientes."
    )
    public["queues"] = [{key: q.get(key) for key in (
        "instancia", "fila", "test_positives", "test_positive_episodes_approx", "decision",
        "existing_shadow", "candidate_hist_gradient_boosting", "rule_current_congestion",
    )} for q in result["queues"]]
    (APP / "static/ml_model_audit_latest.json").write_text(
        json.dumps(public, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
