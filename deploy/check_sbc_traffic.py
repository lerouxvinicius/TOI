"""Read-only post-deployment check of both SBC historical search paths."""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sys

from sbc_check_snapshot import snapshot_sbc_sources

ROOT = Path("/var/www/toi")
sys.path.insert(0, str(ROOT / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"))
from sbc_traffic_history import PAIRS, traffic_report  # noqa: E402


def main() -> None:
    now = datetime.now().astimezone()
    with snapshot_sbc_sources(ROOT, now.date().isoformat()) as frozen:
        result = check(frozen, now)
    print(json.dumps(result, ensure_ascii=False))


def check(root, now):
    result = {}
    for pair, config in PAIRS.items():
        daily = traffic_report(root, pair, now.date().isoformat(), now=now)
        latest = daily["last_observed"]
        assert latest, f"{pair}: no history today"
        last_stamp = datetime.fromisoformat(latest)
        if last_stamp.tzinfo is None:
            last_stamp = last_stamp.replace(tzinfo=now.tzinfo)
        history_age = (now - last_stamp).total_seconds()
        assert -5 <= history_age <= 90, f"{pair}: history delayed by {history_age:.1f}s"
        selected_hour = last_stamp.hour
        all_realms = traffic_report(root, pair, now.date().isoformat(), selected_hour, now=now)
        one_realm = traffic_report(
            root, pair, now.date().isoformat(), selected_hour, config["realms"][0], now=now)
        assert all_realms["current_fresh"], f"{pair}: live reading unavailable"
        assert all_realms["consolidated"]["samples"] > 0, f"{pair}: no current-hour history"
        assert all_realms["consolidated"] == one_realm["consolidated"], (
            f"{pair}: hour consolidation changed with realm filter")
        assert one_realm["selected_realms"] == [config["realms"][0]]
        assert len(all_realms["selected_realms"]) == len(config["realms"])
        result[pair] = {
            "live_age_s": all_realms["current_age_s"],
            "history_age_s": round(history_age, 1),
            "selected_hour_samples": all_realms["consolidated"]["samples"],
            "realms": len(all_realms["selected_realms"]),
            "realm_filter": one_realm["selected_realms"][0],
        }
    return result


if __name__ == "__main__":
    main()
