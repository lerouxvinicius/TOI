"""Read-only post-deployment SBC source check; no SNMP credentials or raw payloads."""
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path("/var/www/toi")
sys.path.insert(0, str(ROOT / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"))
from sbc_card_bridge import card_data  # noqa: E402
from sbc_traffic_history import traffic_report  # noqa: E402


def main():
    feed = card_data(ROOT)
    report = {"sbc_snapshot_fresh": feed["fresh"],
              "snapshot_age_s": feed["age_seconds"], "pairs": {}}
    for key in ("norte", "sul"):
        traffic = traffic_report(ROOT, key)
        pair = next((item for item in feed["pairs"] if item["name"] == traffic["name"]), {})
        nodes = pair.get("nodes") or []
        report["pairs"][key] = {
            "live_card_fresh": pair.get("fresh"),
            "live_card_age_s": pair.get("age_seconds"),
            "live_card_source": pair.get("source"),
            "realm_sample_count_today": traffic["samples"],
            "realm_current_fresh": traffic["current_fresh"],
            "realm_current_age_s": traffic["current_age_s"],
            "last_realm_sample": traffic["last_observed"],
            "realm_readings": {name: (traffic["current"] or {}).get("realms", {}).get(name)
                               for name in traffic["realms"]},
            "agents": {node["ip"]: {agent["ip"]: {
                "observed": agent["observed"],
                "inbound": agent["inbound_active"],
                "outbound": agent["outbound_active"]}
                for agent in node.get("agents", [])} for node in nodes},
        }
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
