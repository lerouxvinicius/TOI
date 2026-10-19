"""Audit remote TOI collection through an interactive SSH password prompt."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shlex

from remote_access import ROOT, connect, run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[2] /
                        "outputs" / "toi_collection_remote_audit.json")
    args = parser.parse_args()
    client, password = connect()
    try:
        report = {"services": {}, "queues": {}}
        for unit in ("toi-core", "toi-gateway", "toi-portal", "toi-alerts"):
            report["services"][unit] = run(
                client, f"systemctl show -p ActiveState --value {unit}.service"
            ).strip()
        report["commit"] = run(client, f"git -C {ROOT} rev-parse --short HEAD").strip()
        source = (Path(__file__).parent / "diagnose_queue_cadence.py").read_text(encoding="utf-8")
        for instancia, codigo, target, hours in (
            ("4", "0249700009", 18, 4),
            ("4", "0249700010", 18, 4),
            ("4", "0249700007", 60, 4),
            ("6", "0255800015", 300, 4),
            ("6", "0255800003", 10, 4),
        ):
            command = (
                "sudo -S -p '' -u toi python3 -c " + shlex.quote(source) +
                f" --instancia {instancia} --codigo {codigo} --hours {hours} --target-s {target}"
            )
            report["queues"][f"{instancia}:{codigo}"] = json.loads(run(client, command, password))
        source = (Path(__file__).parent / "diagnose_all_instances.py").read_text(encoding="utf-8")
        command = "sudo -S -p '' -u toi python3 -c " + shlex.quote(source)
        report["all_instances"] = json.loads(run(client, command, password))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Auditoria de leitura concluída. Resultado:", args.output)
    finally:
        client.close()


if __name__ == "__main__":
    main()
