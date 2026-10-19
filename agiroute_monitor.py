"""Read-only health check for the two Agiroute hosts of instance 6.

Runs as a systemd oneshot using the TOI radius SSH key. No password or
host-key acceptance is performed here.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


HOSTS = ("10.90.10.165", "10.90.10.124")
UNITS = ("agiroutey.service", "proxysql.service")
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW/static/toi_agiroute_latest.json"
REMOTE_COMMAND = (
    "for unit in agiroutey.service proxysql.service; do "
    "printf '__UNIT__=%s\\n' \"$unit\"; "
    "systemctl show --no-pager -p LoadState -p ActiveState -p SubState -p UnitFileState \"$unit\" "
    "|| exit 1; done"
)


def parse_systemctl(output: str) -> dict:
    result: dict[str, dict] = {}
    current = None
    for line in output.splitlines():
        if line.startswith("__UNIT__="):
            current = line.partition("=")[2]
            if current not in UNITS or current in result:
                raise ValueError("unexpected or duplicate unit")
            result[current] = {}
        elif current and "=" in line:
            key, value = line.split("=", 1)
            if key in ("LoadState", "ActiveState", "SubState", "UnitFileState"):
                result[current][key] = value
    if set(result) != set(UNITS) or any(
        not all(result[unit].get(key) for key in ("LoadState", "ActiveState", "SubState"))
        for unit in UNITS
    ):
        raise ValueError("incomplete systemctl response")
    return {
        unit.removesuffix(".service"): {
            "load": values["LoadState"],
            "active": values["ActiveState"],
            "sub": values["SubState"],
            "boot": values.get("UnitFileState", "unknown"),
            "status": "OK" if values["LoadState"] == "loaded" and values["ActiveState"] == "active" else "DOWN",
        }
        for unit, values in result.items()
    }


def check_host(host: str, *, user: str, key: str, known_hosts: str, timeout: int = 12) -> dict:
    if host not in HOSTS or not re.fullmatch(r"[a-z_][a-z0-9_.-]*", user):
        raise ValueError("invalid monitor target or SSH user")
    command = [
        "ssh", "-F", os.devnull, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
        "-o", "IdentitiesOnly=yes", "-o", "ConnectionAttempts=1", "-o", "ConnectTimeout=6",
        "-o", f"UserKnownHostsFile={known_hosts}", "-i", key, f"{user}@{host}", REMOTE_COMMAND,
    ]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
        if completed.returncode == 255:
            return {"ip": host, "status": "UNREACHABLE", "services": {}, "reason": "SSH_OR_REMOTE_COMMAND_FAILED"}
        if completed.returncode:
            return {"ip": host, "status": "UNKNOWN", "services": {}, "reason": "REMOTE_COMMAND_FAILED"}
        return {"ip": host, "status": "OK", "services": parse_systemctl(completed.stdout)}
    except (subprocess.TimeoutExpired, OSError):
        return {"ip": host, "status": "UNREACHABLE", "services": {}, "reason": "SSH_TIMEOUT_OR_UNAVAILABLE"}
    except ValueError:
        return {"ip": host, "status": "UNKNOWN", "services": {}, "reason": "INVALID_SYSTEMCTL_RESPONSE"}


def snapshot(*, user: str, key: str, known_hosts: str, checker=check_host) -> dict:
    configured = bool(user and key and known_hosts)
    if configured and not (Path(key).is_file() and Path(known_hosts).is_file()):
        configured = False
    hosts = ([checker(ip, user=user, key=key, known_hosts=known_hosts) for ip in HOSTS]
             if configured else [{"ip": ip, "status": "NOT_CONFIGURED", "services": {}} for ip in HOSTS])
    return {
        "schema": "toi_agiroute_health_v1", "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "instancia": "6", "configured": configured, "hosts": hosts,
    }


def write_atomic(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, prefix=".agiroute-", suffix=".json", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(data, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        os.chmod(temporary, 0o640)
        os.replace(temporary, path)
    finally:
        if temporary and temporary.exists():
            temporary.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    data = snapshot(user=os.getenv("TOI_AGIROUTE_SSH_USER", "radius"),
                    key=os.getenv("TOI_AGIROUTE_SSH_KEY", "/home/radius/.ssh/id_ed25519"),
                    known_hosts=os.getenv("TOI_AGIROUTE_KNOWN_HOSTS", "/home/radius/.ssh/known_hosts"))
    write_atomic(args.output, data)
    print(json.dumps({"generated_at": data["generated_at"], "configured": data["configured"],
                      "hosts": {item["ip"]: item["status"] for item in data["hosts"]}}))


if __name__ == "__main__":
    main()
