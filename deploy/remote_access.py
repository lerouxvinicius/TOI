"""Shared interactive SSH access for TOI deployment and rollback."""
from __future__ import annotations

import getpass
from pathlib import Path
import shlex
import time

HOST = "10.90.11.20"
USER = "radius"
ROOT = "/var/www/toi"


def connect():
    import paramiko
    secret = getpass.getpass(f"Senha SSH/sudo de {USER}@{HOST}: ")
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.load_host_keys(str(Path.home() / ".ssh" / "known_hosts"))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(HOST, username=USER, password=secret, look_for_keys=False,
                   allow_agent=False, timeout=12)
    return client, secret


def run(client, command: str, password: str | None = None, *, timeout_s: float = 900):
    stdin, stdout, stderr = client.exec_command(command, timeout=30)
    if password is not None:
        stdin.write(password + "\n")
        stdin.flush()
    stdin.channel.shutdown_write()
    channel = stdout.channel
    output_chunks = []
    error_chunks = []
    deadline = time.monotonic() + timeout_s
    # Paramiko can deadlock if recv_exit_status is called while the remote
    # process has filled the channel window. Drain both streams first.
    while True:
        while channel.recv_ready():
            output_chunks.append(channel.recv(65536))
        while channel.recv_stderr_ready():
            error_chunks.append(channel.recv_stderr(65536))
        if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
            break
        if time.monotonic() >= deadline:
            channel.close()
            raise TimeoutError("Remote command exceeded its time limit")
        time.sleep(0.05)
    code = channel.recv_exit_status()
    output = b"".join(output_chunks).decode(errors="replace")
    error = b"".join(error_chunks).decode(errors="replace")
    if code:
        raise RuntimeError(f"Remote command failed ({code}): {error[-1000:]}")
    return output


def sync_units(client, password):
    units = ("toi-core.service", "toi-gateway.service", "toi-portal.service",
             "toi-alerts.service", "toi-health.service", "toi-health.timer",
             "toi-backup.service", "toi-backup.timer")
    steps = [f"install -m 644 {ROOT}/deploy/{unit} /etc/systemd/system/{unit}"
             for unit in units]
    for optional in ("toi-sbc-live.service", "toi-sbc-sessions.service", "toi-sip-inventory.service"):
        steps.append(f"if test -f {ROOT}/deploy/{optional}; then "
                     f"install -m 644 {ROOT}/deploy/{optional} /etc/systemd/system/{optional}; "
                     f"else rm -f /etc/systemd/system/{optional}; fi")
    steps.append("systemctl daemon-reload")
    run(client, "sudo -S -p '' bash -ec " + shlex.quote("; ".join(steps)), password)
