"""Restore the previous tracked TOI code release without touching its data."""
from __future__ import annotations

from remote_access import connect, run, sync_units, ROOT


def main():
    client, password = connect()
    try:
        if run(client, f"git -C {ROOT} status --porcelain").strip():
            raise RuntimeError("Remote code has uncommitted changes; review them first")
        previous = run(client, f"git -C {ROOT} rev-parse HEAD^").strip()
        sip = 'toi-sip-inventory.service'
        sip_active = run(client, f'systemctl show -p ActiveState --value {sip}').strip() == 'active'
        if sip_active:
            run(client, f"sudo -S -p '' systemctl disable --now {sip}", password)
        sessions = "toi-sbc-sessions.service"
        had_sessions = run(client, f"systemctl show -p LoadState --value {sessions}").strip() == "loaded"
        if had_sessions:
            run(client, f"sudo -S -p '' systemctl disable --now {sessions}", password)
        run(client, f"sudo -S -p '' systemctl stop toi-alerts.service toi-portal.service "
            f"toi-gateway.service toi-core.service", password)
        run(client, f"git -C {ROOT} reset --hard {previous}")
        sync_units(client, password)
        if sip_active and run(client, f"test -f {ROOT}/deploy/{sip} && echo yes || true").strip() == 'yes':
            run(client, f"sudo -S -p '' systemctl enable --now {sip}", password)
        run(client, f"sudo -S -p '' systemctl start toi-core.service toi-gateway.service "
            f"toi-portal.service toi-alerts.service", password)
        if had_sessions and run(client, f"test -f {ROOT}/deploy/{sessions} && echo yes || true").strip() == "yes":
            run(client, f"sudo -S -p '' systemctl enable --now {sessions}", password)
        print("Código anterior restaurado; dados e segredos preservados.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
