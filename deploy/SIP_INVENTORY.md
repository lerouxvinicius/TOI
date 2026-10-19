# SIP Servers and dashboard-only publication

`toi-sip-inventory.service` reads the Portal SIP inventory independently for each
configured instance every 15 seconds. It uses the existing private environment
file and stores samples in `dados/portal_sip.sqlite` (WAL, synchronous FULL).
Failures preserve the last valid sample with an unavailable/stale indication.
The `/api/sip-servers` and `/api/infrastructure` routes require an instance,
not a queue. The former `/api/sip` call-response statistics route is preserved.

For this release run `python deploy/publish.py --dashboard-only --sip-inventory --audit-sbc`.
The script backs up data, checks the supervisor restart budget and process
identities, reloads only its dashboard child, and verifies that the collector PID
has not changed. Acquisition code changes are rejected in dashboard-only mode.
It then verifies fresh inventory across all configured instances. Failure restores
the previous code and service state; SQLite data is retained.

Diagnostics are saved under the local `outputs` directory as
`toi_dashboard_reload.json`, `toi_instance_inventory_audit.json`, and, on SIP
startup failure, `toi_sip_inventory_failure.json`.
