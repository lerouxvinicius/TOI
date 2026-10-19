"""Coleta somente leitura de evidencias SBC para homologacao do peer HA.

Executar no Windows onde SBC_SNMP_COMMUNITY e pysnmp ja estao configurados.
Nao escuta UDP/162 e nao interfere no listener de traps existente.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys

SBCS = ("10.90.11.132", "10.90.11.133", "10.90.13.10", "10.90.13.11")
HISTORY_ENTRY = "1.3.6.1.4.1.9148.3.1.1.2.3"
FIELDS = {"1": "index", "2": "from", "3": "level", "4": "type", "5": "content", "6": "sysuptime"}
OIDS = {
    "sysdescr": "1.3.6.1.2.1.1.1.0",
    "sysname": "1.3.6.1.2.1.1.5.0",
    "ha_role": "1.3.6.1.4.1.9148.3.2.1.1.4.0",
    "syslog_notifications_sent": "1.3.6.1.4.1.9148.3.1.1.1.1.0",
    "syslog_notifications_enabled": "1.3.6.1.4.1.9148.3.1.1.1.2.0",
    "syslog_max_level": "1.3.6.1.4.1.9148.3.1.1.1.3.0",
    "syslog_history_max_length": "1.3.6.1.4.1.9148.3.1.1.2.1.0",
    "syslog_history_flushed": "1.3.6.1.4.1.9148.3.1.1.2.2.0",
}
MATCH = re.compile(r"peer|redundan|switchover|timed\s*out|131106|131104|131105", re.I)


def parse_history(varbinds):
    """Preserva OID e indice de cada coluna; nao interpreta CLEAR automaticamente."""
    rows = {}
    base = HISTORY_ENTRY + "."
    for oid, value in varbinds:
        if not oid.startswith(base):
            continue
        suffix = oid[len(base):].split(".")
        if len(suffix) != 2 or suffix[0] not in FIELDS or not suffix[1].isdigit():
            continue
        field, index = suffix
        row = rows.setdefault(index, {"history_index": index, "varbinds": {}})
        row[FIELDS[field]] = value
        row["varbinds"][oid] = value
    return [rows[key] for key in sorted(rows, key=int)]


async def collect_one(ip, community):
    from pysnmp.hlapi.v3arch.asyncio import (CommunityData, ContextData,
        ObjectIdentity, ObjectType, SnmpEngine, UdpTransportTarget, get_cmd, walk_cmd)

    result = {"emissor": ip, "scalars": {}, "history_matches": [],
              "history_rows_total": 0, "errors": []}
    engine = SnmpEngine()
    try:
        target = await UdpTransportTarget.create((ip, 161), timeout=3, retries=1)
        for label, oid in OIDS.items():
            try:
                indication, status, _, binds = await get_cmd(
                    engine, CommunityData(community, mpModel=1), target,
                    ContextData(), ObjectType(ObjectIdentity(oid)))
                if indication or status or not binds:
                    raise RuntimeError(str(indication or status or "resposta vazia"))
                result["scalars"][label] = {"oid": oid, "value": binds[0][1].prettyPrint()}
            except Exception as exc:
                result["errors"].append(f"GET {label}: {exc}")
        raw = []
        try:
            async for indication, status, _, binds in walk_cmd(
                engine, CommunityData(community, mpModel=1), target,
                ContextData(), ObjectType(ObjectIdentity(HISTORY_ENTRY)),
                lexicographicMode=False):
                if indication or status:
                    raise RuntimeError(str(indication or status))
                raw.extend((oid.prettyPrint(), val.prettyPrint()) for oid, val in binds)
                if len(raw) >= 3600:
                    result["errors"].append("WALK limitado em 3600 varbinds")
                    break
            rows = parse_history(raw)
            result["history_rows_total"] = len(rows)
            result["history_matches"] = [row for row in rows if MATCH.search(
                " ".join(str(row.get(k, "")) for k in ("from", "type", "content")))]
        except Exception as exc:
            result["errors"].append(f"WALK history: {exc}")
    finally:
        engine.transport_dispatcher.close_dispatcher()
    return result


async def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", type=Path, default=Path("logs/sbc_evidencia_peer.json"))
    args = ap.parse_args()
    community = os.environ.get("SBC_SNMP_COMMUNITY", "")
    if not community:
        print("Defina SBC_SNMP_COMMUNITY no ambiente; nenhum GET foi executado.", file=sys.stderr)
        return 2
    report = {"collected_utc": datetime.now(timezone.utc).isoformat(),
              "method": "SNMP GET/WALK read-only; history nao e status de alarme ativo",
              "nodes": []}
    for ip in SBCS:
        report["nodes"].append(await collect_one(ip, community))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Gravado {args.output}; hist\u00f3rico n\u00e3o confirma CLEAR nem heartbeat vivo.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
