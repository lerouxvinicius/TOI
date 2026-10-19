"""Independent, bounded SNMP sampling for the Norte and Sul TOI cards.

The alert monitor retains ownership of incident confirmation. This worker owns
only live SBC gauges and their timestamped history; it never calls Portal Orbe.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import time

from pysnmp.hlapi.v3arch.asyncio import (
    CommunityData, ContextData, ObjectIdentity, ObjectType, SnmpEngine,
    UdpTransportTarget, bulk_walk_cmd, get_cmd,
)

from mgw.sbc_monitor import (
    OID_AGENT_HOSTNAME, OID_AGENT_INBOUND_ACTIVE, OID_AGENT_OUTBOUND_ACTIVE,
    OID_AGENT_STATUS, OID_CPU, OID_CPS, OID_H323_STACK_CURRENT_CALLS,
    OID_HA, OID_HEALTH, OID_MEMORIA, OID_SESSOES, OID_SYSNAME,
    traduzir_ha,
)
from mgw.sip_capacity import (
    AMBIENTES, OID_INBOUND_ACTIVE, OID_OUTBOUND_ACTIVE, OID_REALM_NAME,
)

ROOT = Path(__file__).resolve().parent.parent
CURRENT = ROOT / "dados" / "sbc_live"
INTERVAL_S = 15
POLL_TIMEOUT_S = 12
SCALAR_OIDS = (OID_SYSNAME, OID_HA, OID_CPU, OID_MEMORIA, OID_HEALTH,
               OID_SESSOES, OID_CPS)


def _count(value):
    try:
        number = int(str(value))
        return number if number >= 0 else None
    except (TypeError, ValueError):
        return None


def _index(oid, root):
    full = tuple(int(x) for x in oid)
    prefix = tuple(int(x) for x in root.split("."))
    return ".".join(map(str, full[len(prefix):])) if full[:len(prefix)] == prefix and len(full) > len(prefix) else None


async def _transport(ip):
    return await UdpTransportTarget.create((ip, 161), timeout=1.5, retries=0)


async def _scalars(engine, community, ip):
    error, status, _, bindings = await get_cmd(
        engine, CommunityData(community, mpModel=1), await _transport(ip),
        ContextData(), *(ObjectType(ObjectIdentity(oid)) for oid in SCALAR_OIDS),
    )
    if error or status or len(bindings) != len(SCALAR_OIDS):
        raise RuntimeError(str(error or status or "incomplete scalar reply"))
    values = [value.prettyPrint() for _, value in bindings]
    return {"ip": ip, "hostname": values[0], "ha": traduzir_ha(values[1]),
            "cpu": _count(values[2]), "memoria": _count(values[3]),
            "health": _count(values[4]), "sessions_total": _count(values[5]),
            "cps": _count(values[6])}


async def _walk(engine, community, ip, oid):
    result = {}
    async for error, status, _, bindings in bulk_walk_cmd(
        engine, CommunityData(community, mpModel=1), await _transport(ip),
        ContextData(), 0, 15, ObjectType(ObjectIdentity(oid)),
        lexicographicMode=False,
    ):
        if error or status:
            raise RuntimeError(str(error or status))
        for key, value in bindings:
            suffix = _index(key, oid)
            if suffix is not None:
                result[suffix] = value.prettyPrint()
    return result


async def _agents(engine, community, ip):
    names, statuses, inbound, outbound = await asyncio.gather(*(
        _walk(engine, community, ip, oid) for oid in
        (OID_AGENT_HOSTNAME, OID_AGENT_STATUS,
         OID_AGENT_INBOUND_ACTIVE, OID_AGENT_OUTBOUND_ACTIVE)
    ))
    return {str(name).strip(): {
        "status_bruto": statuses.get(index),
        "inbound_active": _count(inbound.get(index)),
        "outbound_active": _count(outbound.get(index)),
    } for index, name in names.items()}


async def _realms(engine, community, ip, wanted):
    names, inbound, outbound = await asyncio.gather(*(
        _walk(engine, community, ip, oid) for oid in
        (OID_REALM_NAME, OID_INBOUND_ACTIVE, OID_OUTBOUND_ACTIVE)
    ))
    indices = {str(name).strip().casefold(): index for index, name in names.items()}
    return {name: {
        "inbound_active": _count(inbound.get(indices[name.casefold()])) if name.casefold() in indices else None,
        "outbound_active": _count(outbound.get(indices[name.casefold()])) if name.casefold() in indices else None,
    } for name in wanted}


async def collect_pair(community, config):
    engines = {ip: SnmpEngine() for ip in config["sbcs"]}
    try:
        scalar_results = await asyncio.gather(*(
            _scalars(engines[ip], community, ip) for ip in config["sbcs"]
        ), return_exceptions=True)
        nodes = []
        for ip, value in zip(config["sbcs"], scalar_results):
            nodes.append({"ip": ip, "error": str(value) if isinstance(value, Exception) else None}
                         if isinstance(value, Exception) else value)
        active = next((node for node in nodes if node.get("ha") == "ACTIVE"), None)
        tasks = [(_agents(engines[node["ip"]], community, node["ip"])
                  if not node.get("error") else _empty()) for node in nodes]
        tasks.extend((_walk(engines[node["ip"]], community, node["ip"], OID_H323_STACK_CURRENT_CALLS)
                      if not node.get("error") else _empty()) for node in nodes)
        if active:
            tasks.append(_realms(engines[active["ip"]], community,
                                active["ip"], config["realms"]))
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for node, value in zip(nodes, gathered[:len(nodes)]):
            node["agents"] = value if isinstance(value, dict) else {}
            if isinstance(value, Exception):
                node["agent_error"] = str(value)
        for node, value in zip(nodes, gathered[len(nodes):2 * len(nodes)]):
            h323 = sum(_count(item) or 0 for item in value.values()) if isinstance(value, dict) else 0
            total = node.get("sessions_total")
            node["sip_sessions_ativas"] = max(0, total - h323) if total is not None else None
        realm_result = gathered[-1] if active else None
        realms = realm_result if isinstance(realm_result, dict) else {
            name: {"inbound_active": None, "outbound_active": None}
            for name in config["realms"]}
        source_ok = active is not None and isinstance(realm_result, dict) and any(
            value["inbound_active"] is not None or value["outbound_active"] is not None
            for value in realms.values())
        stamp = datetime.now().astimezone().isoformat(timespec="seconds")
        return {"timestamp": stamp, "ambiente": next(
                    name for name, item in AMBIENTES.items() if item["slug"] == config["slug"]),
                "sbc_active": {"ip": active["ip"], "hostname": active.get("hostname")}
                if active else None,
                "source_ok": bool(source_ok), "realms": realms, "nodes": nodes,
                "error": str(realm_result) if isinstance(realm_result, Exception) else
                ("No ACTIVE SBC" if not active else None),
                "interval_s": INTERVAL_S}
    finally:
        for engine in engines.values():
            engine.close_dispatcher()


async def _empty():
    return {}


def save_pair(config, snapshot, *, root=ROOT):
    root = Path(root)
    slug = config["slug"]
    encoded = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    if snapshot["source_ok"]:
        legacy = root / "dados" / "sip_capacity"
        if slug != "norte":
            legacy /= slug
        legacy.mkdir(parents=True, exist_ok=True)
        day = snapshot["timestamp"][:10]
        with (legacy / f"sip_capacity_{day}.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(encoded + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    current_dir = root / "dados" / "sbc_live"
    current_dir.mkdir(parents=True, exist_ok=True)
    current_path = current_dir / f"{slug}.json"
    temporary = current_dir / f".{slug}.tmp"
    temporary.write_text(encoded, encoding="utf-8")
    temporary.replace(current_path)


async def pair_loop(community, config):
    while True:
        started = time.monotonic()
        try:
            snapshot = await asyncio.wait_for(collect_pair(community, config), POLL_TIMEOUT_S)
            save_pair(config, snapshot)
            print(f"SBC live {config['slug']}: age=0 source_ok={snapshot['source_ok']} "
                  f"active={(snapshot['sbc_active'] or {}).get('ip')} "
                  f"duration={time.monotonic() - started:.1f}s", flush=True)
        except Exception as exc:
            stamp = datetime.now().astimezone().isoformat(timespec="seconds")
            save_pair(config, {"timestamp": stamp, "source_ok": False,
                               "sbc_active": None, "realms": {}, "nodes": [],
                               "error": f"{type(exc).__name__}: {exc}",
                               "interval_s": INTERVAL_S})
            print(f"SBC live {config['slug']} failed: {type(exc).__name__}: {exc}", flush=True)
        await asyncio.sleep(max(0.0, INTERVAL_S - (time.monotonic() - started)))


def main():
    community = os.environ.get("SBC_SNMP_COMMUNITY", "").strip()
    if not community:
        raise RuntimeError("SBC_SNMP_COMMUNITY is required")
    async def run():
        await asyncio.gather(*(pair_loop(community, config) for config in AMBIENTES.values()))
    asyncio.run(run())


if __name__ == "__main__":
    main()
