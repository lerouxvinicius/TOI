#!/usr/bin/env python3
"""Check local SNMP trap reception or wait for a genuine SBC notification.

Run as the ``toi`` user so the private community and raw capture are readable.
The synthetic mode sends only to loopback and never claims to test the SBC path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

from dotenv import dotenv_values
from pysnmp.hlapi.v3arch.asyncio import (
    CommunityData,
    ContextData,
    NotificationType,
    ObjectIdentity,
    ObjectType,
    SnmpEngine,
    UdpTransportTarget,
    send_notification,
)
from pysnmp.proto.rfc1902 import OctetString

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from teste_receber_traps_sbc import OID_TOI_SELFTEST, SBCS_CONHECIDOS  # noqa: E402

RAW_FILE = ROOT / "dados" / "sbc_traps_raw.jsonl"
TOKEN_OID = "1.3.6.1.3.9999.1.1.0"


def community() -> str:
    value = os.environ.get("SBC_TRAP_COMMUNITY")
    if value:
        return value
    env_file = ROOT / "private" / "toi.env"
    if env_file.exists():
        value = dotenv_values(env_file).get("SBC_TRAP_COMMUNITY")
        if value:
            return str(value)
    return "MGW MONITOR"


async def send_synthetic(token: str, port: int) -> None:
    snmp_engine = SnmpEngine()
    target = await UdpTransportTarget.create(("127.0.0.1", port), timeout=2, retries=0)
    try:
        result = await send_notification(
            snmp_engine,
            CommunityData(community(), mpModel=1),
            target,
            ContextData(),
            "trap",
            NotificationType(ObjectIdentity(OID_TOI_SELFTEST)).add_varbinds(
                ObjectType(ObjectIdentity(TOKEN_OID), OctetString(token))
            ),
        )
        if result[0] or result[1]:
            raise RuntimeError(f"Falha de envio SNMP: {result[0] or result[1]}")
    finally:
        snmp_engine.close_dispatcher()


def watch_records(path: Path, timeout: float, matches, *, start_position: int | None = None) -> dict | None:
    """Read only records appended after invocation, including after rotation."""
    deadline = time.monotonic() + timeout
    inode = None
    position = 0
    if path.exists():
        stat = path.stat()
        inode, position = stat.st_ino, stat.st_size if start_position is None else start_position
    while time.monotonic() < deadline:
        if path.exists():
            stat = path.stat()
            if stat.st_ino != inode or stat.st_size < position:
                inode, position = stat.st_ino, 0
            with path.open("r", encoding="utf-8") as stream:
                stream.seek(position)
                while line := stream.readline():
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if matches(record):
                        return record
                position = stream.tell()
        time.sleep(0.2)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("synthetic", "real"), default="synthetic")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--port", type=int, default=162, help="porta do receptor local")
    parser.add_argument("--source-ip", action="append", help="IP SBC esperado em modo real; repetível")
    parser.add_argument("--raw-file", type=Path, default=RAW_FILE)
    args = parser.parse_args()
    if args.timeout <= 0 or not 1 <= args.port <= 65535:
        parser.error("timeout e porta devem ser positivos e válidos")
    if not args.raw_file.parent.exists():
        parser.error(f"diretório de captura não encontrado: {args.raw_file.parent}")
    try:
        args.raw_file.stat()
    except PermissionError:
        parser.error("sem acesso à captura bruta; execute como usuário toi")

    if args.mode == "synthetic":
        token = uuid.uuid4().hex
        # Snapshot the file before sending; watch_records starts at its end.
        start = args.raw_file.stat().st_size if args.raw_file.exists() else 0
        asyncio.run(send_synthetic(token, args.port))
        # A fresh receiver writes after the packet is sent. Scan from the
        # pre-send offset so a fast callback cannot be missed.
        def is_ours(record):
            return (record.get("src_ip") == "127.0.0.1"
                    and record.get("notification_oid") == OID_TOI_SELFTEST
                    and any(item.get("oid") == TOKEN_OID and item.get("value") == token
                            for item in record.get("varbinds", [])))

        record = watch_records(args.raw_file, args.timeout, is_ours, start_position=start)
        if record:
            print(f"OK: trap sintético recebido localmente em UDP/{args.port}; token={token}")
            print("O caminho de rede e a configuração dos SBCs ainda exigem um trap real.")
            return 0
        print("FALHA: receptor não gravou o trap sintético; confira toi-alerts e community.", file=sys.stderr)
        return 1

    sources = set(args.source_ip or SBCS_CONHECIDOS)
    print(f"Aguardando trap real de {', '.join(sorted(sources))} por {args.timeout:g}s...", flush=True)
    record = watch_records(args.raw_file, args.timeout, lambda r: r.get("src_ip") in sources)
    if record:
        print(f"OK: trap real de {record['src_ip']} recebido em {record.get('received_utc')}; "
              f"OID={record.get('notification_oid') or record.get('v1_enterprise') or 'desconhecido'}")
        return 0
    print("SEM TRAP REAL no intervalo; verificar destino, rota e firewall nos SBCs.", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
