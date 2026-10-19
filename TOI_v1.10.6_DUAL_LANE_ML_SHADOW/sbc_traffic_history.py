"""Read-only SBC realm history for the TOI card.

Each value is an observed SNMP gauge. Missing or stale readings remain unknown;
realm counts are never summed because a call may appear on two realms.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

from sbc_card_bridge import AGENTES_MONITORADOS
from sbc_session_counts import session_report

PAIRS = {
    "norte": {
        "name": "SBCs NORTE", "folder": "",
        "realms": ("a-alfa-2", "a-internet-corpx", "a-thetalink-corpx",
                   "a-alfa-4", "a-alfa-4-stir"),
    },
    "sul": {
        "name": "SBCs SUL", "folder": "sul",
        "realms": ("a-beta-link1", "a-beta-link2", "a-Epsilon", "a-Prefeitura"),
    },
}


def _count(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _day_path(root: Path, config: dict, day: date) -> Path:
    return root / "dados" / "sip_capacity" / config["folder"] / f"sip_capacity_{day.isoformat()}.jsonl"


def _live(root: Path, pair: str) -> dict | None:
    path = root / "dados" / "sbc_live" / f"{pair}.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (OSError, ValueError, UnicodeError):
        return None


def _rows(path: Path, config: dict, *, now: datetime) -> list[dict]:
    if not path.is_file():
        return []
    result = []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                source = json.loads(line)
                stamp = datetime.fromisoformat(source["timestamp"])
                if not isinstance(source.get("realms"), dict):
                    continue
                observed = stamp if stamp.tzinfo else stamp.replace(tzinfo=now.tzinfo)
                if observed > now:
                    continue
            except (ValueError, KeyError, TypeError):
                continue
            realms = {}
            for name in config["realms"]:
                observed = source["realms"].get(name)
                observed = observed if isinstance(observed, dict) else {}
                realms[name] = {
                    "inbound": _count(observed.get("inbound_active")),
                    "outbound": _count(observed.get("outbound_active")),
                }
            active = source.get("sbc_active") or {}
            result.append({"timestamp": stamp.isoformat(timespec="seconds"),
                           "sbc_ip": active.get("ip"), "realms": realms})
    result.sort(key=lambda item: item["timestamp"])
    return result


def _peak(rows: list[dict], realm: str, direction: str) -> dict:
    values = [(row["realms"][realm][direction], row["timestamp"]) for row in rows
              if row["realms"][realm][direction] is not None]
    if not values:
        return {"peak": None, "at": None, "mean": None, "readings": 0}
    peak, at = max(values, key=lambda item: item[0])
    return {"peak": peak, "at": at,
            "mean": round(sum(value for value, _ in values) / len(values), 1),
            "readings": len(values)}


def live_report(root: Path, pair: str, *, now: datetime | None = None) -> dict:
    if pair not in PAIRS:
        raise ValueError("Par SBC desconhecido")
    now = now or datetime.now().astimezone()
    config = PAIRS[pair]
    live = _live(Path(root), pair)
    age = None
    if live and live.get("timestamp"):
        try:
            stamp = datetime.fromisoformat(live["timestamp"])
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=now.tzinfo)
            age = (now - stamp).total_seconds()
        except ValueError:
            pass
    fresh = bool(live and live.get("source_ok") and age is not None and -5 <= age <= 45)
    current = None
    if fresh:
        current = {"timestamp": live["timestamp"],
                   "sbc_ip": (live.get("sbc_active") or {}).get("ip"),
                   "realms": {name: {
                       "inbound": _count((live.get("realms", {}).get(name) or {}).get("inbound_active")),
                       "outbound": _count((live.get("realms", {}).get(name) or {}).get("outbound_active")),
                   } for name in config["realms"]}}
    nodes = []
    if fresh:
        members = live.get("nodes")
        for node in members if isinstance(members, list) else []:
            if not isinstance(node, dict):
                continue
            found = node.get("agents") if isinstance(node.get("agents"), dict) else {}
            agents = []
            for ip, label in AGENTES_MONITORADOS[config["name"]].items():
                item = found.get(ip) if isinstance(found.get(ip), dict) else None
                agents.append({"ip": ip, "name": label, "observed": item is not None,
                               "status": item.get("status_bruto") if item else None,
                               "inbound_active": _count(item.get("inbound_active")) if item else None,
                               "outbound_active": _count(item.get("outbound_active")) if item else None})
            nodes.append({"ip": node.get("ip"), "hostname": node.get("hostname"),
                          "role": node.get("ha"), "agents": agents})
    return {"pair": pair, "name": config["name"], "realms": list(config["realms"]),
            "current": current,
            "current_age_s": round(max(0.0, age), 1) if age is not None else None,
            "current_fresh": fresh,
            "nodes": nodes,
            "live_error": (live or {}).get("error"),
            "poll_interval_s": 15, "stale_after_s": 45}


def traffic_report(root: Path, pair: str, day: str | None = None,
                   hour: int | None = None, realm: str | None = None,
                   *, now: datetime | None = None) -> dict:
    if pair not in PAIRS:
        raise ValueError("Par SBC desconhecido")
    now = now or datetime.now().astimezone()
    try:
        selected = date.fromisoformat(day) if day else now.date()
    except ValueError as exc:
        raise ValueError("Data inválida") from exc
    if selected > now.date() or selected < now.date() - timedelta(days=366):
        raise ValueError("Data fora do intervalo consultável")
    if hour is not None and (not isinstance(hour, int) or not 0 <= hour <= 23):
        raise ValueError("Hora inválida")
    config = PAIRS[pair]
    if realm and realm not in config["realms"]:
        raise ValueError("Realm desconhecido para este par SBC")
    selected_realms = (realm,) if realm else config["realms"]
    root = Path(root)
    rows = _rows(_day_path(root, config, selected), config, now=now)
    live = live_report(root, pair, now=now)
    today_rows = rows if selected == now.date() else _rows(_day_path(root, config, now.date()), config, now=now)
    last_observed = today_rows[-1]["timestamp"] if today_rows else None
    buckets = [[] for _ in range(24)]
    for row in rows:
        buckets[datetime.fromisoformat(row["timestamp"]).hour].append(row)
    hours = []
    for value, bucket in enumerate(buckets):
        hours.append({"hour": value, "samples": len(bucket),
                      "realms": {name: {"inbound": _peak(bucket, name, "inbound"),
                                        "outbound": _peak(bucket, name, "outbound")}
                                 for name in config["realms"]}})
    scope = buckets[hour] if hour is not None else rows
    detail = [{**row, "realms": {name: row["realms"][name] for name in selected_realms}}
              for row in scope] if hour is not None else []
    realm_summary = {name: {"inbound": _peak(scope, name, "inbound"),
                             "outbound": _peak(scope, name, "outbound")}
                     for name in selected_realms}
    def greatest(direction: str) -> dict:
        values = [(row["realms"][name][direction], row["timestamp"], name)
                  for row in scope for name in config["realms"]
                  if row["realms"][name][direction] is not None]
        if not values:
            return {"peak": None, "at": None, "realm": None}
        value, stamp, name = max(values, key=lambda item: item[0])
        return {"peak": value, "at": stamp, "realm": name}
    active_realms = sum(any(
        (row["realms"][name][direction] or 0) > 0
        for row in scope for direction in ("inbound", "outbound")
    ) for name in config["realms"])
    return {
        "pair": pair, "name": config["name"], "date": selected.isoformat(),
        "hour": hour, "realm": realm, "realms": list(config["realms"]),
        "selected_realms": list(selected_realms), "samples": len(rows),
        "consolidated": {
            "samples": len(scope),
            "first_at": scope[0]["timestamp"] if scope else None,
            "last_at": scope[-1]["timestamp"] if scope else None,
            "realms_with_traffic": active_realms,
            "total_realms": len(config["realms"]),
            "inbound": greatest("inbound"),
            "outbound": greatest("outbound"),
        },
        "realm_summary": realm_summary,
        "new_sessions": session_report(root, pair, config["realms"], selected, hour, now=now),
        **{key: live[key] for key in ("current", "current_age_s", "current_fresh",
                                     "nodes", "live_error", "poll_interval_s", "stale_after_s")},
        "last_observed": last_observed,
        "daily": {name: {"inbound": _peak(rows, name, "inbound"),
                         "outbound": _peak(rows, name, "outbound")}
                  for name in config["realms"]},
        "hourly": hours,
        "readings": detail,
        "source": "SBC SNMP apSigRealmStatsEntry (.2/.3/.5)",
        "note": "A MIB agrega SIP e H.323 quando ambos estão configurados no realm.",
    }
