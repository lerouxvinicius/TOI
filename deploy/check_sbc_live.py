"""Exit successfully only when both independent SBC sources are recent."""
import json
from datetime import datetime
from pathlib import Path

ROOT = Path("/var/www/toi/dados/sbc_live")
now = datetime.now().astimezone()
report = {}
for pair in ("norte", "sul"):
    payload = json.loads((ROOT / f"{pair}.json").read_text(encoding="utf-8"))
    stamp = datetime.fromisoformat(payload["timestamp"])
    age = (now - stamp).total_seconds()
    ok = bool(payload.get("source_ok")) and -5 <= age <= 45
    report[pair] = {"age_s": round(age, 1), "ok": ok,
                    "active": (payload.get("sbc_active") or {}).get("ip")}
    if not ok:
        raise RuntimeError(f"{pair} SBC live source is stale or unavailable: {report[pair]}")
print(json.dumps(report))
