"""Create and remove a restricted test account while checking live authorization."""
from pathlib import Path
import json
import secrets
import sys

import requests

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"))
import toi_shared_admin_v1 as admin

admin.setup()
login = "__audit_" + secrets.token_hex(6)
password = "Aa7!" + secrets.token_urlsafe(20)
with admin.db(admin.AUTH_DB) as con:
    cur = con.execute(
        "INSERT INTO users(login,name,hash,role,instances,active,must_change) "
        "VALUES(?,?,?,?,?,1,0)",
        (login, "Temporary authorization test", admin.HASHER.hash(password),
         "Operador", json.dumps(["4"])),
    )
    user_id = cur.lastrowid

try:
    base = "http://127.0.0.1:8099/toi"
    session = requests.Session()
    response = session.post(base + "/api/admin/login",
                            json={"login": login, "password": password}, timeout=10)
    assert response.status_code == 200, f"login {response.status_code}"
    csrf = response.json()["csrf"]
    queues = session.get(base + "/api/queues", timeout=10)
    assert queues.status_code == 200
    assert all(str(row["instancia"]) == "4" for row in queues.json())
    checks = [
        ("/api/admin/users", 403),
        ("/api/overview?instancia=5&codigo=0247400047", 403),
        ("/api/overview", 400),
        ("/api/knowledge/cases", 400),
        ("/toi_gateway_health_latest.json", 403),
    ]
    for path, expected in checks:
        actual = session.get(base + path, timeout=10).status_code
        assert actual == expected, f"{path}: {actual}, wanted {expected}"
    response = session.post(base + "/api/operational/feedback",
                            headers={"X-CSRF-Token": csrf},
                            json={"instancia": "5", "entity": "test"}, timeout=10)
    assert response.status_code == 403, f"feedback: {response.status_code}"
    response = session.post(base + "/api/ipbx/trace?status=19",
                            headers={"X-CSRF-Token": csrf}, timeout=10)
    assert response.status_code == 403, f"IPBX trace: {response.status_code}"
    print("Role, instance, snapshot, feedback and report authorization OK")
finally:
    with admin.db(admin.AUTH_DB) as con:
        con.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        con.execute("DELETE FROM users WHERE id=?", (user_id,))
