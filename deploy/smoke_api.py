"""Authenticated smoke check; reads the one-time password from standard input."""
import sys

import requests

base = "http://127.0.0.1:8099/toi"
password = sys.stdin.readline().rstrip("\n")
assert password, "password required on stdin"
session = requests.Session()

def get(path):
    return session.get(base + path, timeout=10)

assert get("/login").status_code == 200
assert get("/styles.css").status_code == 200
assert get("/api/session").status_code == 401
assert get("/api/radar").status_code == 401
assert get("/toi_gateway_health_latest.json").status_code == 401

response = session.post(base + "/api/admin/login",
                        json={"login": "100001", "password": password}, timeout=10)
assert response.status_code == 200, f"login HTTP {response.status_code}"
cookie = response.headers.get("Set-Cookie", "")
assert "HttpOnly" in cookie and "SameSite=Strict" in cookie and "Path=/toi/" in cookie
me = get("/api/admin/me")
assert me.status_code == 200
csrf = me.json()["csrf"]
assert get("/api/session").status_code == 200
assert get("/api/radar").status_code == 403  # first password change required
assert session.post(base + "/api/admin/logout", json={}, timeout=10).status_code == 403
assert session.post(base + "/api/admin/logout", json={},
                    headers={"X-CSRF-Token": csrf}, timeout=10).status_code == 200
assert get("/api/session").status_code == 401
print("TOI login, session, CSRF and first-password gate OK")
