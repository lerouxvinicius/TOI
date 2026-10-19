"""Sobe o TOI em modo demonstração, com dados fictícios, num único comando.

    python demo/run_demo.py

1. gera a base demo (se ainda não existir) e continua gravando amostras a cada 10 s;
2. inicia o painel em 127.0.0.1:8088;
3. publica o painel em http://127.0.0.1:8099/toi/ — o mesmo prefixo que o Nginx usa em produção.
"""
from __future__ import annotations

import http.client
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "dados_toi" / "live" / "toi_demo.sqlite"
BACKEND = ("127.0.0.1", 8088)
PUBLIC_PORT = 8099
HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "upgrade", "proxy-connection"}


class PrefixProxy(BaseHTTPRequestHandler):
    """Equivalente local de `location /toi/ { proxy_pass http://127.0.0.1:8088/; }`."""

    def _forward(self):
        if self.path in ("/", "/toi"):
            self.send_response(302)
            self.send_header("Location", "/toi/login")
            self.end_headers()
            return
        if not self.path.startswith("/toi/"):
            self.send_error(404)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0)) or None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP}
        headers["X-Forwarded-For"] = self.client_address[0]
        conn = http.client.HTTPConnection(*BACKEND, timeout=60)
        try:
            conn.request(self.command, self.path[len("/toi"):], body, headers)
            resp = conn.getresponse()
            data = resp.read()
        except OSError:
            self.send_error(502, "Painel ainda iniciando")
            return
        finally:
            conn.close()
        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() not in HOP_BY_HOP and k.lower() != "content-length":
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _forward

    def log_message(self, *args):
        pass


def main() -> int:
    py = sys.executable
    seed_args = [py, str(ROOT / "demo" / "seed_demo.py"), "--follow"]
    if DB.exists():
        seed_args += ["--hours", "0"]
    seeder = subprocess.Popen(seed_args, cwd=ROOT)
    while not DB.exists():
        time.sleep(0.5)
    time.sleep(2)
    dashboard = subprocess.Popen([py, str(ROOT / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW" / "toi_dashboard_server_v1_10_6.py"),
                                  "--db", str(DB), "--port", str(BACKEND[1])], cwd=ROOT)
    server = ThreadingHTTPServer(("127.0.0.1", PUBLIC_PORT), PrefixProxy)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"\nTOI demo: http://127.0.0.1:{PUBLIC_PORT}/toi/login  (usuário e senha em demo/seed_demo.py)\n")
    try:
        dashboard.wait()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        for proc in (dashboard, seeder):
            proc.terminate()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
