"""Usuários, sessões e auditoria próprios do TOI antigo, com senhas Argon2id."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import threading
import time

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError


ROOT = Path(__file__).resolve().parent
STATE_DB = ROOT.parent / "dados_toi" / "admin" / "toi_legacy_admin.sqlite"
AUTH_DB = STATE_DB
CONTEXT_FILE = ROOT / "static" / "toi_operational_context_v1.json"
HASHER = PasswordHasher()
ROLES = {"Administrador", "Gestor", "Operador", "Consulta"}
_LOCK = threading.Lock()


@contextmanager
def db(path):
    con = sqlite3.connect(path, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def setup():
    STATE_DB.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK, db(STATE_DB) as con:
        con.executescript("""
          CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,user_id INTEGER,expires REAL,csrf TEXT);
          CREATE TABLE IF NOT EXISTS attempts(key TEXT PRIMARY KEY,count INTEGER,since REAL);
          CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY,at TEXT,user TEXT,action TEXT,target TEXT);
          CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY,login TEXT NOT NULL UNIQUE,name TEXT,hash TEXT,
            role TEXT,instances TEXT,active INTEGER DEFAULT 1,must_change INTEGER DEFAULT 1);
        """)


def audit(user, action, target=""):
    with db(STATE_DB) as con:
        con.execute("INSERT INTO audit(at,user,action,target) VALUES(?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat(), user, action, str(target)))


def audit_events(limit=100):
    setup()
    with db(STATE_DB) as con:
        return [dict(row) for row in con.execute(
            "SELECT at,user,action,target FROM audit ORDER BY id DESC LIMIT ?",
            (max(1, min(int(limit), 200)),))]


def fingerprint(value):
    return hashlib.sha256(value.encode()).hexdigest()


def safe_user(user):
    return {key: user[key] for key in
            ("id", "login", "name", "role", "instances", "active", "must_change")}


def login(login_name, password, ip):
    setup()
    key = fingerprint(ip)
    with db(STATE_DB) as state:
        attempt = state.execute("SELECT * FROM attempts WHERE key=?", (key,)).fetchone()
        if attempt and time.time()-attempt["since"] < 900 and attempt["count"] >= 10:
            return None, "Muitas tentativas. Aguarde 15 minutos.", 429
    with db(AUTH_DB) as auth:
        user = auth.execute("SELECT * FROM users WHERE login=? AND active=1",
                            (str(login_name)[:120],)).fetchone()
    valid = False
    try:
        valid = bool(user and HASHER.verify(user["hash"], str(password)[:512]))
    except (VerificationError, InvalidHashError):
        pass
    if not valid:
        with db(STATE_DB) as state:
            count = attempt["count"]+1 if attempt and time.time()-attempt["since"] < 900 else 1
            since = attempt["since"] if count > 1 else time.time()
            state.execute("INSERT OR REPLACE INTO attempts VALUES(?,?,?)", (key, count, since))
        audit("anônimo", "login_failed")
        return None, "Login ou senha inválidos.", 401
    token, csrf = secrets.token_urlsafe(40), secrets.token_urlsafe(32)
    with db(STATE_DB) as state:
        state.execute("DELETE FROM attempts WHERE key=?", (key,))
        state.execute("DELETE FROM sessions WHERE expires<?", (time.time(),))
        state.execute("INSERT INTO sessions VALUES(?,?,?,?)",
                      (fingerprint(token), user["id"], time.time()+8*3600, csrf))
    audit(user["login"], "login")
    return (token, csrf, safe_user(user)), None, 200


def current(token):
    if not token:
        return None
    setup()
    with db(STATE_DB) as state:
        session = state.execute("SELECT * FROM sessions WHERE token=? AND expires>?",
                                (fingerprint(token), time.time())).fetchone()
    if not session:
        return None
    with db(AUTH_DB) as auth:
        user = auth.execute("SELECT * FROM users WHERE id=? AND active=1",
                            (session["user_id"],)).fetchone()
    if not user:
        return None
    return {**dict(user), "csrf": session["csrf"]}


def logout(token, user):
    with db(STATE_DB) as state:
        state.execute("DELETE FROM sessions WHERE token=?", (fingerprint(token),))
    audit(user["login"], "logout")


def require_admin(user, csrf=None):
    if not user:
        raise PermissionError("Autenticação necessária")
    if user["role"] != "Administrador":
        raise PermissionError("Acesso de administrador necessário")
    if csrf is not None and not secrets.compare_digest(str(csrf), user["csrf"]):
        raise PermissionError("Sessão de formulário inválida")


def users():
    with db(AUTH_DB) as auth:
        return [safe_user(row) for row in auth.execute("SELECT * FROM users ORDER BY login")]


def _instances(value):
    if not isinstance(value, list) or not value or any(
            not isinstance(item, str) or not 1 <= len(item) <= 30 for item in value):
        raise ValueError("Informe as instâncias autorizadas")
    return json.dumps(value)


def _revoke(ident):
    with db(STATE_DB) as state:
        state.execute("DELETE FROM sessions WHERE user_id=?", (ident,))


def validate_password(password):
    value = str(password)
    if not (10 <= len(value) <= 128 and all(char.isprintable() and not char.isspace() for char in value)
            and any(char.isupper() for char in value)
            and any(char.islower() for char in value)
            and any(char.isdigit() for char in value)
            and any(not char.isalnum() for char in value)):
        raise ValueError("A senha deve ter de 10 a 128 caracteres, com letra maiúscula, minúscula, número e caractere especial, sem espaços")


def change_password(user, current_password, new_password):
    validate_password(new_password)
    with db(AUTH_DB) as auth:
        row = auth.execute("SELECT hash FROM users WHERE id=? AND active=1", (user["id"],)).fetchone()
        try:
            valid = bool(row and HASHER.verify(row["hash"], str(current_password)))
        except (VerificationError, InvalidHashError):
            valid = False
        if not valid:
            raise PermissionError("Senha atual inválida")
        auth.execute("UPDATE users SET hash=?,must_change=0 WHERE id=?",
                     (HASHER.hash(str(new_password)), user["id"]))
        auth.execute("DELETE FROM sessions WHERE user_id=?", (user["id"],))
    audit(user["login"], "password_changed")


def create_user(actor, body):
    login_name = str(body.get("login", "")).strip()
    name = str(body.get("name", "")).strip()
    password = str(body.get("password", ""))
    role = body.get("role")
    if not re.fullmatch(r"[A-Za-z0-9_.@-]{3,120}", login_name) or not 1 <= len(name) <= 120:
        raise ValueError("Login ou nome inválido")
    validate_password(password)
    if role not in ROLES:
        raise ValueError("Perfil inválido")
    instances = _instances(body.get("instances"))
    with db(AUTH_DB) as auth:
        auth.execute("INSERT INTO users(login,name,hash,role,instances) VALUES(?,?,?,?,?)",
                     (login_name, name, HASHER.hash(password), role, instances))
    audit(actor["login"], "user_created", login_name)


def update_user(actor, ident, body):
    with db(AUTH_DB) as auth:
        target = auth.execute("SELECT * FROM users WHERE id=?", (ident,)).fetchone()
        if not target:
            raise ValueError("Usuário não encontrado")
        action = body.get("action")
        if action == "active":
            if ident == actor["id"]:
                raise ValueError("Não desative sua própria conta")
            auth.execute("UPDATE users SET active=? WHERE id=?",
                         (int(bool(body.get("active"))), ident))
        elif action == "reset":
            password = str(body.get("password", ""))
            validate_password(password)
            auth.execute("UPDATE users SET hash=?,must_change=1 WHERE id=?",
                         (HASHER.hash(password), ident))
        elif action == "permissions":
            role = body.get("role")
            instances = _instances(body.get("instances"))
            if role not in ROLES:
                raise ValueError("Perfil inválido")
            if ident == actor["id"] and (role != "Administrador" or "*" not in body["instances"]):
                raise ValueError("Preserve seu acesso de administrador")
            auth.execute("UPDATE users SET role=?,instances=? WHERE id=?",
                         (role, instances, ident))
        elif action == "delete":
            if ident == actor["id"]:
                raise ValueError("Não remova sua própria conta")
            auth.execute("DELETE FROM sessions WHERE user_id=?", (ident,))
            auth.execute("DELETE FROM users WHERE id=?", (ident,))
        elif action != "revoke":
            raise ValueError("Ação inválida")
    _revoke(ident)
    audit(actor["login"], "user_"+action, target["login"])


def settings():
    data = json.loads(CONTEXT_FILE.read_text(encoding="utf-8"))
    return {"operational_window": data.get("default") or {},
            "queue_overrides": len(data.get("queues") or {}),
            "ipbx_schedule": "Sob demanda · arquivo Trace de Chamadas XLS/XLSX",
            "ipbx_limit_per_queue_status": 100}


def save_settings(actor, body):
    window = body.get("operational_window")
    if not isinstance(window, dict) or set(window) != {"enabled", "start_time", "end_time"}:
        raise ValueError("Configuração inválida")
    if type(window["enabled"]) is not bool or any(
            not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", str(window[key]))
            for key in ("start_time", "end_time")):
        raise ValueError("Horário inválido")
    if window["enabled"] and window["start_time"] == window["end_time"]:
        raise ValueError("O início e o fim não podem ser iguais")
    with _LOCK:
        data = json.loads(CONTEXT_FILE.read_text(encoding="utf-8"))
        data["default"] = window
        temp = CONTEXT_FILE.with_name(CONTEXT_FILE.name+".tmp."+str(os.getpid()))
        try:
            with temp.open("w", encoding="utf-8") as stream:
                json.dump(data, stream, ensure_ascii=False, indent=2)
                stream.flush(); os.fsync(stream.fileno())
            os.replace(temp, CONTEXT_FILE)
        finally:
            temp.unlink(missing_ok=True)
    audit(actor["login"], "settings_updated", "operational_window")
