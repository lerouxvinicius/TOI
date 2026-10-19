"""One-time interactive first-administrator setup for a fresh TOI database."""
from __future__ import annotations

import argparse
import getpass
import json
import sqlite3
import sys

import toi_shared_admin_v1 as admin


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--login", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--password-stdin", action="store_true")
    args = parser.parse_args()
    admin.setup()
    with admin.db(admin.AUTH_DB) as connection:
        if connection.execute("SELECT 1 FROM users LIMIT 1").fetchone():
            raise SystemExit("O bootstrap só pode ser executado em uma base sem usuários.")
    password = (sys.stdin.readline().rstrip("\n") if args.password_stdin
                else getpass.getpass("Senha inicial do administrador: "))
    admin.validate_password(password)
    if not args.password_stdin:
        confirmation = getpass.getpass("Repita a senha inicial: ")
        if password != confirmation:
            raise SystemExit("As senhas não coincidem.")
    with admin.db(admin.AUTH_DB) as connection:
        connection.execute(
            "INSERT INTO users(login,name,hash,role,instances,active,must_change) "
            "VALUES(?,?,?,?,?,1,1)",
            (args.login, args.name, admin.HASHER.hash(password),
             "Administrador", json.dumps(["*"]))
        )
    admin.audit(args.login, "bootstrap_admin")
    print("Administrador inicial criado; a troca de senha será obrigatória no primeiro acesso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
