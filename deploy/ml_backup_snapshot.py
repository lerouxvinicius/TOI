"""Materialize one historical TOI SQLite snapshot for offline ML training.

Only the LIVE database is extracted; the backup archive also contains private
state and must never be handed to a model or served over HTTP.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import os
from pathlib import Path
import shutil
import sqlite3
import tarfile

ROOT = Path(os.environ.get("TOI_ROOT", "/var/www/toi")).resolve()
BACKUPS = Path(os.environ.get("TOI_BACKUP_DIR", "/var/backups/toi")).resolve()
OUTPUT = ROOT / "dados_toi" / "ml_snapshots"
MEMBER = "dados_toi/live/toi_live.sqlite"


def archives():
    return sorted(BACKUPS.glob("toi-????????_??????.tar.gz"), reverse=True)


def materialize(archive: Path, output_dir: Path) -> Path:
    archive = archive.resolve(strict=True)
    if archive.parent != BACKUPS:
        raise ValueError("Arquivo fora do diretório de backup")
    checksum = archive.with_suffix("").with_suffix(".sha256")
    expected = checksum.read_text(encoding="ascii").split()[0]
    with archive.open("rb") as source:
        if hashlib.file_digest(source, "sha256").hexdigest() != expected:
            raise RuntimeError("Checksum do backup inválido")
    output_dir = output_dir.resolve()
    data_dir = (ROOT / "dados_toi").resolve()
    if not output_dir.is_relative_to(data_dir) or output_dir.is_relative_to(data_dir / "live"):
        raise ValueError("Saída deve ficar em dados_toi, fora do banco LIVE")
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = output_dir / f"toi-history-{archive.name[4:19]}.sqlite"
    partial = target.with_suffix(".partial")
    try:
        with tarfile.open(archive, "r:gz") as bundle:
            member = bundle.getmember(MEMBER)
            if not member.isfile():
                raise RuntimeError("Snapshot SQLite ausente")
            source = bundle.extractfile(member)
            if source is None:
                raise RuntimeError("Snapshot SQLite ilegível")
            with source, partial.open("wb") as stream:
                shutil.copyfileobj(source, stream, length=1024 * 1024)
        with closing(sqlite3.connect(f"file:{partial.as_posix()}?mode=ro", uri=True)) as db:
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("Snapshot SQLite inconsistente")
        os.chmod(partial, 0o600)
        os.replace(partial, target)
        return target
    finally:
        partial.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--archive", help="Nome do arquivo; padrão: backup mais recente")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    args = parser.parse_args()
    available = archives()
    if args.list:
        for item in available:
            print(item.name)
        return
    chosen = BACKUPS / args.archive if args.archive else (available[0] if available else None)
    if chosen is None:
        raise SystemExit("Nenhum backup disponível")
    print(materialize(chosen, args.output_dir))


if __name__ == "__main__":
    main()
