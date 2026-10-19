"""Daily private backup of TOI state; SQLite files use the online backup API."""
from __future__ import annotations

import fcntl
import hashlib
import os
from pathlib import Path
import shutil
import sqlite3
import tarfile
import tempfile
import time
from datetime import datetime

ROOT = Path(os.environ.get("TOI_ROOT", "/var/www/toi")).resolve()
DEST = Path(os.environ.get("TOI_BACKUP_DIR", "/var/backups/toi"))
REPLICA = os.environ.get("TOI_BACKUP_REPLICA_DIR", "").strip()
KEEP_DAILY = int(os.environ.get("TOI_BACKUP_KEEP_DAILY", "30"))
KEEP_MONTHLY = int(os.environ.get("TOI_BACKUP_KEEP_MONTHLY", "12"))
SOURCES = ("dados", "dados_toi", "relatorios", "private")
SQLITE_SUFFIXES = (".sqlite", ".db")
SKIP_SUFFIXES = (".sqlite-wal", ".sqlite-shm", ".sqlite-journal", ".db-wal", ".db-shm", ".db-journal")


def source_files():
    for name in SOURCES:
        base = ROOT / name
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*")):
            if "ml_snapshots" in path.relative_to(ROOT).parts:
                continue
            if path.is_symlink() or not path.is_file() or path.name.endswith(SKIP_SUFFIXES):
                continue
            yield path


def copy_file(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.suffix in SQLITE_SUFFIXES:
        source = sqlite3.connect(f"file:{src.as_posix()}?mode=ro", uri=True, timeout=30)
        target = sqlite3.connect(dst)
        try:
            # A multi-step backup restarts when this busy source changes between
            # steps. Copy in one SQLite read transaction so writers in WAL mode
            # cannot keep pushing the finish line away indefinitely.
            source.backup(target, pages=-1)
            if target.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                raise RuntimeError(f"SQLite backup verification failed: {src}")
        finally:
            target.close()
            source.close()
    else:
        shutil.copyfile(src, dst)
    os.chmod(dst, 0o600)


def prune_backups(directory: Path):
    archives = sorted(directory.glob("toi-????????_??????.tar.gz"), reverse=True)
    monthly = set()
    keep = set(archives[:KEEP_DAILY])
    for archive in archives:
        month = archive.name[4:10]
        if month not in monthly and len(monthly) < KEEP_MONTHLY:
            keep.add(archive)
            monthly.add(month)
    for archive in archives:
        if archive not in keep:
            archive.unlink()
            archive.with_suffix("").with_suffix(".sha256").unlink(missing_ok=True)


def replicate(archive: Path, checksum: Path):
    if not REPLICA:
        return
    target_dir = Path(REPLICA).resolve()
    if target_dir == DEST.resolve():
        raise RuntimeError("Destino da réplica deve ser diferente do backup local")
    target_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    for source in (archive, checksum):
        partial = target_dir / (source.name + ".partial")
        shutil.copyfile(source, partial)
        os.chmod(partial, 0o600)
        os.replace(partial, target_dir / source.name)
    with (target_dir / archive.name).open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != checksum.read_text(encoding="ascii").split()[0]:
            raise RuntimeError("Falha na verificação da réplica")
    prune_backups(target_dir)


def main():
    DEST.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (DEST / ".lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        files = list(source_files())
        total = sum(path.stat().st_size for path in files)
        if shutil.disk_usage(DEST).free < total * 2 + 1_000_000_000:
            raise RuntimeError("Espaço insuficiente para backup consistente")
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive = DEST / f"toi-{stamp}.tar.gz"
        partial = DEST / f"toi-{stamp}.partial"
        with tempfile.TemporaryDirectory(prefix="snapshot-", dir=DEST) as work:
            work = Path(work)
            for src in files:
                started = time.monotonic()
                print(f"Backup copying {src.relative_to(ROOT)} ({src.stat().st_size} bytes)", flush=True)
                copy_file(src, work / src.relative_to(ROOT))
                print(f"Backup copied {src.relative_to(ROOT)} in {time.monotonic() - started:.1f}s", flush=True)
            started = time.monotonic()
            print("Backup compressing snapshot", flush=True)
            with tarfile.open(partial, "w:gz", compresslevel=1) as tar:
                for path in sorted(work.rglob("*")):
                    if path.is_file():
                        tar.add(path, arcname=path.relative_to(work).as_posix(), recursive=False)
            print(f"Backup compressed snapshot in {time.monotonic() - started:.1f}s", flush=True)
        os.chmod(partial, 0o600)
        os.replace(partial, archive)
        with archive.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        checksum = DEST / f"toi-{stamp}.sha256"
        checksum.write_text(f"{digest}  {archive.name}\n", encoding="ascii")
        os.chmod(checksum, 0o600)
        replicate(archive, checksum)
        prune_backups(DEST)
        print(f"TOI backup OK: {archive.name}; files={len(files)}; sha256={digest[:16]}")


if __name__ == "__main__":
    main()
