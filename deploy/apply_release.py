"""Apply a reviewed code-only ZIP in /var/www/toi; never touch TOI state."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path("/var/www/toi").resolve()
STATE = {".git", ".venv", ".browsers", ".incoming", ".releases",
         ".env", "private", "dados", "dados_toi", "logs", "relatorios"}


def safe_name(name: str) -> Path:
    pure = PurePosixPath(name)
    if (pure.is_absolute() or not pure.parts or ".." in pure.parts or
            pure.parts[0] in STATE or "\\" in name or
            "runtime" in pure.parts or "homologacao" in pure.parts or
            name.endswith((".sqlite", ".db", ".jsonl", ".xlsx", ".xls")) or
            ("static" in pure.parts and name.endswith("_latest.json"))):
        raise ValueError("Unsafe release path")
    target = ROOT.joinpath(*pure.parts)
    if not target.resolve().is_relative_to(ROOT):
        raise ValueError("Release path escapes TOI root")
    return target


def main(archive: Path) -> None:
    archive = archive.resolve()
    if not archive.is_relative_to(ROOT / ".incoming"):
        raise ValueError("Release must be uploaded to .incoming")
    with zipfile.ZipFile(archive) as bundle:
        manifest = json.loads(bundle.read("__manifest__.json"))
        files = manifest["files"]
        if not isinstance(files, dict) or not files:
            raise ValueError("Empty release")
        if set(bundle.namelist()) != set(files) | {"__manifest__.json"}:
            raise ValueError("Release manifest mismatch")
        for name, expected in files.items():
            safe_name(name)
            data = bundle.read(name)
            if hashlib.sha256(data).hexdigest() != expected:
                raise ValueError("Release file hash mismatch")
        old = subprocess.check_output(["git", "-C", str(ROOT), "ls-files", "-z"])
        old_names = {name.decode() for name in old.split(b"\0") if name}
        for name in old_names - set(files):
            path = safe_name(name)
            path.unlink(missing_ok=True)
        for name in sorted(files):
            path = safe_name(name)
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(prefix=".toi-", dir=path.parent, delete=False) as temp:
                temp.write(bundle.read(name))
                temp_path = Path(temp.name)
            try:
                os.chmod(temp_path, 0o644)
                os.replace(temp_path, path)
            finally:
                temp_path.unlink(missing_ok=True)
    print(f"Applied {len(files)} reviewed code files")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: apply_release.py /var/www/toi/.incoming/release.zip")
    main(Path(sys.argv[1]))
