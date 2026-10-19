"""Freeze read-only SBC sources for comparisons while collectors keep running."""
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import tempfile


def _copy_observed_file(source, target):
    try:
        stream = source.open('rb')
    except FileNotFoundError:
        return
    with stream:
        # Bound the copy to the length observed on open, including a possible
        # incomplete JSONL tail. Report readers already ignore incomplete rows.
        stream.seek(0, 2)
        remaining = stream.tell()
        stream.seek(0)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('wb') as output:
            while remaining:
                chunk = stream.read(min(remaining, 1024 * 1024))
                if not chunk:
                    break
                output.write(chunk)
                remaining -= len(chunk)


@contextmanager
def snapshot_sbc_sources(root, day, *, include_sessions=False):
    """One immutable input set for all realm/hour comparisons in a check.

    SQLite's online backup includes committed WAL data; copying the database
    file alone could omit cycles. No collector is stopped or modified.
    """
    root = Path(root)
    with tempfile.TemporaryDirectory(prefix='toi-sbc-check-') as directory:
        frozen = Path(directory)
        for folder in ('', 'sul'):
            relative = Path('dados/sip_capacity') / folder / f'sip_capacity_{day}.jsonl'
            _copy_observed_file(root / relative, frozen / relative)
        for pair in ('norte', 'sul'):
            relative = Path('dados/sbc_live') / f'{pair}.json'
            _copy_observed_file(root / relative, frozen / relative)
        if include_sessions:
            relative = Path('dados/sbc_sessions.sqlite')
            source = sqlite3.connect((root / relative).resolve().as_uri() + '?mode=ro',
                                     uri=True, timeout=2)
            try:
                (frozen / relative).parent.mkdir(parents=True, exist_ok=True)
                target = sqlite3.connect(frozen / relative)
                try:
                    source.backup(target)
                finally:
                    target.close()
            finally:
                source.close()
        yield frozen
