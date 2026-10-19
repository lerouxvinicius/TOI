import sqlite3
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deploy"))
if sys.platform == "win32":
    sys.modules["fcntl"] = types.ModuleType("fcntl")
from backup_toi import copy_file  # noqa: E402


class BackupTests(unittest.TestCase):
    def test_sqlite_copy_completes_while_wal_writes_continue(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "source.sqlite"
            target = Path(temp) / "copy.sqlite"
            db = sqlite3.connect(source)
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE samples (id INTEGER PRIMARY KEY, payload TEXT)")
            db.executemany("INSERT INTO samples(payload) VALUES (?)",
                           [("x" * 500,) for _ in range(8000)])
            db.commit()
            db.close()
            stop = threading.Event()

            def write():
                connection = sqlite3.connect(source)
                try:
                    while not stop.is_set():
                        connection.execute("INSERT INTO samples(payload) VALUES ('live')")
                        connection.commit()
                finally:
                    connection.close()

            writer = threading.Thread(target=write)
            writer.start()
            try:
                started = time.monotonic()
                copy_file(source, target)
                self.assertLess(time.monotonic() - started, 10)
                copy = sqlite3.connect(target)
                try:
                    self.assertEqual(copy.execute("PRAGMA quick_check(1)").fetchone()[0], "ok")
                    self.assertGreaterEqual(copy.execute("SELECT COUNT(*) FROM samples").fetchone()[0], 8000)
                finally:
                    copy.close()
            finally:
                stop.set()
                writer.join(timeout=5)
                self.assertFalse(writer.is_alive())


if __name__ == "__main__":
    unittest.main()
