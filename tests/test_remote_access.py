import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deploy"))
from remote_access import run  # noqa: E402


class Channel:
    def __init__(self):
        self.out = [b"A" * 100000, b"done"]
        self.err = [b"warning"]
        self.closed_input = False

    def shutdown_write(self):
        self.closed_input = True

    def recv_ready(self):
        return bool(self.out)

    def recv(self, size):
        return self.out.pop(0)

    def recv_stderr_ready(self):
        return bool(self.err)

    def recv_stderr(self, size):
        return self.err.pop(0)

    def exit_status_ready(self):
        return not self.out and not self.err

    def recv_exit_status(self):
        assert not self.out and not self.err
        return 0


class Stream:
    def __init__(self, channel):
        self.channel = channel
        self.written = ""

    def write(self, value):
        self.written += value

    def flush(self):
        pass


class RemoteAccessTests(unittest.TestCase):
    def test_drains_large_stdout_before_waiting_for_exit(self):
        channel = Channel()
        stdin = Stream(channel)
        client = type("Client", (), {"exec_command": lambda self, command, timeout:
                        (stdin, Stream(channel), Stream(channel))})()
        result = run(client, "safe-command", "secret")
        self.assertTrue(result.endswith("done"))
        self.assertEqual(len(result), 100004)
        self.assertEqual(stdin.written, "secret\n")
        self.assertTrue(channel.closed_input)


if __name__ == "__main__":
    unittest.main()
