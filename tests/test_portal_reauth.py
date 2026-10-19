"""Expired Portal sessions must recover without a collector restart."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


APP = Path(__file__).resolve().parents[1] / "TOI_v1.10.6_DUAL_LANE_ML_SHADOW"
sys.path.insert(0, str(APP))
from toi_collector_core_v3 import PortalSession  # noqa: E402


class Response:
    def __init__(self, status: int):
        self.status_code = status
        self.url = "https://portal.example/metrics/get"
        self.text = '{}'

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return {"ok": True}


class PortalReauthTest(unittest.TestCase):
    def session(self):
        session = PortalSession("4", "user", "password", "https://portal.example/")
        session.cookies = {"auth": "old"}
        return session

    def test_401_renews_cookie_once_and_retries(self):
        session = self.session()
        calls = []

        def get(*args, **kwargs):
            calls.append(kwargs["cookies"]["auth"])
            return Response(401 if len(calls) == 1 else 200)

        def reauth(expected):
            self.assertEqual(expected, {"auth": "old"})
            session.cookies = {"auth": "new"}

        with patch("toi_collector_core_v3.requests.get", side_effect=get):
            with patch.object(session, "reauth", side_effect=reauth) as login:
                data, _, _ = session.get_json("metrics/get", retries=0)
        self.assertEqual(data, {"ok": True})
        self.assertEqual(calls, ["old", "new"])
        login.assert_called_once()

    def test_persistent_403_stops_after_one_reauth(self):
        session = self.session()
        with patch("toi_collector_core_v3.requests.get", return_value=Response(403)) as get:
            with patch.object(session, "reauth") as login:
                with self.assertRaisesRegex(RuntimeError, "PermissionError"):
                    session.get_json("metrics/get", retries=1)
        self.assertEqual(get.call_count, 2)
        login.assert_called_once()

    def test_other_lane_reauth_does_not_trigger_second_login(self):
        session = self.session()
        session.cookies = {"auth": "new"}
        with patch.object(session, "auth") as login:
            session.reauth({"auth": "old"})
        login.assert_not_called()


if __name__ == "__main__":
    unittest.main()
