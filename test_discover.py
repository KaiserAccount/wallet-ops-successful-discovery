"""Unit tests for the smoke gate and pooler connect shape. No network, no secrets."""
import os
import unittest
from unittest import mock

import psycopg2

import discover


class SmokeGateTest(unittest.TestCase):
    def test_unset_and_false_are_disarmed(self):
        for value in (None, "", "0", "false", "no", "off"):
            self.assertFalse(discover.smoke_enabled(value), value)

    def test_truthy_values(self):
        for value in ("1", "true", "TRUE", " yes ", "Yes"):
            self.assertTrue(discover.smoke_enabled(value), value)

    def test_disarmed_log_line(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DISCOVER_SMOKE", None)
            with self.assertLogs("discover", level="INFO") as captured:
                self.assertTrue(discover.disarmed())
        self.assertTrue(
            any("DISCOVER_SMOKE unset; skip run" in line for line in captured.output)
        )

    def test_main_skips_without_connecting(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DISCOVER_SMOKE", None)
            with mock.patch("discover.connect") as connect:
                with self.assertLogs("discover", level="INFO"):
                    self.assertEqual(discover.main([]), 0)
                connect.assert_not_called()

    def test_main_connects_when_smoke_set(self):
        with mock.patch.dict(os.environ, {"DISCOVER_SMOKE": "1"}):
            with mock.patch("discover.connect", side_effect=RuntimeError("stop-before-db")):
                with self.assertLogs("discover", level="INFO") as captured:
                    with self.assertRaises(RuntimeError):
                        discover.main([])
        self.assertTrue(
            any("running paper harvest once" in line for line in captured.output)
        )


class ConnectShapeTest(unittest.TestCase):
    def test_promote_cap_stays_100(self):
        self.assertEqual(discover.PROMOTE_CAP, 100)

    def test_database_url_keeps_pooler_user(self):
        host, port, user, password, dbname = discover.split_database_url(
            "postgresql://postgres.trzfysszmrgogpeitzfk:secret%40word"
            "@aws-0-us-west-2.pooler.supabase.com:5432/postgres"
        )
        self.assertEqual(host, "aws-0-us-west-2.pooler.supabase.com")
        self.assertEqual(port, 5432)
        self.assertEqual(user, "postgres.trzfysszmrgogpeitzfk")
        self.assertEqual(password, "secret@word")
        self.assertEqual(dbname, "postgres")

    def test_transaction_pooler_port_rejected(self):
        with self.assertRaises(RuntimeError) as raised:
            discover.validate_endpoint(
                "aws-0-us-west-2.pooler.supabase.com", 6543, "postgres.trzfysszmrgogpeitzfk", ["1.1.1.1"]
            )
        self.assertIn("5432", str(raised.exception))
        self.assertIn("6543", str(raised.exception))

    def test_direct_host_without_ipv4_rejected(self):
        with self.assertRaises(RuntimeError) as raised:
            discover.validate_endpoint(
                "db.trzfysszmrgogpeitzfk.supabase.co", 5432, "postgres", []
            )
        self.assertIn("AAAA-only", str(raised.exception))
        self.assertIn("pooler", str(raised.exception))

    def test_bare_pooler_user_rejected_before_dial(self):
        def connect(**_kwargs):
            raise AssertionError("must not dial with a bare pooler user")

        with self.assertRaises(RuntimeError) as raised:
            discover.connect_supabase(
                "aws-0-us-west-2.pooler.supabase.com",
                5432,
                "postgres",
                "secret",
                "postgres",
                resolve=lambda _host: ["35.160.209.8"],
                connect=connect,
                prepare=None,
            )
        self.assertIn("<role>.<project-ref>", str(raised.exception))

    def test_retries_eauthquery_then_uses_next_address(self):
        calls = []

        def connect(**kwargs):
            calls.append(kwargs["hostaddr"])
            if kwargs["hostaddr"] == "35.160.209.8":
                raise psycopg2.OperationalError(
                    'connection to server at "35.160.209.8", port 5432 failed: '
                    "FATAL: (EAUTHQUERY) authentication query failed: "
                    "connection to database not available"
                )
            return mock.Mock()

        conn = discover.connect_supabase(
            "aws-0-us-west-2.pooler.supabase.com",
            5432,
            "postgres.trzfysszmrgogpeitzfk",
            "secret",
            "postgres",
            resolve=lambda _host: ["35.160.209.8", "54.70.143.232"],
            connect=connect,
            prepare=None,
        )
        self.assertIsNotNone(conn)
        self.assertEqual(calls, ["35.160.209.8", "54.70.143.232"])

    def test_password_failure_does_not_try_the_next_address(self):
        calls = []

        def connect(**kwargs):
            calls.append(kwargs["hostaddr"])
            raise psycopg2.OperationalError(
                'FATAL: password authentication failed for user "postgres"'
            )

        with self.assertRaises(RuntimeError) as raised:
            discover.connect_supabase(
                "aws-0-us-west-2.pooler.supabase.com",
                5432,
                "postgres.trzfysszmrgogpeitzfk",
                "secret",
                "postgres",
                resolve=lambda _host: ["35.160.209.8", "54.70.143.232"],
                connect=connect,
                prepare=None,
            )
        self.assertEqual(calls, ["35.160.209.8"])
        self.assertIn("rejected the password", str(raised.exception))
        self.assertNotIn("secret", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
