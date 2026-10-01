"""Matrix enumeration and fail-fast rules. No live database and no secrets."""
import os
import unittest
from unittest import mock

import psycopg2

import connect_matrix
from connect_matrix import Case


REF = "trzfysszmrgogpeitzfk"
AWS0 = "aws-0-us-west-2.pooler.supabase.com"
AWS1 = "aws-1-us-west-2.pooler.supabase.com"
DIRECT = f"db.{REF}.supabase.co"


def _v4(host):
    if host == AWS0:
        return ["10.0.0.1", "10.0.0.2"]
    if host == AWS1:
        return ["10.1.0.1"]
    return []


def _v6(host):
    if host == DIRECT:
        return ["2600::1"]
    return []


def _cases(**kwargs):
    params = dict(resolve_v4=_v4, resolve_v6=_v6, include_ipv6=True)
    params.update(kwargs)
    return connect_matrix.build_cases(REF, "us-west-2", **params)


def _case(n, **kwargs):
    base = dict(
        id=f"c{n:03d}", host=AWS0, port=5432,
        user=f"postgres.{REF}", family="ipv4", hostaddr="10.0.0.1",
        mode="session-pooler", dial=True, skip_reason="", note="",
    )
    base.update(kwargs)
    return Case(**base)


class _Conn:
    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, *args):
        return None

    def fetchone(self):
        return (1,)

    def close(self):
        return None


class EnumerateTest(unittest.TestCase):
    def test_session_pooler_postgres_ref_is_first(self):
        cases = _cases()
        first = cases[0]
        self.assertEqual(first.host, AWS0)
        self.assertEqual(first.port, 5432)
        self.assertEqual(first.user, f"postgres.{REF}")
        self.assertEqual(first.hostaddr, "10.0.0.1")
        self.assertEqual(first.mode, "session-pooler")
        self.assertTrue(first.dial)
        self.assertEqual(cases[1].hostaddr, "10.0.0.2")
        self.assertEqual(cases[1].user, f"postgres.{REF}")

    def test_archiver_ref_follows_postgres_ref_on_the_same_port(self):
        cases = _cases()
        dialed = [c for c in cases if c.dial and c.host == AWS0 and c.port == 5432]
        self.assertEqual(
            [c.user for c in dialed],
            [f"postgres.{REF}", f"postgres.{REF}", f"archiver.{REF}", f"archiver.{REF}"],
        )

    def test_bare_pooler_users_are_not_dialed(self):
        cases = _cases()
        bare = [
            c for c in cases
            if "pooler.supabase.com" in c.host and c.user in {"postgres", "archiver"}
        ]
        self.assertTrue(bare)
        self.assertTrue(all(not c.dial for c in bare))
        self.assertTrue(all(c.skip_reason == "NEEDS_TENANT_FORM" for c in bare))

    def test_https_api_host_is_a_skip_and_not_a_dial_target(self):
        cases = _cases(api_hosts=["https://trzfysszmrgogpeitzfk.supabase.co"])
        api = [c for c in cases if c.skip_reason == "HTTP_API_HOST"]
        self.assertEqual(len(api), 1)
        self.assertEqual(api[0].host, "trzfysszmrgogpeitzfk.supabase.co")
        self.assertFalse(api[0].dial)
        self.assertFalse(any(
            c.dial and c.host == "trzfysszmrgogpeitzfk.supabase.co" for c in cases
        ))

    def test_direct_without_ipv6_is_one_skip(self):
        cases = _cases(include_ipv6=False)
        direct = [c for c in cases if c.host == DIRECT]
        self.assertEqual(len(direct), 1)
        self.assertEqual(direct[0].skip_reason, "NO_IPV6")
        self.assertFalse(direct[0].dial)

    def test_direct_ipv6_is_after_the_pooler_and_uses_bare_postgres_first(self):
        cases = _cases()
        direct = [c for c in cases if c.host == DIRECT and c.dial]
        self.assertTrue(direct)
        self.assertGreater(cases.index(direct[0]), 0)
        self.assertTrue(all("pooler" in c.host for c in cases[:cases.index(direct[0])] if c.dial or c.skip_reason))
        self.assertEqual(direct[0].user, "postgres")
        self.assertEqual(direct[0].family, "ipv6")
        self.assertEqual(direct[0].mode, "direct-ipv6")
        self.assertEqual(direct[0].port, 5432)

    def test_aws1_comes_after_aws0_session_tenant_users(self):
        cases = _cases()
        aws1 = next(c for c in cases if c.host == AWS1 and c.dial)
        aws0_archiver = [
            c for c in cases
            if c.host == AWS0 and c.port == 5432 and c.user == f"archiver.{REF}" and c.dial
        ]
        self.assertLess(cases.index(aws0_archiver[-1]), cases.index(aws1))


class FailFastTest(unittest.TestCase):
    def test_password_failure_does_not_try_the_next_address_or_log_the_secret(self):
        cases = [
            _case(1, hostaddr="10.0.0.1"),
            _case(2, hostaddr="10.0.0.2"),
            _case(3, user=f"archiver.{REF}", hostaddr="10.0.0.9"),
        ]
        calls = []

        def connect(**kwargs):
            calls.append((kwargs["user"], kwargs["hostaddr"]))
            raise psycopg2.OperationalError(
                'FATAL: password authentication failed for user "postgres" hunter2'
            )

        winner, results = connect_matrix.run_cases(cases, "hunter2", connect=connect)
        self.assertIsNone(winner)
        self.assertEqual(calls, [
            (f"postgres.{REF}", "10.0.0.1"),
            (f"archiver.{REF}", "10.0.0.9"),
        ])
        self.assertEqual(results[1].result, "SKIP")
        self.assertEqual(results[1].error, "AUTH_FAILED")
        blob = "\n".join(row.line() for row in results)
        self.assertNotIn("hunter2", blob)

    def test_tenant_not_found_stops_that_host_after_one_try(self):
        cases = [
            _case(1, host=AWS1, hostaddr="10.1.0.1"),
            _case(2, host=AWS1, hostaddr="10.1.0.2"),
            _case(3, host=AWS1, port=6543, hostaddr="10.1.0.1", mode="transaction-pooler"),
            _case(4, host=AWS1, user=f"archiver.{REF}", hostaddr="10.1.0.1"),
        ]
        calls = []

        def connect(**kwargs):
            calls.append((kwargs["port"], kwargs["user"], kwargs["hostaddr"]))
            raise psycopg2.OperationalError(
                f"(ENOTFOUND) tenant/user {kwargs['user']} not found"
            )

        _winner, results = connect_matrix.run_cases(cases, "hunter2", connect=connect)
        self.assertEqual(calls, [(5432, f"postgres.{REF}", "10.1.0.1")])
        self.assertTrue(all(row.result == "SKIP" for row in results[1:]))
        self.assertTrue(all(row.error == "ENOTFOUND" for row in results[1:]))

    def test_eauthquery_walks_addresses_then_skips_the_other_user(self):
        cases = [
            _case(1, hostaddr="10.0.0.1"),
            _case(2, hostaddr="10.0.0.2"),
            _case(3, user=f"archiver.{REF}", hostaddr="10.0.0.1"),
        ]
        calls = []

        def connect(**kwargs):
            calls.append(kwargs["hostaddr"])
            raise psycopg2.OperationalError(
                "(EAUTHQUERY) authentication query failed: "
                "connection to database not available"
            )

        _winner, results = connect_matrix.run_cases(cases, "hunter2", connect=connect)
        self.assertEqual(calls, ["10.0.0.1", "10.0.0.2"])
        self.assertEqual(results[2].result, "SKIP")
        self.assertEqual(results[2].error, "EAUTHQUERY_HOST")

    def test_circuit_breaker_dials_nothing_else(self):
        cases = [
            _case(1, hostaddr="10.0.0.1"),
            _case(2, hostaddr="10.0.0.2"),
            _case(3, host=AWS1, hostaddr="10.1.0.1"),
        ]
        calls = []

        def connect(**kwargs):
            calls.append(kwargs["hostaddr"])
            raise psycopg2.OperationalError(
                "(ECIRCUITBREAKER) too many authentication failures, "
                "new connections are temporarily blocked"
            )

        winner, results = connect_matrix.run_cases(cases, "hunter2", connect=connect)
        self.assertIsNone(winner)
        self.assertEqual(calls, ["10.0.0.1"])
        self.assertEqual(results[0].error, "ECIRCUITBREAKER")
        self.assertTrue(all(row.error == "ABORTED_CIRCUITBREAKER" for row in results[1:]))

    def test_ipv6_no_route_skips_the_rest_of_ipv6(self):
        cases = [
            _case(
                1, host=DIRECT, user="postgres", family="ipv6", hostaddr="2600::1",
                mode="direct-ipv6",
            ),
            _case(
                2, host=DIRECT, user="archiver", family="ipv6", hostaddr="2600::2",
                mode="direct-ipv6",
            ),
        ]
        calls = []

        def connect(**kwargs):
            calls.append(kwargs["hostaddr"])
            raise OSError("Network is unreachable")

        _winner, results = connect_matrix.run_cases(cases, "hunter2", connect=connect)
        self.assertEqual(calls, ["2600::1"])
        self.assertEqual(results[1].result, "SKIP")
        self.assertEqual(results[1].error, "NO_ROUTE")

    def test_first_pass_stops_and_the_example_has_no_secret(self):
        cases = [
            _case(1, hostaddr="10.0.0.1"),
            _case(2, hostaddr="10.0.0.2"),
        ]
        calls = []

        def connect(**kwargs):
            calls.append(kwargs["hostaddr"])
            self.assertEqual(kwargs["password"], "hunter2")
            return _Conn()

        winner, results = connect_matrix.run_cases(cases, "hunter2", connect=connect)
        self.assertEqual(calls, ["10.0.0.1"])
        self.assertEqual(winner.id, "c001")
        self.assertEqual(results[0].result, "PASS")
        self.assertEqual(len(results), 1)
        text = connect_matrix.winner_example(winner, "postgres")
        self.assertIn("CONNECT_MODE=session-pooler", text)
        self.assertIn(f"SUPABASE_USER=postgres.{REF}", text)
        self.assertIn("SUPABASE_PORT=5432", text)
        self.assertNotIn("hunter2", text)
        self.assertNotIn(winner_line_password(text), text)
        for line in text.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            key, _, value = line.partition("=")
            self.assertNotIn("PASSWORD", key)
            self.assertNotIn("hunter2", value)
        line = connect_matrix.winner_line(winner, "postgres")
        self.assertTrue(line.startswith("WINNER "))
        self.assertNotIn("hunter2", line)


def winner_line_password(text):
    return "SUPABASE_PASSWORD=" + "hunter2"


class RunMatrixTest(unittest.TestCase):
    def test_enumerate_does_not_dial(self):
        lines = []
        code = connect_matrix.run_matrix(
            connect=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("dialed")),
            log=lines.append,
            enumerate_only=True,
            cases=_cases(),
        )
        self.assertEqual(code, 0)
        self.assertTrue(any(line.startswith("CASE id=c001 result=DIAL") for line in lines))
        self.assertTrue(any("NEEDS_TENANT_FORM" in line for line in lines))
        self.assertTrue(any(line.startswith("ENUMERATED ") for line in lines))

    def test_missing_password_does_not_dial(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            lines = []
            code = connect_matrix.run_matrix(
                connect=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("dialed")),
                log=lines.append,
                cases=_cases(),
                example_path=None,
            )
        self.assertEqual(code, 2)
        self.assertTrue(any("Not dialing" in line for line in lines))

    def test_pass_writes_example_without_the_password(self):
        import tempfile
        from pathlib import Path

        def connect(**_kwargs):
            return _Conn()

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "connect_winner.env.example"
            with mock.patch.dict(os.environ, {"SUPABASE_PASSWORD": "hunter2"}, clear=False):
                code = connect_matrix.run_matrix(
                    connect=connect,
                    log=lambda line: None,
                    cases=[_case(1)],
                    example_path=path,
                )
            body = path.read_text(encoding="utf-8")
        self.assertEqual(code, 0)
        self.assertNotIn("hunter2", body)
        self.assertIn("CONNECT_MODE=session-pooler", body)


if __name__ == "__main__":
    unittest.main()
