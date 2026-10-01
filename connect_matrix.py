#!/usr/bin/env python3
"""Paper-only Supabase connect matrix for Successful Wallet Discovery.

Tries pooler and direct Postgres endpoints until one login runs SELECT 1.
No swaps, no purchases, no writes. Passwords and full DSNs are never logged.

Tenant form (pooler only):
  aws-<n>-<region>.pooler.supabase.com requires ``<role>.<project-ref>``
  (``postgres.<ref>`` or ``archiver.<ref>``). A bare role is not dialed:
  Supavisor answers ENOIDENTIFIER and those failures feed ECIRCUITBREAKER.

Native form (direct host):
  ``db.<ref>.supabase.co`` uses bare ``postgres`` or ``archiver``.
  The matrix also tries the tenant form there, once per address.

HTTP API hosts (``https://<ref>.supabase.co``, including SwapTable /
TokensAlertLogger ``SUPABASE_DB_HOST``) are logged as SKIP and never dialed.
"""
from __future__ import annotations

import argparse
import os
import re
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

DEFAULT_REF = "trzfysszmrgogpeitzfk"
DEFAULT_REGION = "us-west-2"
DEFAULT_DBNAME = "postgres"
SESSION_PORT = 5432
TRANSACTION_PORT = 6543
_REF = re.compile(r"^[a-z0-9]{20}$")
_TAG = re.compile(r"\(([A-Z][A-Z0-9_]+)\)")
_MODES = {
    "session": "session-pooler",
    "session-pooler": "session-pooler",
    "transaction": "transaction-pooler",
    "transaction-pooler": "transaction-pooler",
    "tx": "transaction-pooler",
    "direct": "direct",
    "direct-ipv4": "direct",
    "direct-ipv6": "direct-ipv6",
    "ipv6": "direct-ipv6",
}


@dataclass(frozen=True)
class Case:
    id: str
    host: str
    port: int
    user: str
    family: str
    hostaddr: str
    mode: str
    dial: bool
    skip_reason: str = ""
    note: str = ""


@dataclass
class Attempt:
    id: str
    result: str
    host: str
    port: int
    user: str
    family: str
    hostaddr: str
    ms: int
    error: str
    snippet: str
    mode: str = ""

    def line(self):
        return (
            f"CASE id={self.id} result={self.result} host={self.host} "
            f"port={self.port} user={self.user} family={self.family} "
            f"hostaddr={self.hostaddr} ms={self.ms} error={self.error} "
            f"mode={self.mode} snippet={self.snippet}"
        )


def normalize_mode(mode):
    key = (mode or "session-pooler").strip().lower().replace("_", "-")
    if not key:
        key = "session-pooler"
    found = _MODES.get(key)
    if not found:
        known = "session-pooler, transaction-pooler, direct, direct-ipv6"
        raise RuntimeError(f"CONNECT_MODE={mode!r} is unknown. Use {known}.")
    return found


def matrix_enabled(value=None):
    if value is None:
        value = os.getenv("MATRIX_RUN")
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def database_password():
    """Password from SUPABASE_PASSWORD or SUPABASE_DB_PASSWORD. May be empty."""
    for key in ("SUPABASE_PASSWORD", "SUPABASE_DB_PASSWORD"):
        value = os.getenv(key)
        if value:
            return value
    return ""


def hostname_of(value):
    raw = (value or "").strip()
    if not raw:
        return ""
    if "://" in raw:
        return (urlparse(raw).hostname or "").lower().rstrip(".")
    host = raw.split("/")[0]
    if host.count(":") == 1:
        host = host.split(":", 1)[0]
    return host.lower().rstrip(".")


def is_http_api_url(value):
    return (value or "").strip().lower().startswith(("http://", "https://"))


def reject_http_api_host(raw):
    """Return a Postgres hostname, or raise when the value is an HTTP API URL."""
    text = (raw or "").strip()
    if is_http_api_url(text):
        host = hostname_of(text) or text
        raise RuntimeError(
            f"SUPABASE_HOST is an HTTP API URL ({host}), not a Postgres host. "
            "That name serves HTTPS. SwapTable and TokensAlertLogger "
            "SUPABASE_DB_HOST values are API URLs and are not a psycopg template. "
            "Set SUPABASE_HOST to the session pooler "
            "(aws-0-<region>.pooler.supabase.com), SUPABASE_PORT=5432, "
            "SUPABASE_USER=<role>.<project-ref>, and CONNECT_MODE=session-pooler."
        )
    return hostname_of(text)


def project_ref():
    explicit = os.getenv("SUPABASE_PROJECT_REF", "").strip()
    if _REF.match(explicit):
        return explicit
    for key in ("SUPABASE_URL", "SUPABASE_DB_HOST", "SUPABASE_HOST"):
        host = hostname_of(os.getenv(key, ""))
        match = re.match(r"^(?:db\.)?([a-z0-9]{20})\.supabase\.co$", host)
        if match:
            return match.group(1)
    user = os.getenv("SUPABASE_USER", "")
    _role, dot, ref = user.partition(".")
    if dot and _REF.match(ref):
        return ref
    return DEFAULT_REF


def project_region():
    region = os.getenv("SUPABASE_REGION", DEFAULT_REGION).strip() or DEFAULT_REGION
    return region


def ipv4_addresses(host):
    """A records only, DNS order preserved. [] when the name is IPv6-only."""
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
    except socket.gaierror:
        return []
    seen = []
    for info in infos:
        addr = info[4][0]
        if addr not in seen:
            seen.append(addr)
    return seen


def ipv6_addresses(host):
    """AAAA records only, DNS order preserved."""
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET6, socket.SOCK_STREAM)
    except socket.gaierror:
        return []
    seen = []
    for info in infos:
        addr = info[4][0]
        if addr not in seen:
            seen.append(addr)
    return seen


def addresses_for_mode(host, mode):
    """Addresses discover should dial for CONNECT_MODE. Session stays IPv4-only."""
    mode = normalize_mode(mode)
    if mode == "direct-ipv6":
        return ipv6_addresses(host)
    if mode == "direct":
        v4 = ipv4_addresses(host)
        return v4 or ipv6_addresses(host)
    return ipv4_addresses(host)


def _pooler_host(region, index):
    return f"aws-{index}-{region}.pooler.supabase.com"


def _kind(host):
    if "pooler.supabase.com" in host:
        return "pooler"
    if host.startswith("db.") and host.endswith(".supabase.co"):
        return "direct"
    return "other"


def _mode_for(kind, port, family):
    if kind == "pooler":
        return "transaction-pooler" if port == TRANSACTION_PORT else "session-pooler"
    if family == "ipv6":
        return "direct-ipv6"
    return "direct"


def _append_unique(items, host):
    if host and host not in items:
        items.append(host)


def build_cases(
    ref=DEFAULT_REF,
    region=DEFAULT_REGION,
    *,
    resolve_v4=ipv4_addresses,
    resolve_v6=ipv6_addresses,
    include_ipv6=True,
    extra_hosts=(),
    api_hosts=(),
):
    """Enumerate attempts. Session pooler :5432 + postgres.<ref> is first.

    Bare roles on a pooler are SKIP (tenant form required) and are not dialed.
    """
    poolers = [_pooler_host(region, 0), _pooler_host(region, 1)]
    directs = [f"db.{ref}.supabase.co"]
    apis = []
    for raw in list(extra_hosts) + list(api_hosts):
        if is_http_api_url(raw):
            _append_unique(apis, hostname_of(raw) or raw.strip())
            continue
        host = hostname_of(raw)
        if not host:
            continue
        if host == f"{ref}.supabase.co":
            _append_unique(apis, host)
            continue
        kind = _kind(host)
        if kind == "pooler":
            _append_unique(poolers, host)
        else:
            _append_unique(directs, host)

    tenant_users = [f"postgres.{ref}", f"archiver.{ref}"]
    bare_users = ["postgres", "archiver"]
    cases = []

    def add(**kwargs):
        cases.append(Case(id=f"c{len(cases) + 1:03d}", **kwargs))

    for host in poolers:
        v4 = list(resolve_v4(host) or [])
        for port in (SESSION_PORT, TRANSACTION_PORT):
            mode = _mode_for("pooler", port, "ipv4")
            for user in tenant_users:
                if not v4:
                    add(
                        host=host, port=port, user=user, family="ipv4", hostaddr="-",
                        mode=mode, dial=False, skip_reason="NO_ADDRESS",
                        note="pooler published no A record",
                    )
                    continue
                for addr in v4:
                    add(
                        host=host, port=port, user=user, family="ipv4", hostaddr=addr,
                        mode=mode, dial=True, note="pooler tenant form role.ref",
                    )
            for user in bare_users:
                add(
                    host=host, port=port, user=user, family="ipv4", hostaddr="-",
                    mode=mode, dial=False, skip_reason="NEEDS_TENANT_FORM",
                    note="pooler rejects a bare role; use role.ref",
                )

    for host in directs:
        v4 = list(resolve_v4(host) or [])
        v6 = list(resolve_v6(host) or []) if include_ipv6 else []
        users = bare_users + tenant_users
        if not v4 and not v6:
            reason = "NO_IPV6" if not include_ipv6 else "NO_ADDRESS"
            add(
                host=host, port=SESSION_PORT, user="-", family="ipv6", hostaddr="-",
                mode="direct-ipv6", dial=False, skip_reason=reason,
                note="direct host has no usable address from here",
            )
            continue
        families = [("ipv4", v4)]
        if v6:
            families.append(("ipv6", v6))
        for port in (SESSION_PORT, TRANSACTION_PORT):
            for family, addrs in families:
                for user in users:
                    for addr in addrs:
                        add(
                            host=host, port=port, user=user, family=family,
                            hostaddr=addr, mode=_mode_for("direct", port, family),
                            dial=True,
                            note="direct native role" if "." not in user else "direct tenant form",
                        )

    for host in apis:
        add(
            host=host, port=443, user="-", family="-", hostaddr="-",
            mode="-", dial=False, skip_reason="HTTP_API_HOST",
            note="HTTPS API host is not a Postgres endpoint",
        )
    return cases


def redact(text, password):
    out = str(text or "")
    if password:
        out = out.replace(password, "<redacted>")
    out = re.sub(r":([^:@/\s]+)@", ":<redacted>@", out)
    out = re.sub(r"(?i)(password=)([^\s]+)", r"\1<redacted>", out)
    return " ".join(out.split())[:240]


def classify_error(exc):
    text = str(exc)
    low = text.lower()
    match = _TAG.search(text)
    if match:
        tag = match.group(1)
    elif "password authentication failed" in low or "invalid secret" in low:
        tag = "AUTH_FAILED"
    elif "tenant" in low and "not found" in low:
        tag = "ENOTFOUND"
    elif "no tenant identifier" in low:
        tag = "ENOIDENTIFIER"
    elif "circuit" in low or "too many authentication" in low:
        tag = "ECIRCUITBREAKER"
    elif "network is unreachable" in low or "no route to host" in low or (
        "address family" in low and "not supported" in low
    ):
        tag = "NO_ROUTE"
    elif "connection refused" in low:
        tag = "ECONNREFUSED"
    elif "timeout" in low or "timed out" in low:
        tag = "ETIMEDOUT"
    elif "connection to database not available" in low or "eauthquery" in low:
        tag = "EAUTHQUERY"
    else:
        tag = "UNKNOWN"
    if tag not in {"ECIRCUITBREAKER"} and (
        "circuitbreaker" in low or "too many authentication failures" in low
    ):
        tag = "ECIRCUITBREAKER"
    return tag


def _skip(case, reason, snippet):
    return Attempt(
        id=case.id, result="SKIP", host=case.host, port=case.port, user=case.user,
        family=case.family, hostaddr=case.hostaddr, ms=0, error=reason,
        snippet=snippet, mode=case.mode,
    )


def dial_case(case, password, dbname, timeout, connect=None):
    """Open one TLS session and run SELECT 1. Caller closes nothing; we close."""
    connect = connect or psycopg2.connect
    kwargs = dict(
        host=case.host,
        port=case.port,
        user=case.user,
        password=password,
        dbname=dbname,
        sslmode="require",
        connect_timeout=timeout,
    )
    if case.hostaddr and case.hostaddr != "-":
        kwargs["hostaddr"] = case.hostaddr
    conn = connect(**kwargs)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()


def run_cases(cases, password, dbname="postgres", timeout=12, connect=None, now=None):
    """Dial until the first PASS. Stop cases that would feed the circuit breaker.

    Wrong password: one try for that user, then skip every later case for them.
    Tenant / user not found: one try for that host+user, then skip the host.
    EAUTHQUERY / timeout: walk the remaining A records for that user. When every
    address failed that way, skip other users on the same host and port.
    ECIRCUITBREAKER: dial nothing else.
    IPv6 NO_ROUTE: skip the rest of the IPv6 cases.
    """
    now = now or time.perf_counter
    results = []
    winner = None
    abort = False
    dead_users = set()
    dead_host_users = set()
    dead_host_ports = set()
    ipv6_down = False
    eauth_buckets = {}

    for case in cases:
        if not case.dial:
            results.append(_skip(case, case.skip_reason or "SKIP", case.note))
            continue
        if abort:
            results.append(_skip(case, "ABORTED_CIRCUITBREAKER", "stopped after ECIRCUITBREAKER"))
            continue
        if case.user in dead_users:
            results.append(_skip(case, "AUTH_FAILED", "password already rejected for this user"))
            continue
        if (case.host, case.user) in dead_host_users:
            results.append(_skip(case, "ENOTFOUND", "tenant or user already rejected on this host"))
            continue
        if (case.host, case.port) in dead_host_ports:
            results.append(_skip(
                case, "EAUTHQUERY_HOST",
                "this host and port could not reach the database; not trying another user",
            ))
            continue
        if case.family == "ipv6" and ipv6_down:
            results.append(_skip(case, "NO_ROUTE", "IPv6 path is unreachable from here"))
            continue

        started = now()
        try:
            dial_case(case, password, dbname, timeout, connect=connect)
        except Exception as exc:
            elapsed = int((now() - started) * 1000)
            tag = classify_error(exc)
            snippet = redact(str(exc).strip().splitlines()[0] if str(exc).strip() else tag, password)
            results.append(Attempt(
                id=case.id, result="FAIL", host=case.host, port=case.port,
                user=case.user, family=case.family, hostaddr=case.hostaddr,
                ms=elapsed, error=tag, snippet=snippet, mode=case.mode,
            ))
            if tag == "ECIRCUITBREAKER":
                abort = True
                continue
            if tag in {"AUTH_FAILED"} or "password authentication failed" in snippet.lower():
                dead_users.add(case.user)
                continue
            if tag in {"ENOTFOUND", "ENOIDENTIFIER"}:
                dead_host_users.add((case.host, case.user))
                if tag == "ENOTFOUND":
                    # This cluster does not host the project. Another role will not.
                    for user in {c.user for c in cases if c.host == case.host}:
                        dead_host_users.add((case.host, user))
                continue
            if tag == "NO_ROUTE" and case.family == "ipv6":
                ipv6_down = True
                continue
            if tag in {"EAUTHQUERY", "ETIMEDOUT", "ECONNREFUSED", "UNKNOWN"}:
                key = (case.host, case.port, case.user)
                bucket = eauth_buckets.setdefault(key, {"fail": 0, "left": 0})
                bucket["fail"] += 1
                bucket["left"] = sum(
                    1 for later in cases
                    if later.dial and later.host == case.host and later.port == case.port
                    and later.user == case.user and later.id > case.id
                )
                if bucket["left"] == 0 and bucket["fail"] > 0:
                    dead_host_ports.add((case.host, case.port))
            continue

        elapsed = int((now() - started) * 1000)
        results.append(Attempt(
            id=case.id, result="PASS", host=case.host, port=case.port,
            user=case.user, family=case.family, hostaddr=case.hostaddr,
            ms=elapsed, error="-", snippet="SELECT 1", mode=case.mode,
        ))
        winner = case
        break
    return winner, results


def winner_line(case, dbname):
    return (
        f"WINNER id={case.id} mode={case.mode} host={case.host} port={case.port} "
        f"user={case.user} family={case.family} hostaddr={case.hostaddr} dbname={dbname}"
    )


def winner_example(case, dbname):
    """Env shape with no secret. SUPABASE_PASSWORD is named, never assigned."""
    text = "\n".join((
        "# connect_winner.env.example — generated by connect_matrix.py.",
        "# No secrets. Set SUPABASE_PASSWORD in the Railway dashboard.",
        f"CONNECT_MODE={case.mode}",
        f"SUPABASE_HOST={case.host}",
        f"SUPABASE_PORT={case.port}",
        f"SUPABASE_USER={case.user}",
        f"SUPABASE_DBNAME={dbname}",
        "# SUPABASE_PASSWORD=",
        f"# family={case.family} hostaddr={case.hostaddr} case={case.id}",
        "# Clear MATRIX_RUN before turning cron back on. Cron harvests with DISCOVER_TEST unset.",
        "",
    ))
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        if "PASSWORD" in key and value:
            raise RuntimeError("refusing to write a password into the winner example")
    return text


def write_winner_example(case, dbname, path):
    path = Path(path)
    path.write_text(winner_example(case, dbname), encoding="utf-8")
    return path


def _env_list(*names):
    values = []
    for name in names:
        raw = os.getenv(name, "")
        for part in raw.split(","):
            part = part.strip()
            if part:
                values.append(part)
    return values


def cases_from_env(resolve_v4=ipv4_addresses, resolve_v6=ipv6_addresses, include_ipv6=True):
    ref = project_ref()
    region = project_region()
    extra = []
    apis = []
    for raw in _env_list("SUPABASE_HOST", "SUPABASE_URL", "SUPABASE_DB_HOST"):
        if is_http_api_url(raw) or hostname_of(raw) == f"{ref}.supabase.co":
            apis.append(raw if is_http_api_url(raw) else hostname_of(raw))
        else:
            extra.append(raw)
    return build_cases(
        ref, region,
        resolve_v4=resolve_v4, resolve_v6=resolve_v6, include_ipv6=include_ipv6,
        extra_hosts=extra, api_hosts=apis,
    )


def run_matrix(connect=None, log=print, example_path=None, enumerate_only=False, cases=None):
    """Run the matrix. Exit 0 on PASS, 2 when no password, 1 when every case fails."""
    dbname = os.getenv("SUPABASE_DBNAME", DEFAULT_DBNAME).strip() or DEFAULT_DBNAME
    timeout = int(os.getenv("MATRIX_CONNECT_TIMEOUT", "12") or "12")
    if cases is None:
        cases = cases_from_env()
    if enumerate_only or os.getenv("MATRIX_ENUMERATE", "").strip().lower() in {"1", "true", "yes"}:
        for case in cases:
            flag = "DIAL" if case.dial else f"SKIP:{case.skip_reason}"
            log(
                f"CASE id={case.id} result={flag} host={case.host} port={case.port} "
                f"user={case.user} family={case.family} hostaddr={case.hostaddr} "
                f"ms=0 error={case.skip_reason or '-'} snippet={case.note}"
            )
        log(f"ENUMERATED count={len(cases)} dial={sum(c.dial for c in cases)}")
        return 0
    password = database_password()
    if not password:
        log(
            "MATRIX_RUN refused: SUPABASE_PASSWORD and SUPABASE_DB_PASSWORD are empty. "
            "Not dialing."
        )
        return 2
    winner, results = run_cases(
        cases, password, dbname=dbname, timeout=timeout, connect=connect,
    )
    for row in results:
        log(row.line())
    if not winner:
        log("NO_WINNER every dialed case failed or was skipped")
        return 1
    log(winner_line(winner, dbname))
    path = example_path
    if path is None and os.getenv("MATRIX_WRITE_EXAMPLE", "1").strip().lower() in {"1", "true", "yes"}:
        path = Path(__file__).resolve().parent / "connect_winner.env.example"
    if path:
        write_winner_example(winner, dbname, path)
        log(f"WROTE example={path} secrets=no")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Paper-only Supabase connect matrix")
    parser.add_argument(
        "--enumerate", action="store_true",
        help="Print the matrix and exit without dialing",
    )
    args = parser.parse_args(argv)
    return run_matrix(enumerate_only=args.enumerate)


if __name__ == "__main__":
    sys.exit(main())
