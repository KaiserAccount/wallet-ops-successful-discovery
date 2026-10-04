#!/usr/bin/env python3
"""Paper-only Solana Tracker trader harvest: top ROI quintile per cron run.

One mint's tracker, payload, or database failure is logged and skipped.
The process exits 0 after a partial batch. Connect failure and a ledger
that cannot be written at all still exit 1.
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import re
import socket
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

import httpx
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from connect_matrix import (
    addresses_for_mode,
    database_password,
    matrix_enabled,
    normalize_mode,
    reject_http_api_host,
    run_matrix,
)

load_dotenv(Path(__file__).resolve().parent / ".env")

log = logging.getLogger("discover")

TRACKER_BASE = "https://data.solanatracker.io"
PAGE_LIMIT, MAX_PAGES = 20, 23
REALIZED_FLOOR_USD = Decimal("50")
PROMOTE_CAP = 100
# Leading quintile of the rolling success window. DISCOVER_TOP_FRACTION overrides this.
DEFAULT_TOP_FRACTION = 0.20
# Outcomes older than this are outside the batch. DISCOVER_WINDOW_HOURS overrides it.
DEFAULT_WINDOW_HOURS = 10
# One retry after the first attempt. Enough for a blip, not a hung mint.
RETRY_ATTEMPTS = 2
RETRY_BASE_SEC = 0.5
# Finished scans. status='error' stays eligible so a poison mint can be
# retried next run without blocking the rest of this batch.
COMPLETED_SCAN_STATUSES = ("ok", "empty")
SESSION_MODE_PORT = 5432
TRANSACTION_MODE_PORT = 6543
STATEMENT_TIMEOUT_MS = 120000

# Supabase project refs are 20 lowercase alphanumerics. The pooler username
# is "<role>.<project-ref>", never a bare role.
_POOLER_REF = re.compile(r"^[a-z0-9]{20}$")
_DIRECT_HOST = re.compile(r"^db\.[a-z0-9]+\.supabase\.co$")
_FLAG_TRUTHY = {"1", "true", "yes"}


def flag_truthy(value):
    """True for 1/true/yes. None, empty, and every other string are false."""
    if value is None:
        return False
    return str(value).strip().lower() in _FLAG_TRUTHY


def smoke_enabled(value=None):
    """True when DISCOVER_SMOKE is 1/true/yes.

    Deprecated alias of DISCOVER_TEST. It only labels the run as a one-shot
    test. It does not arm or skip the harvest. Cron runs with it unset.
    """
    if value is None:
        value = os.getenv("DISCOVER_SMOKE")
    return flag_truthy(value)


def test_enabled():
    """True when this start should be labeled a one-shot test.

    DISCOVER_TEST is the switch. DISCOVER_SMOKE is the deprecated alias.
    Neither flag is required for a scheduled paper harvest.
    """
    return flag_truthy(os.getenv("DISCOVER_TEST")) or smoke_enabled()


def run_mode():
    """'test' for a one-shot label, otherwise 'cron'."""
    return "test" if test_enabled() else "cron"


def rescan_enabled(value=None):
    """True when ledgered ok/empty mints should be harvested again.

    DISCOVER_RESCAN uses the same truthy set as DISCOVER_TEST. Unset is
    the cron default: skip mints already ledgered as ok or empty.
    """
    if value is None:
        value = os.getenv("DISCOVER_RESCAN")
    return flag_truthy(value)


def window_hours(value=None):
    """How many hours of outcomes a run ranks. Default 10.

    DISCOVER_WINDOW_HOURS accepts 10 or 10h. Zero, negative, and non-numeric
    values log a warning and use 10. The cron stays every 8 hours; the window
    is longer so a token that appears just after a run is still visible next time.
    """
    if value is None:
        value = os.getenv("DISCOVER_WINDOW_HOURS")
    if value is None or str(value).strip() == "":
        return DEFAULT_WINDOW_HOURS
    raw = str(value).strip().lower()
    if raw.endswith("h"):
        raw = raw[:-1].strip()
    try:
        number = float(raw)
    except ValueError:
        log.warning(
            "DISCOVER_WINDOW_HOURS invalid value; using %s",
            DEFAULT_WINDOW_HOURS,
        )
        return DEFAULT_WINDOW_HOURS
    if not math.isfinite(number) or number <= 0:
        log.warning(
            "DISCOVER_WINDOW_HOURS out of range; using %s",
            DEFAULT_WINDOW_HOURS,
        )
        return DEFAULT_WINDOW_HOURS
    return number


def _as_utc(value):
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def outcome_in_window(detected_at, now, hours):
    """True when detected_at is inside the last `hours` hours, inclusive."""
    stamp = _as_utc(detected_at)
    moment = _as_utc(now)
    if stamp is None or moment is None:
        return False
    return stamp >= moment - timedelta(hours=float(hours))


def _rank_key(row):
    """Best-first key: higher roi_multiple, then later detected_at, then higher id."""
    roi = row[2]
    if isinstance(roi, Decimal):
        roi_value = roi
        roi_known = True
    elif roi is None or isinstance(roi, bool):
        roi_value = Decimal(0)
        roi_known = False
    else:
        try:
            roi_value = Decimal(str(roi))
            roi_known = True
        except Exception:
            roi_value = Decimal(0)
            roi_known = False
    detected = _as_utc(row[3])
    identity = row[0] if isinstance(row[0], int) else 0
    return (
        roi_known,
        roi_value,
        detected is not None,
        detected or datetime.min.replace(tzinfo=timezone.utc),
        identity,
    )


def collapse_outcomes(rows, now, hours):
    """One row per token inside the rolling window, strongest outcome first.

    Rows are (outcome_id, token_mint, roi_multiple, detected_at, scan_status).
    Outcomes with no detected_at, and outcomes older than `hours`, are dropped
    before the collapse. Duplicate outcomes for one mint cannot inflate the
    set: the kept row is the highest roi_multiple, then the latest detected_at,
    then the highest id. The returned list is ranked the same way, before the
    quintile cut.
    """
    best = {}
    for row in rows:
        if not row or len(row) < 5 or not row[1]:
            continue
        if not outcome_in_window(row[3], now, hours):
            continue
        mint = row[1]
        current = best.get(mint)
        if current is None or _rank_key(row) > _rank_key(current):
            best[mint] = tuple(row)
    ranked = list(best.values())
    ranked.sort(key=_rank_key, reverse=True)
    return ranked


def top_fraction(value=None):
    """Fraction of the ranked in-window tokens to harvest. Default 0.20.

    Accepts 0.20, 20%, or 20 (numbers greater than 1 are percents).
    1 means the whole ranked set. Invalid values log a warning and use 0.20.
    """
    if value is None:
        value = os.getenv("DISCOVER_TOP_FRACTION")
    if value is None or str(value).strip() == "":
        return DEFAULT_TOP_FRACTION
    raw = str(value).strip()
    try:
        if raw.endswith("%"):
            number = float(raw[:-1]) / 100.0
        else:
            number = float(raw)
            if number > 1:
                number = number / 100.0
    except ValueError:
        log.warning(
            "DISCOVER_TOP_FRACTION invalid value; using %s",
            DEFAULT_TOP_FRACTION,
        )
        return DEFAULT_TOP_FRACTION
    if not math.isfinite(number) or number <= 0 or number > 1:
        log.warning(
            "DISCOVER_TOP_FRACTION out of range; using %s",
            DEFAULT_TOP_FRACTION,
        )
        return DEFAULT_TOP_FRACTION
    return number


def quintile_size(n, fraction):
    """How many leading rows a fraction covers. 0 when n is 0, else at least 1."""
    if n <= 0:
        return 0
    bps = int(round(float(fraction) * 10000))
    size = (n * bps + 9999) // 10000
    return max(1, min(n, size))


def choose_batch(ranked_rows, fraction, rescan):
    """Top fraction of a best-first ranked book, minus completed scans.

    Each row is (outcome_id, token_mint, roi_multiple, detected_at, scan_status).
    scan_status is None when the mint has no token_trader_scans row.
    ok and empty are completed. error is not, so that mint stays eligible.
    """
    size = quintile_size(len(ranked_rows), fraction)
    leading = list(ranked_rows[:size])
    if rescan:
        return leading
    chosen = []
    for row in leading:
        status = row[-1]
        if status in COMPLETED_SCAN_STATUSES:
            continue
        chosen.append(row)
    return chosen


class FatalLedger(RuntimeError):
    """The scan ledger cannot be written, or the connection is dead."""


def is_fatal_ledger(exc):
    """Schema or privilege failures mean no later mint can record a scan."""
    return isinstance(exc, (
        psycopg2.errors.InsufficientPrivilege,
        psycopg2.errors.UndefinedTable,
        psycopg2.errors.UndefinedColumn,
        psycopg2.errors.InvalidSchemaName,
    ))


def connection_unusable(conn, exc):
    """True when later mints cannot write. A dropped session is fatal."""
    if getattr(conn, "closed", 0):
        return True
    if isinstance(exc, psycopg2.InterfaceError):
        return True
    msg = str(exc).lower()
    return any(marker in msg for marker in (
        "connection already closed",
        "ssl connection has been closed",
        "server closed the connection",
        "could not connect to server",
        "connection timed out",
        "consuming input failed",
        "connection reset",
    ))


def is_transient(exc):
    """Network and database blips. Password failures and poison payloads are not."""
    if isinstance(exc, (TimeoutError, socket.timeout, ConnectionError)):
        return True
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        response = getattr(exc, "response", None)
        code = getattr(response, "status_code", 0) or 0
        return code in {408, 429, 500, 502, 503, 504}
    if isinstance(exc, psycopg2.errors.QueryCanceled):
        # statement_timeout already waited. Another try would block the batch.
        return False
    if isinstance(exc, psycopg2.OperationalError):
        msg = str(exc).lower()
        if "password authentication failed" in msg or "invalid secret" in msg:
            return False
        return True
    return False


def retry_call(fn, attempts=RETRY_ATTEMPTS, sleep=None, label="op"):
    """Run fn, retrying transient failures with linear backoff. No secrets in logs."""
    if sleep is None:
        sleep = time.sleep
    last = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last = exc
            if attempt >= attempts or not is_transient(exc):
                raise
            delay = RETRY_BASE_SEC * attempt
            log.info(
                "retry %s attempt=%s reason=%s",
                label, attempt, type(exc).__name__,
            )
            sleep(delay)
    raise last


def rollback_quietly(conn):
    try:
        conn.rollback()
    except Exception as exc:
        log.error("rollback failed reason=%s", type(exc).__name__)


def _wallet_label(trader):
    try:
        if isinstance(trader, dict):
            raw = trader.get("wallet")
            text = str(raw).strip() if raw else ""
            return text or "-"
    except Exception:
        return "-"
    return "-"


def log_run_start():
    """Loud start line: cron vs test, always paper, never a secret."""
    mode = run_mode()
    if mode == "test":
        if flag_truthy(os.getenv("DISCOVER_TEST")):
            via = "DISCOVER_TEST"
        else:
            via = "DISCOVER_SMOKE (deprecated alias of DISCOVER_TEST)"
        log.info(
            "run start mode=test paper=true via=%s; "
            "ONE-SHOT TEST paper harvest of the top ROI quintile, "
            "running once then exit",
            via,
        )
    else:
        log.info(
            "run start mode=cron paper=true; "
            "scheduled paper harvest of the top ROI quintile, once then exit "
            "(DISCOVER_TEST unset; DISCOVER_SMOKE is not required)"
        )
    return mode


def configure_logging():
    """INFO on stdout, flushed per record, so Railway shows harvest progress.

    logging.basicConfig writes to stderr, and Railway tags stderr as errors.
    StreamHandler.emit flushes after each line. The image also sets
    PYTHONUNBUFFERED=1; set that when running outside Docker.
    """
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    root.addHandler(handler)


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


def _pooler_user_ok(user):
    role, dot, ref = user.partition(".")
    return bool(dot and role and _POOLER_REF.match(ref))


def validate_endpoint(host, port, user, addrs, mode="session-pooler"):
    """Reject shapes that cannot work on Railway before any login attempt.

    A bad username still reaches Supavisor and can open the auth circuit
    breaker, so this fails closed without dialing.

    ``mode`` is the CONNECT_MODE strategy. The default session-pooler keeps
    port 6543 and a bare pooler role rejected. transaction-pooler allows
    6543. direct and direct-ipv6 allow the AAAA-only db host when an
    address was actually resolved.
    """
    mode = normalize_mode(mode)
    if mode == "session-pooler" and port == TRANSACTION_MODE_PORT:
        raise RuntimeError(
            "SUPABASE_PORT=6543 is the transaction pooler. "
            "This job needs session mode: set SUPABASE_PORT=5432 and "
            "SUPABASE_HOST to the session pooler hostname from the Supabase "
            "Connect dialog (aws-<n>-<region>.pooler.supabase.com). "
            "The cluster index is not always 0. "
            "SUPABASE_USER must be <role>.<project-ref>."
        )
    if not addrs:
        if mode in {"direct", "direct-ipv6"}:
            raise RuntimeError(
                f"No address for {host} in CONNECT_MODE={mode}. "
                "db.<project-ref>.supabase.co is often AAAA-only. "
                "direct-ipv6 needs a working IPv6 route. "
                "session-pooler uses aws-<n>-<region>.pooler.supabase.com "
                "port 5432 and SUPABASE_USER=<role>.<project-ref>."
            )
        if _DIRECT_HOST.match(host):
            raise RuntimeError(
                f"No IPv4 address for {host}. "
                "db.<project-ref>.supabase.co is AAAA-only. "
                "This Railway service has no working IPv6 path to that name. "
                "Set SUPABASE_HOST to the session pooler "
                "(aws-<n>-<region>.pooler.supabase.com) copied from the "
                "Supabase Connect dialog, SUPABASE_PORT=5432, and "
                "SUPABASE_USER=<role>.<project-ref> "
                "(for example postgres.<project-ref>)."
            )
        raise RuntimeError(
            f"No IPv4 address for {host}. "
            "SUPABASE_HOST must be the session pooler hostname "
            "(aws-<n>-<region>.pooler.supabase.com), which publishes A records. "
            "SUPABASE_PORT must be 5432 and SUPABASE_USER must be "
            "<role>.<project-ref>."
        )
    if "pooler.supabase.com" in host and not _pooler_user_ok(user):
        raise RuntimeError(
            f"SUPABASE_USER={user!r} is the wrong shape for the shared pooler "
            f"{host}. Use <role>.<project-ref> "
            "(for example postgres.<project-ref>). "
            "A bare role makes Supavisor return ENOIDENTIFIER "
            "(no tenant identifier) or a failed auth query. "
            "That failure is not the Solana Tracker API."
        )
    return host, user


def pooler_node_unavailable(exc):
    """True when this A record's Supavisor node could not auth, not bad credentials.

    aws-0-us-west-2 has several A records. One node can return
    EAUTHQUERY 'connection to database not available' while another node
    on the same hostname completes login. Password and tenant errors must
    not be retried: extra attempts trip the pooler circuit breaker.
    """
    msg = str(exc).lower()
    if "password authentication failed" in msg:
        return False
    if "tenant" in msg and "not found" in msg:
        return False
    if "no tenant identifier" in msg or "enoidentifier" in msg:
        return False
    if "circuitbreaker" in msg or "too many authentication failures" in msg:
        return False
    if "invalid secret" in msg:
        return False
    if "connection to database not available" in msg:
        return True
    if "auth_query" in msg and "timed out" in msg:
        return True
    return any(marker in msg for marker in (
        "network is unreachable",
        "no route to host",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "could not connect to server",
        "ssl connection has been closed",
        "server closed the connection",
    ))


def connect_failure_message(host, port, user, tried, exc):
    msg = str(exc).strip()
    low = msg.lower()
    hint = (
        "Check SUPABASE_HOST (session pooler hostname, not an IP and not "
        "db.<ref>.supabase.co), SUPABASE_PORT=5432, and "
        "SUPABASE_USER=<role>.<project-ref>. "
        "This error is Postgres connect, not the Solana Tracker API."
    )
    if "connection to database not available" in low or (
        "auth_query" in low and "timed out" in low
    ):
        hint = (
            "Supavisor accepted TCP but its authentication query could not "
            "reach the database (EAUTHQUERY). Every IPv4 address for this "
            "host was tried. Copy the session pooler host from the Supabase "
            "Connect dialog — the cluster index is not always aws-0. "
            "For this project, aws-1-us-west-2 returns tenant/user not found; "
            "the session host is aws-0-us-west-2.pooler.supabase.com:5432. "
            "SUPABASE_USER must be <role>.<project-ref>, not a bare role. "
            "This is not the Solana Tracker API."
        )
    elif "password authentication failed" in low:
        hint = (
            "The pooler rejected the password. SUPABASE_USER must be "
            "<role>.<project-ref>; Supavisor often prints only the role in "
            "this error. This is not the Solana Tracker API."
        )
    elif "not found" in low and "tenant" in low:
        hint = (
            "This pooler cluster does not host the project. Copy the session "
            "pooler hostname from the Supabase Connect dialog instead of "
            "guessing aws-0 versus aws-1. This is not the Solana Tracker API."
        )
    elif "no tenant identifier" in low:
        hint = (
            "SUPABASE_USER is missing the project ref. "
            "Use <role>.<project-ref>."
        )
    tried_s = ",".join(tried) if tried else "-"
    return (
        f"Postgres connect failed host={host} port={port} user={user} "
        f"hostaddrs={tried_s}. {msg} {hint}"
    )


def _apply_session(conn):
    # Startup `options=-c statement_timeout=...` is not sent. The working
    # archive client does not pass startup options through Supavisor; set
    # the timeout only after login.
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}")
    return conn


def connect_supabase(host, port, user, password, dbname, resolve=None,
                     connect=None, prepare=_apply_session, mode=None):
    """Open one Postgres connection for the CONNECT_MODE strategy.

    host= is the TLS/SNI name. hostaddr= is the address libpq dials, so a
    session-pooler connect never follows an AAAA for db.<ref>.supabase.co.
    Keyword arguments are used so a URL parser cannot truncate postgres.<ref>.
    direct-ipv6 passes AAAA addresses. The password is never logged.
    """
    connect = connect or psycopg2.connect
    mode = normalize_mode(mode if mode is not None else os.getenv("CONNECT_MODE"))
    if resolve is None:
        resolve = lambda host, _mode=mode: addresses_for_mode(host, _mode)
    host = host.strip().rstrip(".").lower()
    user = user.strip()
    if password is None or password == "":
        raise RuntimeError(
            "SUPABASE_PASSWORD is empty. Refusing to attempt a pooler login."
        )
    addrs = list(resolve(host))
    host, user = validate_endpoint(host, port, user, addrs, mode=mode)

    last_exc = None
    tried = []
    for addr in addrs:
        tried.append(addr)
        # Host, port, user, and address only. The password is never logged.
        log.info(
            "connect attempt host=%s port=%s user=%s hostaddr=%s",
            host, port, user, addr,
        )
        try:
            conn = connect(
                host=host,
                hostaddr=addr,
                port=port,
                user=user,
                password=password,
                dbname=dbname,
                sslmode="require",
                connect_timeout=30,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if addr != addrs[-1] and pooler_node_unavailable(exc):
                log.info(
                    "address %s unavailable (%s); trying next",
                    addr, str(exc).strip().splitlines()[0][:240],
                )
                continue
            raise RuntimeError(
                connect_failure_message(host, port, user, tried, exc)
            ) from exc
        log.info(
            "connected host=%s port=%s user=%s hostaddr=%s",
            host, port, user, addr,
        )
        if prepare is not None:
            try:
                prepare(conn)
            except Exception:
                conn.close()
                raise
        return conn
    raise RuntimeError(
        connect_failure_message(host, port, user, tried, last_exc)
    ) from last_exc


def split_database_url(url):
    """Parse a postgres URL without dropping the pooler role suffix.

    urllib and some DSN parsers have historically kept only the text before
    the dot in postgres.<project-ref>. The userinfo is split manually on
    the last '@' and the first ':'.
    """
    parsed = urlparse(url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise RuntimeError("DATABASE_URL must start with postgres:// or postgresql://")
    netloc = parsed.netloc
    if "@" not in netloc or not parsed.hostname:
        raise RuntimeError("Set SUPABASE_* or DATABASE_URL")
    userinfo, _hostport = netloc.rsplit("@", 1)
    if ":" in userinfo:
        raw_user, raw_password = userinfo.split(":", 1)
    else:
        raw_user, raw_password = userinfo, ""
    user = unquote(raw_user)
    if not user:
        raise RuntimeError("Set SUPABASE_* or DATABASE_URL")
    dbname = unquote((parsed.path or "/postgres").lstrip("/")) or "postgres"
    return parsed.hostname, parsed.port or SESSION_MODE_PORT, user, unquote(raw_password), dbname


def connect():
    mode = normalize_mode(os.getenv("CONNECT_MODE"))
    host = os.getenv("SUPABASE_HOST", "").strip()
    if host:
        host = reject_http_api_host(host)
        port = int(os.getenv("SUPABASE_PORT", str(SESSION_MODE_PORT)))
        try:
            user = os.environ["SUPABASE_USER"]
        except KeyError as exc:
            raise RuntimeError(f"{exc.args[0]} is not set") from exc
        password = database_password()
        if not password:
            if (
                "SUPABASE_PASSWORD" not in os.environ
                and "SUPABASE_DB_PASSWORD" not in os.environ
            ):
                raise RuntimeError("SUPABASE_PASSWORD is not set")
            raise RuntimeError(
                "SUPABASE_PASSWORD is empty. Refusing to attempt a pooler login."
            )
        dbname = os.getenv("SUPABASE_DBNAME", "postgres").strip() or "postgres"
        return connect_supabase(host, port, user, password, dbname, mode=mode)
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL") or ""
    if not url:
        raise RuntimeError(
            "Set SUPABASE_HOST, SUPABASE_PORT, SUPABASE_USER, "
            "SUPABASE_PASSWORD, and SUPABASE_DBNAME, or DATABASE_URL"
        )
    host, port, user, password, dbname = split_database_url(url)
    return connect_supabase(host, port, user, password, dbname, mode=mode)


def dec(x):
    try:
        return None if x is None or isinstance(x, bool) else Decimal(str(x))
    except Exception:
        return None


def dig(x, *p):
    for k in p:
        if not isinstance(x, dict):
            return None
        x = x.get(k)
    return x


def _keep_trader(trader):
    if not isinstance(trader, dict) or not trader.get("wallet"):
        return False
    realized = dec(dig(trader, "pnl", "token", "realized"))
    return realized is not None and realized > 0


def pagination_stop_reason(pagination, cursor, floor):
    """Why this page ends the harvest. None means request the next page."""
    if not pagination.get("hasMore"):
        return "hasMore_false"
    if not cursor:
        return "no_cursor"
    if floor is None:
        return "no_floor"
    if floor < REALIZED_FLOOR_USD:
        return "below_realized_floor"
    return None


def _page_floor(traders):
    realized = []
    for trader in traders:
        try:
            value = dec(dig(trader, "pnl", "token", "realized"))
        except Exception as exc:
            log.error(
                "mint=- wallet=%s status=error reason=%s phase=payload",
                _wallet_label(trader), type(exc).__name__,
            )
            continue
        if value is not None:
            realized.append(value)
    return min(realized, default=None)


def _keep_page(traders, mint):
    """Drop poison trader rows without dropping the rest of the page."""
    kept = []
    for trader in traders:
        try:
            if _keep_trader(trader):
                kept.append(trader)
        except Exception as exc:
            log.error(
                "mint=%s wallet=%s status=error reason=%s phase=payload",
                mint, _wallet_label(trader), type(exc).__name__,
            )
    return kept


def fetch(client, key, mint, sleep=None):
    out = []
    cursor = None
    page = 0
    for page in range(1, MAX_PAGES + 1):
        q = {
            "sort": "realized",
            "direction": "desc",
            "limit": PAGE_LIMIT,
            "excludeArbitrage": "true",
            "excludeZeroBuys": "true",
        }
        if cursor:
            q["cursor"] = cursor

        def _get(params=q):
            response = client.get(
                f"{TRACKER_BASE}/v2/pnl/tokens/{quote(mint, safe='')}/traders",
                params=params,
                headers={"x-api-key": key},
                timeout=45,
            )
            response.raise_for_status()
            return response

        try:
            # Type only on failure. httpx messages can echo the request, including the key.
            response = retry_call(
                _get, sleep=sleep, label=f"tracker mint={mint} page={page}",
            )
        except Exception as exc:
            log.error(
                "tracker page=%s failed reason=%s",
                page, type(exc).__name__,
            )
            raise
        try:
            body = response.json()
        except Exception as exc:
            log.error(
                "tracker page=%s failed reason=%s",
                page, type(exc).__name__,
            )
            raise
        if not isinstance(body, dict):
            log.error("tracker page=%s failed reason=bad_payload", page)
            raise ValueError("bad_payload")
        traders = body.get("traders") or []
        if not isinstance(traders, list):
            log.error("tracker page=%s failed reason=bad_payload", page)
            raise ValueError("bad_payload")
        kept = _keep_page(traders, mint)
        out.extend(kept)
        pg = body.get("pagination") or {}
        if not isinstance(pg, dict):
            pg = {}
        floor = _page_floor(traders)
        cursor = pg.get("nextCursor")
        has_more = bool(pg.get("hasMore"))
        log.info(
            "tracker page=%s kept=%s cumulative=%s hasMore=%s",
            page, len(kept), len(out), has_more,
        )
        stop = pagination_stop_reason(pg, cursor, floor)
        if stop:
            log.info("tracker stop page=%s reason=%s", page, stop)
            break
    else:
        log.info("tracker stop page=%s reason=max_pages", page)
    return out, page


# Rolling window, then one row per mint, then roi_multiple. The DISTINCT ON
# runs after the time filter, so an older higher-ROI outcome cannot represent
# the token. Tracker realized PnL ranks traders inside a mint after this choice.
RANKED_MINTS_SQL = (
    "SELECT r.id, r.token_address, r.roi_multiple, r.detected_at, s.status "
    "FROM ("
    "SELECT DISTINCT ON (o.token_address) "
    "o.id, o.token_address, o.roi_multiple, o.detected_at "
    "FROM wallet_intel.telegram_call_outcomes o "
    "WHERE (o.chain IS NULL OR left(lower(o.chain), 3) = 'sol') "
    "AND COALESCE(o.is_success, o.roi_multiple >= 2) = true "
    "AND o.roi_multiple >= 2 AND o.token_address IS NOT NULL "
    "AND o.detected_at >= now() - (%s * interval '1 hour') "
    "ORDER BY o.token_address, o.roi_multiple DESC NULLS LAST, "
    "o.detected_at DESC NULLS LAST, o.id DESC"
    ") r "
    "LEFT JOIN wallet_intel.token_trader_scans s ON s.token_mint = r.token_address "
    "ORDER BY r.roi_multiple DESC NULLS LAST, r.detected_at DESC NULLS LAST, r.id DESC"
)

POSITIONS_SQL = (
    "INSERT INTO wallet_intel.wallet_token_positions "
    "(wallet_address,token_address,current_balance,realized_usd,invested_usd,"
    "proceeds_usd,roi,n_buys,n_sells,first_buy_at,last_sell_at,hold_secs,"
    "career_trades,career_tokens,career_realized_usd,identity_type,identity_tags,"
    "source,won,copy_ok,early) VALUES %s "
    "ON CONFLICT (wallet_address,token_address) DO UPDATE SET "
    "realized_usd=EXCLUDED.realized_usd,invested_usd=EXCLUDED.invested_usd,"
    "proceeds_usd=EXCLUDED.proceeds_usd,roi=EXCLUDED.roi,n_buys=EXCLUDED.n_buys,"
    "n_sells=EXCLUDED.n_sells,hold_secs=EXCLUDED.hold_secs,"
    "career_trades=EXCLUDED.career_trades,career_tokens=EXCLUDED.career_tokens,"
    "career_realized_usd=EXCLUDED.career_realized_usd,"
    "identity_type=EXCLUDED.identity_type,identity_tags=EXCLUDED.identity_tags,"
    "source=EXCLUDED.source,won=EXCLUDED.won,copy_ok=EXCLUDED.copy_ok,"
    "early=EXCLUDED.early,updated_at=now()"
)

SCAN_SQL = (
    "INSERT INTO wallet_intel.token_trader_scans "
    "(token_mint,first_outcome_id,roi_at_scan,pages_fetched,wallets_upserted,"
    "green_usd,status) VALUES (%s,%s,%s,%s,%s,%s,%s) "
    "ON CONFLICT (token_mint) DO UPDATE SET "
    "first_outcome_id=COALESCE(wallet_intel.token_trader_scans.first_outcome_id,"
    "EXCLUDED.first_outcome_id),"
    "roi_at_scan=EXCLUDED.roi_at_scan,scanned_at=now(),"
    "pages_fetched=EXCLUDED.pages_fetched,wallets_upserted=EXCLUDED.wallets_upserted,"
    "green_usd=EXCLUDED.green_usd,status=EXCLUDED.status"
)

HARVEST_SQL = (
    "INSERT INTO public.tracked_wallets "
    "(wallet_address,name,source,is_active,wallet_tier,notes) "
    "SELECT wallet_address,'tracker','tracker_traders',false,'tier_4','harvest' "
    "FROM wallet_intel.v_repeat_winners "
    "ON CONFLICT (wallet_address) DO UPDATE SET "
    "last_imported_at=now(),updated_at=now()"
)

PROMOTE_SQL = (
    "UPDATE public.tracked_wallets t SET is_active="
    "(t.wallet_address IN ("
    "SELECT wallet_address FROM wallet_intel.v_repeat_winners "
    f"ORDER BY n_won DESC,pnl_won DESC NULLS LAST LIMIT {int(PROMOTE_CAP)}"
    ")),updated_at=now() WHERE t.source='tracker_traders'"
)


def _identity_tags(trader):
    tags = dig(trader, "identity", "tags") or []
    if isinstance(tags, str):
        return [tags]
    if isinstance(tags, list):
        return [str(tag) for tag in tags if tag is not None]
    return []


def position_row(trader, mint):
    wallet = str(trader.get("wallet") or "").strip()
    realized = dec(dig(trader, "pnl", "token", "realized"))
    invested = dec(trader.get("invested", trader.get("buyUsd")))
    proceeds = dec(trader.get("proceeds", trader.get("sellUsd")))
    return (
        wallet, mint, 0, realized, invested, proceeds, dec(trader.get("roi")),
        dig(trader, "counts", "buys"), dig(trader, "counts", "sells"),
        None, None, dec(dig(trader, "timing", "holdTimeSecs")),
        dig(trader, "pnl", "wallet", "totalTrades"),
        dig(trader, "pnl", "wallet", "tokensTraded"),
        dec(dig(trader, "pnl", "wallet", "realized")),
        dig(trader, "identity", "type"), _identity_tags(trader),
        "tracker_traders", True, False, False,
    )


def rows_from_traders(traders, mint):
    """Build position rows. One bad wallet is logged and skipped.

    The second value is how many traders raised. A page of only failures
    is an error scan, not an empty one.
    """
    rows = []
    seen = set()
    failures = 0
    for trader in traders:
        wallet = _wallet_label(trader)
        try:
            if not _keep_trader(trader):
                continue
            row = position_row(trader, mint)
            wallet = row[0] or wallet
            if not row[0] or row[0] in seen:
                continue
            seen.add(row[0])
            rows.append(row)
        except Exception as exc:
            failures += 1
            log.error(
                "mint=%s wallet=%s status=error reason=%s phase=payload",
                mint, wallet, type(exc).__name__,
            )
    return rows, failures


def _green(rows):
    total = Decimal(0)
    for row in rows:
        realized = row[3]
        if isinstance(realized, Decimal):
            total += realized
    return total


def _raise_if_fatal(conn, exc):
    if is_fatal_ledger(exc) or connection_unusable(conn, exc):
        raise FatalLedger(type(exc).__name__) from exc


def write_scan(cur, mint, outcome_id, roi, pages, wallets, green, status, sleep=None):
    def _write():
        cur.execute(
            SCAN_SQL,
            (mint, outcome_id, roi, pages, wallets, green, status),
        )

    retry_call(_write, sleep=sleep, label=f"ledger mint={mint}")


def upsert_positions(conn, cur, rows, mint, sleep=None):
    """Insert the page as a batch. A bad row falls back to per-wallet savepoints.

    A statement timeout does not fan out into one timeout per wallet.
    """
    if not rows:
        return []

    def _batch():
        execute_values(cur, POSITIONS_SQL, rows, page_size=200)

    try:
        retry_call(_batch, sleep=sleep, label=f"upsert mint={mint}")
        return list(rows)
    except Exception as exc:
        _raise_if_fatal(conn, exc)
        if isinstance(exc, psycopg2.errors.QueryCanceled) or is_transient(exc):
            raise
        if not isinstance(exc, (psycopg2.DataError, psycopg2.ProgrammingError, psycopg2.IntegrityError)):
            raise
        log.error(
            "mint=%s status=error reason=%s phase=upsert_batch",
            mint, type(exc).__name__,
        )
        rollback_quietly(conn)
        written = []
        for row in rows:
            wallet = row[0]
            try:
                cur.execute("SAVEPOINT discover_wallet")
                execute_values(cur, POSITIONS_SQL, [row], page_size=1)
                cur.execute("RELEASE SAVEPOINT discover_wallet")
                written.append(row)
            except Exception as row_exc:
                log.error(
                    "mint=%s wallet=%s status=error reason=%s phase=upsert",
                    mint, wallet, type(row_exc).__name__,
                )
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT discover_wallet")
                except Exception:
                    rollback_quietly(conn)
                    _raise_if_fatal(conn, row_exc)
                _raise_if_fatal(conn, row_exc)
        return written


def harvest_one(conn, cur, key, outcome_id, mint, roi, sleep=None):
    """Harvest one mint. Raises FatalLedger only when the ledger cannot be written."""
    try:
        with httpx.Client() as client:
            traders, pages = fetch(client, key, mint, sleep=sleep)
        rows, failures = rows_from_traders(traders, mint)
        written = upsert_positions(conn, cur, rows, mint, sleep=sleep)
        green = _green(written)
        if written:
            status = "ok"
        elif failures or rows:
            status = "error"
        else:
            status = "empty"
        write_scan(
            cur, mint, outcome_id, roi, pages, len(written), green, status, sleep=sleep,
        )
        conn.commit()
        if status != "error":
            log.info("upsert progress rows=%s green_usd=%s", len(written), green)
        log.info(
            "mint=%s pages=%s upserted=%s green_usd=%s status=%s",
            mint, pages, len(written), green, status,
        )
        return status
    except FatalLedger:
        rollback_quietly(conn)
        raise
    except Exception as exc:
        log.error(
            "mint=%s status=error reason=%s phase=harvest",
            mint, type(exc).__name__,
        )
        rollback_quietly(conn)
        _raise_if_fatal(conn, exc)
        try:
            write_scan(cur, mint, outcome_id, roi, 0, 0, Decimal(0), "error", sleep=sleep)
            conn.commit()
        except Exception as ledger_exc:
            log.error(
                "mint=%s status=error reason=%s phase=ledger",
                mint, type(ledger_exc).__name__,
            )
            rollback_quietly(conn)
            _raise_if_fatal(conn, ledger_exc)
        return "error"


def select_mints(cur, mint_override, fraction, rescan, sleep=None, now=None):
    """Return (chosen_rows, ranked_count). Empty chosen means exit 0.

    The scheduled batch is the top fraction of distinct tokens whose outcomes
    fall inside the rolling window (default 10 hours). Collapse happens before
    the cut. A mint override still targets that one mint.
    """
    if mint_override:
        def _status():
            cur.execute(
                "SELECT status FROM wallet_intel.token_trader_scans WHERE token_mint=%s",
                (mint_override,),
            )
            return cur.fetchone()

        found = retry_call(_status, sleep=sleep, label="scan status")
        status = found[0] if found else None
        if status in COMPLETED_SCAN_STATUSES and not rescan:
            log.info(
                "mint=%s pages=0 upserted=0 tracker_calls=0 status=skip "
                "reason=already_scanned",
                mint_override,
            )
            return [], 0

        def _outcome():
            cur.execute(
                "SELECT id,token_address,roi_multiple,detected_at "
                "FROM wallet_intel.telegram_call_outcomes "
                "WHERE token_address=%s "
                "ORDER BY roi_multiple DESC NULLS LAST, detected_at DESC NULLS LAST, id DESC "
                "LIMIT 1",
                (mint_override,),
            )
            return cur.fetchone()

        choice = retry_call(_outcome, sleep=sleep, label="mint override")
        if not choice:
            log.info("no unscanned mint; exit 0 reason=no_unscanned_mint")
            return [], 0
        return [choice + (status,)], 1

    hours = window_hours()
    moment = now or datetime.now(timezone.utc)

    def _ranked():
        cur.execute(RANKED_MINTS_SQL, (hours,))
        return cur.fetchall()

    fetched = list(retry_call(_ranked, sleep=sleep, label="ranked mints"))
    # SQL already collapses and applies the window. Collapse again so a
    # duplicate outcome cannot inflate the quintile if it reaches this list.
    ranked = collapse_outcomes(fetched, moment, hours)
    if not ranked:
        if fetched:
            log.info("no unscanned mint; exit 0 reason=outside_window")
        else:
            log.info("no unscanned mint; exit 0 reason=no_unscanned_mint")
        return [], 0
    chosen = choose_batch(ranked, fraction, rescan)
    if not chosen:
        log.info(
            "no unscanned mint; exit 0 reason=top_quintile_already_scanned"
        )
        return [], len(ranked)
    return chosen, len(ranked)


def sync_tracked_wallets(conn, cur, sleep=None):
    """Insert tracker winners inactive, then apply the existing promote cap.

    is_active stays false on insert. The promote update only turns on wallets
    that already qualify inside v_repeat_winners, capped at PROMOTE_CAP.
    A larger mint batch does not loosen that rule.
    """
    try:
        def _harvest():
            cur.execute(HARVEST_SQL)
            return cur.rowcount

        harvested = retry_call(_harvest, sleep=sleep, label="harvest tracked_wallets")
        conn.commit()
        log.info(
            "harvest summary rows=%s source=tracker_traders",
            harvested,
        )

        def _promote():
            cur.execute(PROMOTE_SQL)
            return cur.rowcount

        promoted = retry_call(_promote, sleep=sleep, label="promote tracked_wallets")
        conn.commit()
        log.info(
            "promote summary cap=%s rows_updated=%s source=tracker_traders",
            PROMOTE_CAP, promoted,
        )
        return 0
    except Exception as exc:
        log.error(
            "status=error reason=%s phase=promote",
            type(exc).__name__,
        )
        rollback_quietly(conn)
        if is_fatal_ledger(exc) or connection_unusable(conn, exc):
            return 1
        return 0


def run_harvest(conn, mint_override, sleep=None, now=None):
    cur = conn.cursor()
    fraction = top_fraction()
    rescan = rescan_enabled()
    try:
        chosen, ranked_n = select_mints(
            cur, mint_override, fraction, rescan, sleep=sleep, now=now,
        )
    except FatalLedger as exc:
        log.error("status=error reason=%s phase=fatal", exc)
        return 1
    except Exception as exc:
        log.error("status=error reason=%s phase=select", type(exc).__name__)
        rollback_quietly(conn)
        if is_fatal_ledger(exc) or connection_unusable(conn, exc):
            return 1
        return 0
    if not chosen:
        return 0
    shown_fraction = "override" if mint_override else fraction
    shown_window = "override" if mint_override else window_hours()
    log.info(
        "batch selected ranked=%s quintile=%s chosen=%s fraction=%s "
        "window_hours=%s rescan=%s",
        ranked_n,
        len(chosen) if mint_override else quintile_size(ranked_n, fraction),
        len(chosen),
        shown_fraction,
        shown_window,
        str(rescan).lower(),
    )
    key = os.getenv("SOLANA_TRACKER_API_KEY", "")
    for outcome_id, mint, roi, _detected, _status in chosen:
        log.info("mint selected mint=%s outcome_id=%s roi=%s", mint, outcome_id, roi)
    if not key:
        log.error(
            "mint=%s status=error reason=SOLANA_TRACKER_API_KEY_unset",
            chosen[0][1],
        )
        return 1
    counts = {"ok": 0, "empty": 0, "error": 0}
    for outcome_id, mint, roi, _detected, _status in chosen:
        try:
            status = harvest_one(
                conn, cur, key, outcome_id, mint, roi, sleep=sleep,
            )
        except FatalLedger as exc:
            log.error(
                "mint=%s status=error reason=%s phase=fatal",
                mint, exc,
            )
            log.info(
                "batch summary selected=%s ok=%s empty=%s error=%s",
                len(chosen), counts["ok"], counts["empty"], counts["error"] + 1,
            )
            return 1
        counts[status] = counts.get(status, 0) + 1
    log.info(
        "batch summary selected=%s ok=%s empty=%s error=%s",
        len(chosen), counts["ok"], counts["empty"], counts["error"],
    )
    return sync_tracked_wallets(conn, cur, sleep=sleep)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--mint", default=os.getenv("DISCOVER_MINT"))
    mint_override = ap.parse_args(argv).mint
    if matrix_enabled():
        log.info("MATRIX_RUN enabled; connect matrix only, no harvest")
        return run_matrix()
    log_run_start()
    try:
        conn = connect()
    except Exception as exc:
        log.error("connect failed reason=%s", type(exc).__name__)
        raise
    try:
        return run_harvest(conn, mint_override)
    finally:
        conn.close()


if __name__ == "__main__":
    configure_logging()
    try:
        sys.exit(main())
    except Exception as exc:
        log.exception("unhandled reason=%s", type(exc).__name__)
        sys.exit(1)
