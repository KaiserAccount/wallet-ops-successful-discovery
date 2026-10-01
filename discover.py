#!/usr/bin/env python3
"""Paper-only Solana Tracker trader harvest: one mint per cron run."""
from __future__ import annotations

import argparse
import logging
import os
import re
import socket
import sys
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

import httpx
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).resolve().parent / ".env")

log = logging.getLogger("discover")

TRACKER_BASE = "https://data.solanatracker.io"
PAGE_LIMIT, MAX_PAGES = 20, 23
REALIZED_FLOOR_USD = Decimal("50")
PROMOTE_CAP = 100
SESSION_MODE_PORT = 5432
TRANSACTION_MODE_PORT = 6543
STATEMENT_TIMEOUT_MS = 120000

# Supabase project refs are 20 lowercase alphanumerics. The pooler username
# is "<role>.<project-ref>", never a bare role.
_POOLER_REF = re.compile(r"^[a-z0-9]{20}$")
_DIRECT_HOST = re.compile(r"^db\.[a-z0-9]+\.supabase\.co$")
_SMOKE_TRUTHY = {"1", "true", "yes"}


def smoke_enabled(value=None):
    """True only for DISCOVER_SMOKE=1/true/yes. Unset and false stay disarmed."""
    if value is None:
        value = os.getenv("DISCOVER_SMOKE")
    if value is None:
        return False
    return value.strip().lower() in _SMOKE_TRUTHY


def disarmed():
    """Log and return True when this process must exit without mint work."""
    if smoke_enabled():
        return False
    log.info("DISCOVER_SMOKE unset; skip run")
    return True


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


def validate_endpoint(host, port, user, addrs):
    """Reject shapes that cannot work on Railway before any login attempt.

    A bad username still reaches Supavisor and can open the auth circuit
    breaker, so this fails closed without dialing.
    """
    if port == TRANSACTION_MODE_PORT:
        raise RuntimeError(
            "SUPABASE_PORT=6543 is the transaction pooler. "
            "This job needs session mode: set SUPABASE_PORT=5432 and "
            "SUPABASE_HOST to the session pooler hostname from the Supabase "
            "Connect dialog (aws-<n>-<region>.pooler.supabase.com). "
            "The cluster index is not always 0. "
            "SUPABASE_USER must be <role>.<project-ref>."
        )
    if not addrs:
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


def connect_supabase(host, port, user, password, dbname, resolve=ipv4_addresses,
                     connect=None, prepare=_apply_session):
    """Open one session-mode Postgres connection over IPv4.

    host= is the TLS/SNI name. hostaddr= is the A record libpq dials, so
    Railway never follows an AAAA for db.<ref>.supabase.co. Keyword
    arguments are used so a URL parser cannot truncate postgres.<ref>.
    """
    connect = connect or psycopg2.connect
    host = host.strip().rstrip(".").lower()
    user = user.strip()
    if password is None or password == "":
        raise RuntimeError(
            "SUPABASE_PASSWORD is empty. Refusing to attempt a pooler login."
        )
    addrs = list(resolve(host))
    host, user = validate_endpoint(host, port, user, addrs)

    last_exc = None
    tried = []
    for addr in addrs:
        tried.append(addr)
        # Host, port, user, and address only. The password is never logged.
        log.info(
            "Connecting host=%s port=%s user=%s hostaddr=%s",
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
                    "IPv4 %s unavailable (%s); trying next A record",
                    addr, str(exc).strip().splitlines()[0][:240],
                )
                continue
            raise RuntimeError(
                connect_failure_message(host, port, user, tried, exc)
            ) from exc
        log.info("Connected host=%s port=%s hostaddr=%s", host, port, addr)
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
    host = os.getenv("SUPABASE_HOST", "").strip()
    if host:
        port = int(os.getenv("SUPABASE_PORT", str(SESSION_MODE_PORT)))
        try:
            user = os.environ["SUPABASE_USER"]
            password = os.environ["SUPABASE_PASSWORD"]
        except KeyError as exc:
            raise RuntimeError(f"{exc.args[0]} is not set") from exc
        dbname = os.getenv("SUPABASE_DBNAME", "postgres").strip() or "postgres"
        return connect_supabase(host, port, user, password, dbname)
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL") or ""
    if not url:
        raise RuntimeError(
            "Set SUPABASE_HOST, SUPABASE_PORT, SUPABASE_USER, "
            "SUPABASE_PASSWORD, and SUPABASE_DBNAME, or DATABASE_URL"
        )
    host, port, user, password, dbname = split_database_url(url)
    return connect_supabase(host, port, user, password, dbname)


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


def fetch(client, key, mint):
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
        r = client.get(
            f"{TRACKER_BASE}/v2/pnl/tokens/{quote(mint, safe='')}/traders",
            params=q,
            headers={"x-api-key": key},
            timeout=45,
        )
        r.raise_for_status()
        body = r.json()
        traders = body.get("traders") or []
        out.extend(
            t for t in traders
            if isinstance(t, dict)
            and dec(dig(t, "pnl", "token", "realized")) is not None
            and dec(dig(t, "pnl", "token", "realized")) > 0
            and t.get("wallet")
        )
        pg = body.get("pagination") or {}
        floor = min(
            (
                dec(dig(t, "pnl", "token", "realized"))
                for t in traders
                if dec(dig(t, "pnl", "token", "realized")) is not None
            ),
            default=None,
        )
        cursor = pg.get("nextCursor")
        if not pg.get("hasMore") or not cursor or floor is None or floor < REALIZED_FLOOR_USD:
            break
    return out, page


def run_harvest(conn, mint_override):
    cur = conn.cursor()
    if mint_override:
        cur.execute(
            "SELECT 1 FROM wallet_intel.token_trader_scans WHERE token_mint=%s",
            (mint_override,),
        )
        if cur.fetchone():
            log.info("mint=%s pages=0 upserted=0 tracker_calls=0 status=skip", mint_override)
            return 0
        cur.execute(
            "SELECT id,token_address,roi_multiple,detected_at "
            "FROM wallet_intel.telegram_call_outcomes "
            "WHERE token_address=%s "
            "ORDER BY roi_multiple DESC NULLS LAST, detected_at DESC NULLS LAST, id DESC "
            "LIMIT 1",
            (mint_override,),
        )
    else:
        cur.execute(
            "SELECT o.id,o.token_address,o.roi_multiple,o.detected_at "
            "FROM wallet_intel.telegram_call_outcomes o "
            "WHERE (o.chain IS NULL OR o.chain ILIKE 'sol%') "
            "AND COALESCE(o.is_success,o.roi_multiple>=2)=true "
            "AND o.roi_multiple>=2 AND o.token_address IS NOT NULL "
            "AND NOT EXISTS ("
            "SELECT 1 FROM wallet_intel.token_trader_scans s "
            "WHERE s.token_mint=o.token_address"
            ") "
            "ORDER BY o.roi_multiple DESC,o.detected_at DESC NULLS LAST LIMIT 1"
        )
    choice = cur.fetchone()
    if not choice:
        log.info("no unscanned mint; exit 0")
        return 0
    oid, mint, roi, _detected = choice
    key = os.getenv("SOLANA_TRACKER_API_KEY", "")
    if not key:
        log.error("SOLANA_TRACKER_API_KEY is not set")
        return 1
    with httpx.Client() as client:
        traders, pages = fetch(client, key, mint)
    rows = []
    seen = set()
    for t in traders:
        w = str(t.get("wallet") or "").strip()
        if not w or w in seen:
            continue
        seen.add(w)
        realized = dec(dig(t, "pnl", "token", "realized"))
        invested = dec(t.get("invested", t.get("buyUsd")))
        proceeds = dec(t.get("proceeds", t.get("sellUsd")))
        rows.append((
            w, mint, 0, realized, invested, proceeds, dec(t.get("roi")),
            dig(t, "counts", "buys"), dig(t, "counts", "sells"),
            None, None, dec(dig(t, "timing", "holdTimeSecs")),
            dig(t, "pnl", "wallet", "totalTrades"),
            dig(t, "pnl", "wallet", "tokensTraded"),
            dec(dig(t, "pnl", "wallet", "realized")),
            dig(t, "identity", "type"), dig(t, "identity", "tags") or [],
            "tracker_traders", True, False, False,
        ))
    if rows:
        execute_values(
            cur,
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
            "early=EXCLUDED.early,updated_at=now()",
            rows,
            page_size=200,
        )
    green = sum((r[3] for r in rows), Decimal(0))
    status = "ok" if rows else "empty"
    cur.execute(
        "INSERT INTO wallet_intel.token_trader_scans "
        "(token_mint,first_outcome_id,roi_at_scan,pages_fetched,wallets_upserted,"
        "green_usd,status) VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (token_mint) DO NOTHING",
        (mint, oid, roi, pages, len(rows), green, status),
    )
    conn.commit()
    cur.execute(
        "INSERT INTO public.tracked_wallets "
        "(wallet_address,name,source,is_active,wallet_tier,notes) "
        "SELECT wallet_address,'tracker','tracker_traders',false,'tier_4','harvest' "
        "FROM wallet_intel.v_repeat_winners "
        "ON CONFLICT (wallet_address) DO UPDATE SET "
        "last_imported_at=now(),updated_at=now()"
    )
    conn.commit()
    cur.execute(
        f"UPDATE public.tracked_wallets t SET is_active="
        f"(t.wallet_address IN ("
        f"SELECT wallet_address FROM wallet_intel.v_repeat_winners "
        f"ORDER BY n_won DESC,pnl_won DESC NULLS LAST LIMIT {PROMOTE_CAP}"
        f")),updated_at=now() WHERE t.source='tracker_traders'"
    )
    conn.commit()
    log.info(
        "mint=%s pages=%s upserted=%s green_usd=%s status=%s",
        mint, pages, len(rows), green, status,
    )
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--mint", default=os.getenv("DISCOVER_MINT"))
    mint_override = ap.parse_args(argv).mint
    if disarmed():
        return 0
    log.info("DISCOVER_SMOKE enabled; running paper harvest once")
    conn = connect()
    try:
        return run_harvest(conn, mint_override)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        sys.exit(main())
    except Exception:
        log.exception("unhandled")
        sys.exit(1)
