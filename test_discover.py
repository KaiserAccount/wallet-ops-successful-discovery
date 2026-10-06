"""Unit tests for run mode, progress logs, and pooler connect shape. No network, no secrets."""
import logging
import os
import sys
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

import httpx
import psycopg2

import discover


def _clear_run_flags():
    for name in (
        "DISCOVER_TEST",
        "DISCOVER_SMOKE",
        "DISCOVER_RESCAN",
        "DISCOVER_TOP_FRACTION",
        "DISCOVER_WINDOW_HOURS",
        "DISCOVER_PROMOTE_CAP",
        "DISCOVER_BACKFILL_POSITIONS",
        "DISCOVER_BACKFILL_LIMIT",
        "MATRIX_RUN",
    ):
        os.environ.pop(name, None)


def _ago(hours):
    """Timezone-aware timestamp `hours` before now."""
    return datetime.now(timezone.utc) - timedelta(hours=hours)


class _Cursor:
    def __init__(self, fetched):
        self._fetched = list(fetched)
        self.rowcount = -1
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append(sql)
        compact = " ".join(sql.split())
        if compact.startswith("INSERT INTO public.tracked_wallets"):
            self.rowcount = 4
        elif compact.startswith("UPDATE public.tracked_wallets"):
            self.rowcount = 7
        else:
            self.rowcount = 1

    def fetchone(self):
        if not self._fetched:
            return None
        return self._fetched.pop(0)

    def fetchall(self):
        rows = list(self._fetched)
        self._fetched.clear()
        return rows


class _Conn:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class _Resp:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        return None

    def json(self):
        return self._body


class _Client:
    def __init__(self, bodies):
        self._bodies = list(bodies)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return _Resp(self._bodies.pop(0))


class RunModeTest(unittest.TestCase):
    def test_unset_and_false_are_not_truthy(self):
        self.assertFalse(discover.flag_truthy(None))
        for value in ("", "0", "false", "no", "off"):
            self.assertFalse(discover.flag_truthy(value), value)
            self.assertFalse(discover.smoke_enabled(value), value)
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DISCOVER_SMOKE", None)
            self.assertFalse(discover.smoke_enabled(None))
            self.assertFalse(discover.smoke_enabled())

    def test_truthy_values(self):
        for value in ("1", "true", "TRUE", " yes ", "Yes"):
            self.assertTrue(discover.smoke_enabled(value), value)
            self.assertTrue(discover.flag_truthy(value), value)

    def test_cron_when_both_flags_absent(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            _clear_run_flags()
            self.assertFalse(discover.test_enabled())
            self.assertEqual(discover.run_mode(), "cron")

    def test_discover_test_labels_test_mode(self):
        with mock.patch.dict(os.environ, {"DISCOVER_TEST": "true"}, clear=False):
            os.environ.pop("DISCOVER_SMOKE", None)
            self.assertTrue(discover.test_enabled())
            self.assertEqual(discover.run_mode(), "test")

    def test_smoke_is_deprecated_test_alias(self):
        with mock.patch.dict(os.environ, {"DISCOVER_SMOKE": "yes"}, clear=False):
            os.environ.pop("DISCOVER_TEST", None)
            self.assertTrue(discover.test_enabled())
            self.assertEqual(discover.run_mode(), "test")

    def test_false_flags_stay_cron(self):
        with mock.patch.dict(os.environ, {"DISCOVER_TEST": "0", "DISCOVER_SMOKE": "false"}):
            self.assertFalse(discover.test_enabled())
            self.assertEqual(discover.run_mode(), "cron")

    def test_main_cron_connects_without_smoke(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            _clear_run_flags()
            with mock.patch("discover.connect", side_effect=RuntimeError("stop-before-db")) as connect:
                with self.assertLogs("discover", level="INFO") as captured:
                    with self.assertRaises(RuntimeError):
                        discover.main([])
                connect.assert_called_once()
        blob = "\n".join(captured.output)
        self.assertIn("mode=cron", blob)
        self.assertIn("paper=true", blob)
        self.assertIn("connect failed reason=RuntimeError", blob)
        self.assertNotIn("skip run", blob)
        self.assertNotIn("ONE-SHOT", blob)

    def test_main_logs_one_shot_for_discover_test(self):
        with mock.patch.dict(os.environ, {"DISCOVER_TEST": "1"}, clear=False):
            os.environ.pop("DISCOVER_SMOKE", None)
            os.environ.pop("MATRIX_RUN", None)
            with mock.patch("discover.connect", side_effect=RuntimeError("stop-before-db")) as connect:
                with self.assertLogs("discover", level="INFO") as captured:
                    with self.assertRaises(RuntimeError):
                        discover.main([])
                connect.assert_called_once()
        blob = "\n".join(captured.output)
        self.assertIn("mode=test", blob)
        self.assertIn("ONE-SHOT TEST", blob)
        self.assertIn("via=DISCOVER_TEST", blob)
        self.assertNotIn("deprecated", blob)

    def test_main_logs_deprecated_smoke_alias(self):
        with mock.patch.dict(os.environ, {"DISCOVER_SMOKE": "1"}, clear=False):
            os.environ.pop("DISCOVER_TEST", None)
            os.environ.pop("MATRIX_RUN", None)
            with mock.patch("discover.connect", side_effect=RuntimeError("stop-before-db")):
                with self.assertLogs("discover", level="INFO") as captured:
                    with self.assertRaises(RuntimeError):
                        discover.main([])
        blob = "\n".join(captured.output)
        self.assertIn("deprecated alias of DISCOVER_TEST", blob)
        self.assertIn("ONE-SHOT TEST", blob)

    def test_configure_logging_uses_stdout(self):
        root = logging.getLogger()
        old_handlers = root.handlers[:]
        old_level = root.level
        try:
            discover.configure_logging()
            self.assertEqual(root.level, logging.INFO)
            streams = [
                handler.stream
                for handler in root.handlers
                if isinstance(handler, logging.StreamHandler)
            ]
            self.assertEqual(streams, [sys.stdout])
            self.assertNotIn(sys.stderr, streams)
        finally:
            root.handlers = old_handlers
            root.setLevel(old_level)


class ProgressLogTest(unittest.TestCase):
    def test_page_log_counts_and_hides_api_key(self):
        client = _Client([
            {
                "traders": [
                    {"wallet": "W1", "pnl": {"token": {"realized": "80"}}},
                    {"wallet": "W2", "pnl": {"token": {"realized": "60"}}},
                    {"wallet": "", "pnl": {"token": {"realized": "90"}}},
                ],
                "pagination": {"hasMore": True, "nextCursor": "c2"},
            },
            {
                "traders": [
                    {"wallet": "W3", "pnl": {"token": {"realized": "40"}}},
                ],
                "pagination": {"hasMore": True, "nextCursor": "c3"},
            },
        ])
        with self.assertLogs("discover", level="INFO") as captured:
            traders, pages = discover.fetch(client, "super-secret-key", "Mint111")
        self.assertEqual(pages, 2)
        self.assertEqual([t["wallet"] for t in traders], ["W1", "W2", "W3"])
        self.assertEqual(client.calls[0]["headers"], {"x-api-key": "super-secret-key"})
        self.assertEqual(client.calls[1]["params"]["cursor"], "c2")
        blob = "\n".join(captured.output)
        self.assertIn("tracker page=1 kept=2 cumulative=2 hasMore=True", blob)
        self.assertIn("tracker page=2 kept=1 cumulative=3 hasMore=True", blob)
        self.assertIn("tracker stop page=2 reason=below_realized_floor", blob)
        self.assertNotIn("super-secret-key", blob)

    def test_tracker_error_logs_type_only(self):
        class _BadClient:
            def get(self, url, params=None, headers=None, timeout=None):
                raise RuntimeError("request had x-api-key super-secret-key")

        with self.assertLogs("discover", level="INFO") as captured:
            with self.assertRaises(RuntimeError):
                discover.fetch(_BadClient(), "super-secret-key", "Mint111")
        blob = "\n".join(captured.output)
        self.assertIn("tracker page=1 failed reason=RuntimeError", blob)
        self.assertNotIn("super-secret-key", blob)

    def test_max_pages_stop(self):
        client = _Client([
            {
                "traders": [{"wallet": "W1", "pnl": {"token": {"realized": "80"}}}],
                "pagination": {"hasMore": True, "nextCursor": "c2"},
            },
        ])
        with mock.patch.object(discover, "MAX_PAGES", 1):
            with self.assertLogs("discover", level="INFO") as captured:
                _traders, pages = discover.fetch(client, "k", "Mint111")
        self.assertEqual(pages, 1)
        self.assertIn("reason=max_pages", "\n".join(captured.output))

    def test_pagination_stop_reasons(self):
        self.assertEqual(
            discover.pagination_stop_reason({"hasMore": False}, "c", Decimal("80")),
            "hasMore_false",
        )
        self.assertEqual(
            discover.pagination_stop_reason({"hasMore": True}, None, Decimal("80")),
            "no_cursor",
        )
        self.assertEqual(
            discover.pagination_stop_reason({"hasMore": True}, "c", None),
            "no_floor",
        )
        self.assertIsNone(
            discover.pagination_stop_reason({"hasMore": True}, "c", Decimal("80")),
        )

    def test_run_harvest_logs_mint_upsert_and_summary(self):
        cursor = _Cursor([(9, "MintABC", Decimal("3.5"), _ago(1), None)])
        conn = _Conn(cursor)
        traders = [{
            "wallet": "Wal1",
            "invested": "10",
            "pnl": {
                "token": {"realized": "25"},
                "wallet": {"totalTrades": 3, "tokensTraded": 2, "realized": "100"},
            },
            "counts": {"buys": 1, "sells": 1},
            "identity": {"type": "wallet", "tags": ["x"]},
        }]
        with mock.patch.dict(os.environ, {"SOLANA_TRACKER_API_KEY": "super-secret-key"}):
            with mock.patch("discover.fetch", return_value=(traders, 2)):
                with mock.patch("discover.execute_values") as execute_values:
                    with self.assertLogs("discover", level="INFO") as captured:
                        self.assertEqual(discover.run_harvest(conn, None), 0)
        execute_values.assert_called_once()
        self.assertEqual(conn.commits, 3)
        blob = "\n".join(captured.output)
        self.assertIn("mint selected mint=MintABC outcome_id=9 roi=3.5", blob)
        self.assertIn("upsert progress rows=1 green_usd=25", blob)
        self.assertIn(
            "harvest summary rows=4 source=tracker_traders min_mints=1",
            blob,
        )
        self.assertIn("promote summary cap=100 rows_updated=7 source=tracker_traders", blob)
        self.assertIn("mint=MintABC early=0 copy_ok=0", blob)
        self.assertIn(
            "discovery summary scanned_mints_kept=0 positions_total=0 "
            "positions_copy_ok=0 harvest_rows=4 promote_activated=0",
            blob,
        )
        self.assertIn("mint=MintABC pages=2 upserted=1 green_usd=25 status=ok", blob)
        self.assertNotIn("super-secret-key", blob)

    def test_already_scanned_skip_names_the_reason(self):
        cursor = _Cursor([("ok",)])
        with self.assertLogs("discover", level="INFO") as captured:
            self.assertEqual(discover.run_harvest(_Conn(cursor), "MintABC"), 0)
        self.assertEqual(len(cursor.calls), 2)
        self.assertIn(
            "status=skip reason=already_scanned",
            "\n".join(captured.output),
        )

    def test_error_scan_is_not_treated_as_already_scanned(self):
        cursor = _Cursor([
            ("error",),
            (9, "MintABC", Decimal("3"), None),
        ])
        traders = [{
            "wallet": "Wal1",
            "pnl": {"token": {"realized": "25"}},
        }]
        with mock.patch.dict(os.environ, {"SOLANA_TRACKER_API_KEY": "k"}, clear=False):
            os.environ.pop("DISCOVER_RESCAN", None)
            with mock.patch("discover.fetch", return_value=(traders, 1)):
                with mock.patch("discover.execute_values"):
                    with self.assertLogs("discover", level="INFO") as captured:
                        self.assertEqual(discover.run_harvest(_Conn(cursor), "MintABC"), 0)
        blob = "\n".join(captured.output)
        self.assertIn("mint selected mint=MintABC", blob)
        self.assertNotIn("already_scanned", blob)

    def test_rescan_includes_ok_mint(self):
        cursor = _Cursor([
            ("ok",),
            (9, "MintABC", Decimal("3"), None),
        ])
        traders = [{
            "wallet": "Wal1",
            "pnl": {"token": {"realized": "25"}},
        }]
        with mock.patch.dict(os.environ, {
            "SOLANA_TRACKER_API_KEY": "k",
            "DISCOVER_RESCAN": "1",
        }):
            with mock.patch("discover.fetch", return_value=(traders, 1)) as fetch:
                with mock.patch("discover.execute_values"):
                    with self.assertLogs("discover", level="INFO"):
                        self.assertEqual(discover.run_harvest(_Conn(cursor), "MintABC"), 0)
        fetch.assert_called_once()

    def test_no_unscanned_mint_names_the_reason(self):
        with self.assertLogs("discover", level="INFO") as captured:
            self.assertEqual(discover.run_harvest(_Conn(_Cursor([])), None), 0)
        self.assertIn(
            "no unscanned mint; exit 0 reason=no_unscanned_mint",
            "\n".join(captured.output),
        )

    def test_missing_api_key_names_the_reason(self):
        cursor = _Cursor([(9, "MintABC", Decimal("3"), _ago(1), None)])
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SOLANA_TRACKER_API_KEY", None)
            with self.assertLogs("discover", level="INFO") as captured:
                self.assertEqual(discover.run_harvest(_Conn(cursor), None), 1)
        blob = "\n".join(captured.output)
        self.assertIn("mint selected mint=MintABC outcome_id=9 roi=3", blob)
        self.assertIn("reason=SOLANA_TRACKER_API_KEY_unset", blob)


class _BoomTrader(dict):
    def get(self, key, default=None):
        if key == "wallet":
            raise RuntimeError("bad payload")
        return super().get(key, default)


class QuintileAndIsolationTest(unittest.TestCase):
    def test_quintile_size_is_ceil_of_the_fraction(self):
        self.assertEqual(discover.quintile_size(0, 0.20), 0)
        self.assertEqual(discover.quintile_size(1, 0.20), 1)
        self.assertEqual(discover.quintile_size(4, 0.20), 1)
        self.assertEqual(discover.quintile_size(5, 0.20), 1)
        self.assertEqual(discover.quintile_size(6, 0.20), 2)
        self.assertEqual(discover.quintile_size(10, 0.20), 2)
        self.assertEqual(discover.quintile_size(11, 0.20), 3)
        self.assertEqual(discover.quintile_size(10, 1), 10)

    def test_top_fraction_accepts_percent_and_rejects_garbage(self):
        self.assertEqual(discover.top_fraction("0.20"), 0.20)
        self.assertEqual(discover.top_fraction("20%"), 0.20)
        self.assertEqual(discover.top_fraction("20"), 0.20)
        self.assertEqual(discover.top_fraction("1"), 1.0)
        with self.assertLogs("discover", level="INFO") as captured:
            self.assertEqual(discover.top_fraction("nope"), 0.20)
            self.assertEqual(discover.top_fraction("0"), 0.20)
        self.assertIn("DISCOVER_TOP_FRACTION", "\n".join(captured.output))

    def test_choose_batch_is_the_leading_quintile_skipping_completed_scans(self):
        ranked = [
            (1, "A", Decimal("10"), None, "ok"),
            (2, "B", Decimal("9"), None, "empty"),
            (3, "C", Decimal("8"), None, "error"),
            (4, "D", Decimal("7"), None, None),
            (5, "E", Decimal("6"), None, None),
        ]
        # 20% of 5 is the single best mint, and that mint is already ok.
        self.assertEqual(discover.choose_batch(ranked, 0.20, False), [])
        self.assertEqual(
            [row[1] for row in discover.choose_batch(ranked, 0.20, True)],
            ["A"],
        )
        # 60% covers A, B, and C. Completed ok/empty drop out. error stays.
        self.assertEqual(
            [row[1] for row in discover.choose_batch(ranked, 0.60, False)],
            ["C"],
        )
        self.assertEqual(
            [row[1] for row in discover.choose_batch(ranked, 1, False)],
            ["C", "D", "E"],
        )

    def test_ranked_sql_uses_roi_and_not_limit_one(self):
        self.assertIn("roi_multiple DESC", discover.RANKED_MINTS_SQL)
        self.assertIn("DISTINCT ON (o.token_address)", discover.RANKED_MINTS_SQL)
        self.assertIn("left(lower(o.chain), 3) = 'sol'", discover.RANKED_MINTS_SQL)
        self.assertIn(
            "o.message_timestamp >= now() - (%s * interval '1 hour')",
            discover.RANKED_MINTS_SQL,
        )
        self.assertIn(
            "o.message_timestamp DESC NULLS LAST, o.id DESC",
            discover.RANKED_MINTS_SQL,
        )
        self.assertNotIn("detected_at", discover.RANKED_MINTS_SQL)
        self.assertNotIn("LIMIT", discover.RANKED_MINTS_SQL)
        harvest = discover.harvest_sql(2, 100).replace(" ", "")
        self.assertIn("'tracker_traders',true,", harvest)
        self.assertNotIn("'tracker_traders',false,", harvest)
        self.assertIn("LIMIT 100", discover.promote_sql(2, 100))
        self.assertIn("WHERE t.source='tracker_traders'", discover.promote_sql(2, 100))
        self.assertNotIn("gmgn", discover.promote_sql(2, 100).lower())

    def test_bad_wallet_is_skipped_and_the_good_wallet_remains(self):
        traders = [
            {"wallet": "GOOD", "pnl": {"token": {"realized": "10"}}},
            _BoomTrader(pnl={"token": {"realized": "12"}}),
        ]
        with self.assertLogs("discover", level="INFO") as captured:
            rows, failures = discover.rows_from_traders(traders, "MintABC")
        self.assertEqual(failures, 1)
        self.assertEqual([row[0] for row in rows], ["GOOD"])
        blob = "\n".join(captured.output)
        self.assertIn("mint=MintABC wallet=- status=error reason=RuntimeError phase=payload", blob)
        self.assertNotIn("bad payload", blob)

    def test_fetch_keeps_the_page_when_one_trader_payload_raises(self):
        client = _Client([{
            "traders": [
                {"wallet": "GOOD", "pnl": {"token": {"realized": "80"}}},
                _BoomTrader(pnl={"token": {"realized": "90"}}),
            ],
            "pagination": {"hasMore": False},
        }])
        with self.assertLogs("discover", level="INFO") as captured:
            traders, pages = discover.fetch(client, "super-secret-key", "MintABC")
        self.assertEqual(pages, 1)
        self.assertEqual([t["wallet"] for t in traders], ["GOOD"])
        blob = "\n".join(captured.output)
        self.assertIn("mint=MintABC wallet=- status=error reason=RuntimeError phase=payload", blob)
        self.assertNotIn("super-secret-key", blob)

    def test_one_bad_mint_does_not_abort_the_batch(self):
        # Ten ranked mints → quintile of 2. The leader's tracker call fails.
        ranked = [
            (1, "BAD", Decimal("50"), _ago(1), None),
            (2, "GOOD", Decimal("40"), _ago(1), None),
        ]
        for index in range(3, 11):
            ranked.append((index, f"M{index}", Decimal(str(30 - index)), _ago(1), None))
        cursor = _Cursor(ranked)
        conn = _Conn(cursor)

        def fetch(_client, _key, mint, sleep=None):
            if mint == "BAD":
                raise RuntimeError("tracker down")
            return ([{
                "wallet": "WalGood",
                "pnl": {"token": {"realized": "25"}},
            }], 1)

        with mock.patch.dict(os.environ, {"SOLANA_TRACKER_API_KEY": "super-secret-key"}):
            _clear_run_flags()
            with mock.patch("discover.fetch", side_effect=fetch):
                with mock.patch("discover.execute_values") as execute_values:
                    with self.assertLogs("discover", level="INFO") as captured:
                        self.assertEqual(discover.run_harvest(conn, None), 0)
        execute_values.assert_called_once()
        self.assertEqual(execute_values.call_args.args[2][0][1], "GOOD")
        blob = "\n".join(captured.output)
        self.assertIn(
            "batch selected ranked=10 quintile=2 chosen=2 fraction=0.2 "
            "window_hours=10 rescan=false",
            blob,
        )
        self.assertIn("mint=BAD status=error reason=RuntimeError phase=harvest", blob)
        self.assertIn("mint=GOOD pages=1 upserted=1 green_usd=25 status=ok", blob)
        self.assertIn("batch summary selected=2 ok=1 empty=0 error=1", blob)
        self.assertNotIn("mint selected mint=M3", blob)
        self.assertNotIn("super-secret-key", blob)
        self.assertGreaterEqual(conn.rollbacks, 1)

    def test_postgres_error_on_one_mint_does_not_block_the_next(self):
        ranked = [
            (1, "BAD", Decimal("50"), _ago(1), None),
            (2, "GOOD", Decimal("40"), _ago(1), None),
        ]
        for index in range(3, 11):
            ranked.append((index, f"M{index}", Decimal("2"), _ago(1), None))
        cursor = _Cursor(ranked)

        def execute_values(_cur, _sql, rows, page_size=200):
            if rows and rows[0][1] == "BAD":
                raise psycopg2.DataError("bad numeric")

        traders = [{"wallet": "Wal1", "pnl": {"token": {"realized": "25"}}}]
        with mock.patch.dict(os.environ, {"SOLANA_TRACKER_API_KEY": "k"}):
            _clear_run_flags()
            with mock.patch("discover.fetch", return_value=(traders, 1)):
                with mock.patch("discover.execute_values", side_effect=execute_values):
                    with self.assertLogs("discover", level="INFO") as captured:
                        self.assertEqual(
                            discover.run_harvest(_Conn(cursor), None, sleep=lambda _s: None),
                            0,
                        )
        blob = "\n".join(captured.output)
        self.assertIn("mint=BAD wallet=Wal1 status=error reason=DataError phase=upsert", blob)
        self.assertIn("mint=GOOD pages=1 upserted=1 green_usd=25 status=ok", blob)
        self.assertIn("batch summary selected=2 ok=1 empty=0 error=1", blob)

    def test_fatal_ledger_stops_the_batch(self):
        ranked = [
            (1, "BAD", Decimal("50"), _ago(1), None),
            (2, "GOOD", Decimal("40"), _ago(1), None),
        ]
        for index in range(3, 11):
            ranked.append((index, f"M{index}", Decimal("2"), _ago(1), None))

        class _FatalCursor(_Cursor):
            def execute(self, sql, params=None):
                super().execute(sql, params)
                if sql.startswith("INSERT INTO wallet_intel.token_trader_scans"):
                    raise psycopg2.errors.UndefinedTable(
                        'relation "wallet_intel.token_trader_scans" does not exist'
                    )

        fetched = []

        def fetch(_client, _key, mint, sleep=None):
            fetched.append(mint)
            raise RuntimeError("tracker down")

        cursor = _FatalCursor(ranked)
        with mock.patch.dict(os.environ, {"SOLANA_TRACKER_API_KEY": "k"}):
            _clear_run_flags()
            with mock.patch("discover.fetch", side_effect=fetch):
                with self.assertLogs("discover", level="INFO") as captured:
                    self.assertEqual(discover.run_harvest(_Conn(cursor), None), 1)
        self.assertEqual(fetched, ["BAD"])
        blob = "\n".join(captured.output)
        self.assertIn("mint=BAD status=error reason=UndefinedTable phase=fatal", blob)
        self.assertNotIn("mint=GOOD pages=", blob)
        self.assertNotIn("status=ok", blob)

    def test_timeout_is_retried_then_skipped(self):
        calls = {"n": 0}

        class _TimeoutClient:
            def get(self, url, params=None, headers=None, timeout=None):
                calls["n"] += 1
                raise httpx.TimeoutException("timed out")

        sleeps = []
        with self.assertLogs("discover", level="INFO") as captured:
            with self.assertRaises(httpx.TimeoutException):
                discover.fetch(
                    _TimeoutClient(), "super-secret-key", "MintABC",
                    sleep=sleeps.append,
                )
        self.assertEqual(calls["n"], 2)
        self.assertEqual(sleeps, [discover.RETRY_BASE_SEC])
        blob = "\n".join(captured.output)
        self.assertIn("retry tracker mint=MintABC page=1 attempt=1 reason=TimeoutException", blob)
        self.assertIn("tracker page=1 failed reason=TimeoutException", blob)
        self.assertNotIn("super-secret-key", blob)

    def test_window_hours_default_and_override(self):
        self.assertEqual(discover.window_hours(None), 10)
        self.assertEqual(discover.window_hours(""), 10)
        self.assertEqual(discover.window_hours("10"), 10)
        self.assertEqual(discover.window_hours("10h"), 10)
        self.assertEqual(discover.window_hours("2.5"), 2.5)
        with self.assertLogs("discover", level="INFO") as captured:
            self.assertEqual(discover.window_hours("0"), 10)
            self.assertEqual(discover.window_hours("nope"), 10)
        self.assertIn("DISCOVER_WINDOW_HOURS", "\n".join(captured.output))

    def test_duplicate_outcomes_count_once_and_old_outcomes_drop(self):
        now = datetime(2026, 10, 4, 22, 0, tzinfo=timezone.utc)
        rows = [
            (1, "DUP", Decimal("4"), now - timedelta(hours=2), None),
            (2, "DUP", Decimal("9"), now - timedelta(hours=5), None),
            (3, "DUP", Decimal("9"), now - timedelta(hours=1), None),
            (4, "OLD", Decimal("100"), now - timedelta(hours=10, seconds=1), None),
            (5, "EDGE", Decimal("3"), now - timedelta(hours=10), None),
            (6, "NEW", Decimal("5"), now - timedelta(hours=1), "error"),
            (7, "STALE", Decimal("8"), None, None),
        ]
        collapsed = discover.collapse_outcomes(rows, now, 10)
        self.assertEqual([row[1] for row in collapsed], ["DUP", "NEW", "EDGE"])
        # Same roi: the later message_timestamp wins, so DUP is outcome 3, not 2.
        self.assertEqual(collapsed[0][0], 3)
        self.assertEqual(collapsed[0][2], Decimal("9"))
        self.assertNotIn("OLD", [row[1] for row in collapsed])
        self.assertNotIn("STALE", [row[1] for row in collapsed])
        self.assertEqual(len(collapsed), 3)

    def test_run_harvest_does_not_double_count_or_include_old_mints(self):
        # Four outcomes for HOT plus four other recent mints would be 8 rows.
        # Collapse makes 5 distinct tokens, so the default quintile is 1.
        # OLD's higher ROI is outside the window and must not take that slot.
        ranked = [
            (1, "HOT", Decimal("7"), _ago(2), None),
            (2, "HOT", Decimal("10"), _ago(3), None),
            (3, "HOT", Decimal("8"), _ago(1), None),
            (4, "M6", Decimal("6"), _ago(1), None),
            (5, "M5", Decimal("5"), _ago(1), None),
            (6, "M4", Decimal("4"), _ago(1), None),
            (7, "M3", Decimal("3"), _ago(1), None),
            (99, "OLD", Decimal("1000"), _ago(11), None),
        ]
        fetched = []

        def fetch(_client, _key, mint, sleep=None):
            fetched.append(mint)
            return ([{
                "wallet": "WalHot",
                "pnl": {"token": {"realized": "25"}},
            }], 1)

        with mock.patch.dict(os.environ, {"SOLANA_TRACKER_API_KEY": "k"}):
            _clear_run_flags()
            with mock.patch("discover.fetch", side_effect=fetch):
                with mock.patch("discover.execute_values") as execute_values:
                    with self.assertLogs("discover", level="INFO") as captured:
                        self.assertEqual(discover.run_harvest(_Conn(_Cursor(ranked)), None), 0)
        self.assertEqual(fetched, ["HOT"])
        self.assertEqual(execute_values.call_args.args[2][0][1], "HOT")
        blob = "\n".join(captured.output)
        self.assertIn(
            "batch selected ranked=5 quintile=1 chosen=1 fraction=0.2 "
            "window_hours=10 rescan=false",
            blob,
        )
        self.assertIn("mint selected mint=HOT outcome_id=2 roi=10", blob)
        self.assertNotIn("mint selected mint=OLD", blob)
        self.assertNotIn("mint selected mint=M6", blob)
        self.assertNotIn("is_active=true", blob)

    def test_top_quintile_already_scanned_exits_zero(self):
        ranked = [(index, f"M{index}", Decimal("10"), _ago(1), "ok") for index in range(1, 6)]
        with self.assertLogs("discover", level="INFO") as captured:
            self.assertEqual(discover.run_harvest(_Conn(_Cursor(ranked)), None), 0)
        self.assertIn(
            "reason=top_quintile_already_scanned",
            "\n".join(captured.output),
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


class ConnectModeTest(unittest.TestCase):
    def test_transaction_mode_allows_6543(self):
        host, user = discover.validate_endpoint(
            "aws-0-us-west-2.pooler.supabase.com",
            6543,
            "postgres.trzfysszmrgogpeitzfk",
            ["1.1.1.1"],
            mode="transaction-pooler",
        )
        self.assertEqual(host, "aws-0-us-west-2.pooler.supabase.com")
        self.assertEqual(user, "postgres.trzfysszmrgogpeitzfk")

    def test_direct_ipv6_dials_the_aaaa_address(self):
        seen = {}

        def connect(**kwargs):
            seen.update(kwargs)
            return mock.Mock()

        discover.connect_supabase(
            "db.trzfysszmrgogpeitzfk.supabase.co",
            5432,
            "postgres",
            "secret",
            "postgres",
            resolve=lambda _host: ["2600::1"],
            connect=connect,
            prepare=None,
            mode="direct-ipv6",
        )
        self.assertEqual(seen["hostaddr"], "2600::1")
        self.assertEqual(seen["user"], "postgres")
        self.assertNotIn("secret", repr({k: v for k, v in seen.items() if k != "password"}))

    def test_https_host_is_rejected_before_dial(self):
        env = {
            "SUPABASE_HOST": "https://trzfysszmrgogpeitzfk.supabase.co",
            "SUPABASE_USER": "postgres.trzfysszmrgogpeitzfk",
            "SUPABASE_PASSWORD": "secret",
            "CONNECT_MODE": "session-pooler",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            with mock.patch("discover.connect_supabase") as connect:
                with self.assertRaises(RuntimeError) as raised:
                    discover.connect()
                connect.assert_not_called()
        self.assertIn("HTTP API", str(raised.exception))
        self.assertNotIn("secret", str(raised.exception))

    def test_db_password_fallback_is_passed_through(self):
        env = {
            "SUPABASE_HOST": "aws-0-us-west-2.pooler.supabase.com",
            "SUPABASE_PORT": "5432",
            "SUPABASE_USER": "archiver.trzfysszmrgogpeitzfk",
            "SUPABASE_DB_PASSWORD": "from-db-password",
            "CONNECT_MODE": "session-pooler",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            os.environ.pop("SUPABASE_PASSWORD", None)
            with mock.patch("discover.connect_supabase", return_value=mock.Mock()) as connect:
                discover.connect()
        self.assertEqual(connect.call_args.args[3], "from-db-password")
        self.assertEqual(connect.call_args.kwargs["mode"], "session-pooler")

    def test_matrix_run_does_not_harvest(self):
        with mock.patch.dict(os.environ, {"MATRIX_RUN": "1", "DISCOVER_SMOKE": "1"}):
            with mock.patch("discover.run_matrix", return_value=0) as run_matrix:
                with mock.patch("discover.connect") as connect:
                    with self.assertLogs("discover", level="INFO") as captured:
                        self.assertEqual(discover.main([]), 0)
        connect.assert_not_called()
        run_matrix.assert_called_once()
        self.assertTrue(any("no harvest" in line for line in captured.output))

    def test_matrix_run_wins_over_discover_test(self):
        with mock.patch.dict(os.environ, {"MATRIX_RUN": "1", "DISCOVER_TEST": "1"}):
            with mock.patch("discover.run_matrix", return_value=0) as run_matrix:
                with mock.patch("discover.run_harvest") as run_harvest:
                    with self.assertLogs("discover", level="INFO"):
                        self.assertEqual(discover.main([]), 0)
        run_matrix.assert_called_once()
        run_harvest.assert_not_called()


class ScoreAndRetentionTest(unittest.TestCase):
    def _call(self, **delta):
        return datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc) + timedelta(**delta)

    def _trader(self, **overrides):
        called = self._call()
        first_buy = int((called - timedelta(minutes=5)).timestamp() * 1000)
        trader = {
            "wallet": "Human",
            "invested": "100",
            "proceeds": "300",
            "roi": "200",
            "pnl": {
                "token": {"realized": "200"},
                "wallet": {"totalTrades": 40, "tokensTraded": 12, "realized": "500"},
            },
            "counts": {"buys": 2, "sells": 2},
            "timing": {
                "firstBuy": first_buy,
                "lastSell": first_buy + 600_000,
                "holdTimeSecs": 600,
            },
            "identity": {"type": "kol", "tags": ["kol"]},
        }
        trader.update(overrides)
        return trader

    def test_early_window_and_copy_ok(self):
        called = self._call()
        row = discover.position_row(self._trader(), "MintA", called)
        self.assertTrue(row[discover.COL_EARLY])
        self.assertTrue(row[discover.COL_COPY_OK])
        self.assertTrue(row[18])
        self.assertEqual(row[1], "MintA")

        late = self._trader()
        late["timing"] = dict(late["timing"])
        late["timing"]["firstBuy"] = int(
            (called + timedelta(minutes=10, seconds=1)).timestamp() * 1000
        )
        late_row = discover.position_row(late, "MintA", called)
        self.assertFalse(late_row[discover.COL_EARLY])
        self.assertFalse(late_row[discover.COL_COPY_OK])

        edge = self._trader()
        edge["timing"] = dict(edge["timing"])
        edge["timing"]["firstBuy"] = int(
            (called - timedelta(minutes=60)).timestamp() * 1000
        )
        self.assertTrue(discover.position_row(edge, "MintA", called)[discover.COL_EARLY])

        too_soon = self._trader()
        too_soon["timing"] = dict(too_soon["timing"])
        too_soon["timing"]["firstBuy"] = int(
            (called - timedelta(minutes=60, seconds=1)).timestamp() * 1000
        )
        self.assertFalse(
            discover.position_row(too_soon, "MintA", called)[discover.COL_EARLY]
        )

    def test_snipe_and_identity_trap_are_not_copy_ok(self):
        called = self._call()
        sniper = self._trader()
        sniper["timing"] = dict(sniper["timing"], holdTimeSecs=11)
        sniper["roi"] = "4000"
        row = discover.position_row(sniper, "MintA", called)
        self.assertTrue(row[discover.COL_EARLY])
        self.assertFalse(row[discover.COL_COPY_OK])

        bot = self._trader(identity={"type": "bot", "tags": ["axiom", "bot"]})
        self.assertTrue(discover.identity_trap(bot))
        self.assertFalse(discover.position_row(bot, "MintA", called)[discover.COL_COPY_OK])

        frontend = self._trader(identity={"type": "axiom", "tags": ["axiom", "photon"]})
        self.assertFalse(discover.identity_trap(frontend))
        self.assertTrue(discover.position_row(frontend, "MintA", called)[discover.COL_COPY_OK])

        # A gmgn trading-app tag is not tracked_wallets source=gmgn.
        gmgn_app = self._trader(identity={"type": "gmgn", "tags": ["gmgn"]})
        self.assertTrue(discover.position_row(gmgn_app, "MintA", called)[discover.COL_COPY_OK])

        career = self._trader()
        career["pnl"] = {
            "token": {"realized": "200"},
            "wallet": {"totalTrades": 8000, "tokensTraded": 12, "realized": "500"},
        }
        self.assertFalse(discover.position_row(career, "MintA", called)[discover.COL_COPY_OK])

    def test_missing_call_time_is_not_early(self):
        row = discover.position_row(self._trader(), "MintA", None)
        self.assertFalse(row[discover.COL_EARLY])
        self.assertFalse(row[discover.COL_COPY_OK])

    def test_repeat_threshold_and_single_mint_gate(self):
        self.assertEqual(discover.repeat_mint_threshold(0), 1)
        self.assertEqual(discover.repeat_mint_threshold(1), 1)
        self.assertEqual(discover.repeat_mint_threshold(2), 2)
        thin = discover.harvest_sql(1, 100)
        thick = discover.harvest_sql(2, 100)
        self.assertIn(">= 300", thin)
        self.assertIn(">= 100", thin)
        self.assertNotIn(">= 300", thick)
        self.assertIn("COUNT(DISTINCT token_address) >= 2", thick)
        self.assertIn("source='tracker_traders'", discover.promote_sql(1, 50))
        self.assertIn("LIMIT 50", discover.promote_sql(1, 50))

    def test_upsert_mint_a_then_mint_b_leaves_both(self):
        store = {}

        def execute_values(_cur, sql, rows, page_size=None):
            self.assertNotIn("DELETE", sql.upper())
            self.assertNotIn("TRUNCATE", sql.upper())
            self.assertIn("ON CONFLICT (wallet_address,token_address)", sql)
            update = sql.split("DO UPDATE SET", 1)[1]
            self.assertNotIn("token_address=", update)
            for row in rows:
                store[(row[0], row[1])] = row

        with mock.patch("discover.execute_values", side_effect=execute_values):
            discover.upsert_positions(mock.Mock(), mock.Mock(), [
                discover.position_row(
                    {"wallet": "W", "pnl": {"token": {"realized": "10"}}}, "MintA",
                ),
                discover.position_row(
                    {"wallet": "OnlyA", "pnl": {"token": {"realized": "8"}}}, "MintA",
                ),
            ], "MintA")
            discover.upsert_positions(mock.Mock(), mock.Mock(), [
                discover.position_row(
                    {"wallet": "W", "pnl": {"token": {"realized": "12"}}}, "MintB",
                ),
            ], "MintB")
        self.assertEqual(
            set(store),
            {("W", "MintA"), ("OnlyA", "MintA"), ("W", "MintB")},
        )
        self.assertEqual(store[("W", "MintA")][3], Decimal("10"))
        self.assertEqual(store[("W", "MintB")][3], Decimal("12"))

    def test_position_sql_never_deletes_other_mints(self):
        self.assertNotIn("DELETE", discover.POSITIONS_SQL.upper())
        self.assertNotIn("TRUNCATE", discover.POSITIONS_SQL.upper())
        self.assertIn("first_buy_at=EXCLUDED.first_buy_at", discover.POSITIONS_SQL)
        self.assertNotIn("DELETE", discover.BACKFILL_SQL.upper())
        self.assertIn("first_buy_at IS NOT NULL", discover.BACKFILL_SQL)

    def test_rebuild_keeps_tracker_rows_across_truncate(self):
        text = (
            Path(__file__).resolve().parent / "migrations" / "20261006_keep_tracker_positions.sql"
        ).read_text()
        self.assertLess(
            text.index("CREATE TEMP TABLE _tracker_keep"),
            text.index("TRUNCATE wallet_intel.wallet_token_positions"),
        )
        self.assertLess(
            text.index("TRUNCATE wallet_intel.wallet_token_positions"),
            text.index("SELECT * FROM _tracker_keep"),
        )
        self.assertIn("source = 'tracker_traders'", text)
        self.assertIn("COALESCE(p.source, '') <> 'tracker_traders'", text)


if __name__ == "__main__":
    unittest.main()
