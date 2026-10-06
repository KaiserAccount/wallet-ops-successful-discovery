# wallet-ops-discover
NINETY PAPER Successful Wallet Discovery — Solana Tracker mint→trader harvest for Wallet Ops Core.

Paper only. `python connect_matrix.py` (or `MATRIX_RUN=1 python discover.py`) finds a working Supabase login. See RAILWAY.md.

## Run

`python discover.py` harvests the top 20% of distinct tokens whose outcomes are within the last 10 hours, then exits. It does not trade.

- **Cron** (Railway schedule `0 */8 * * *`, restart NEVER): leave `DISCOVER_TEST` unset. `DISCOVER_SMOKE` is not required and no longer skips the run.
- **One-shot test:** set `DISCOVER_TEST` to `1`, `true`, or `yes`, then redeploy. The log says `mode=test` and `ONE-SHOT TEST`. `DISCOVER_SMOKE` is a deprecated alias of that switch (same truthy values, test label only).
- **Batch size:** `DISCOVER_TOP_FRACTION` (default `0.20`). `1` harvests the whole ranked window. `20` and `20%` mean the same quintile.
- **Window:** `DISCOVER_WINDOW_HOURS` (default `10`). Only outcomes newer than that many hours are ranked. The Railway cron stays `0 */8 * * *`.
- **Rescan:** `DISCOVER_RESCAN=1` harvests mints already ledgered `ok` or `empty`. Unset skips those. A ledger row with `status=error` stays eligible either way.
- **Promote cap:** `DISCOVER_PROMOTE_CAP` (default `100`). Only `source='tracker_traders'` is inserted or updated, and those rows are written with `is_active=true`. `source=gmgn` is never activated.
- **Backfill:** `DISCOVER_BACKFILL_POSITIONS=1` is a one-shot. It re-calls Tracker only for the newest `DISCOVER_BACKFILL_LIMIT` (default `8`) ledgered `ok` mints that have no scored tracker position (`first_buy_at` still null). Leave it unset on the cron. The next scheduled runs accumulate new mints without a backfill.

## Top 20%

A run is the top 20% of distinct tokens whose outcomes are within the last 10 hours.

The ranked set is Solana success rows in `wallet_intel.telegram_call_outcomes` with `message_timestamp` inside that window. That column is the call time. `detected_at` is null on this book and is not the window. A row qualifies when `chain` is null or starts with `sol`, `COALESCE(is_success, roi_multiple >= 2)` is true, `roi_multiple >= 2`, and `token_address` is set. The book can hold several outcomes for one mint. Those are collapsed to one row per token before ranking and before the 20% cut. The kept row is that token's strongest outcome inside the window: highest `roi_multiple`, then latest `message_timestamp`, then highest `id`. An older call cannot represent the token and cannot add a second copy of the mint.

**Top 20%** is `ceil(N × fraction)` of that distinct list (at least 1 when N ≥ 1), default fraction `0.20`. Mints whose `wallet_intel.token_trader_scans.status` is `ok` or `empty` are dropped unless `DISCOVER_RESCAN` is set. `error` is not a completed scan.

`roi_multiple` is the ranking this job already used (`ORDER BY roi_multiple DESC`, previously `LIMIT 1`). Solana Tracker realized PnL sorts traders inside a mint after the mint is chosen. Tracker `/tokens/volume` and `/tokens/trending` also carry liquidity, but they are a different universe (about 100 market leaders), not this success book. Promote does not use `wallet_intel.v_repeat_winners`. It keeps wallets that are `copy_ok` on at least N distinct mints (N=2 once two mints have a `copy_ok` row, otherwise N=1 with a stronger single-mint gate) and sets `is_active=true` on `source='tracker_traders'` only.

## Positions accumulate

Each mint is upserted on `(wallet_address, token_address)`. This process does not `DELETE` or `TRUNCATE` other mints. Prior mints disappeared because `wallet_intel.rebuild_wallet_positions()` (called every night at 03:30 UTC by `wallet_intel.run_pnl_pipeline()`) truncated `wallet_intel.wallet_token_positions` and rebuilt it only from `wallet_intel.events`. `migrations/20261006_keep_tracker_positions.sql` snapshots `source='tracker_traders'` rows and puts them back after that truncate. The leaderboard skips those rows so they are not scored as SOL positions. Apply that migration on project `trzfysszmrgogpeitzfk` before the next 03:30 UTC pipeline or the truncate will wipe the cohort again.

## early and copy_ok

`timing.firstBuy` (else `timing.firstTrade`) is the first buy, Unix milliseconds. The anchor is `telegram_call_outcomes.message_timestamp`. This book has no token-launch time (`detected_at` is null).

- **early:** first buy is inside `[call − 60 minutes, call + 10 minutes]`. The hour before the call covers a launch the telegram call follows. Ten minutes after is still the opening push.
- **won:** token realized USD > 0.
- **copy_ok:** won, early, `holdTimeSecs` ≥ 60, and not an identity or career trap. A missing hold is not copyable. On the USEFUL scan, 30 of 160 profitable wallets held under 30 seconds; that is the snipe this gate drops.
- **Identity trap:** `bot`, `arbitrage` / `arb`, `mev`, `sniper`, `hacker`, `spam_dusting`, `exchange`, `pool`, `developer`, or a populated `identity.bot` / `hacker` / `spamDusting` / `exchange` / `pool` object. Trading frontends (`axiom`, `photon`, `fomo`, `gmgn`, `bloom`, `terminal`, `pumpfun-app`) are not traps. A gmgn identity tag is not `tracked_wallets.source=gmgn`.
- **Career trap:** `career_tokens` ≥ 2000 or `career_trades` ≥ 8000 when that field is present. Null career counts are unknown, not a bot.

Harvest requires `copy_ok` on **2** distinct mints once the table holds two mints that have any `copy_ok` row. Until then history is thin and N=1, with extra gates: realized USD ≥ 100, ROI ≥ 100 percent (2×), and hold ≥ 300 seconds. Each mint logs `early=` and `copy_ok=`. The run ends with `discovery summary scanned_mints_kept=… positions_total=… positions_copy_ok=… harvest_rows=… promote_activated=…`.

`--mint` / `DISCOVER_MINT` still harvests one mint and still skips it when the ledger is `ok` or `empty`, unless `DISCOVER_RESCAN` is set.

## Failure isolation

One bad mint or wallet does not abort the batch. Tracker errors, timeouts, bad payloads, Postgres data errors, and unexpected exceptions are logged at ERROR on stdout (same flushed handler as the progress lines) with `mint=` and, for a wallet, `wallet=`. The reason is the exception type, not the message, so an API key cannot be echoed. That item is skipped. Transient HTTP and database blips are retried once. A statement timeout is not retried. The process exits 0 after a partial batch.

Exit 1 is reserved for a failed connect, a missing `SOLANA_TRACKER_API_KEY`, or a ledger that cannot be written at all (missing relation, privilege, or a dead connection). A poison mint is recorded as `token_trader_scans.status=error` when the ledger write still works, then the next mint runs.

## Logs

INFO goes to **stdout** through `logging.StreamHandler(stream=sys.stdout)`, and each record is flushed. The Docker image sets `PYTHONUNBUFFERED=1`; set that yourself when you run the script outside Docker. Passwords, API keys, and full DSNs are not logged.

```
run start mode=cron paper=true; scheduled paper harvest of the top ROI quintile, once then exit
connect attempt host=… port=5432 user=postgres.<ref> hostaddr=…
connected host=… port=5432 user=postgres.<ref> hostaddr=…
batch selected ranked=40 quintile=8 chosen=8 fraction=0.2 window_hours=10 rescan=false
mint selected mint=… outcome_id=… roi=…
tracker page=1 kept=12 cumulative=12 hasMore=True
tracker stop page=2 reason=below_realized_floor
upsert progress rows=20 green_usd=1500
mint=… pages=2 upserted=20 green_usd=1500 status=ok
mint=… status=error reason=TimeoutException phase=harvest
batch summary selected=8 ok=7 empty=0 error=1
mint=… early=4 copy_ok=2
harvest summary rows=3 source=tracker_traders min_mints=2
promote summary cap=100 rows_updated=100 source=tracker_traders
discovery summary scanned_mints_kept=6 positions_total=900 positions_copy_ok=40 harvest_rows=3 promote_activated=3
```
