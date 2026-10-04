# wallet-ops-discover
NINETY PAPER Successful Wallet Discovery — Solana Tracker mint→trader harvest for Wallet Ops Core.

Paper only. `python connect_matrix.py` (or `MATRIX_RUN=1 python discover.py`) finds a working Supabase login. See RAILWAY.md.

## Run

`python discover.py` harvests the top ROI quintile and exits. It does not trade.

- **Cron** (Railway schedule `0 */8 * * *`, restart NEVER): leave `DISCOVER_TEST` unset. `DISCOVER_SMOKE` is not required and no longer skips the run.
- **One-shot test:** set `DISCOVER_TEST` to `1`, `true`, or `yes`, then redeploy. The log says `mode=test` and `ONE-SHOT TEST`. `DISCOVER_SMOKE` is a deprecated alias of that switch (same truthy values, test label only).
- **Batch size:** `DISCOVER_TOP_FRACTION` (default `0.20`). `1` harvests the whole ranked set. `20` and `20%` mean the same quintile.
- **Rescan:** `DISCOVER_RESCAN=1` harvests mints already ledgered `ok` or `empty`. Unset skips those. A ledger row with `status=error` stays eligible either way.

## Top 20%

The ranked set is distinct Solana success mints in `wallet_intel.telegram_call_outcomes`. A mint qualifies when `chain` is null or starts with `sol`, `COALESCE(is_success, roi_multiple >= 2)` is true, `roi_multiple >= 2`, and `token_address` is set. One outcome row is kept per mint: the highest `roi_multiple`, then latest `detected_at`, then highest `id`.

**Top 20%** is `ceil(N × fraction)` of that list (at least 1 when N ≥ 1), default fraction `0.20`. Mints whose `wallet_intel.token_trader_scans.status` is `ok` or `empty` are dropped unless `DISCOVER_RESCAN` is set. `error` is not a completed scan.

`roi_multiple` is the ranking this job already used (`ORDER BY roi_multiple DESC`, previously `LIMIT 1`). Solana Tracker realized PnL sorts traders inside a mint after the mint is chosen. Tracker `/tokens/volume` and `/tokens/trending` also carry liquidity, but they are a different universe (about 100 market leaders), not this success book. `wallet_intel.v_repeat_winners` ranks wallets for the promote cap. It does not choose mints, and new `tracker_traders` rows are still inserted with `is_active=false` until that existing cap says otherwise.

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
batch selected ranked=40 quintile=8 chosen=8 fraction=0.2 rescan=false
mint selected mint=… outcome_id=… roi=…
tracker page=1 kept=12 cumulative=12 hasMore=True
tracker stop page=2 reason=below_realized_floor
upsert progress rows=20 green_usd=1500
mint=… pages=2 upserted=20 green_usd=1500 status=ok
mint=… status=error reason=TimeoutException phase=harvest
batch summary selected=8 ok=7 empty=0 error=1
harvest summary rows=3 source=tracker_traders
promote summary cap=100 rows_updated=100 source=tracker_traders
```
