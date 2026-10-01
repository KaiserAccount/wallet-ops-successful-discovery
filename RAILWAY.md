# Railway — discover

**What this service does**

One Solana mint every 8 hours. Paper only: no swaps, no purchases, no live trading.

1. Pick the highest-ROI unscanned Solana row in `wallet_intel.telegram_call_outcomes`.
2. Call Solana Tracker traders and upsert profitable wallets into `wallet_intel.wallet_token_positions`.
3. Insert `wallet_intel.token_trader_scans` so that mint is never pulled again.
4. Harvest repeat winners into `public.tracked_wallets` inactive, then promote at most 100 tracker wallets.

**Start**

- Cron: `0 */8 * * *` (UTC)
- Command: `python discover.py`
- Restart policy: **NEVER**

**Env**

- `SOLANA_TRACKER_API_KEY`
- `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_USER`, `SUPABASE_PASSWORD`, `SUPABASE_DBNAME`, or `DATABASE_URL`

Apply `migrations/20260930_token_trader_harvest.sql` before the first cron run.
