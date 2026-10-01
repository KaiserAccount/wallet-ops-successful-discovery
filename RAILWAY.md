# Railway — discover

**What this service does**

One Solana mint every 8 hours. Paper only: no swaps, no purchases, no live trading.

1. Pick the highest-ROI unscanned Solana row in `wallet_intel.telegram_call_outcomes`.
2. Call Solana Tracker traders and upsert profitable wallets into `wallet_intel.wallet_token_positions`.
3. Insert `wallet_intel.token_trader_scans` so that mint is never pulled again.
4. Harvest repeat winners into `public.tracked_wallets` inactive, then promote at most 100 tracker wallets.

Nothing in that list runs unless `DISCOVER_SMOKE` is truthy. See the sequence below.

**Start**

- Cron: `0 */8 * * *` (UTC)
- Command: `python discover.py`
- Restart policy: **NEVER**

A cron service does **not** start on deploy or when a variable changes. Railway starts it only when the schedule fires. To get one run immediately, clear the Cron Schedule (leave it empty) so the latest deployment starts at once. After that run exits, put the schedule back. Restart policy NEVER means a finished run stays stopped until the next schedule (or until you clear the schedule again).

**Env**

- `SOLANA_TRACKER_API_KEY`
- `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_USER`, `SUPABASE_PASSWORD`, `SUPABASE_DBNAME`, or `DATABASE_URL`
- `DISCOVER_SMOKE` — arming switch for the paper harvest (below)

Apply `migrations/20260930_token_trader_harvest.sql` before the first real cron run.

## Postgres (Railway → Supabase session pooler)

The two crashed runs (`EAUTHQUERY` / `connection to database not available`) dialed `35.160.209.8` and `44.238.118.41`. Those are two of the three A records for `aws-0-us-west-2.pooler.supabase.com`. The third, `54.70.143.232`, is an address `wallet_ops_archive` has connected through. `discover.py` now tries every A record and moves on only when the failure is that node-level auth-query outage (or a dial timeout). It does **not** retry a password failure or `tenant/user not found`, so a bad password cannot hammer the pooler circuit breaker.

`db.trzfysszmrgogpeitzfk.supabase.co` is AAAA-only. This service does not use it.

| Variable | Value |
| --- | --- |
| `SUPABASE_HOST` | Session pooler hostname copied from the Supabase Connect dialog. For project `trzfysszmrgogpeitzfk` that is `aws-0-us-west-2.pooler.supabase.com`. `aws-1-us-west-2.pooler.supabase.com` returns tenant/user not found. Do not guess the cluster index, and do not paste a raw IP (TLS needs the hostname). |
| `SUPABASE_PORT` | `5432` (session mode). `6543` is rejected before dial. |
| `SUPABASE_USER` | `<role>.<project-ref>`, for example `postgres.trzfysszmrgogpeitzfk`. A bare `postgres` is rejected before dial. |
| `SUPABASE_PASSWORD` | Database password. Not committed. |
| `SUPABASE_DBNAME` | `postgres` |

`SUPABASE_HOST` wins over `DATABASE_URL`. The URL form is accepted as a fallback and is parsed so `postgres.<project-ref>` is not truncated at the dot. Connect uses keyword arguments plus `hostaddr` (IPv4 only), `sslmode=require`, and TCP keepalives. `statement_timeout` is set after login, not in the startup packet.

A failed connect raises a message that names the host, port, user, and hostaddrs tried, and says the failure is not the Tracker API.

## `DISCOVER_SMOKE`

| Value | Behavior |
| --- | --- |
| unset, empty, `0`, `false`, `no` | Log `DISCOVER_SMOKE unset; skip run` and exit 0. No Postgres connection and no Tracker call. |
| `1`, `true`, `yes` | Log `DISCOVER_SMOKE enabled; running paper harvest once`, run the paper harvest, exit. |

The schedule does not bypass the switch. An 8-hour cron start harvests only while `DISCOVER_SMOKE` is truthy. Clearing the variable disarms the job and leaves the cron in place.

### Safe sequence

Service: **Successful Wallet Discovery** (Wallet Ops Core). Settings → Variables, and Settings → Cron Schedule. Redeploy is not required for a variable change, but a cron service still will not boot until the schedule fires or the schedule is cleared.

1. **Deploy inert.** Leave `DISCOVER_SMOKE` unset. Leave cron at `0 */8 * * *`. The next scheduled start logs `DISCOVER_SMOKE unset; skip run` and exits 0.
2. **One real paper run now.** Set `DISCOVER_SMOKE=1`. Clear Cron Schedule (empty) and save. Railway starts the service immediately because it is no longer a cron service. Confirm logs show `DISCOVER_SMOKE enabled; running paper harvest once` and then either `mint=... status=...` or `no unscanned mint`. The process exits (restart NEVER). This is one mint, paper only.
3. **Unattended harvests.** Set Cron Schedule back to `0 */8 * * *`. Leave `DISCOVER_SMOKE=1`. Each scheduled start runs one paper mint. To stop harvests without deleting the cron, clear `DISCOVER_SMOKE` (or set it to `0`). The next start logs `DISCOVER_SMOKE unset; skip run` and exits 0.

Do not clear the cron while `DISCOVER_SMOKE` is unset if you wanted a real run: the immediate start would still skip.
