# Railway — discover

**What this service does**

Every 8 hours, harvest the top 20% of distinct tokens whose outcomes are within the last 10 hours. Paper only: no swaps, no purchases, no live trading.

1. Keep `wallet_intel.telegram_call_outcomes` rows with `detected_at` inside the last 10 hours (`DISCOVER_WINDOW_HOURS`, default `10`). Collapse to one row per token (highest `roi_multiple`, then latest `detected_at`, then highest `id`) before the cut. Take the top 20% of that distinct list (`DISCOVER_TOP_FRACTION`, default `0.20`).
2. Skip mints already ledgered `ok` or `empty` unless `DISCOVER_RESCAN` is set. `status=error` stays eligible.
3. For each chosen mint, call Solana Tracker traders and upsert profitable wallets into `wallet_intel.wallet_token_positions`. One mint's failure is logged and skipped.
4. Insert or update `wallet_intel.token_trader_scans` (`ok`, `empty`, or `error`).
5. Harvest repeat winners into `public.tracked_wallets` inactive (`is_active=false`), then promote at most 100 tracker wallets under the existing `v_repeat_winners` rule. A larger mint batch does not activate wallets.

Each cron start runs that list once and exits. `DISCOVER_TEST` only labels a one-shot check; it does not gate the harvest. See the sequence below.

**Start**

- Cron: `0 */8 * * *` (UTC). This schedule does not change. The selection window is 10 hours, so it overlaps the previous run by about 2 hours.
- Command: `python discover.py`
- Restart policy: **NEVER**

A cron service does **not** start on deploy or when a variable changes. Railway starts it only when the schedule fires. To get one run immediately, clear the Cron Schedule (leave it empty) so the latest deployment starts at once. After that run exits, put the schedule back. Restart policy NEVER means a finished run stays stopped until the next schedule (or until you clear the schedule again).

**Env**

- `SOLANA_TRACKER_API_KEY`
- `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_USER`, `SUPABASE_PASSWORD`, `SUPABASE_DBNAME`, or `DATABASE_URL`
- `DISCOVER_TEST` — optional one-shot label (`1` / `true` / `yes`). Unset is the normal cron harvest. `DISCOVER_SMOKE` is a deprecated alias of `DISCOVER_TEST` and does not gate cron.
- `DISCOVER_TOP_FRACTION` — optional. Default `0.20` (top quintile of the window). `1` is every distinct token in the window. `20` and `20%` are the same as `0.20`.
- `DISCOVER_WINDOW_HOURS` — optional. Default `10`. Outcomes older than this are not ranked. `10h` is the same as `10`.
- `DISCOVER_RESCAN` — optional (`1` / `true` / `yes`). Unset skips mints ledgered `ok` or `empty`.
- `PYTHONUNBUFFERED=1` — already set in the image. Logs are INFO and ERROR on stdout (not stderr) and flushed per line. Do not rely on stderr, which Railway tags as errors.

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

## `DISCOVER_TEST`

`DISCOVER_TEST` does not arm or disarm the harvest. Scheduled production runs with it unset.

| Value | Behavior |
| --- | --- |
| unset, empty, `0`, `false`, `no` | Cron mode. Log `run start mode=cron paper=true` and run one paper harvest of the top ROI quintile, then exit. |
| `1`, `true`, `yes` | One-shot test. Log `run start mode=test paper=true via=DISCOVER_TEST` and `ONE-SHOT TEST`, run one paper harvest of the top ROI quintile, then exit. |

`DISCOVER_SMOKE` is a deprecated alias with the same truthy set. If it is still `1` from the old arming switch, the run is labeled test (`via=DISCOVER_SMOKE (deprecated alias of DISCOVER_TEST)`) and still harvests. Unset it when you want cron lines to say `mode=cron`. Leaving both unset is the production cron setup.

Progress lines (stdout, INFO, flushed) look like:

```
run start mode=test paper=true via=DISCOVER_TEST; ONE-SHOT TEST paper harvest of the top ROI quintile, running once then exit
connect attempt host=aws-0-us-west-2.pooler.supabase.com port=5432 user=postgres.<ref> hostaddr=54.70.143.232
connected host=aws-0-us-west-2.pooler.supabase.com port=5432 user=postgres.<ref> hostaddr=54.70.143.232
batch selected ranked=40 quintile=8 chosen=8 fraction=0.2 window_hours=10 rescan=false
mint selected mint=<mint> outcome_id=123 roi=4.2
tracker page=1 kept=12 cumulative=12 hasMore=True
tracker stop page=2 reason=below_realized_floor
upsert progress rows=20 green_usd=1500
mint=<mint> pages=2 upserted=20 green_usd=1500 status=ok
mint=<mint> status=error reason=TimeoutException phase=harvest
batch summary selected=8 ok=7 empty=0 error=1
harvest summary rows=3 source=tracker_traders
promote summary cap=100 rows_updated=100 source=tracker_traders
```

Connect lines include host, port, user, and hostaddr only. A skip or failure names `reason=` (`already_scanned`, `no_unscanned_mint`, `top_quintile_already_scanned`, `SOLANA_TRACKER_API_KEY_unset`, `connect failed reason=...`). Tracker and mint failures log the exception type, not the message, so the API key cannot be echoed. ERROR lines use the same stdout handler as the INFO progress lines.

## Top 20%

**Definition.** A run is the top 20% of distinct tokens whose outcomes are within the last 10 hours.

Those rows are Solana successes in `wallet_intel.telegram_call_outcomes` with `detected_at >= now() - window`. A row qualifies when `chain` is null or starts with `sol` (`left(lower(chain), 3) = 'sol'`), `COALESCE(is_success, roi_multiple >= 2)` is true, `roi_multiple >= 2`, and `token_address` is not null. `detected_at` null is outside the window. The cron stays `0 */8 * * *`. Default window is 10 hours (`DISCOVER_WINDOW_HOURS`).

`telegram_call_outcomes` can store several rows for one mint. Collapse to one row per token after the time filter and before the 20% cut, so duplicates cannot inflate N or harvest the same mint twice. The kept row is the strongest outcome inside the window: `roi_multiple` descending, then `detected_at` descending, then `id` descending. An older outcome, including a higher ROI from outside the window, does not represent the token.

The batch is `ceil(N × DISCOVER_TOP_FRACTION)` of that distinct list, at least 1 when N ≥ 1. Default fraction `0.20`.

Completed ledger rows (`token_trader_scans.status` of `ok` or `empty`) inside that quintile are skipped unless `DISCOVER_RESCAN` is set. `error` stays eligible so a failed mint can be retried next cron without a rescan flag. If the whole quintile is already `ok` or `empty`, the process logs `reason=top_quintile_already_scanned` and exits 0. If every fetched outcome is outside the window, it logs `reason=outside_window` and exits 0. Tokens below the quintile are not harvested on that run.

**Why this ranking.** It is the order the job already used (`ORDER BY roi_multiple DESC LIMIT 1`). Checked and not used as the mint rank:

| Source | What it ranks | Why it is not the quintile |
| --- | --- | --- |
| `telegram_call_outcomes.roi_multiple` | Distinct tokens inside the last 10 hours | This is the quintile. |
| Solana Tracker traders (`sort=realized`) | Wallets inside one mint, by realized PnL, with a $50 floor | Applied after the mint is chosen. |
| Tracker `/tokens/volume` and `/tokens/trending` | About 100 tokens by volume; pool objects include liquidity | A different universe from the success book. Not stored by this job. |
| `wallet_intel.v_repeat_winners` (`n_won`, `pnl_won`) | Wallets for the promote cap of 100 | Does not choose mints. Inserts stay `is_active=false` until this view's existing rule turns them on. |

This service's selection SQL reads `id`, `token_address`, `roi_multiple`, `detected_at`, and the scan `status`. A live column listing of the book timed out from the audit environment, so no extra volume or liquidity column was confirmed on `telegram_call_outcomes`. None is referenced by the harvest.

## Failure isolation

What the audit found, and what a run does now:

| Failure | Before | Now |
| --- | --- | --- |
| Tracker HTTP error, timeout, or bad JSON on one mint | Logged, then re-raised. `main` exited 1. There was only one mint, so the run died before promote. | Retried once when the error is a timeout, transport error, or HTTP 408/429/5xx. Then ERROR `mint=… reason=<type> phase=harvest`, ledger `status=error` if the write works, next mint continues. Exit 0. |
| Bad trader payload on a page | A raise inside the page aborted the mint and the process. JSON parsing sat outside the HTTP try. | That wallet is logged (`mint` and `wallet`) and skipped. The rest of the page is kept. A non-object body fails the mint only. |
| Postgres data error on `wallet_token_positions` | Aborted the transaction and the process. No rollback, so the session was unusable. | Batch insert is retried once for a connection blip. A data error rolls back and retries per wallet under a savepoint. Poison rows are logged and skipped. A statement timeout is not fanned out per wallet and is not retried. |
| `token_trader_scans` insert | `ON CONFLICT DO NOTHING`, and `status=error` was never written. A crash left no ledger row. | Upsert updates the scan. `error` is written when the mint fails and the table is writable. A later success replaces `error`. |
| Missing table, privilege error, or dead connection while writing the ledger | Exit 1, often as an unhandled exception. | Still exit 1 (`phase=fatal`). Later mints are not attempted. |
| Connect failure, or `SOLANA_TRACKER_API_KEY` unset | Exit 1. | Still exit 1. The key failure happens before any Tracker call. |
| Promote / `tracked_wallets` write after a partial batch | Never reached if the single mint raised. | Runs after the batch. Inserts still use `is_active=false`. Promote still caps at 100 `v_repeat_winners`. A promote failure is logged; it exits 1 only when the ledger class of error or a dead connection is what failed. |

Passwords, API keys, and full DSNs are not logged. Tracker failures log the exception type only.

### One-shot test, then cron

Service: **Successful Wallet Discovery** (Wallet Ops Core). Settings → Variables, and Settings → Cron Schedule. Restart policy stays **NEVER**.

1. **One-shot now.** Set `DISCOVER_TEST=1`. Redeploy. A cron service does not boot on deploy while a schedule is set, so clear Cron Schedule (empty) before that redeploy if you need the run immediately rather than at the next `0 */8 * * *` tick. Confirm logs show `mode=test`, `ONE-SHOT TEST`, then either `batch summary` or `no unscanned mint`. The process exits. This is the top ROI quintile, paper only.
2. **Unattended harvests.** Unset `DISCOVER_TEST` (and unset `DISCOVER_SMOKE` if it is still present). Set Cron Schedule back to `0 */8 * * *`. Each scheduled start runs one paper quintile and logs `mode=cron`. No smoke flag is required. Leave `DISCOVER_RESCAN` unset unless a completed mint must be fetched again.

`MATRIX_RUN=1` still runs the connect matrix only, even when `DISCOVER_TEST` is set.

## Connect matrix (one shot)

`connect_matrix.py` tries a fixed matrix until one login can run `SELECT 1`. It does not trade, scan mints, or write wallets. Passwords and full connection strings are never logged.

Order, first hit wins:

1. Session pooler `aws-0-<region>.pooler.supabase.com:5432` as `postgres.<ref>`, then `archiver.<ref>`, walking every A record.
2. The same host on port `6543` (transaction pooler).
3. `aws-1-<region>` the same way. For `trzfysszmrgogpeitzfk` this cluster returns tenant/user not found; the matrix dials it once per host and then skips that host.
4. `db.<ref>.supabase.co` (bare `postgres` / `archiver`, then the tenant form), IPv4 if any, otherwise IPv6.
5. `https://<ref>.supabase.co` (and any `http://` or `https://` value in `SUPABASE_HOST`, `SUPABASE_URL`, or `SUPABASE_DB_HOST`) is logged as `SKIP` / `HTTP_API_HOST` and is not dialed. That is the API host SwapTable and TokensAlertLogger store in `SUPABASE_DB_HOST`. It is not a psycopg host.

Pooler cases with a bare `postgres` or `archiver` user are `SKIP` / `NEEDS_TENANT_FORM`. Supavisor requires `<role>.<project-ref>` on `*.pooler.supabase.com`. Dialing a bare role returns `ENOIDENTIFIER` and counts toward `ECIRCUITBREAKER`.

Stop rules:

| Result | What the matrix does |
| --- | --- |
| `AUTH_FAILED` (wrong password) | One try for that user. Later cases for the same user are `SKIP`. |
| `ENOTFOUND` (tenant/user not found) | One try for that host. Later users and ports on that host are `SKIP`. |
| `EAUTHQUERY` or timeout | Try the other A records for that user. If every address fails that way, skip other users on the same host and port. |
| `ECIRCUITBREAKER` | Dial nothing else. |
| IPv6 `NO_ROUTE` | Skip the rest of the IPv6 cases. |
| `PASS` | Print one `WINNER` line and exit 0. |

Each line is `CASE id=... result=PASS|FAIL|SKIP host=... port=... user=... family=... hostaddr=... ms=... error=... mode=... snippet=...`. The `WINNER` line repeats the winning mode, host, port, user, family, and address. On a pass, the process also writes `connect_winner.env.example` with those names and a blank password comment.

`CONNECT_MODE` is how `discover.py` keeps using that winner:

| `CONNECT_MODE` | Dial policy |
| --- | --- |
| unset or `session-pooler` | Port 5432, user `<role>.<ref>`, IPv4 failover. Port 6543 is rejected before dial. |
| `transaction-pooler` | Port 6543 is allowed. The harvest still prefers session mode: `SET` and the multi-statement upsert need a session. |
| `direct` | `db.<ref>.supabase.co`, IPv4 or IPv6 when the name has no A record. Bare `postgres` is allowed. |
| `direct-ipv6` | AAAA addresses only. |

### Run the matrix once on Railway

Service: **Successful Wallet Discovery**. Start command stays `python discover.py` (`python connect_matrix.py` is the same matrix). Restart policy stays **NEVER**.

1. A matrix run does not harvest, even if `DISCOVER_TEST` or `DISCOVER_SMOKE` is set.
2. Set `MATRIX_RUN=1`. Password is `SUPABASE_PASSWORD` or `SUPABASE_DB_PASSWORD` (already on the service). Do not put the password in the start command.
3. Clear Cron Schedule (empty) and save so this starts immediately. An 8-hour cron must not be left on while `MATRIX_RUN=1`, or every tick will dial the pooler again.
4. Read the log. A pass is one `WINNER` line and exit 0. No pass is `NO_WINNER` and exit 1. Empty password is exit 2 and does not dial. `MATRIX_ENUMERATE=1` prints the case list and does not dial, if the circuit breaker is still hot and you only want the plan.
5. Copy `CONNECT_MODE`, `SUPABASE_HOST`, `SUPABASE_PORT`, and `SUPABASE_USER` from the `WINNER` line into the service variables. Leave the password where it is.
6. Unset `MATRIX_RUN` (and `MATRIX_ENUMERATE`). Then follow the `DISCOVER_TEST` sequence above for one paper quintile, and only then put cron back to `0 */8 * * *` with `DISCOVER_TEST` unset.

Preferred shape when session mode is the winner (no secret in this file):

```
CONNECT_MODE=session-pooler
SUPABASE_HOST=aws-0-us-west-2.pooler.supabase.com
SUPABASE_PORT=5432
SUPABASE_USER=postgres.trzfysszmrgogpeitzfk
SUPABASE_DBNAME=postgres
# SUPABASE_PASSWORD is set in Railway, not here
```

`aws-1-us-west-2.pooler.supabase.com` is the wrong cluster for this project. `https://trzfysszmrgogpeitzfk.supabase.co` is the wrong scheme. Until a `WINNER` line says otherwise, keep the user that this service already authenticates as (`archiver.trzfysszmrgogpeitzfk` on the runs that reached Supavisor) and the session host above. The matrix still tries `postgres.<ref>` first; a wrong password for that role is a single failure, then it moves on.
