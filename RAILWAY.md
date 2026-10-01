# Railway — discover

**What this service does**

One Solana mint every 8 hours. Paper only: no swaps, no purchases, no live trading.

1. Pick the highest-ROI unscanned Solana row in `wallet_intel.telegram_call_outcomes`.
2. Call Solana Tracker traders and upsert profitable wallets into `wallet_intel.wallet_token_positions`.
3. Insert `wallet_intel.token_trader_scans` so that mint is never pulled again.
4. Harvest repeat winners into `public.tracked_wallets` inactive, then promote at most 100 tracker wallets.

Each cron start runs that list once and exits. `DISCOVER_TEST` only labels a one-shot check; it does not gate the harvest. See the sequence below.

**Start**

- Cron: `0 */8 * * *` (UTC)
- Command: `python discover.py`
- Restart policy: **NEVER**

A cron service does **not** start on deploy or when a variable changes. Railway starts it only when the schedule fires. To get one run immediately, clear the Cron Schedule (leave it empty) so the latest deployment starts at once. After that run exits, put the schedule back. Restart policy NEVER means a finished run stays stopped until the next schedule (or until you clear the schedule again).

**Env**

- `SOLANA_TRACKER_API_KEY`
- `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_USER`, `SUPABASE_PASSWORD`, `SUPABASE_DBNAME`, or `DATABASE_URL`
- `DISCOVER_TEST` — optional one-shot label (`1` / `true` / `yes`). Unset is the normal cron harvest. `DISCOVER_SMOKE` is a deprecated alias of `DISCOVER_TEST` and does not gate cron.
- `PYTHONUNBUFFERED=1` — already set in the image. Logs are INFO on stdout (not stderr) and flushed per line. Do not rely on stderr, which Railway tags as errors.

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
| unset, empty, `0`, `false`, `no` | Cron mode. Log `run start mode=cron paper=true` and run one paper harvest, then exit. |
| `1`, `true`, `yes` | One-shot test. Log `run start mode=test paper=true via=DISCOVER_TEST` and `ONE-SHOT TEST`, run one paper harvest, then exit. |

`DISCOVER_SMOKE` is a deprecated alias with the same truthy set. If it is still `1` from the old arming switch, the run is labeled test (`via=DISCOVER_SMOKE (deprecated alias of DISCOVER_TEST)`) and still harvests. Unset it when you want cron lines to say `mode=cron`. Leaving both unset is the production cron setup.

Progress lines (stdout, INFO, flushed) look like:

```
run start mode=test paper=true via=DISCOVER_TEST; ONE-SHOT TEST paper harvest, running once then exit
connect attempt host=aws-0-us-west-2.pooler.supabase.com port=5432 user=postgres.<ref> hostaddr=54.70.143.232
connected host=aws-0-us-west-2.pooler.supabase.com port=5432 user=postgres.<ref> hostaddr=54.70.143.232
mint selected mint=<mint> outcome_id=123 roi=4.2
tracker page=1 kept=12 cumulative=12 hasMore=True
tracker stop page=2 reason=below_realized_floor
upsert progress rows=20 green_usd=1500
harvest summary rows=3 source=tracker_traders
promote summary cap=100 rows_updated=100 source=tracker_traders
mint=<mint> pages=2 upserted=20 green_usd=1500 status=ok
```

Connect lines include host, port, user, and hostaddr only. A skip or failure names `reason=` (`already_scanned`, `no_unscanned_mint`, `SOLANA_TRACKER_API_KEY_unset`, `connect failed reason=...`). Tracker failures log the exception type, not the message, so the API key cannot be echoed.

### One-shot test, then cron

Service: **Successful Wallet Discovery** (Wallet Ops Core). Settings → Variables, and Settings → Cron Schedule. Restart policy stays **NEVER**.

1. **One-shot now.** Set `DISCOVER_TEST=1`. Redeploy. A cron service does not boot on deploy while a schedule is set, so clear Cron Schedule (empty) before that redeploy if you need the run immediately rather than at the next `0 */8 * * *` tick. Confirm logs show `mode=test`, `ONE-SHOT TEST`, then either `mint=... status=...` or `no unscanned mint`. The process exits. This is one mint, paper only.
2. **Unattended harvests.** Unset `DISCOVER_TEST` (and unset `DISCOVER_SMOKE` if it is still present). Set Cron Schedule back to `0 */8 * * *`. Each scheduled start runs one paper mint and logs `mode=cron`. No smoke flag is required.

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
6. Unset `MATRIX_RUN` (and `MATRIX_ENUMERATE`). Then follow the `DISCOVER_TEST` sequence above for one paper mint, and only then put cron back to `0 */8 * * *` with `DISCOVER_TEST` unset.

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
