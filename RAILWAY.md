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

1. Leave `DISCOVER_SMOKE` unset. A matrix run does not harvest even if the smoke switch is on.
2. Set `MATRIX_RUN=1`. Password is `SUPABASE_PASSWORD` or `SUPABASE_DB_PASSWORD` (already on the service). Do not put the password in the start command.
3. Clear Cron Schedule (empty) and save so this starts immediately. An 8-hour cron must not be left on while `MATRIX_RUN=1`, or every tick will dial the pooler again.
4. Read the log. A pass is one `WINNER` line and exit 0. No pass is `NO_WINNER` and exit 1. Empty password is exit 2 and does not dial. `MATRIX_ENUMERATE=1` prints the case list and does not dial, if the circuit breaker is still hot and you only want the plan.
5. Copy `CONNECT_MODE`, `SUPABASE_HOST`, `SUPABASE_PORT`, and `SUPABASE_USER` from the `WINNER` line into the service variables. Leave the password where it is.
6. Unset `MATRIX_RUN` (and `MATRIX_ENUMERATE`). Then follow the `DISCOVER_SMOKE` sequence above for one paper mint, and only then put cron back to `0 */8 * * *`.

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
