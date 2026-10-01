# wallet-ops-discover
NINETY PAPER Successful Wallet Discovery — Solana Tracker mint→trader harvest for Wallet Ops Core.

Paper only. `python connect_matrix.py` (or `MATRIX_RUN=1 python discover.py`) finds a working Supabase login. See RAILWAY.md.

## Run

`python discover.py` harvests one mint and exits. It does not trade.

- **Cron** (Railway schedule `0 */8 * * *`, restart NEVER): leave `DISCOVER_TEST` unset. `DISCOVER_SMOKE` is not required and no longer skips the run.
- **One-shot test:** set `DISCOVER_TEST` to `1`, `true`, or `yes`, then redeploy. The log says `mode=test` and `ONE-SHOT TEST`. `DISCOVER_SMOKE` is a deprecated alias of that switch (same truthy values, test label only).

## Logs

INFO goes to **stdout** through `logging.StreamHandler(stream=sys.stdout)`, and each record is flushed. The Docker image sets `PYTHONUNBUFFERED=1`; set that yourself when you run the script outside Docker. Passwords, API keys, and full DSNs are not logged.

```
run start mode=cron paper=true; scheduled paper harvest once then exit
connect attempt host=… port=5432 user=postgres.<ref> hostaddr=…
connected host=… port=5432 user=postgres.<ref> hostaddr=…
mint selected mint=… outcome_id=… roi=…
tracker page=1 kept=12 cumulative=12 hasMore=True
tracker stop page=2 reason=below_realized_floor
upsert progress rows=20 green_usd=1500
harvest summary rows=3 source=tracker_traders
promote summary cap=100 rows_updated=100 source=tracker_traders
mint=… pages=2 upserted=20 green_usd=1500 status=ok
```
