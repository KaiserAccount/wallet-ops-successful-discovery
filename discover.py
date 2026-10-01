#!/usr/bin/env python3
"""Paper-only Solana Tracker trader harvest: one mint per cron run."""
from __future__ import annotations
import argparse, logging, os, socket, sys
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote, unquote, urlparse
import httpx, psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
load_dotenv(Path(__file__).resolve().parent / ".env")
log = logging.getLogger("discover")
TRACKER_BASE = "https://data.solanatracker.io"
PAGE_LIMIT, MAX_PAGES = 20, 23
REALIZED_FLOOR_USD = Decimal("50")
PROMOTE_CAP = 100

def ipv4(host):
    try: return next(iter({x[4][0] for x in socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)}))
    except (socket.gaierror, StopIteration): raise RuntimeError(f"No IPv4 address for {host}")

def connect():
    host=os.getenv("SUPABASE_HOST","").strip()
    if host: return psycopg2.connect(host=host,hostaddr=ipv4(host),port=int(os.getenv("SUPABASE_PORT","5432")),user=os.environ["SUPABASE_USER"],password=os.environ["SUPABASE_PASSWORD"],dbname=os.getenv("SUPABASE_DBNAME","postgres"),sslmode="require",connect_timeout=30,options="-c statement_timeout=120000")
    d=urlparse(os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL") or "")
    if not d.hostname or not d.username: raise RuntimeError("Set SUPABASE_* or DATABASE_URL")
    return psycopg2.connect(host=d.hostname,hostaddr=ipv4(d.hostname),port=d.port or 5432,user=unquote(d.username),password=unquote(d.password or ""),dbname=(d.path or "/postgres").lstrip("/"),sslmode="require",connect_timeout=30,options="-c statement_timeout=120000")

def dec(x):
    try: return None if x is None or isinstance(x,bool) else Decimal(str(x))
    except Exception: return None

def dig(x,*p):
    for k in p:
        if not isinstance(x,dict): return None
        x=x.get(k)
    return x

def fetch(client,key,mint):
    out=[]; cursor=None
    for page in range(1,MAX_PAGES+1):
        q={"sort":"realized","direction":"desc","limit":PAGE_LIMIT,"excludeArbitrage":"true","excludeZeroBuys":"true"}
        if cursor: q["cursor"]=cursor
        r=client.get(f"{TRACKER_BASE}/v2/pnl/tokens/{quote(mint,safe='')}/traders",params=q,headers={"x-api-key":key},timeout=45)
        r.raise_for_status(); body=r.json(); traders=body.get("traders") or []
        out.extend(t for t in traders if isinstance(t,dict) and dec(dig(t,"pnl","token","realized")) is not None and dec(dig(t,"pnl","token","realized"))>0 and t.get("wallet"))
        pg=body.get("pagination") or {}; floor=min((dec(dig(t,"pnl","token","realized")) for t in traders if dec(dig(t,"pnl","token","realized")) is not None),default=None)
        cursor=pg.get("nextCursor")
        if not pg.get("hasMore") or not cursor or floor is None or floor < REALIZED_FLOOR_USD: break
    return out, page

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--mint",default=os.getenv("DISCOVER_MINT")); mint_override=ap.parse_args().mint
    conn=connect(); cur=conn.cursor()
    if mint_override:
        cur.execute("SELECT 1 FROM wallet_intel.token_trader_scans WHERE token_mint=%s",(mint_override,));
        if cur.fetchone(): log.info("mint=%s pages=0 upserted=0 tracker_calls=0 status=skip",mint_override); return 0
        cur.execute("SELECT id,token_address,roi_multiple,detected_at FROM wallet_intel.telegram_call_outcomes WHERE token_address=%s ORDER BY roi_multiple DESC NULLS LAST, detected_at DESC NULLS LAST, id DESC LIMIT 1",(mint_override,))
    else:
        cur.execute("SELECT o.id,o.token_address,o.roi_multiple,o.detected_at FROM wallet_intel.telegram_call_outcomes o WHERE (o.chain IS NULL OR o.chain ILIKE 'sol%') AND COALESCE(o.is_success,o.roi_multiple>=2)=true AND o.roi_multiple>=2 AND o.token_address IS NOT NULL AND NOT EXISTS (SELECT 1 FROM wallet_intel.token_trader_scans s WHERE s.token_mint=o.token_address) ORDER BY o.roi_multiple DESC,o.detected_at DESC NULLS LAST LIMIT 1")
    choice=cur.fetchone()
    if not choice: log.info("no unscanned mint; exit 0"); return 0
    oid,mint,roi,detected=choice; key=os.getenv("SOLANA_TRACKER_API_KEY","")
    if not key: log.error("SOLANA_TRACKER_API_KEY is not set"); return 1
    with httpx.Client() as client: traders,pages=fetch(client,key,mint)
    rows=[]; seen=set()
    for t in traders:
        w=str(t.get("wallet") or "").strip()
        if not w or w in seen: continue
        seen.add(w); realized=dec(dig(t,"pnl","token","realized")); invested=dec(t.get("invested",t.get("buyUsd"))); proceeds=dec(t.get("proceeds",t.get("sellUsd")))
        rows.append((w,mint,0,realized,invested,proceeds,dec(t.get("roi")),dig(t,"counts","buys"),dig(t,"counts","sells"),None,None,dec(dig(t,"timing","holdTimeSecs")),dig(t,"pnl","wallet","totalTrades"),dig(t,"pnl","wallet","tokensTraded"),dec(dig(t,"pnl","wallet","realized")),dig(t,"identity","type"),dig(t,"identity","tags") or [],"tracker_traders",True,False,False))
    if rows:
        execute_values(cur,"INSERT INTO wallet_intel.wallet_token_positions (wallet_address,token_address,current_balance,realized_usd,invested_usd,proceeds_usd,roi,n_buys,n_sells,first_buy_at,last_sell_at,hold_secs,career_trades,career_tokens,career_realized_usd,identity_type,identity_tags,source,won,copy_ok,early) VALUES %s ON CONFLICT (wallet_address,token_address) DO UPDATE SET realized_usd=EXCLUDED.realized_usd,invested_usd=EXCLUDED.invested_usd,proceeds_usd=EXCLUDED.proceeds_usd,roi=EXCLUDED.roi,n_buys=EXCLUDED.n_buys,n_sells=EXCLUDED.n_sells,hold_secs=EXCLUDED.hold_secs,career_trades=EXCLUDED.career_trades,career_tokens=EXCLUDED.career_tokens,career_realized_usd=EXCLUDED.career_realized_usd,identity_type=EXCLUDED.identity_type,identity_tags=EXCLUDED.identity_tags,source=EXCLUDED.source,won=EXCLUDED.won,copy_ok=EXCLUDED.copy_ok,early=EXCLUDED.early,updated_at=now()",rows,page_size=200)
    green=sum((r[3] for r in rows),Decimal(0)); status="ok" if rows else "empty"
    cur.execute("INSERT INTO wallet_intel.token_trader_scans (token_mint,first_outcome_id,roi_at_scan,pages_fetched,wallets_upserted,green_usd,status) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (token_mint) DO NOTHING",(mint,oid,roi,pages,len(rows),green,status)); conn.commit()
    cur.execute("INSERT INTO public.tracked_wallets (wallet_address,name,source,is_active,wallet_tier,notes) SELECT wallet_address,'tracker','tracker_traders',false,'tier_4','harvest' FROM wallet_intel.v_repeat_winners ON CONFLICT (wallet_address) DO UPDATE SET last_imported_at=now(),updated_at=now()"); conn.commit()
    cur.execute(f"UPDATE public.tracked_wallets t SET is_active=(t.wallet_address IN (SELECT wallet_address FROM wallet_intel.v_repeat_winners ORDER BY n_won DESC,pnl_won DESC NULLS LAST LIMIT {PROMOTE_CAP})),updated_at=now() WHERE t.source='tracker_traders'"); conn.commit()
    log.info("mint=%s pages=%s upserted=%s green_usd=%s status=%s",mint,pages,len(rows),green,status); return 0

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s");
    try: sys.exit(main())
    except Exception: log.exception("unhandled"); sys.exit(1)
