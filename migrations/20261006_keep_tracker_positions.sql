-- Keep Successful Wallet Discovery positions across the nightly PnL rebuild.
--
-- wallet_intel.run_pnl_pipeline() (pg_cron job, 03:30 UTC) calls
-- wallet_intel.rebuild_wallet_positions(), which TRUNCATEd
-- wallet_intel.wallet_token_positions and rebuilt only from
-- wallet_intel.events. That deleted every prior mint the discovery cron
-- had upserted, so the table only ever held the latest post-03:30 scan.
-- Discovery itself upserts on (wallet_address, token_address) and does
-- not delete other mints.
--
-- This rebuild still truncates event-derived rows, then puts
-- source='tracker_traders' rows back. The leaderboard ignores those rows
-- so USD tracker scores are not counted as SOL positions.

CREATE OR REPLACE FUNCTION wallet_intel.rebuild_wallet_positions()
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE v_rows int;
BEGIN
  DROP TABLE IF EXISTS _tracker_keep;
  CREATE TEMP TABLE _tracker_keep ON COMMIT DROP AS
  SELECT *
  FROM wallet_intel.wallet_token_positions
  WHERE source = 'tracker_traders';

  TRUNCATE wallet_intel.wallet_token_positions;

  INSERT INTO wallet_intel.wallet_token_positions (
    wallet_address, token_address, current_balance,
    avg_entry_price, total_cost_sol,
    first_buy_at, last_buy_at, buy_count, total_bought,
    first_sell_at, last_sell_at, sell_count, total_sold,
    realized_pnl_sol, realized_pnl_percent,
    hold_time_minutes, is_closed, closed_at, updated_at
  )
  SELECT
    wallet_address, token_mint,
    GREATEST(COALESCE(tok_bought,0) - COALESCE(tok_sold,0), 0),
    CASE WHEN tok_bought>0 THEN sol_in / tok_bought END,
    sol_in,
    first_buy_at, last_buy_at, buy_count, tok_bought,
    first_sell_at, last_sell_at, sell_count, tok_sold,
    (sol_out - sol_in),
    CASE WHEN sol_in>0
         THEN LEAST(GREATEST(round(((sol_out - sol_in)/sol_in*100)::numeric,2), -100), 999999)
    END,
    CASE WHEN first_buy_at IS NOT NULL AND last_sell_at IS NOT NULL
         THEN GREATEST(EXTRACT(EPOCH FROM (last_sell_at - first_buy_at))/60, 0)::int END,
    (tok_bought > 0 AND tok_sold >= tok_bought * 0.9),
    CASE WHEN (tok_bought>0 AND tok_sold >= tok_bought*0.9) THEN last_sell_at END,
    now()
  FROM (
    SELECT
      wallet_address, token_mint,
      sum(amount_sol)   FILTER (WHERE action='buy')  AS sol_in,
      sum(amount_sol)   FILTER (WHERE action='sell') AS sol_out,
      sum(amount_token) FILTER (WHERE action='buy')  AS tok_bought,
      sum(amount_token) FILTER (WHERE action='sell') AS tok_sold,
      min(occurred_at)  FILTER (WHERE action='buy')  AS first_buy_at,
      max(occurred_at)  FILTER (WHERE action='buy')  AS last_buy_at,
      count(*)          FILTER (WHERE action='buy')  AS buy_count,
      min(occurred_at)  FILTER (WHERE action='sell') AS first_sell_at,
      max(occurred_at)  FILTER (WHERE action='sell') AS last_sell_at,
      count(*)          FILTER (WHERE action='sell') AS sell_count
    FROM wallet_intel.events
    WHERE amount_sol > 0
    GROUP BY wallet_address, token_mint
    HAVING sum(amount_token) FILTER (WHERE action='buy') > 0
  ) agg;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  INSERT INTO wallet_intel.wallet_token_positions
  SELECT * FROM _tracker_keep
  ON CONFLICT (wallet_address, token_address) DO UPDATE SET
    current_balance = EXCLUDED.current_balance,
    avg_entry_price = EXCLUDED.avg_entry_price,
    total_cost_sol = EXCLUDED.total_cost_sol,
    first_buy_at = EXCLUDED.first_buy_at,
    last_buy_at = EXCLUDED.last_buy_at,
    buy_count = EXCLUDED.buy_count,
    total_bought = EXCLUDED.total_bought,
    first_sell_at = EXCLUDED.first_sell_at,
    last_sell_at = EXCLUDED.last_sell_at,
    sell_count = EXCLUDED.sell_count,
    total_sold = EXCLUDED.total_sold,
    realized_pnl_sol = EXCLUDED.realized_pnl_sol,
    realized_pnl_percent = EXCLUDED.realized_pnl_percent,
    unrealized_pnl_sol = EXCLUDED.unrealized_pnl_sol,
    unrealized_pnl_percent = EXCLUDED.unrealized_pnl_percent,
    hold_time_minutes = EXCLUDED.hold_time_minutes,
    time_to_peak_return_minutes = EXCLUDED.time_to_peak_return_minutes,
    time_to_exit_minutes = EXCLUDED.time_to_exit_minutes,
    max_unrealized_gain_percent = EXCLUDED.max_unrealized_gain_percent,
    max_drawdown_percent = EXCLUDED.max_drawdown_percent,
    is_closed = EXCLUDED.is_closed,
    closed_at = EXCLUDED.closed_at,
    became_illiquid_trap = EXCLUDED.became_illiquid_trap,
    updated_at = EXCLUDED.updated_at,
    realized_usd = EXCLUDED.realized_usd,
    invested_usd = EXCLUDED.invested_usd,
    proceeds_usd = EXCLUDED.proceeds_usd,
    roi = EXCLUDED.roi,
    n_buys = EXCLUDED.n_buys,
    n_sells = EXCLUDED.n_sells,
    hold_secs = EXCLUDED.hold_secs,
    career_trades = EXCLUDED.career_trades,
    career_tokens = EXCLUDED.career_tokens,
    career_realized_usd = EXCLUDED.career_realized_usd,
    identity_type = EXCLUDED.identity_type,
    identity_tags = EXCLUDED.identity_tags,
    source = EXCLUDED.source,
    won = EXCLUDED.won,
    copy_ok = EXCLUDED.copy_ok,
    early = EXCLUDED.early;

  RETURN jsonb_build_object(
    'positions_built', v_rows,
    'tracker_positions_kept', (SELECT count(*) FROM _tracker_keep),
    'computed_at', now()
  );
END;
$function$;

CREATE OR REPLACE FUNCTION wallet_intel.rebuild_wallet_leaderboard()
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE v_rows int;
BEGIN
  TRUNCATE wallet_intel.wallet_leaderboard;

  INSERT INTO wallet_intel.wallet_leaderboard (
    wallet_address, wallet_tier, priority_score,
    total_trades, closed_positions, open_positions,
    win_rate, realized_pnl_sol, avg_win_sol, avg_loss_sol,
    profit_factor, expectancy, net_pnl_under_10k_sol, pct_pnl_under_10k,
    open_position_count, open_cost_sol, last_computed_at)
  SELECT
    p.wallet_address, tw.wallet_tier, tw.priority_score,
    count(*),
    count(*) FILTER (WHERE p.is_closed),
    count(*) FILTER (WHERE NOT p.is_closed),
    round((100.0*count(*) FILTER (WHERE p.is_closed AND p.realized_pnl_sol>0)/nullif(count(*) FILTER (WHERE p.is_closed),0))::numeric,2),
    round(sum(p.realized_pnl_sol) FILTER (WHERE p.is_closed)::numeric,3),
    round(avg(p.realized_pnl_sol) FILTER (WHERE p.is_closed AND p.realized_pnl_sol>0)::numeric,4),
    round(avg(p.realized_pnl_sol) FILTER (WHERE p.is_closed AND p.realized_pnl_sol<0)::numeric,4),
    round((sum(p.realized_pnl_sol) FILTER (WHERE p.is_closed AND p.realized_pnl_sol>0)/nullif(abs(sum(p.realized_pnl_sol) FILTER (WHERE p.is_closed AND p.realized_pnl_sol<0)),0))::numeric,2),
    round(avg(p.realized_pnl_sol) FILTER (WHERE p.is_closed)::numeric,4),
    round(sum(p.realized_pnl_sol) FILTER (WHERE p.is_closed AND ev.entry_mc<10000)::numeric,3),
    round((100.0*sum(p.realized_pnl_sol) FILTER (WHERE p.is_closed AND ev.entry_mc<10000)/nullif(sum(p.realized_pnl_sol) FILTER (WHERE p.is_closed),0))::numeric,1),
    count(*) FILTER (WHERE NOT p.is_closed),
    round(sum(p.total_cost_sol) FILTER (WHERE NOT p.is_closed)::numeric,3),
    now()
  FROM wallet_intel.wallet_token_positions p
  JOIN public.tracked_wallets tw ON tw.wallet_address=p.wallet_address
  LEFT JOIN LATERAL (SELECT min(market_cap_at_entry) AS entry_mc FROM wallet_intel.events e
    WHERE e.wallet_address=p.wallet_address AND e.token_mint=p.token_address AND e.action='buy' AND e.market_cap_at_entry>0) ev ON true
  WHERE COALESCE(p.source, '') <> 'tracker_traders'
  GROUP BY p.wallet_address, tw.wallet_tier, tw.priority_score;
  GET DIAGNOSTICS v_rows = ROW_COUNT;

  -- ===== v3 PNL_SCORE: WIN-RATE DOMINANT =====
  WITH metrics AS (
    SELECT p.wallet_address,
      count(*) FILTER (WHERE p.is_closed) AS n,
      100.0*count(*) FILTER (WHERE p.is_closed AND p.realized_pnl_sol>0)/nullif(count(*) FILTER (WHERE p.is_closed),0) AS wr,
      sum(p.realized_pnl_sol) FILTER (WHERE p.is_closed) AS total_pnl,
      sum(p.total_cost_sol) FILTER (WHERE p.is_closed) AS total_cost,
      sum(p.realized_pnl_sol) FILTER (WHERE p.is_closed AND p.realized_pnl_sol>0) AS gw,
      abs(sum(p.realized_pnl_sol) FILTER (WHERE p.is_closed AND p.realized_pnl_sol<0)) AS gl,
      sum(p.realized_pnl_sol*exp(-0.009627*GREATEST(EXTRACT(EPOCH FROM (now()-p.closed_at))/3600,0)))
        FILTER (WHERE p.is_closed AND p.closed_at IS NOT NULL) AS rec_pnl,
      count(*) FILTER (WHERE NOT p.is_closed AND p.current_balance>0 AND p.first_buy_at<now()-interval '24 hours') AS stuck,
      count(*) AS n_total
    FROM wallet_intel.wallet_token_positions p
    WHERE COALESCE(p.source, '') <> 'tracker_traders'
    GROUP BY p.wallet_address
  )
  UPDATE wallet_intel.wallet_leaderboard lb SET pnl_score = round((
    ( LEAST(m.wr/100,1)*25
      + CASE WHEN m.wr>=60 THEN 18 WHEN m.wr>=50 THEN 6 ELSE 0 END
      + LEAST(GREATEST(ln(1+GREATEST(CASE WHEN m.total_pnl>=5 THEN m.total_pnl/nullif(m.total_cost,0) ELSE 0 END,0))/ln(31),0),1)*25
      + LEAST(GREATEST(ln(1+GREATEST(m.total_pnl,0))/ln(501),0),1)*12
      + LEAST(LEAST(m.gw/nullif(m.gl,0),20)/20,1)*8
      + LEAST(GREATEST(ln(1+GREATEST(m.rec_pnl,0))/ln(301),0),1)*5 )
    * (m.n::numeric/(m.n+12))
    * (1 - 0.5*LEAST(m.stuck::numeric/nullif(m.n_total,0),1))
    * CASE WHEN m.wr<35 THEN 0.5 ELSE 1 END
  )::numeric,1)
  FROM metrics m WHERE m.wallet_address=lb.wallet_address AND m.n >= 8;

  UPDATE wallet_intel.wallet_leaderboard SET wallet_archetype = CASE
    WHEN closed_positions < 8 THEN 'insufficient_data'
    WHEN realized_pnl_sol <= 0 THEN 'unprofitable'
    WHEN closed_positions <= 80 AND win_rate >= 70 THEN 'sniper'
    WHEN closed_positions > 150 AND profit_factor >= 1.5 THEN 'grinder'
    WHEN profit_factor >= 1.3 THEN 'profitable_mixed'
    ELSE 'marginal' END;

  RETURN jsonb_build_object('wallets_ranked', v_rows, 'computed_at', now());
END;
$function$;
