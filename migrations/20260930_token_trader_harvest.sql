-- One-shot preflight for the 8-hour Tracker harvest.
UPDATE public.tracked_wallets SET is_active = false, updated_at = now()
WHERE source IN ('gmgn', 'top_2000_json') AND is_active = true;

CREATE TABLE IF NOT EXISTS wallet_intel.token_trader_scans (
 token_mint text PRIMARY KEY, first_outcome_id bigint, roi_at_scan numeric,
 scanned_at timestamptz NOT NULL DEFAULT now(), pages_fetched int,
 wallets_upserted int, green_usd numeric, status text NOT NULL DEFAULT 'ok',
 CONSTRAINT token_trader_scans_status_chk CHECK (status IN ('ok','empty','error'))
);
REVOKE ALL ON TABLE wallet_intel.token_trader_scans FROM PUBLIC, anon, authenticated;
GRANT ALL ON TABLE wallet_intel.token_trader_scans TO postgres, service_role;

ALTER TABLE wallet_intel.wallet_token_positions
 ADD COLUMN IF NOT EXISTS realized_usd numeric, ADD COLUMN IF NOT EXISTS invested_usd numeric,
 ADD COLUMN IF NOT EXISTS proceeds_usd numeric, ADD COLUMN IF NOT EXISTS roi numeric,
 ADD COLUMN IF NOT EXISTS n_buys integer, ADD COLUMN IF NOT EXISTS n_sells integer,
 ADD COLUMN IF NOT EXISTS hold_secs numeric, ADD COLUMN IF NOT EXISTS career_trades bigint,
 ADD COLUMN IF NOT EXISTS career_tokens bigint, ADD COLUMN IF NOT EXISTS career_realized_usd numeric,
 ADD COLUMN IF NOT EXISTS identity_type text, ADD COLUMN IF NOT EXISTS identity_tags text[],
 ADD COLUMN IF NOT EXISTS source text, ADD COLUMN IF NOT EXISTS won boolean,
 ADD COLUMN IF NOT EXISTS copy_ok boolean, ADD COLUMN IF NOT EXISTS early boolean;

CREATE OR REPLACE VIEW wallet_intel.v_repeat_winners WITH (security_invoker = true) AS
SELECT wallet_address, COUNT(*) FILTER (WHERE won) AS n_won, COUNT(*) AS n_mints,
 SUM(realized_usd) FILTER (WHERE won) AS pnl_won, MAX(career_trades) AS career_trades,
 MAX(career_tokens) AS career_tokens
FROM wallet_intel.wallet_token_positions GROUP BY 1
HAVING COUNT(*) FILTER (WHERE won) >= 2 AND MAX(career_tokens) < 2000 AND MAX(career_trades) < 8000;
REVOKE ALL ON wallet_intel.v_repeat_winners FROM PUBLIC, anon, authenticated;
GRANT SELECT ON wallet_intel.v_repeat_winners TO postgres, service_role;
