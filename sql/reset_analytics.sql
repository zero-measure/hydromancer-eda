\set ON_ERROR_STOP on

-- One-time destructive reset for analytics schema version 2. Operational
-- checkpoints and ingestion rejects are intentionally preserved.
BEGIN;
DROP TABLE IF EXISTS public.direction_backfill_runs CASCADE;
DROP TABLE IF EXISTS public.wallet_flow_labels CASCADE;
DROP TABLE IF EXISTS public.wallet_markout_samples CASCADE;
DROP TABLE IF EXISTS public.wallet_leverage_state CASCADE;
DROP TABLE IF EXISTS public.leverage_updates CASCADE;
DROP TABLE IF EXISTS public.wallet_order_aggregates CASCADE;
DROP TABLE IF EXISTS public.l4_order_events CASCADE;
DROP TABLE IF EXISTS public.l2_book_ticks CASCADE;
DROP TABLE IF EXISTS public.l2_event_counters CASCADE;
DROP TABLE IF EXISTS public.l2_book_state CASCADE;
DROP TABLE IF EXISTS public.l2_book_levels CASCADE;
DROP TABLE IF EXISTS public.orderflow_events CASCADE;
DROP TABLE IF EXISTS public.analytics_schema_meta CASCADE;
COMMIT;
