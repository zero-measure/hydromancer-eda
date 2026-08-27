\set ON_ERROR_STOP on

-- One-time reset for the observed-lifetime label schema. The next ingestion
-- startup recreates wallet_flow_labels from the packaged schema. Raw events,
-- ingestion checkpoints, and rejects are intentionally preserved.
DROP TABLE IF EXISTS public.wallet_flow_labels CASCADE;
