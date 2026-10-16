\set ON_ERROR_STOP on
\pset pager off

-- DESTRUCTIVE, explicit dataset migration. Stop all ingestion before running.
-- Retain existing SOL/BTC/ETH history; permanently disallow all other symbols.
BEGIN;
SET LOCAL lock_timeout = '10s';
SELECT pg_advisory_xact_lock(hashtext('hydromancer-analytics-schema'));

DO $$
DECLARE
    dataset_table TEXT;
    removed BIGINT;
BEGIN
    FOREACH dataset_table IN ARRAY ARRAY[
        'orderflow_events',
        'l2_book_ticks',
        'l2_book_levels',
        'l2_book_state',
        'l2_event_counters',
        'l4_order_events',
        'wallet_order_aggregates',
        'leverage_updates',
        'wallet_leverage_state',
        'wallet_markout_samples'
    ] LOOP
        EXECUTE format(
            'DELETE FROM public.%I WHERE symbol NOT IN (''SOL'', ''BTC'', ''ETH'')',
            dataset_table
        );
        GET DIAGNOSTICS removed = ROW_COUNT;
        RAISE NOTICE '%: removed % non-major rows', dataset_table, removed;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = format('public.%I', dataset_table)::regclass
              AND conname = 'dataset_majors_only'
        ) THEN
            EXECUTE format(
                'ALTER TABLE public.%I ADD CONSTRAINT dataset_majors_only '
                'CHECK (symbol IN (''SOL'', ''BTC'', ''ETH''))',
                dataset_table
            );
        END IF;
    END LOOP;
END $$;

-- Labels have no symbol column and may blend retained and removed markets.
-- Recompute them from retained/new majors using FlowLabeler after collection.
DELETE FROM public.wallet_flow_labels;

-- Do not retain quarantined payloads explicitly belonging to removed markets.
DELETE FROM public.ingestion_rejects
WHERE COALESCE(raw -> 1 ->> 'coin', raw ->> 'coin') NOT IN ('SOL', 'BTC', 'ETH');

-- A resumed session restores its old subscriptions. Force fresh subscriptions
-- while keeping opaque cursors for whatever replay the source can still supply.
UPDATE public.ingestion_checkpoints
SET session_id = NULL, updated_at = NOW()
WHERE channel IN ('allFills', 'l2BookDiff', 'l4BookUpdates', 'allLeverageUpdates');
COMMIT;
