-- =====================================================================================
-- V2: columns required by the claim / fencing / retry protocol.
--
-- Rationale for each addition (defended in docs/concurrency.md and docs/recovery.md):
--
--  processing_token  Fencing token. A *new* UUID is generated on every claim and on every
--                    re-claim. Every write that finalises a transaction is conditioned on
--                    "processing_token = <token the worker holds>". A worker that was
--                    declared stale and had its work reassigned can therefore never commit
--                    a late result: its token no longer matches. Without this column the
--                    stale-recovery race in requirement 47 is unsolvable.
--
--  next_attempt_at   Retry backoff scheduling. A failed transaction returns to NEW with a
--                    future next_attempt_at, so the claim query naturally skips it until
--                    the backoff has elapsed. Using a timestamp (rather than sleeping in
--                    Java) keeps the retry schedule durable across restarts.
--
--  recovery_count    How many times this row was reclaimed after a lease expiry. Separates
--                    "the worker died" from "the business logic failed" in metrics and in
--                    the poison-pill guard.
--
--  updated_at        Debugging / operational visibility on a high-churn table.
-- =====================================================================================

ALTER TABLE transactions
    ADD COLUMN processing_token UUID,
    ADD COLUMN next_attempt_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    ADD COLUMN recovery_count   INTEGER   NOT NULL DEFAULT 0,
    ADD COLUMN updated_at       TIMESTAMP NOT NULL DEFAULT NOW();

-- The status machine is small and closed; enforce it in the database so that no code path
-- (including ad-hoc SQL during an incident) can invent a status the poller cannot see.
ALTER TABLE transactions
    ADD CONSTRAINT ck_transactions_status
        CHECK (status IN ('NEW', 'PROCESSING', 'PROCESSED', 'ERROR'));

-- A row is owned if and only if it is PROCESSING. This invariant is what makes the
-- fencing predicate "status = 'PROCESSING' AND processing_token = ?" sufficient.
ALTER TABLE transactions
    ADD CONSTRAINT ck_transactions_ownership
        CHECK ((status = 'PROCESSING') = (processing_token IS NOT NULL));

ALTER TABLE processed_transactions
    ADD CONSTRAINT ck_processed_operation_type
        CHECK (operation_type IN ('INTERNAL', 'EXTERNAL'));

ALTER TABLE account_statistics
    ADD CONSTRAINT ck_account_statistics_non_negative
        CHECK (transactions_count >= 0);

ALTER TABLE outbox_events
    ADD COLUMN published_at TIMESTAMP,
    ADD COLUMN attempts     INTEGER NOT NULL DEFAULT 0;

ALTER TABLE outbox_events
    ADD CONSTRAINT ck_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'));

-- transactions is a high-churn table: every row is UPDATEd at least twice (claim, finalise).
-- Leaving free space in each page lets PostgreSQL keep more updates on the same page and
-- reduces index bloat; a more aggressive autovacuum keeps the dead tuples from accumulating
-- in front of the claim index.
ALTER TABLE transactions SET (
    fillfactor = 85,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01
);
