-- =====================================================================================
-- V3: indexes. Each index here exists to serve exactly one hot query.
-- =====================================================================================

-- (1) THE CLAIM INDEX.
-- Serves:
--     SELECT id FROM transactions
--      WHERE status = 'NEW' AND next_attempt_at <= now()
--      ORDER BY next_attempt_at, id
--      LIMIT :batch FOR UPDATE SKIP LOCKED
--
-- It is PARTIAL on status = 'NEW'. That is the single most important design decision for
-- scale: the table holds millions of rows but the index only holds the *backlog*. Once a
-- row reaches PROCESSED or ERROR it leaves the index entirely, so the claim query cost
-- stays proportional to the work that is left, not to the size of the table. A plain
-- composite index on (status, next_attempt_at) would keep an entry for every processed row
-- forever and would degrade continuously.
--
-- Column order matches the ORDER BY so the LIMIT is satisfied by an index scan with no sort.
CREATE INDEX idx_transactions_claim
    ON transactions (next_attempt_at, id)
    WHERE status = 'NEW';

-- (2) THE STALE-RECOVERY INDEX.
-- Serves:
--     SELECT id FROM transactions
--      WHERE status = 'PROCESSING' AND processing_started_at < now() - :timeout
--      ORDER BY processing_started_at LIMIT :n FOR UPDATE SKIP LOCKED
--
-- Also partial. In a healthy system this index contains only the currently in-flight rows
-- (instances x workers, i.e. tens or hundreds of entries), so the recovery sweep is
-- effectively free even against a 50M row table.
CREATE INDEX idx_transactions_stale_processing
    ON transactions (processing_started_at)
    WHERE status = 'PROCESSING';

-- (3) Operational lookups: "what happened to external id X?" and per-instance forensics
-- after a crash. Not on the hot path.
CREATE INDEX idx_transactions_external_id ON transactions (external_id);
CREATE INDEX idx_transactions_instance
    ON transactions (processing_instance)
    WHERE status = 'PROCESSING';

-- NOTE: external_id is deliberately NOT UNIQUE here, because the assignment's schema does
-- not declare it unique and the ingestion side is out of scope. If the upstream feed can
-- deliver the same external_id twice, add
--     CREATE UNIQUE INDEX uq_transactions_external_id ON transactions (external_id);
-- to push idempotency one step further upstream. Idempotency of *processing* does not
-- depend on it: that is guaranteed by uk_processed_transaction on transaction_id.

-- (4) processed_transactions: uk_processed_transaction (V1) is the idempotency barrier and
-- is also the index used to detect "already processed". This one supports reporting joins
-- against account_statistics.
CREATE INDEX idx_processed_account ON processed_transactions (account);

-- (5) THE OUTBOX RELAY INDEX. Partial on PENDING for the same reason as the claim index:
-- published events leave the index, so the relay's cost tracks the unpublished backlog.
CREATE INDEX idx_outbox_pending
    ON outbox_events (created_at)
    WHERE status = 'PENDING';
