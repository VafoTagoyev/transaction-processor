-- =====================================================================================
-- V1: baseline schema, exactly as specified in the assignment (sections 3, 7, 9, 10).
-- Structural additions required by the ownership / retry protocol are added in V2 so
-- that the delta against the assignment is explicit and reviewable.
-- =====================================================================================

CREATE TABLE transactions (
    id                    BIGSERIAL PRIMARY KEY,
    external_id           VARCHAR(100) NOT NULL,
    card_id               VARCHAR(100),
    terminal_id           VARCHAR(100),
    amount                NUMERIC(18, 2) NOT NULL,
    currency              VARCHAR(3)   NOT NULL,
    transaction_type      VARCHAR(50),
    status                VARCHAR(30)  NOT NULL DEFAULT 'NEW',
    retry_count           INTEGER      NOT NULL DEFAULT 0,
    processing_started_at TIMESTAMP,
    processing_instance   VARCHAR(100),
    processed_at          TIMESTAMP,
    error_message         TEXT,
    created_at            TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_transactions (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT       NOT NULL,
    external_id    VARCHAR(100) NOT NULL,
    client_id      VARCHAR(100),
    account        VARCHAR(50),
    product_id     VARCHAR(100),
    merchant_id    VARCHAR(100),
    branch_code    VARCHAR(50),
    amount         NUMERIC(18, 2),
    commission     NUMERIC(18, 2),
    operation_type VARCHAR(30),
    processed_by   VARCHAR(100),
    created_at     TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uk_processed_transaction UNIQUE (transaction_id)
);

CREATE TABLE account_statistics (
    account            VARCHAR(50) PRIMARY KEY,
    transactions_count BIGINT        NOT NULL,
    total_amount       NUMERIC(20, 2) NOT NULL,
    total_commission   NUMERIC(20, 2) NOT NULL,
    updated_at         TIMESTAMP     NOT NULL
);

CREATE TABLE outbox_events (
    id           UUID PRIMARY KEY,
    aggregate_id BIGINT       NOT NULL,
    event_type   VARCHAR(100) NOT NULL,
    payload      JSONB        NOT NULL,
    status       VARCHAR(30)  NOT NULL,
    created_at   TIMESTAMP    NOT NULL
);
