-- Acceptance verification. Every query here maps to a criterion in section 15 of the assignment.
-- Run with:  docker compose exec -T postgres psql -U txprocessor -d txprocessor -f - < scripts/verify.sql
-- or:        ./scripts/verify.sh

\echo '=== C1: every transaction reached a terminal status ==='
SELECT status, count(*) AS rows
FROM transactions
GROUP BY status
ORDER BY status;

\echo ''
\echo '=== C1: nothing left un-terminal (must be 0) ==='
SELECT count(*) AS not_terminal
FROM transactions
WHERE status NOT IN ('PROCESSED', 'ERROR');

\echo ''
\echo '=== C2 + C7: duplicate processed results (must be 0) ==='
SELECT count(*) AS duplicate_transaction_ids
FROM (
    SELECT transaction_id
    FROM processed_transactions
    GROUP BY transaction_id
    HAVING count(*) > 1
) d;

\echo ''
\echo '=== C6: PROCESSED transactions missing a result row (must be 0) ==='
SELECT count(*) AS processed_without_result
FROM transactions t
LEFT JOIN processed_transactions p ON p.transaction_id = t.id
WHERE t.status = 'PROCESSED' AND p.id IS NULL;

\echo ''
\echo '=== C6: result rows whose transaction is not PROCESSED (must be 0) ==='
SELECT count(*) AS result_without_processed_status
FROM processed_transactions p
JOIN transactions t ON t.id = p.transaction_id
WHERE t.status <> 'PROCESSED';

\echo ''
\echo '=== C5: PROCESSING rows older than the processing timeout (must be 0 once idle) ==='
SELECT count(*) AS stuck_processing
FROM transactions
WHERE status = 'PROCESSING'
  AND processing_started_at < now() - INTERVAL '2 minutes';

\echo ''
\echo '=== C8: retry budget was respected (max retry_count must be <= max-retries + 1) ==='
SELECT max(retry_count) AS max_retry_count,
       max(recovery_count) AS max_recovery_count
FROM transactions;

\echo ''
\echo '=== Work distribution across instances (C3 / C4: all instances contributed) ==='
SELECT processed_by, count(*) AS processed
FROM processed_transactions
GROUP BY processed_by
ORDER BY processed DESC;

\echo ''
\echo '=== Business rules: operation type split and commission totals ==='
SELECT operation_type,
       count(*) AS transactions,
       round(100.0 * count(*) / NULLIF(sum(count(*)) OVER (), 0), 2) AS pct,
       sum(commission) AS total_commission
FROM processed_transactions
GROUP BY operation_type
ORDER BY operation_type;

\echo ''
\echo '=== Error breakdown ==='
SELECT split_part(error_message, ':', 1) AS error_code, count(*) AS rows
FROM transactions
WHERE status = 'ERROR'
GROUP BY 1
ORDER BY rows DESC;

\echo ''
\echo '=== Aggregation consistency: account_statistics must equal the results table ==='
SELECT (SELECT coalesce(sum(transactions_count), 0) FROM account_statistics) AS statistics_count,
       (SELECT count(*) FROM processed_transactions WHERE account IS NOT NULL) AS result_count,
       (SELECT coalesce(sum(total_amount), 0) FROM account_statistics) AS statistics_amount,
       (SELECT coalesce(sum(amount), 0) FROM processed_transactions WHERE account IS NOT NULL) AS result_amount,
       (SELECT coalesce(sum(total_commission), 0) FROM account_statistics) AS statistics_commission,
       (SELECT coalesce(sum(commission), 0) FROM processed_transactions WHERE account IS NOT NULL) AS result_commission;

\echo ''
\echo '=== Outbox: one event per result, publication progress ==='
SELECT status, count(*) AS events FROM outbox_events GROUP BY status ORDER BY status;

\echo ''
\echo '=== Throughput window (first to last result) ==='
SELECT count(*) AS processed,
       min(created_at) AS first_result,
       max(created_at) AS last_result,
       EXTRACT(EPOCH FROM (max(created_at) - min(created_at)))::numeric(12,2) AS seconds,
       round(count(*) / GREATEST(EXTRACT(EPOCH FROM (max(created_at) - min(created_at))), 0.001)::numeric, 1) AS tps
FROM processed_transactions;
