package com.example.txprocessor.repository;

import com.example.txprocessor.domain.ProcessingResult;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ProcessedTransactionRepository {

    /**
     * The final, unconditional idempotency barrier.
     *
     * <p>{@code ON CONFLICT (transaction_id) DO NOTHING} turns "this transaction was already
     * processed" from an exception into a return value: 1 = we wrote the result, 0 = somebody
     * else already had. Everything downstream of the insert (statistics, outbox) is executed
     * only when the return value is 1, which is what makes those side effects exactly-once
     * rather than at-least-once.
     *
     * <p>This works no matter how the duplicate arose: two racing workers, a re-claim after a
     * crash, a replayed batch, or an operator re-running the job. It does not depend on any
     * Java lock, any Redis lock, or on the fencing token being correct. It is enforced by
     * uk_processed_transaction and is therefore the property the whole design falls back on.
     */
    private static final String INSERT_IF_ABSENT = """
            INSERT INTO processed_transactions
                (transaction_id, external_id, client_id, account, product_id, merchant_id,
                 branch_code, amount, commission, operation_type, processed_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (transaction_id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    public ProcessedTransactionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** @return true if this call created the row, false if the result already existed. */
    public boolean insertIfAbsent(ProcessingResult result, String processedBy) {
        int inserted = jdbcTemplate.update(INSERT_IF_ABSENT,
                result.transactionId(),
                result.externalId(),
                result.clientId(),
                result.account(),
                result.productId(),
                result.merchantId(),
                result.branchCode(),
                result.amount(),
                result.commission(),
                result.operationType().name(),
                processedBy);
        return inserted == 1;
    }

    public long count() {
        Long count = jdbcTemplate.queryForObject("SELECT count(*) FROM processed_transactions", Long.class);
        return count == null ? 0L : count;
    }

    /** Acceptance check C7: must always be zero. Kept here so tests and scripts assert the same thing. */
    public long countDuplicateTransactionIds() {
        Long count = jdbcTemplate.queryForObject("""
                SELECT count(*) FROM (
                    SELECT transaction_id FROM processed_transactions
                    GROUP BY transaction_id HAVING count(*) > 1
                ) duplicates
                """, Long.class);
        return count == null ? 0L : count;
    }
}
