package com.example.txprocessor.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;

@Repository
public class AccountStatisticsRepository {

    /**
     * Lost-update-proof aggregation.
     *
     * <p>The arithmetic happens <em>inside the database</em>, in a single statement, on a row
     * the statement itself locks. There is no SELECT into Java, no read-modify-write window and
     * therefore nothing to lose. Concurrent writers to the same account serialise on the row
     * lock for the duration of one addition and are otherwise unaffected.
     *
     * <p>Under READ COMMITTED, a concurrent UPDATE of the same row blocks and then re-reads the
     * committed row version before applying its own increment, so increments compose. A
     * SELECT ... FOR UPDATE followed by an UPDATE would also be correct but costs two round
     * trips and holds the lock longer; a plain SELECT then UPDATE would be wrong.
     *
     * <p>Deadlock freedom: one persistence transaction touches exactly one account row, and
     * always in the same order relative to the other tables (transactions, then
     * processed_transactions, then account_statistics, then outbox_events). With no transaction
     * ever holding two account rows, there is no cycle to form.
     */
    private static final String UPSERT = """
            INSERT INTO account_statistics
                (account, transactions_count, total_amount, total_commission, updated_at)
            VALUES (?, 1, ?, ?, now())
            ON CONFLICT (account) DO UPDATE
            SET transactions_count = account_statistics.transactions_count + 1,
                total_amount       = account_statistics.total_amount + EXCLUDED.total_amount,
                total_commission   = account_statistics.total_commission + EXCLUDED.total_commission,
                updated_at         = now()
            """;

    private final JdbcTemplate jdbcTemplate;

    public AccountStatisticsRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void addTransaction(String account, BigDecimal amount, BigDecimal commission) {
        jdbcTemplate.update(UPSERT, account, amount, commission);
    }
}
