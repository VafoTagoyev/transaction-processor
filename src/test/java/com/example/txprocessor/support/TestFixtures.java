package com.example.txprocessor.support;

import com.example.txprocessor.domain.CardInfo;
import com.example.txprocessor.domain.TerminalInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/** Seeds reference data and transactions for integration tests. */
public class TestFixtures {

    public static final String BANK_A = "00444";
    public static final String BANK_B = "00445";
    public static final String DEFAULT_ACCOUNT = "20208000123456789001";

    private final JdbcTemplate jdbcTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public TestFixtures(JdbcTemplate jdbcTemplate, StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    public void putCard(String cardId, String account, String bankCode) {
        put("card:" + cardId, new CardInfo("client-" + cardId, account, "VISA_GOLD", bankCode));
    }

    public void putTerminal(String terminalId, String bankCode) {
        put("terminal:" + terminalId, new TerminalInfo("M-" + terminalId, "001", "POS", bankCode));
    }

    /** One card and one terminal in the same bank: every transaction is INTERNAL and free. */
    public void seedInternalReferenceData() {
        putCard("100001", DEFAULT_ACCOUNT, BANK_A);
        putTerminal("50001", BANK_A);
    }

    /** Card and terminal in different banks: every transaction is EXTERNAL and chargeable. */
    public void seedExternalReferenceData() {
        putCard("100002", DEFAULT_ACCOUNT, BANK_A);
        putTerminal("50002", BANK_B);
    }

    /** @return the ids of the rows inserted by this call, in ascending order. */
    public List<Long> insertNewTransactions(int count, String cardId, String terminalId, BigDecimal amount) {
        String prefix = "TX-" + System.nanoTime() + "-";
        List<Object[]> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            batch.add(new Object[]{prefix + i, cardId, terminalId, amount});
        }
        jdbcTemplate.batchUpdate("""
                INSERT INTO transactions (external_id, card_id, terminal_id, amount, currency,
                                          transaction_type, status, retry_count)
                VALUES (?, ?, ?, ?, 'UZS', 'PURCHASE', 'NEW', 0)
                """, batch);
        return jdbcTemplate.queryForList(
                "SELECT id FROM transactions WHERE external_id LIKE ? ORDER BY id", Long.class, prefix + "%");
    }

    public long insertNewTransaction(String cardId, String terminalId, BigDecimal amount) {
        Long id = jdbcTemplate.queryForObject("""
                INSERT INTO transactions (external_id, card_id, terminal_id, amount, currency,
                                          transaction_type, status, retry_count)
                VALUES (?, ?, ?, ?, 'UZS', 'PURCHASE', 'NEW', 0)
                RETURNING id
                """, Long.class, "TX-" + System.nanoTime(), cardId, terminalId, amount);
        if (id == null) {
            throw new IllegalStateException("Insert did not return an id");
        }
        return id;
    }

    /** Ages a lease so the recovery sweep considers it expired, without waiting in real time. */
    public void backdateProcessingStart(long transactionId, long seconds) {
        jdbcTemplate.update("""
                UPDATE transactions
                SET processing_started_at = now() - (?::double precision * INTERVAL '1 second')
                WHERE id = ?
                """, (double) seconds, transactionId);
    }

    /** Ages only the leases held by one instance - used to age a *crashed* instance's rows. */
    public int backdateProcessingStartsForInstance(String instanceId, long seconds) {
        return jdbcTemplate.update("""
                UPDATE transactions
                SET processing_started_at = now() - (?::double precision * INTERVAL '1 second')
                WHERE status = 'PROCESSING' AND processing_instance = ?
                """, (double) seconds, instanceId);
    }

    /** Ages a specific set of leases. */
    public int backdateProcessingStarts(List<Long> ids, long seconds) {
        if (ids.isEmpty()) {
            return 0;
        }
        String array = ids.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(",", "{", "}"));
        return jdbcTemplate.update("""
                UPDATE transactions
                SET processing_started_at = now() - (?::double precision * INTERVAL '1 second')
                WHERE status = 'PROCESSING' AND id = ANY (?::bigint[])
                """, (double) seconds, array);
    }

    public void backdateAllProcessingStarts(long seconds) {
        jdbcTemplate.update("""
                UPDATE transactions
                SET processing_started_at = now() - (?::double precision * INTERVAL '1 second')
                WHERE status = 'PROCESSING'
                """, (double) seconds);
    }

    public String statusOf(long transactionId) {
        return jdbcTemplate.queryForObject("SELECT status FROM transactions WHERE id = ?", String.class, transactionId);
    }

    public Integer retryCountOf(long transactionId) {
        return jdbcTemplate.queryForObject("SELECT retry_count FROM transactions WHERE id = ?",
                Integer.class, transactionId);
    }

    public long countByStatus(String status) {
        Long count = jdbcTemplate.queryForObject("SELECT count(*) FROM transactions WHERE status = ?",
                Long.class, status);
        return count == null ? 0 : count;
    }

    public long countProcessedResults() {
        Long count = jdbcTemplate.queryForObject("SELECT count(*) FROM processed_transactions", Long.class);
        return count == null ? 0 : count;
    }

    public long countDuplicateResults() {
        Long count = jdbcTemplate.queryForObject("""
                SELECT count(*) FROM (
                    SELECT transaction_id FROM processed_transactions
                    GROUP BY transaction_id HAVING count(*) > 1
                ) d
                """, Long.class);
        return count == null ? 0 : count;
    }

    public long countOutboxEvents() {
        Long count = jdbcTemplate.queryForObject("SELECT count(*) FROM outbox_events", Long.class);
        return count == null ? 0 : count;
    }

    private void put(String key, Object value) {
        try {
            redisTemplate.opsForValue().set(key, objectMapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
