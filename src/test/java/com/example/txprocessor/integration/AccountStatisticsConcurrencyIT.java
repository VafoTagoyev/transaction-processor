package com.example.txprocessor.integration;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.support.IntegrationTestBase;
import com.example.txprocessor.support.ProcessorTestHarness;
import com.example.txprocessor.support.TestFixtures;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lost-update protection for the optional aggregation task (assignment section 9, question 79).
 *
 * <p>Every transaction in these tests belongs to the <em>same</em> account, so every worker on
 * every instance contends for the same {@code account_statistics} row on every single write.
 * If the aggregation were a SELECT into Java followed by an UPDATE, this test would fail by a
 * wide margin; with the in-database UPSERT the totals are exact.
 */
class AccountStatisticsConcurrencyIT extends IntegrationTestBase {

    private static final int TRANSACTIONS = 1500;
    private static final BigDecimal AMOUNT = new BigDecimal("1000.00");
    private static final BigDecimal EXPECTED_COMMISSION_PER_TX = new BigDecimal("10.00"); // EXTERNAL, 1%

    @Test
    @DisplayName("1500 transactions on one account across 3 instances: counts and totals are exact")
    void concurrentAggregationIsExact() {
        TestFixtures fixtures = fixtures();
        fixtures.seedExternalReferenceData();
        fixtures.insertNewTransactions(TRANSACTIONS, "100002", "50002", AMOUNT);

        List<ProcessorTestHarness> instances = List.of(
                harness("stats-1"), harness("stats-2"), harness("stats-3"));
        instances.forEach(ProcessorTestHarness::start);

        try {
            Awaitility.await()
                    .atMost(Duration.ofMinutes(2))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertThat(fixtures.countProcessedResults()).isEqualTo(TRANSACTIONS));
        } finally {
            instances.forEach(ProcessorTestHarness::close);
        }

        Map<String, Object> statistics = jdbcTemplate.queryForMap(
                "SELECT * FROM account_statistics WHERE account = ?", TestFixtures.DEFAULT_ACCOUNT);

        assertThat(((Number) statistics.get("transactions_count")).longValue())
                .as("no increment was lost")
                .isEqualTo(TRANSACTIONS);
        assertThat((BigDecimal) statistics.get("total_amount"))
                .isEqualByComparingTo(AMOUNT.multiply(BigDecimal.valueOf(TRANSACTIONS)));
        assertThat((BigDecimal) statistics.get("total_commission"))
                .isEqualByComparingTo(EXPECTED_COMMISSION_PER_TX.multiply(BigDecimal.valueOf(TRANSACTIONS)));

        assertThat(fixtures.countDuplicateResults()).isZero();
        assertThat(fixtures.countByStatus("PROCESSED")).isEqualTo(TRANSACTIONS);
    }

    @Test
    @DisplayName("Direct hammering of the UPSERT from 32 threads loses nothing")
    void upsertIsAtomicUnderDirectContention() throws Exception {
        int threads = 32;
        int perThread = 100;
        CountDownLatch gate = new CountDownLatch(1);

        try (ProcessorTestHarness harness = harness("stats-direct");
             ExecutorService pool = Executors.newFixedThreadPool(threads)) {

            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    gate.await();
                    for (int j = 0; j < perThread; j++) {
                        harness.statisticsRepository.addTransaction(
                                TestFixtures.DEFAULT_ACCOUNT, new BigDecimal("2.50"), new BigDecimal("0.25"));
                    }
                    return null;
                });
            }
            gate.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }

        Map<String, Object> statistics = jdbcTemplate.queryForMap(
                "SELECT * FROM account_statistics WHERE account = ?", TestFixtures.DEFAULT_ACCOUNT);

        assertThat(((Number) statistics.get("transactions_count")).longValue()).isEqualTo(threads * perThread);
        assertThat((BigDecimal) statistics.get("total_amount")).isEqualByComparingTo("8000.00");
        assertThat((BigDecimal) statistics.get("total_commission")).isEqualByComparingTo("800.00");
    }

    private ProcessorTestHarness harness(String instanceId) {
        ProcessorProperties properties = ProcessorTestHarness.properties(instanceId);
        return new ProcessorTestHarness(jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties);
    }
}
