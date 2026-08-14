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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Three instances, one dataset (assignment section 8, criteria C1, C2, C6, C7).
 *
 * <p>End to end through the real pipeline: poller, bounded queue, worker pool, Valkey enrichment,
 * business rules, fenced persistence, statistics and outbox.
 */
class MultiInstanceProcessingIT extends IntegrationTestBase {

    private static final int INTERNAL_TRANSACTIONS = 1200;
    private static final int EXTERNAL_TRANSACTIONS = 300;
    private static final int MISSING_CARD_TRANSACTIONS = 40;
    private static final int MISSING_TERMINAL_TRANSACTIONS = 40;
    private static final int TOTAL =
            INTERNAL_TRANSACTIONS + EXTERNAL_TRANSACTIONS + MISSING_CARD_TRANSACTIONS + MISSING_TERMINAL_TRANSACTIONS;
    private static final int EXPECTED_SUCCESSFUL = INTERNAL_TRANSACTIONS + EXTERNAL_TRANSACTIONS;
    private static final int EXPECTED_FAILED = MISSING_CARD_TRANSACTIONS + MISSING_TERMINAL_TRANSACTIONS;

    @Test
    @DisplayName("Three instances drain a mixed dataset: nothing lost, nothing duplicated, errors terminal")
    void threeInstancesProcessEverythingExactlyOnce() {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        fixtures.seedExternalReferenceData();

        fixtures.insertNewTransactions(INTERNAL_TRANSACTIONS, "100001", "50001", new BigDecimal("500.00"));
        fixtures.insertNewTransactions(EXTERNAL_TRANSACTIONS, "100002", "50002", new BigDecimal("1000000.00"));
        fixtures.insertNewTransactions(MISSING_CARD_TRANSACTIONS, "999999999", "50001", new BigDecimal("10.00"));
        fixtures.insertNewTransactions(MISSING_TERMINAL_TRANSACTIONS, "100001", "888888888", new BigDecimal("10.00"));

        List<ProcessorTestHarness> instances = List.of(
                harness("processor-1"), harness("processor-2"), harness("processor-3"));
        instances.forEach(ProcessorTestHarness::start);

        try {
            Awaitility.await()
                    .atMost(Duration.ofMinutes(3))
                    .pollInterval(Duration.ofMillis(250))
                    .untilAsserted(() -> {
                        assertThat(fixtures.countByStatus("NEW")).isZero();
                        assertThat(fixtures.countByStatus("PROCESSING")).isZero();
                    });
        } finally {
            instances.forEach(ProcessorTestHarness::close);
        }

        // C1: everything reached a terminal status with no manual intervention.
        assertThat(fixtures.countByStatus("PROCESSED") + fixtures.countByStatus("ERROR")).isEqualTo(TOTAL);
        assertThat(fixtures.countByStatus("PROCESSED")).isEqualTo(EXPECTED_SUCCESSFUL);
        assertThat(fixtures.countByStatus("ERROR")).isEqualTo(EXPECTED_FAILED);

        // C2 + C7: one result per transaction, no duplicates.
        assertThat(fixtures.countProcessedResults()).isEqualTo(EXPECTED_SUCCESSFUL);
        assertThat(fixtures.countDuplicateResults()).isZero();

        // The outbox is exactly as large as the set of results.
        assertThat(fixtures.countOutboxEvents()).isEqualTo(EXPECTED_SUCCESSFUL);

        // Business rules held across the whole dataset.
        assertThat(operationTypeCount("INTERNAL")).isEqualTo(INTERNAL_TRANSACTIONS);
        assertThat(operationTypeCount("EXTERNAL")).isEqualTo(EXTERNAL_TRANSACTIONS);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT sum(commission) FROM processed_transactions WHERE operation_type = 'INTERNAL'",
                BigDecimal.class)).isEqualByComparingTo("0.00");
        // 1 000 000 exactly -> the reduced 0.5% rate -> 5000 per transaction.
        assertThat(jdbcTemplate.queryForObject(
                "SELECT sum(commission) FROM processed_transactions WHERE operation_type = 'EXTERNAL'",
                BigDecimal.class))
                .isEqualByComparingTo(new BigDecimal("5000.00").multiply(BigDecimal.valueOf(EXTERNAL_TRANSACTIONS)));

        // Every instance actually did work: the load really was shared.
        Map<String, Object> perInstance = jdbcTemplate.queryForList(
                        "SELECT processed_by, count(*) AS c FROM processed_transactions GROUP BY processed_by")
                .stream()
                .collect(java.util.stream.Collectors.toMap(
                        row -> (String) row.get("processed_by"), row -> row.get("c")));
        assertThat(perInstance.keySet()).containsExactlyInAnyOrder("processor-1", "processor-2", "processor-3");

        // Aggregation is consistent with the results table.
        assertThat(jdbcTemplate.queryForObject(
                "SELECT sum(transactions_count) FROM account_statistics", Long.class))
                .isEqualTo((long) EXPECTED_SUCCESSFUL);
    }

    private long operationTypeCount(String operationType) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_transactions WHERE operation_type = ?", Long.class, operationType);
        return count == null ? 0 : count;
    }

    private ProcessorTestHarness harness(String instanceId) {
        ProcessorProperties properties = ProcessorTestHarness.properties(instanceId);
        return new ProcessorTestHarness(jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties);
    }
}
