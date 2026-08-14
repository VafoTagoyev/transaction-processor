package com.example.txprocessor.integration;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.error.CacheUnavailableException;
import com.example.txprocessor.error.CardNotFoundException;
import com.example.txprocessor.support.IntegrationTestBase;
import com.example.txprocessor.support.ProcessorTestHarness;
import com.example.txprocessor.support.TestFixtures;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Retry and error policy end to end (assignment stage 11, criterion C8). */
class ErrorFlowIT extends IntegrationTestBase {

    @Test
    @DisplayName("A transient failure returns the transaction to NEW with a future next_attempt_at")
    void transientFailureSchedulesABackedOffRetry() {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("100.00"));

        try (ProcessorTestHarness harness = harness("errors-1")) {
            ClaimedTransaction claim = harness.claimRepository.claimBatch("errors-1", 1).get(0);
            harness.failureHandler.handle(claim, new CacheUnavailableException("valkey timeout", null));

            assertThat(fixtures.statusOf(id)).isEqualTo("NEW");
            assertThat(fixtures.retryCountOf(id)).isEqualTo(1);
            assertThat(jdbcTemplate.queryForObject(
                    "SELECT error_message FROM transactions WHERE id = ?", String.class, id))
                    .startsWith("CACHE_UNAVAILABLE");
            assertThat(jdbcTemplate.queryForObject(
                    "SELECT processing_token FROM transactions WHERE id = ?", String.class, id)).isNull();
            assertThat(jdbcTemplate.queryForObject(
                    "SELECT next_attempt_at >= created_at FROM transactions WHERE id = ?",
                    Boolean.class, id))
                    .as("the retry is scheduled, not immediate")
                    .isTrue();
        }
    }

    @Test
    @DisplayName("Retries are bounded: after max-retries the transaction becomes ERROR and stays there")
    void retriesAreBounded() {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("100.00"));

        try (ProcessorTestHarness harness = harness("errors-2")) {
            for (int attempt = 0; attempt <= harness.properties.getMaxRetries(); attempt++) {
                jdbcTemplate.update("UPDATE transactions SET next_attempt_at = now() WHERE id = ?", id);
                ClaimedTransaction claim = harness.claimRepository.claimBatch("errors-2", 1).get(0);
                harness.failureHandler.handle(claim, new CacheUnavailableException("still down", null));
            }

            assertThat(fixtures.statusOf(id)).isEqualTo("ERROR");
            assertThat(fixtures.retryCountOf(id)).isEqualTo(harness.properties.getMaxRetries());

            // Terminal: no further claim can pick it up, so the loop cannot restart.
            jdbcTemplate.update("UPDATE transactions SET next_attempt_at = now() WHERE id = ?", id);
            assertThat(harness.claimRepository.claimBatch("errors-2", 10)).isEmpty();
        }
    }

    @Test
    @DisplayName("A permanent failure goes straight to ERROR without burning any retries")
    void permanentFailureSkipsRetries() {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("999999999", "50001", new BigDecimal("100.00"));

        try (ProcessorTestHarness harness = harness("errors-3")) {
            ClaimedTransaction claim = harness.claimRepository.claimBatch("errors-3", 1).get(0);
            harness.failureHandler.handle(claim, new CardNotFoundException("card:999999999"));
        }

        assertThat(fixtures.statusOf(id)).isEqualTo("ERROR");
        assertThat(fixtures.retryCountOf(id)).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT error_message FROM transactions WHERE id = ?", String.class, id))
                .startsWith("CARD_NOT_FOUND");
    }

    @Test
    @DisplayName("End to end: missing reference data lands in ERROR, everything else in PROCESSED")
    void missingReferenceDataEndsInError() {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        fixtures.insertNewTransactions(100, "100001", "50001", new BigDecimal("100.00"));
        fixtures.insertNewTransactions(10, "999999999", "50001", new BigDecimal("100.00"));
        fixtures.insertNewTransactions(10, "100001", "888888888", new BigDecimal("100.00"));

        try (ProcessorTestHarness harness = harness("errors-4")) {
            harness.start();
            Awaitility.await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofMillis(200))
                    .until(() -> fixtures.countByStatus("NEW") == 0 && fixtures.countByStatus("PROCESSING") == 0);
        }

        assertThat(fixtures.countByStatus("PROCESSED")).isEqualTo(100);
        assertThat(fixtures.countByStatus("ERROR")).isEqualTo(20);
        assertThat(fixtures.countProcessedResults()).isEqualTo(100);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM transactions WHERE status = 'ERROR' AND error_message LIKE 'CARD_NOT_FOUND%'",
                Long.class)).isEqualTo(10L);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM transactions WHERE status = 'ERROR' AND error_message LIKE 'TERMINAL_NOT_FOUND%'",
                Long.class)).isEqualTo(10L);
        // Failures are terminal, so they carry a completion timestamp like successes do.
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM transactions WHERE status = 'ERROR' AND processed_at IS NULL",
                Long.class)).isZero();
    }

    @Test
    @DisplayName("Backoff really delays the next attempt rather than spinning")
    void backoffDelaysTheNextClaim() {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("100.00"));

        ProcessorProperties properties = ProcessorTestHarness.properties("errors-5");
        properties.setRetryInitialDelay(Duration.ofSeconds(30));
        properties.setRetryMaxDelay(Duration.ofMinutes(10));

        try (ProcessorTestHarness harness = new ProcessorTestHarness(
                jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties)) {

            ClaimedTransaction claim = harness.claimRepository.claimBatch("errors-5", 1).get(0);
            harness.failureHandler.handle(claim, new CacheUnavailableException("down", null));

            Timestamp nextAttempt = jdbcTemplate.queryForObject(
                    "SELECT next_attempt_at FROM transactions WHERE id = ?", Timestamp.class, id);
            Timestamp now = jdbcTemplate.queryForObject("SELECT now()::timestamp", Timestamp.class);

            assertThat(nextAttempt).isNotNull();
            assertThat(now).isNotNull();
            assertThat(nextAttempt.getTime() - now.getTime()).isGreaterThan(25_000L);
            assertThat(harness.claimRepository.claimBatch("errors-5", 10))
                    .as("the transaction is invisible until the backoff has elapsed")
                    .isEmpty();
        }
    }

    private ProcessorTestHarness harness(String instanceId) {
        ProcessorProperties properties = ProcessorTestHarness.properties(instanceId);
        return new ProcessorTestHarness(jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties);
    }
}
