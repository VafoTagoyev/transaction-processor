package com.example.txprocessor.integration;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.EnrichedTransaction;
import com.example.txprocessor.domain.ProcessingResult;
import com.example.txprocessor.error.OwnershipLostException;
import com.example.txprocessor.support.IntegrationTestBase;
import com.example.txprocessor.support.ProcessorTestHarness;
import com.example.txprocessor.support.TestFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The single most important test in this repository.
 *
 * <p>It reproduces the exact race the assignment calls out in requirement 47 and defence
 * question 74: a slow but perfectly healthy worker is declared stale, its transaction is handed
 * to somebody else, and then the original worker wakes up and tries to commit. Without fencing,
 * that is a duplicated statistics increment, a duplicated outbox event and a corrupted status.
 * With fencing, the late writer's UPDATE matches zero rows and its entire transaction rolls back.
 */
class FencingTokenIT extends IntegrationTestBase {

    @Test
    @DisplayName("A worker whose lease was reassigned cannot commit: no second result, no double counting")
    void staleWorkerCannotCommitAfterReclaim() {
        TestFixtures fixtures = fixtures();
        fixtures.seedExternalReferenceData();
        long transactionId = fixtures.insertNewTransaction("100002", "50002", new BigDecimal("2000.00"));

        try (ProcessorTestHarness slowWorker = harness("instance-slow");
             ProcessorTestHarness newOwner = harness("instance-new")) {

            // 1. The slow worker claims the transaction and computes its result, but has not
            //    reached the database yet - it is stuck somewhere between enrichment and persist.
            ClaimedTransaction slowClaim = slowWorker.claimRepository.claimBatch("instance-slow", 1).get(0);
            EnrichedTransaction enriched = slowWorker.enrichmentService.enrich(slowClaim);
            ProcessingResult slowResult = slowWorker.classifier.classify(enriched);

            // 2. Its lease expires (here: forced, in production: the process stopped renewing).
            fixtures.backdateProcessingStart(transactionId, 3600);
            assertThat(newOwner.recoveryService.recoverOnce()).hasSize(1);
            assertThat(fixtures.statusOf(transactionId)).isEqualTo("NEW");

            // 3. Somebody else picks it up and finishes it properly.
            ClaimedTransaction newClaim = newOwner.claimRepository.claimBatch("instance-new", 1).get(0);
            assertThat(newClaim.processingToken()).isNotEqualTo(slowClaim.processingToken());
            ProcessingResult newResult = newOwner.classifier.classify(newOwner.enrichmentService.enrich(newClaim));
            assertThat(newOwner.persistenceService.persist(newClaim, newResult)).isTrue();

            // 4. The slow worker finally wakes up and tries to commit its stale result.
            assertThatThrownBy(() -> slowWorker.persistenceService.persist(slowClaim, slowResult))
                    .isInstanceOf(OwnershipLostException.class);
        }

        // 5. Nothing the stale worker did survived, and nothing was counted twice.
        assertThat(fixtures.statusOf(transactionId)).isEqualTo("PROCESSED");
        assertThat(fixtures.countProcessedResults()).isEqualTo(1);
        assertThat(fixtures.countDuplicateResults()).isZero();
        assertThat(fixtures.countOutboxEvents()).isEqualTo(1);
        assertStatistics(1L, "2000.00", "20.00");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT processed_by FROM processed_transactions WHERE transaction_id = ?",
                String.class, transactionId)).isEqualTo("instance-new");
    }

    @Test
    @DisplayName("The whole persistence transaction rolls back, not just the status update")
    void losingOwnershipRollsBackEverySideEffect() {
        TestFixtures fixtures = fixtures();
        fixtures.seedExternalReferenceData();
        long transactionId = fixtures.insertNewTransaction("100002", "50002", new BigDecimal("2000.00"));

        try (ProcessorTestHarness worker = harness("instance-slow");
             ProcessorTestHarness other = harness("instance-other")) {

            ClaimedTransaction claim = worker.claimRepository.claimBatch("instance-slow", 1).get(0);
            ProcessingResult result = worker.classifier.classify(worker.enrichmentService.enrich(claim));

            fixtures.backdateProcessingStart(transactionId, 3600);
            other.recoveryService.recoverOnce();

            assertThatThrownBy(() -> worker.persistenceService.persist(claim, result))
                    .isInstanceOf(OwnershipLostException.class);
        }

        // The transaction is back in the pool, untouched by the loser.
        assertThat(fixtures.statusOf(transactionId)).isEqualTo("NEW");
        assertThat(fixtures.countProcessedResults()).isZero();
        assertThat(fixtures.countOutboxEvents()).isZero();
        Long statisticsRows = jdbcTemplate.queryForObject("SELECT count(*) FROM account_statistics", Long.class);
        assertThat(statisticsRows).isZero();
    }

    @Test
    @DisplayName("A failure handler that arrives late cannot overwrite the new owner's work")
    void staleFailureHandlingIsAlsoFenced() {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        long transactionId = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("100.00"));

        try (ProcessorTestHarness slow = harness("instance-slow");
             ProcessorTestHarness owner = harness("instance-new")) {

            ClaimedTransaction slowClaim = slow.claimRepository.claimBatch("instance-slow", 1).get(0);

            fixtures.backdateProcessingStart(transactionId, 3600);
            owner.recoveryService.recoverOnce();
            ClaimedTransaction newClaim = owner.claimRepository.claimBatch("instance-new", 1).get(0);
            owner.persistenceService.persist(newClaim,
                    owner.classifier.classify(owner.enrichmentService.enrich(newClaim)));

            // The stale worker tries to record a failure it observed a long time ago.
            boolean errorApplied = slow.claimRepository.markError(
                    transactionId, slowClaim.processingToken(), "stale failure");
            boolean retryApplied = slow.claimRepository.scheduleRetry(
                    transactionId, slowClaim.processingToken(), 1.0, "stale failure");

            assertThat(errorApplied).isFalse();
            assertThat(retryApplied).isFalse();
        }

        assertThat(fixtures.statusOf(transactionId)).isEqualTo("PROCESSED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT error_message FROM transactions WHERE id = ?", String.class, transactionId)).isNull();
    }

    private void assertStatistics(long expectedCount, String expectedAmount, String expectedCommission) {
        List<java.util.Map<String, Object>> rows =
                jdbcTemplate.queryForList("SELECT * FROM account_statistics");
        assertThat(rows).hasSize(1);
        assertThat(((Number) rows.get(0).get("transactions_count")).longValue()).isEqualTo(expectedCount);
        assertThat((BigDecimal) rows.get(0).get("total_amount")).isEqualByComparingTo(expectedAmount);
        assertThat((BigDecimal) rows.get(0).get("total_commission")).isEqualByComparingTo(expectedCommission);
    }

    private ProcessorTestHarness harness(String instanceId) {
        ProcessorProperties properties = ProcessorTestHarness.properties(instanceId);
        return new ProcessorTestHarness(jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties);
    }
}
