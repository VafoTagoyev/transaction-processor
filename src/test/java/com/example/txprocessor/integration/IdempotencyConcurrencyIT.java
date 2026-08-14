package com.example.txprocessor.integration;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.ProcessingResult;
import com.example.txprocessor.error.OwnershipLostException;
import com.example.txprocessor.support.IntegrationTestBase;
import com.example.txprocessor.support.ProcessorTestHarness;
import com.example.txprocessor.support.TestFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Idempotency, attacked from three directions (assignment stage 10, criterion C7).
 */
class IdempotencyConcurrencyIT extends IntegrationTestBase {

    private static final int THREADS = 16;

    @Test
    @DisplayName("16 threads racing to persist the same transaction produce exactly one result")
    void concurrentPersistsProduceOneResult() throws Exception {
        TestFixtures fixtures = fixtures();
        fixtures.seedExternalReferenceData();
        long transactionId = fixtures.insertNewTransaction("100002", "50002", new BigDecimal("1000.00"));

        try (ProcessorTestHarness harness = harness("instance-a")) {
            ClaimedTransaction claim = harness.claimRepository.claimBatch("instance-a", 1).get(0);
            ProcessingResult result = harness.classifier.classify(harness.enrichmentService.enrich(claim));

            AtomicInteger created = new AtomicInteger();
            AtomicInteger lostOwnership = new AtomicInteger();
            CountDownLatch gate = new CountDownLatch(1);

            try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
                for (int i = 0; i < THREADS; i++) {
                    pool.submit(() -> {
                        gate.await();
                        try {
                            if (harness.persistenceService.persist(claim, result)) {
                                created.incrementAndGet();
                            }
                        } catch (OwnershipLostException e) {
                            lostOwnership.incrementAndGet();
                        }
                        return null;
                    });
                }
                gate.countDown();
                pool.shutdown();
                assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
            }

            assertThat(created.get()).as("exactly one thread produced the result").isEqualTo(1);
            assertThat(lostOwnership.get()).as("every other thread was fenced out").isEqualTo(THREADS - 1);
        }

        assertThat(fixtures.countProcessedResults()).isEqualTo(1);
        assertThat(fixtures.countDuplicateResults()).isZero();
        assertThat(fixtures.countOutboxEvents()).isEqualTo(1);
        assertAccountCount(1L);
        assertThat(fixtures.statusOf(transactionId)).isEqualTo("PROCESSED");
    }

    @Test
    @DisplayName("Many instances racing to claim and process the same single transaction: one result")
    void concurrentInstancesProcessingOneTransaction() throws Exception {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        fixtures.insertNewTransaction("100001", "50001", new BigDecimal("777.00"));

        CountDownLatch gate = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
            for (int i = 0; i < THREADS; i++) {
                String instanceId = "instance-" + i;
                pool.submit(() -> {
                    try (ProcessorTestHarness harness = harness(instanceId)) {
                        gate.await();
                        List<ClaimedTransaction> claimed = harness.claimRepository.claimBatch(instanceId, 10);
                        claimed.forEach(harness.processingService::process);
                    }
                    return null;
                });
            }
            gate.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(fixtures.countProcessedResults()).isEqualTo(1);
        assertThat(fixtures.countDuplicateResults()).isZero();
        assertAccountCount(1L);
    }

    @Test
    @DisplayName("Reprocessing after a crash-and-recover replay does not double count anything")
    void replayAfterRecoveryIsIdempotent() {
        TestFixtures fixtures = fixtures();
        fixtures.seedExternalReferenceData();
        long transactionId = fixtures.insertNewTransaction("100002", "50002", new BigDecimal("3000.00"));

        try (ProcessorTestHarness harness = harness("instance-a")) {
            ClaimedTransaction first = harness.claimRepository.claimBatch("instance-a", 1).get(0);
            ProcessingResult result = harness.classifier.classify(harness.enrichmentService.enrich(first));
            assertThat(harness.persistenceService.persist(first, result)).isTrue();

            // Simulate the state a crash-after-commit can leave if an operator forces the row back
            // into the pool: the result exists, but the transaction is claimable again.
            jdbcTemplate.update("""
                    UPDATE transactions
                    SET status = 'NEW', processing_token = NULL, next_attempt_at = now()
                    WHERE id = ?
                    """, transactionId);

            ClaimedTransaction second = harness.claimRepository.claimBatch("instance-a", 1).get(0);
            ProcessingResult recomputed = harness.classifier.classify(harness.enrichmentService.enrich(second));

            // The recomputation is allowed - the assignment explicitly permits repeated work.
            // What is not allowed is a second result, a second statistics increment or a second event.
            assertThat(harness.persistenceService.persist(second, recomputed)).isFalse();
        }

        assertThat(fixtures.countProcessedResults()).isEqualTo(1);
        assertThat(fixtures.countOutboxEvents()).isEqualTo(1);
        assertAccountCount(1L);
        assertThat(fixtures.statusOf(transactionId)).isEqualTo("PROCESSED");
    }

    private void assertAccountCount(long expected) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT transactions_count FROM account_statistics WHERE account = ?",
                Long.class, TestFixtures.DEFAULT_ACCOUNT);
        assertThat(count).isEqualTo(expected);
    }

    private ProcessorTestHarness harness(String instanceId) {
        ProcessorProperties properties = ProcessorTestHarness.properties(instanceId);
        return new ProcessorTestHarness(jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties);
    }
}
