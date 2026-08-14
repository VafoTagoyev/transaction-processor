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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Backpressure (assignment section 11, criterion C9).
 *
 * <p>The scenario is the one from the assignment: far more work in the database than the write
 * side can absorb. The poller is started with the workers deliberately stopped, which is the
 * most extreme possible version of "consumers cannot keep up".
 *
 * <p>The invariant being proven is stronger than "memory stays bounded": the number of rows this
 * instance has claimed never exceeds the queue capacity, so the backlog stays in PostgreSQL and
 * remains visible to the other instances instead of being locked away in a container that has no
 * capacity to process it.
 */
class BackpressureIT extends IntegrationTestBase {

    private static final int QUEUE_CAPACITY = 25;
    private static final int TRANSACTIONS = 3000;

    @Test
    @DisplayName("With workers stopped, the poller claims at most queue-capacity rows and then idles")
    void pollerNeverClaimsMoreThanTheQueueCanHold() {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        fixtures.insertNewTransactions(TRANSACTIONS, "100001", "50001", new BigDecimal("100.00"));

        ProcessorProperties properties = ProcessorTestHarness.properties("backpressure-1");
        properties.setQueueCapacity(QUEUE_CAPACITY);
        properties.setBatchSize(1000);
        properties.setWorkers(4);

        try (ProcessorTestHarness harness = new ProcessorTestHarness(
                jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties)) {

            harness.poller.start();

            Awaitility.await().atMost(Duration.ofSeconds(30))
                    .until(() -> harness.queue.size() == QUEUE_CAPACITY);

            // Give the poller several more cycles to misbehave, if it were going to.
            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> harness.metrics.backpressureCount() > 3);

            assertThat(harness.queue.size())
                    .as("the queue is bounded by construction")
                    .isEqualTo(QUEUE_CAPACITY);
            assertThat(fixtures.countByStatus("PROCESSING"))
                    .as("claims never exceed local capacity, so the backlog stays claimable by others")
                    .isEqualTo(QUEUE_CAPACITY);
            assertThat(fixtures.countByStatus("NEW"))
                    .isEqualTo(TRANSACTIONS - QUEUE_CAPACITY);
            assertThat(harness.metrics.backpressureCount())
                    .as("backpressure is observable in the metrics, not silent")
                    .isGreaterThan(0.0);

            // Now let the consumers run: the whole backlog drains, nothing was lost or duplicated.
            harness.workerPool.start();
            Awaitility.await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofMillis(200))
                    .until(() -> fixtures.countByStatus("PROCESSED") == TRANSACTIONS);
        }

        assertThat(fixtures.countProcessedResults()).isEqualTo(TRANSACTIONS);
        assertThat(fixtures.countDuplicateResults()).isZero();
        assertThat(fixtures.countByStatus("NEW")).isZero();
        assertThat(fixtures.countByStatus("PROCESSING")).isZero();
    }

    @Test
    @DisplayName("A slow consumer is never starved by a fast producer: the queue drains completely")
    void queueDrainsWithSlowConsumers() {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        fixtures.insertNewTransactions(800, "100001", "50001", new BigDecimal("100.00"));

        ProcessorProperties properties = ProcessorTestHarness.properties("backpressure-2");
        properties.setQueueCapacity(16);
        properties.setBatchSize(500);
        properties.setWorkers(1);

        try (ProcessorTestHarness harness = new ProcessorTestHarness(
                jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties)) {
            harness.start();
            Awaitility.await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        assertThat(harness.queue.size()).isLessThanOrEqualTo(16);
                        assertThat(fixtures.countByStatus("PROCESSED")).isEqualTo(800);
                    });
        }

        assertThat(fixtures.countDuplicateResults()).isZero();
    }
}
