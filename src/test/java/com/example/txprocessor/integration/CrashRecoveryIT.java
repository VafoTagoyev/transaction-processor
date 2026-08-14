package com.example.txprocessor.integration;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.support.IntegrationTestBase;
import com.example.txprocessor.support.ProcessorTestHarness;
import com.example.txprocessor.support.TestFixtures;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The blocking requirement: crash recovery (assignment section 6, criteria C3-C6).
 *
 * <p>The scripted, containerised version of this (real {@code kill -9} on one of three
 * containers) lives in {@code scripts/crash-recovery-demo.sh}; this is the automated equivalent.
 */
class CrashRecoveryIT extends IntegrationTestBase {

    private static final int TRANSACTIONS = 2000;

    @Test
    @DisplayName("One instance is killed mid-batch: the survivors finish everything, no loss, no duplicates")
    void survivorsFinishTheWorkAfterOneInstanceDies() {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        fixtures.insertNewTransactions(TRANSACTIONS, "100001", "50001", new BigDecimal("250.00"));

        ProcessorTestHarness dying = harness("processor-doomed");
        ProcessorTestHarness survivorA = harness("processor-1");
        ProcessorTestHarness survivorB = harness("processor-2");

        dying.start();
        survivorA.start();
        survivorB.start();

        try {
            // Let real processing get under way before pulling the plug.
            Awaitility.await().atMost(Duration.ofSeconds(60))
                    .until(() -> fixtures.countProcessedResults() > 50);

            int abandoned = dying.simulateHardCrash();

            // The survivors keep going without any intervention (C3).
            long processedAtCrash = fixtures.countProcessedResults();
            Awaitility.await().atMost(Duration.ofSeconds(60))
                    .until(() -> fixtures.countProcessedResults() > processedAtCrash);

            // Whatever the dead instance was holding is still PROCESSING and owned by nobody.
            // Age ONLY the dead instance's leases - ageing every lease would also expire the
            // survivors' live work and would be testing the fence rather than recovery (C5).
            Awaitility.await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofMillis(250))
                    .untilAsserted(() -> {
                        fixtures.backdateProcessingStartsForInstance("processor-doomed", 600);
                        survivorA.recoveryService.recoverOnce();
                        assertThat(fixtures.countByStatus("NEW")).isZero();
                        assertThat(fixtures.countByStatus("PROCESSING")).isZero();
                    });

            assertThat(abandoned).isGreaterThanOrEqualTo(0);
        } finally {
            survivorA.close();
            survivorB.close();
            dying.close();
        }

        // C6 + C7: everything finished, exactly once.
        assertThat(fixtures.countByStatus("PROCESSED")).isEqualTo(TRANSACTIONS);
        assertThat(fixtures.countProcessedResults()).isEqualTo(TRANSACTIONS);
        assertThat(fixtures.countDuplicateResults()).isZero();
        assertThat(fixtures.countOutboxEvents()).isEqualTo(TRANSACTIONS);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT sum(transactions_count) FROM account_statistics", Long.class)).isEqualTo((long) TRANSACTIONS);
    }

    @Test
    @DisplayName("A restarted instance resumes automatically, with no offset and no manual input")
    void restartedInstanceResumesFromDatabaseStateAlone() {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        fixtures.insertNewTransactions(500, "100001", "50001", new BigDecimal("100.00"));

        // First life: claim a chunk, then die without finishing it.
        List<Long> orphaned;
        try (ProcessorTestHarness firstLife = harness("processor-restart")) {
            List<ClaimedTransaction> claimed = firstLife.claimRepository.claimBatch("processor-restart", 200);
            assertThat(claimed).hasSize(200);
            orphaned = claimed.stream().map(ClaimedTransaction::id).toList();
        }
        assertThat(fixtures.countByStatus("PROCESSING")).isEqualTo(200);

        // Second life: a brand new process with no memory of the first. It is given no offset,
        // no id list and no checkpoint - only a connection to the same database.
        try (ProcessorTestHarness secondLife = harness("processor-restart")) {
            secondLife.start();

            // The 300 rows that were never claimed are picked up with no help at all.
            Awaitility.await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofMillis(200))
                    .until(() -> fixtures.countByStatus("NEW") == 0);

            // The 200 orphaned leases come back only through the recovery sweep, once expired.
            assertThat(fixtures.backdateProcessingStarts(orphaned, 600)).isEqualTo(200);
            assertThat(secondLife.recoveryService.recoverOnce()).hasSize(200);

            Awaitility.await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofMillis(200))
                    .until(() -> fixtures.countByStatus("PROCESSED") == 500);
        }

        assertThat(fixtures.countProcessedResults()).isEqualTo(500);
        assertThat(fixtures.countDuplicateResults()).isZero();
        assertThat(fixtures.countByStatus("PROCESSING")).isZero();
        assertThat(fixtures.countByStatus("NEW")).isZero();
    }

    @Test
    @DisplayName("Restarting every instance changes nothing for transactions that were already PROCESSED")
    void restartingEverythingDoesNotReprocessCompletedWork() {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        fixtures.insertNewTransactions(300, "100001", "50001", new BigDecimal("100.00"));

        for (int generation = 0; generation < 3; generation++) {
            try (ProcessorTestHarness instance = harness("processor-gen-" + generation)) {
                instance.start();
                Awaitility.await().atMost(Duration.ofMinutes(1))
                        .until(() -> fixtures.countByStatus("NEW") == 0 && fixtures.countByStatus("PROCESSING") == 0);
            }
        }

        assertThat(fixtures.countProcessedResults()).isEqualTo(300);
        assertThat(fixtures.countDuplicateResults()).isZero();
        assertThat(fixtures.countOutboxEvents()).isEqualTo(300);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT sum(transactions_count) FROM account_statistics", Long.class)).isEqualTo(300L);
    }

    private ProcessorTestHarness harness(String instanceId) {
        ProcessorProperties properties = ProcessorTestHarness.properties(instanceId);
        return new ProcessorTestHarness(jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties);
    }
}
