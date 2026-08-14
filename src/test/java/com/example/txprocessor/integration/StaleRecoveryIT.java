package com.example.txprocessor.integration;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.TransactionStatus;
import com.example.txprocessor.repository.TransactionClaimRepository.RecoveredTransaction;
import com.example.txprocessor.support.IntegrationTestBase;
import com.example.txprocessor.support.ProcessorTestHarness;
import com.example.txprocessor.support.TestFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Stale-lease recovery and its guard rails (assignment stage 13, criteria C5 and C8). */
class StaleRecoveryIT extends IntegrationTestBase {

    @Test
    @DisplayName("Expired leases return to NEW; healthy ones are left alone")
    void onlyExpiredLeasesAreReclaimed() {
        TestFixtures fixtures = fixtures();
        List<Long> ids = fixtures.insertNewTransactions(10, "100001", "50001", new BigDecimal("10.00"));

        try (ProcessorTestHarness harness = harness("processor-1")) {
            harness.claimRepository.claimBatch("processor-1", 10);

            // Age half of them past the 30 second test lease.
            ids.subList(0, 5).forEach(id -> fixtures.backdateProcessingStart(id, 600));

            List<RecoveredTransaction> recovered = harness.recoveryService.recoverOnce();

            assertThat(recovered).hasSize(5);
            assertThat(recovered).allSatisfy(r -> assertThat(r.newStatus()).isEqualTo(TransactionStatus.NEW));
            assertThat(recovered).allSatisfy(r -> assertThat(r.previousInstance()).isEqualTo("processor-1"));
            assertThat(fixtures.countByStatus("NEW")).isEqualTo(5);
            assertThat(fixtures.countByStatus("PROCESSING")).isEqualTo(5);
        }
    }

    @Test
    @DisplayName("Recovery invalidates the old fencing token, which is what makes the reclaim safe")
    void recoveryInvalidatesTheOldToken() {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("10.00"));

        try (ProcessorTestHarness harness = harness("processor-1")) {
            ClaimedTransaction claim = harness.claimRepository.claimBatch("processor-1", 1).get(0);
            fixtures.backdateProcessingStart(id, 600);
            harness.recoveryService.recoverOnce();

            // Every fenced transition made with the stale token is a no-op.
            assertThat(harness.claimRepository.markProcessed(id, claim.processingToken())).isFalse();
            assertThat(harness.claimRepository.markError(id, claim.processingToken(), "late")).isFalse();
            assertThat(harness.claimRepository.scheduleRetry(id, claim.processingToken(), 1.0, "late")).isFalse();
            assertThat(harness.claimRepository.releaseClaim(id, claim.processingToken())).isFalse();

            assertThat(fixtures.statusOf(id)).isEqualTo("NEW");
        }
    }

    @Test
    @DisplayName("A row inside an active commit is skipped, not stolen, thanks to SKIP LOCKED")
    void rowsLockedByAnActiveCommitAreSkipped() throws Exception {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("10.00"));

        try (ProcessorTestHarness harness = harness("processor-1")) {
            harness.claimRepository.claimBatch("processor-1", 1);
            fixtures.backdateProcessingStart(id, 600);

            // Hold a row lock on the transaction from a separate connection, as an in-flight
            // persistence transaction would.
            Thread locker = new Thread(() -> {
                try (var connection = dataSource.getConnection()) {
                    connection.setAutoCommit(false);
                    try (var statement = connection.prepareStatement(
                            "SELECT id FROM transactions WHERE id = ? FOR UPDATE")) {
                        statement.setLong(1, id);
                        statement.executeQuery();
                    }
                    Thread.sleep(1500);
                    connection.rollback();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            locker.start();
            Thread.sleep(400);

            assertThat(harness.recoveryService.recoverOnce())
                    .as("the locked row must be skipped rather than reclaimed mid-commit")
                    .isEmpty();

            locker.join();

            // Once the lock is gone the sweep reclaims it normally.
            assertThat(harness.recoveryService.recoverOnce()).hasSize(1);
        }
    }

    @Test
    @DisplayName("A transaction that keeps expiring is failed permanently instead of looping forever")
    void repeatedLeaseExpiryIsBoundedByTheRetryBudget() {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("10.00"));

        try (ProcessorTestHarness harness = harness("processor-1")) {
            // max-retries = 3 in the test profile: three reclaims, then ERROR on the fourth.
            for (int attempt = 0; attempt < 3; attempt++) {
                harness.claimRepository.claimBatch("processor-1", 1);
                fixtures.backdateProcessingStart(id, 600);
                List<RecoveredTransaction> recovered = harness.recoveryService.recoverOnce();
                assertThat(recovered).hasSize(1);
                assertThat(recovered.get(0).newStatus()).isEqualTo(TransactionStatus.NEW);
            }

            harness.claimRepository.claimBatch("processor-1", 1);
            fixtures.backdateProcessingStart(id, 600);
            List<RecoveredTransaction> last = harness.recoveryService.recoverOnce();

            assertThat(last).hasSize(1);
            assertThat(last.get(0).newStatus()).isEqualTo(TransactionStatus.ERROR);
        }

        assertThat(fixtures.statusOf(id)).isEqualTo("ERROR");
        assertThat(fixtures.retryCountOf(id)).isEqualTo(4);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT error_message FROM transactions WHERE id = ?", String.class, id))
                .startsWith("LEASE_EXPIRED");
        // Terminal means terminal: it is not claimable any more, so the loop really is closed.
        try (ProcessorTestHarness harness = harness("processor-2")) {
            assertThat(harness.claimRepository.claimBatch("processor-2", 10)).isEmpty();
        }
    }

    @Test
    @DisplayName("A live worker's heartbeat keeps its lease, so a slow transaction is never reclaimed")
    void heartbeatPreventsSpuriousRecovery() {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("10.00"));

        try (ProcessorTestHarness harness = harness("processor-slow")) {
            ClaimedTransaction claim = harness.claimRepository.claimBatch("processor-slow", 1).get(0);
            harness.ownershipRegistry.register(claim.id(), claim.processingToken());

            // The lease is about to expire...
            fixtures.backdateProcessingStart(id, 600);
            // ...but the worker is alive and says so.
            assertThat(harness.leaseRenewalService.renewOnce()).isEqualTo(1);

            assertThat(harness.recoveryService.recoverOnce())
                    .as("a renewed lease is not stale")
                    .isEmpty();
            assertThat(fixtures.statusOf(id)).isEqualTo("PROCESSING");
        }
    }

    @Test
    @DisplayName("A heartbeat for a lease that was already reassigned revokes it locally")
    void heartbeatDetectsRevokedLeases() {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("10.00"));

        try (ProcessorTestHarness slow = harness("processor-slow");
             ProcessorTestHarness other = harness("processor-other")) {

            ClaimedTransaction claim = slow.claimRepository.claimBatch("processor-slow", 1).get(0);
            slow.ownershipRegistry.register(claim.id(), claim.processingToken());

            fixtures.backdateProcessingStart(id, 600);
            other.recoveryService.recoverOnce();

            assertThat(slow.leaseRenewalService.renewOnce()).isZero();
            assertThat(slow.ownershipRegistry.isRevoked(id))
                    .as("the worker learns it lost the lease before wasting more effort")
                    .isTrue();
        }
    }

    private ProcessorTestHarness harness(String instanceId) {
        ProcessorProperties properties = ProcessorTestHarness.properties(instanceId);
        return new ProcessorTestHarness(jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties);
    }
}
