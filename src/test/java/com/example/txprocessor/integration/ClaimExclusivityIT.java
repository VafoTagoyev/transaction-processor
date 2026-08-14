package com.example.txprocessor.integration;

import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.repository.TransactionClaimRepository;
import com.example.txprocessor.support.IntegrationTestBase;
import com.example.txprocessor.support.TestFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The claim itself, under maximum contention. This is the test that answers defence question 65
 * ("what happens if two instances run SELECT ... LIMIT 1000 without locking?") empirically:
 * with the CTE + FOR UPDATE SKIP LOCKED + atomic UPDATE, the answer is that they partition the
 * rows perfectly instead of both getting the same ones.
 */
class ClaimExclusivityIT extends IntegrationTestBase {

    private static final int TRANSACTIONS = 2000;
    private static final int CLAIMERS = 12;

    @Test
    @DisplayName("Concurrent claimers never receive the same transaction twice, and lose none")
    void concurrentClaimersPartitionTheBacklog() throws Exception {
        TestFixtures fixtures = fixtures();
        fixtures.insertNewTransactions(TRANSACTIONS, "100001", "50001", new BigDecimal("500.00"));

        TransactionClaimRepository repository = new TransactionClaimRepository(jdbcTemplate);
        ConcurrentLinkedQueue<Long> allClaimed = new ConcurrentLinkedQueue<>();
        CountDownLatch startGate = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newFixedThreadPool(CLAIMERS)) {
            for (int i = 0; i < CLAIMERS; i++) {
                String instanceId = "claimer-" + i;
                pool.submit(() -> {
                    startGate.await();
                    while (true) {
                        List<ClaimedTransaction> batch = repository.claimBatch(instanceId, 50);
                        if (batch.isEmpty()) {
                            return null;
                        }
                        batch.forEach(t -> allClaimed.add(t.id()));
                    }
                });
            }
            startGate.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }

        List<Long> claimed = List.copyOf(allClaimed);
        Set<Long> distinct = Set.copyOf(claimed);

        assertThat(claimed).as("every transaction was claimed exactly once").hasSize(TRANSACTIONS);
        assertThat(distinct).as("no transaction was handed to two claimers").hasSize(TRANSACTIONS);
        assertThat(fixtures.countByStatus("NEW")).isZero();
        assertThat(fixtures.countByStatus("PROCESSING")).isEqualTo(TRANSACTIONS);
    }

    @Test
    @DisplayName("Every claim writes ownership: status, timestamp, instance and a unique fencing token")
    void claimPersistsOwnership() {
        TestFixtures fixtures = fixtures();
        fixtures.insertNewTransactions(10, "100001", "50001", new BigDecimal("10.00"));

        TransactionClaimRepository repository = new TransactionClaimRepository(jdbcTemplate);
        List<ClaimedTransaction> claimed = repository.claimBatch("instance-a", 10);

        assertThat(claimed).hasSize(10);
        assertThat(claimed).allSatisfy(transaction -> assertThat(transaction.processingToken()).isNotNull());
        assertThat(claimed.stream().map(ClaimedTransaction::processingToken).collect(Collectors.toSet()))
                .as("tokens are per claim, never reused")
                .hasSize(10);

        List<String> instances = jdbcTemplate.queryForList(
                "SELECT DISTINCT processing_instance FROM transactions WHERE status = 'PROCESSING'", String.class);
        assertThat(instances).containsExactly("instance-a");

        Long withoutTimestamp = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM transactions WHERE status = 'PROCESSING' AND processing_started_at IS NULL",
                Long.class);
        assertThat(withoutTimestamp).isZero();
    }

    @Test
    @DisplayName("A transaction scheduled for a future retry is invisible to the claim query")
    void backoffHidesTransactionsFromTheClaim() {
        TestFixtures fixtures = fixtures();
        long id = fixtures.insertNewTransaction("100001", "50001", new BigDecimal("10.00"));
        jdbcTemplate.update("UPDATE transactions SET next_attempt_at = now() + INTERVAL '1 hour' WHERE id = ?", id);

        TransactionClaimRepository repository = new TransactionClaimRepository(jdbcTemplate);

        assertThat(repository.claimBatch("instance-a", 10)).isEmpty();

        jdbcTemplate.update("UPDATE transactions SET next_attempt_at = now() WHERE id = ?", id);
        assertThat(repository.claimBatch("instance-a", 10)).hasSize(1);
    }
}
