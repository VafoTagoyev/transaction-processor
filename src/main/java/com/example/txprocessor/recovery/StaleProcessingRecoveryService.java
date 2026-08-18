package com.example.txprocessor.recovery;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.TransactionStatus;
import com.example.txprocessor.logging.LogContext;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.repository.TransactionClaimRepository;
import com.example.txprocessor.repository.TransactionClaimRepository.RecoveredTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The crash-recovery mechanism (assignment stage 13, acceptance criteria C3-C6).
 *
 * <h2>How the service knows where it stopped after a restart</h2>
 * It does not need to know. There is no offset, no checkpoint and no resume position anywhere
 * in this codebase — the position <em>is</em> the {@code status} column. Anything NEW is
 * pending, anything PROCESSED or ERROR is finished, and anything PROCESSING is owned by a lease
 * that either gets renewed or expires. A restarted instance simply starts claiming NEW rows
 * again; it does not care what it was doing before it died, and it does not have to.
 *
 * <h2>The reclaim race, and why it is safe</h2>
 * A lease can expire while its owner is still alive but slow. Distinguishing "slow" from "dead"
 * is impossible in general, so this design does not try. It does two things instead:
 *
 * <ol>
 *   <li><b>Prevention.</b> A live worker renews the lease of every transaction it holds
 *       (see {@link LeaseRenewalService}). A slow-but-healthy worker therefore keeps its lease
 *       indefinitely; only a worker that has stopped executing — dead JVM, frozen container,
 *       partitioned from the database — lets it lapse.</li>
 *   <li><b>Containment.</b> If a reclaim happens anyway, it invalidates the previous owner's
 *       fencing token. Every write the old owner can still make is conditioned on that token,
 *       so all of them match zero rows and its whole persistence transaction rolls back. Two
 *       workers may briefly <em>compute</em> the same transaction; only one can ever
 *       <em>commit</em> it.</li>
 * </ol>
 *
 * <p>Additionally, the CTE takes {@code FOR UPDATE SKIP LOCKED}, so a row that is at this very
 * moment inside another worker's commit is locked and gets skipped rather than stolen mid-flight.
 *
 * <h2>No infinite recovery loop</h2>
 * Each reclaim charges the retry budget. A transaction whose processing reliably kills its
 * worker is reclaimed at most {@code max-retries} times and then becomes ERROR, so a poison
 * pill cannot cycle forever between instances.
 *
 * <h2>Why every instance runs this</h2>
 * A dedicated leader would need leader election, and would be a single point of failure for the
 * one mechanism that exists to survive failures. SKIP LOCKED already makes concurrent sweeps
 * safe and idempotent, so all three instances sweep and simply partition the work.
 */
@Service
@ConditionalOnProperty(prefix = "processor.recovery", name = "enabled", havingValue = "true", matchIfMissing = true)
public class StaleProcessingRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(StaleProcessingRecoveryService.class);

    private final TransactionClaimRepository repository;
    private final ProcessorMetrics metrics;
    private final ProcessorProperties properties;

    public StaleProcessingRecoveryService(TransactionClaimRepository repository,
                                          ProcessorMetrics metrics,
                                          ProcessorProperties properties) {
        this.repository = repository;
        this.metrics = metrics;
        this.properties = properties;
    }

    /** Scheduled by {@code SchedulingConfig} at {@code processor.recovery.interval}. */
    public void recoverStaleTransactions() {
        try {
            recoverOnce();
        } catch (RuntimeException e) {
            log.error("Stale-lease recovery sweep failed; it will run again on the next interval", e);
        }
    }

    /** @return the transactions reclaimed by this sweep. Exposed so tests can run it deterministically. */
    public List<RecoveredTransaction> recoverOnce() {
        LogContext.putInstanceId(properties.getInstanceId());

        double timeoutSeconds = properties.getProcessingTimeout().toMillis() / 1000.0;
        List<RecoveredTransaction> recovered = repository.recoverStale(
                timeoutSeconds, properties.getRecovery().getBatchSize(), properties.getMaxRetries());

        if (recovered.isEmpty()) {
            return recovered;
        }

        metrics.recovered(recovered.size());

        Map<TransactionStatus, Long> byStatus = recovered.stream()
                .collect(Collectors.groupingBy(RecoveredTransaction::newStatus, Collectors.counting()));
        Map<String, Long> byInstance = recovered.stream()
                .collect(Collectors.groupingBy(
                        r -> r.previousInstance() == null ? "unknown" : r.previousInstance(),
                        Collectors.counting()));

        log.warn("Recovered {} transactions with expired leases: {} (previous owners: {})",
                recovered.size(), byStatus, byInstance);

        recovered.stream()
                .filter(r -> r.newStatus() == TransactionStatus.ERROR)
                .forEach(r -> log.error("Transaction {} (externalId={}) exhausted its retry budget through "
                        + "repeated lease expiry and was failed permanently", r.id(), r.externalId()));

        return recovered;
    }

    /** Convenience for tests and scripts: how many rows are past their lease right now. */
    public long countStuck() {
        return repository.countStuckProcessing(properties.getProcessingTimeout().toMillis() / 1000.0);
    }
}
