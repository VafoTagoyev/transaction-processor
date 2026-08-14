package com.example.txprocessor.processing;

import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.repository.TransactionClaimRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Hands a claim back without penalty.
 *
 * <p>Used when this instance owns a transaction it has not attempted: the queue was full, or the
 * process is shutting down. Returning the row to NEW immediately means the work is picked up on
 * the next poll instead of after a full processing-timeout, which is what makes an ordinary
 * rolling restart invisible in the throughput graph. retry_count is deliberately untouched -
 * nothing was attempted, so nothing should be charged against the retry budget.
 */
@Component
public class ClaimReleaser {

    private static final Logger log = LoggerFactory.getLogger(ClaimReleaser.class);

    private final TransactionClaimRepository repository;
    private final OwnershipRegistry ownershipRegistry;
    private final ProcessorMetrics metrics;

    public ClaimReleaser(TransactionClaimRepository repository,
                         OwnershipRegistry ownershipRegistry,
                         ProcessorMetrics metrics) {
        this.repository = repository;
        this.ownershipRegistry = ownershipRegistry;
        this.metrics = metrics;
    }

    public void release(ClaimedTransaction transaction) {
        try {
            repository.releaseClaim(transaction.id(), transaction.processingToken());
        } catch (RuntimeException e) {
            // Not fatal: the row stays PROCESSING, its lease stops being renewed, and the
            // recovery sweep reclaims it after the timeout. Slower, but never lost.
            log.warn("Could not release claim on transaction {}; recovery will reclaim it after the lease expires",
                    transaction.id(), e);
        } finally {
            ownershipRegistry.release(transaction.id());
            metrics.inFlightRemoved();
        }
    }

    public void releaseAll(List<ClaimedTransaction> transactions) {
        transactions.forEach(this::release);
        if (!transactions.isEmpty()) {
            metrics.claimReleased(transactions.size());
        }
    }
}
