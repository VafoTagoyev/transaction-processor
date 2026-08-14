package com.example.txprocessor.processing;

import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.error.ProcessingException;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.repository.TransactionClaimRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Turns a failed attempt into a durable state transition: either back to NEW with a backoff, or
 * to the terminal ERROR status.
 *
 * <p>Both transitions are fenced. If this instance has meanwhile lost the lease, the update
 * matches zero rows and we do nothing: the new owner is authoritative and must not have its
 * work overwritten by a stale error message.
 *
 * <p>If this method itself cannot reach the database, the row simply stays PROCESSING and its
 * lease stops being renewed, so the recovery sweep picks it up after the processing timeout.
 * There is no failure mode in which a transaction is lost — the worst case is that it takes
 * one lease period longer.
 */
@Component
public class FailureHandler {

    private static final Logger log = LoggerFactory.getLogger(FailureHandler.class);

    /** transactions.error_message is TEXT, but a runaway stack trace in a hot column is not useful. */
    private static final int MAX_ERROR_MESSAGE_LENGTH = 1000;

    private final TransactionClaimRepository repository;
    private final RetryPolicy retryPolicy;
    private final ProcessorMetrics metrics;

    public FailureHandler(TransactionClaimRepository repository, RetryPolicy retryPolicy, ProcessorMetrics metrics) {
        this.repository = repository;
        this.retryPolicy = retryPolicy;
        this.metrics = metrics;
    }

    public void handle(ClaimedTransaction transaction, ProcessingException failure) {
        String message = truncate(failure.errorCode() + ": " + failure.getMessage());

        if (retryPolicy.shouldRetry(failure, transaction.retryCount())) {
            Duration backoff = retryPolicy.backoffFor(transaction.retryCount());
            boolean applied = repository.scheduleRetry(
                    transaction.id(), transaction.processingToken(), backoff.toMillis() / 1000.0, message);
            if (applied) {
                metrics.retry();
                log.warn("Attempt {} failed, retrying in {}s: {}",
                        transaction.retryCount() + 1, backoff.toSeconds(), message);
            } else {
                reportLostOwnership(transaction, "retry");
            }
            return;
        }

        boolean applied = repository.markError(transaction.id(), transaction.processingToken(), message);
        if (applied) {
            metrics.error();
            log.warn("Transaction moved to ERROR after {} attempt(s): {}", transaction.retryCount() + 1, message);
        } else {
            reportLostOwnership(transaction, "error");
        }
    }

    private void reportLostOwnership(ClaimedTransaction transaction, String intendedTransition) {
        metrics.ownershipLost();
        log.warn("Skipped '{}' transition for transaction {}: the lease is no longer held by this instance",
                intendedTransition, transaction.id());
    }

    private String truncate(String message) {
        if (message == null) {
            return "";
        }
        return message.length() <= MAX_ERROR_MESSAGE_LENGTH
                ? message
                : message.substring(0, MAX_ERROR_MESSAGE_LENGTH);
    }
}
