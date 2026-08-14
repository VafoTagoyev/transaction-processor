package com.example.txprocessor.error;

import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.sql.SQLTransientException;

/**
 * Maps an arbitrary Throwable onto the transient/permanent axis that drives the retry policy.
 *
 * <p>The default for an *unrecognised* exception is transient. Rationale: a transaction wrongly
 * classified as transient costs a bounded number of extra attempts and then still lands in
 * ERROR, whereas a transaction wrongly classified as permanent is dropped on the floor after a
 * single infrastructure blip. Under a "no lost transactions" requirement the asymmetry is clear.
 */
@Component
public class ErrorClassifier {

    public ProcessingException classify(Throwable throwable) {
        if (throwable instanceof ProcessingException processingException) {
            return processingException;
        }

        // Uniqueness violation on processed_transactions is *not* a failure: it means the work
        // was already done. It is handled explicitly at the insert site and never reaches here,
        // but if it ever does, retrying it forever would be wrong.
        if (throwable instanceof DataIntegrityViolationException) {
            return new PermanentProcessingException("DATA_INTEGRITY",
                    "Data integrity violation: " + rootMessage(throwable), throwable);
        }

        if (throwable instanceof QueryTimeoutException
                || throwable instanceof CannotAcquireLockException
                || throwable instanceof ConcurrencyFailureException
                || throwable instanceof DataAccessResourceFailureException
                || throwable instanceof TransientDataAccessException
                || throwable instanceof SQLTransientException) {
            return new TransientProcessingException("DB_TRANSIENT",
                    "Transient database failure: " + rootMessage(throwable), throwable);
        }

        if (throwable instanceof SocketTimeoutException || throwable instanceof IOException) {
            return new TransientProcessingException("NETWORK",
                    "Network failure: " + rootMessage(throwable), throwable);
        }

        if (throwable instanceof InterruptedException) {
            return new TransientProcessingException("INTERRUPTED",
                    "Worker interrupted, transaction will be retried", throwable);
        }

        return new TransientProcessingException("UNKNOWN",
                "Unclassified failure: " + rootMessage(throwable), throwable);
    }

    private String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current.getClass().getSimpleName() + ": " + current.getMessage();
    }
}
