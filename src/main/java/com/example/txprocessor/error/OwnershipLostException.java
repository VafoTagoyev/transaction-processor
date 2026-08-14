package com.example.txprocessor.error;

/**
 * Thrown when a fenced write matches zero rows: while this worker was busy, its lease expired
 * and recovery handed the transaction to someone else (or the transaction already reached a
 * terminal status).
 *
 * <p>This is the linchpin of the stale-recovery race (assignment requirement 47 / question 74).
 * It is thrown from inside the persistence transaction, which therefore rolls back in full:
 * no processed_transactions row, no statistics increment, no outbox event. The worker that
 * legitimately owns the transaction now is unaffected.
 *
 * <p>It is a normal, expected outcome, not an error: it is counted separately and never
 * escalated to a retry, because retrying would mean fighting the current owner.
 */
public class OwnershipLostException extends RuntimeException {

    private final long transactionId;

    public OwnershipLostException(long transactionId, String detail) {
        super("Lost ownership of transaction " + transactionId + ": " + detail);
        this.transactionId = transactionId;
    }

    public long getTransactionId() {
        return transactionId;
    }
}
