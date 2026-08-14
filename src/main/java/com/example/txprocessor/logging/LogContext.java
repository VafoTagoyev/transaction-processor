package com.example.txprocessor.logging;

import org.slf4j.MDC;

/**
 * MDC keys carried on every log line emitted while a transaction is being processed. With the
 * {@code json} profile these become first class fields; with the default pattern they are
 * rendered as {@code [instance=... tx=... extId=...]}. Either way a log aggregator can group
 * every line of one transaction across three containers.
 */
public final class LogContext implements AutoCloseable {

    public static final String INSTANCE_ID = "instanceId";
    public static final String TRANSACTION_ID = "transactionId";
    public static final String EXTERNAL_ID = "externalId";

    private LogContext() {
    }

    public static void putInstanceId(String instanceId) {
        MDC.put(INSTANCE_ID, instanceId);
    }

    public static LogContext forTransaction(long transactionId, String externalId) {
        MDC.put(TRANSACTION_ID, Long.toString(transactionId));
        MDC.put(EXTERNAL_ID, externalId);
        return new LogContext();
    }

    @Override
    public void close() {
        MDC.remove(TRANSACTION_ID);
        MDC.remove(EXTERNAL_ID);
    }
}
