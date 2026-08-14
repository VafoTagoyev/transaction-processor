package com.example.txprocessor.outbox;

import java.math.BigDecimal;
import java.time.Instant;

/** Payload of the TRANSACTION_PROCESSED outbox event. */
public record TransactionProcessedEvent(
        long transactionId,
        String externalId,
        String account,
        String clientId,
        BigDecimal amount,
        BigDecimal commission,
        String operationType,
        String processedBy,
        Instant occurredAt) {

    public static final String TYPE = "TRANSACTION_PROCESSED";
}
