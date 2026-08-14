package com.example.txprocessor.domain;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * A transaction this instance currently owns.
 *
 * @param processingToken the fencing token written by the claim. Every subsequent write that
 *                        finalises this transaction is conditioned on it. If recovery has
 *                        reassigned the row, the token in the database differs and this
 *                        worker's writes match zero rows.
 */
public record ClaimedTransaction(
        long id,
        String externalId,
        String cardId,
        String terminalId,
        BigDecimal amount,
        String currency,
        String transactionType,
        int retryCount,
        UUID processingToken) {
}
