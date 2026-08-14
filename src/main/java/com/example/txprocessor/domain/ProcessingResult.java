package com.example.txprocessor.domain;

import java.math.BigDecimal;

/** The immutable outcome of business processing, ready to be persisted. */
public record ProcessingResult(
        long transactionId,
        String externalId,
        String clientId,
        String account,
        String productId,
        String merchantId,
        String branchCode,
        BigDecimal amount,
        BigDecimal commission,
        OperationType operationType) {
}
