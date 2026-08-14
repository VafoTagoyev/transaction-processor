package com.example.txprocessor.domain;

/** A claimed transaction plus the reference data resolved from the cache. */
public record EnrichedTransaction(ClaimedTransaction transaction, CardInfo card, TerminalInfo terminal) {
}
