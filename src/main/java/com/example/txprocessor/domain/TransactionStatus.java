package com.example.txprocessor.domain;

/**
 * Lifecycle:
 *
 *   NEW ──claim──▶ PROCESSING ──success──▶ PROCESSED   (terminal)
 *                       │
 *                       ├──transient failure, attempts left──▶ NEW (with backoff)
 *                       ├──permanent failure / attempts exhausted──▶ ERROR (terminal)
 *                       └──lease expired (owner presumed dead)──▶ NEW (recovery) or ERROR
 *
 * PROCESSING is the only non-terminal owned state, and it always carries a fencing token.
 */
public enum TransactionStatus {
    NEW,
    PROCESSING,
    PROCESSED,
    ERROR
}
