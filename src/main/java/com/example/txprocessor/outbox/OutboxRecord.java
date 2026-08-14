package com.example.txprocessor.outbox;

import java.util.UUID;

public record OutboxRecord(UUID id, long aggregateId, String eventType, String payload, int attempts) {
}
