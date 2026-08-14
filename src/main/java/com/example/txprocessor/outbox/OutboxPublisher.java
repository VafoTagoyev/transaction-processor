package com.example.txprocessor.outbox;

import java.util.List;
import java.util.UUID;

/**
 * Sink for outbox events. Kafka is intentionally not implemented (the assignment says it is
 * optional); the point being demonstrated is that the event and the state change are committed
 * by the <em>same</em> database transaction, which makes the choice of broker an implementation
 * detail behind this interface.
 */
public interface OutboxPublisher {

    /** @return the ids that were published successfully; the rest stay PENDING and are retried. */
    List<UUID> publish(List<OutboxRecord> records);
}
