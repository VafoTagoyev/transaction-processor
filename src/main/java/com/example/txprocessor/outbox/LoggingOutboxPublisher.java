package com.example.txprocessor.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Default sink: writes the event to the log and reports success. Replacing it with a Kafka
 * producer is a one-class change and requires no modification to the persistence path, which is
 * exactly the property the outbox pattern is supposed to buy.
 */
@Component
public class LoggingOutboxPublisher implements OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(LoggingOutboxPublisher.class);

    @Override
    public List<UUID> publish(List<OutboxRecord> records) {
        for (OutboxRecord record : records) {
            log.debug("Publishing outbox event type={} aggregateId={} id={}",
                    record.eventType(), record.aggregateId(), record.id());
        }
        return records.stream().map(OutboxRecord::id).toList();
    }
}
