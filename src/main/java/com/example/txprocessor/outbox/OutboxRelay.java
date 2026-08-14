package com.example.txprocessor.outbox;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Drains outbox_events. Runs on every instance; SKIP LOCKED in claimPending means the instances
 * partition the backlog between themselves without coordination and without publishing the same
 * event twice.
 *
 * <p>Delivery is at-least-once: the publish happens before the COMMIT that marks the row
 * PUBLISHED, so a crash in that window republishes on the next sweep. That is the standard and
 * unavoidable trade-off — consumers must be idempotent on the event id, which is why the id is
 * a stable UUID rather than a sequence.
 */
@Component
@ConditionalOnProperty(prefix = "processor.outbox", name = "relay-enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outboxRepository;
    private final OutboxPublisher publisher;
    private final ProcessorMetrics metrics;
    private final int batchSize;

    public OutboxRelay(OutboxRepository outboxRepository,
                       OutboxPublisher publisher,
                       ProcessorMetrics metrics,
                       ProcessorProperties properties) {
        this.outboxRepository = outboxRepository;
        this.publisher = publisher;
        this.metrics = metrics;
        this.batchSize = properties.getOutbox().getRelayBatchSize();
    }

    @Scheduled(fixedDelayString = "${processor.outbox.relay-interval:1s}")
    @Transactional
    public void relay() {
        List<OutboxRecord> pending = outboxRepository.claimPending(batchSize);
        if (pending.isEmpty()) {
            return;
        }
        List<UUID> published = publisher.publish(pending);
        outboxRepository.markPublished(published);

        List<UUID> failed = pending.stream()
                .map(OutboxRecord::id)
                .filter(id -> !published.contains(id))
                .toList();
        outboxRepository.markAttemptFailed(failed);

        metrics.outboxPublished(published.size());
        if (!failed.isEmpty()) {
            log.warn("Outbox relay could not publish {} of {} events; they stay PENDING",
                    failed.size(), pending.size());
        }
    }
}
