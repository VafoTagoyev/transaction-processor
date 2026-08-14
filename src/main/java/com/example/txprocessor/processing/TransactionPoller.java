package com.example.txprocessor.processing;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.logging.LogContext;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.repository.TransactionClaimRepository;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The only producer. A single thread claims batches and hands them to the bounded queue.
 *
 * <h2>Backpressure</h2>
 * The poller asks the queue how much room is left and claims at most that many rows:
 *
 * <pre>{@code  limit = min(batch-size, queue.remainingCapacity()) }</pre>
 *
 * This is stronger than the usual "block when the queue is full". Because a claim also writes
 * PROCESSING to the database, over-claiming would not merely use memory — it would mark rows as
 * owned that this instance has no capacity to work on, keeping them invisible to the other
 * instances until the lease expired. Sizing the claim by the free capacity means the service
 * never owns more work than it can hold, so when the write side is the bottleneck the backlog
 * stays in PostgreSQL where it belongs, the poller idles, and the other instances are free to
 * take the work instead.
 *
 * <p>Being single threaded is deliberate: a second poller thread would double the contention on
 * the head of the claim index without increasing throughput, since one claim statement already
 * returns up to batch-size rows in one round trip.
 */
@Component
public class TransactionPoller implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TransactionPoller.class);

    private final TransactionClaimRepository repository;
    private final ProcessingQueue queue;
    private final OwnershipRegistry ownershipRegistry;
    private final ClaimReleaser claimReleaser;
    private final ProcessorMetrics metrics;
    private final ProcessorProperties properties;

    private volatile boolean running;
    private Thread thread;

    public TransactionPoller(TransactionClaimRepository repository,
                             ProcessingQueue queue,
                             OwnershipRegistry ownershipRegistry,
                             ClaimReleaser claimReleaser,
                             ProcessorMetrics metrics,
                             ProcessorProperties properties) {
        this.repository = repository;
        this.queue = queue;
        this.ownershipRegistry = ownershipRegistry;
        this.claimReleaser = claimReleaser;
        this.metrics = metrics;
        this.properties = properties;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this, "tx-poller");
        thread.setDaemon(false);
        thread.start();
        log.info("Poller started (batch-size={}, queue-capacity={})",
                properties.getBatchSize(), queue.capacity());
    }

    public synchronized void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }

    @Override
    public void run() {
        LogContext.putInstanceId(properties.getInstanceId());
        while (running) {
            long sleepMillis;
            try {
                sleepMillis = pollOnce();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // A database outage must not kill the poller. Back off and try again; the rows
                // are safe in PostgreSQL and nothing has been claimed.
                log.error("Poll cycle failed, backing off", e);
                sleepMillis = properties.getPollingIdleDelay().toMillis();
            }
            if (!sleep(sleepMillis)) {
                return;
            }
        }
    }

    /** @return how long to wait before the next cycle. */
    private long pollOnce() throws InterruptedException {
        int free = queue.remainingCapacity();
        if (free < properties.getMinClaimBatch()) {
            metrics.backpressure();
            log.debug("Backpressure: queue full ({}/{}), skipping poll cycle", queue.size(), queue.capacity());
            return properties.getPollingDelay().toMillis();
        }

        int limit = Math.min(properties.getBatchSize(), free);

        Timer.Sample sample = metrics.startClaim();
        List<ClaimedTransaction> claimed = repository.claimBatch(properties.getInstanceId(), limit);
        metrics.recordClaim(sample);

        if (claimed.isEmpty()) {
            return properties.getPollingIdleDelay().toMillis();
        }

        metrics.received(claimed.size());
        log.debug("Claimed {} transactions (requested {}, queue {}/{})",
                claimed.size(), limit, queue.size(), queue.capacity());

        for (ClaimedTransaction transaction : claimed) {
            // Register before enqueueing: a transaction can wait in the queue, and its lease
            // must be renewed while it waits or recovery would reclaim work we are holding.
            ownershipRegistry.register(transaction.id(), transaction.processingToken());
            metrics.inFlightAdded(1);

            boolean accepted = queue.offer(transaction, properties.getQueueOfferTimeout().toMillis());
            if (!accepted) {
                // Should not happen (we sized the claim by the free capacity and we are the only
                // producer), but if it ever does, give the row straight back rather than sitting
                // on a claim we cannot honour.
                log.warn("Queue refused a claimed transaction; releasing it back to NEW");
                claimReleaser.release(transaction);
                metrics.claimReleased(1);
            }
        }

        return properties.getPollingDelay().toMillis();
    }

    private boolean sleep(long millis) {
        if (millis <= 0) {
            return true;
        }
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public boolean isRunning() {
        return running;
    }
}
