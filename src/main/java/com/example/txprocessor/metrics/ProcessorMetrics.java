package com.example.txprocessor.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Every meter the service publishes, in one place.
 *
 * <p>Meter names follow the Micrometer dot convention; the Prometheus registry renders them
 * with underscores and appends {@code _total} to counters, which produces exactly the metric
 * names required by the assignment (section 13). The mapping is tabulated in the README.
 */
// Explicit bean name: the derived default would be "processorMetrics", which collides with
// Actuator's SystemMetricsAutoConfiguration#processorMetrics (Micrometer's CPU binder - an
// unrelated class that also happens to be named ProcessorMetrics). Both beans are wanted, so
// this one is renamed rather than either being suppressed. Every injection site wires by type.
@Component("txProcessorMetrics")
public class ProcessorMetrics {

    private final MeterRegistry registry;

    private final Counter received;
    private final Counter processed;
    private final Counter error;
    private final Counter retry;
    private final Counter recovered;
    private final Counter duplicateSkipped;
    private final Counter ownershipLost;
    private final Counter backpressure;
    private final Counter claimReleased;
    private final Counter cacheFailure;
    private final Counter outboxPublished;

    private final Timer processingDuration;
    private final Timer redisLookupDuration;
    private final Timer dbWriteDuration;
    private final Timer claimDuration;

    private final AtomicInteger activeWorkers = new AtomicInteger();
    private final AtomicInteger inFlight = new AtomicInteger();

    public ProcessorMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.received = counter("processor.received", "Transactions claimed from the database");
        this.processed = counter("processor.processed", "Transactions written to processed_transactions");
        this.error = counter("processor.error", "Transactions moved to the terminal ERROR status");
        this.retry = counter("processor.retry", "Transactions returned to NEW for another attempt");
        this.recovered = counter("processor.recovered", "PROCESSING rows reclaimed after lease expiry");
        this.duplicateSkipped = counter("processor.duplicate.skipped",
                "Results that already existed; the idempotency barrier absorbed a replay");
        this.ownershipLost = counter("processor.ownership.lost",
                "Fenced writes that matched no row because the lease had been reassigned");
        this.backpressure = counter("processor.backpressure",
                "Poll cycles skipped because the worker queue had no room");
        this.claimReleased = counter("processor.claim.released",
                "Claims handed back to NEW without an attempt (queue full or shutdown)");
        this.cacheFailure = counter("processor.cache.failure", "Failed cache round trips");
        this.outboxPublished = counter("processor.outbox.published", "Outbox events handed to the publisher");

        this.processingDuration = timer("processing.duration", "End to end time to process one transaction");
        this.redisLookupDuration = timer("redis.lookup.duration", "Time of one cache round trip");
        this.dbWriteDuration = timer("db.write.duration", "Time of the result persistence transaction");
        this.claimDuration = timer("claim.duration", "Time of one claim statement");

        Gauge.builder("active.workers", activeWorkers, AtomicInteger::get)
                .description("Worker threads currently executing a transaction")
                .register(registry);
        Gauge.builder("processor.inflight", inFlight, AtomicInteger::get)
                .description("Transactions owned by this instance (queued or executing)")
                .register(registry);
    }

    private Counter counter(String name, String description) {
        return Counter.builder(name).description(description).register(registry);
    }

    private Timer timer(String name, String description) {
        return Timer.builder(name)
                .description(description)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry);
    }

    /** Bound by the queue itself so the gauge always reflects the live object. */
    public void bindQueue(Supplier<Number> size, int capacity) {
        Gauge.builder("processing.queue.size", size)
                .description("Transactions waiting in the bounded hand-off queue")
                .register(registry);
        Gauge.builder("processing.queue.capacity", () -> capacity)
                .description("Configured capacity of the bounded hand-off queue")
                .register(registry);
    }

    public void received(int count) { received.increment(count); }
    public void processed() { processed.increment(); }
    public void error() { error.increment(); }
    public void retry() { retry.increment(); }
    public void recovered(int count) { recovered.increment(count); }
    public void duplicateSkipped() { duplicateSkipped.increment(); }
    public void ownershipLost() { ownershipLost.increment(); }
    public void backpressure() { backpressure.increment(); }
    public void claimReleased(int count) { claimReleased.increment(count); }
    public void cacheFailure() { cacheFailure.increment(); }
    public void outboxPublished(int count) { outboxPublished.increment(count); }

    public Timer.Sample start() { return Timer.start(registry); }
    public void recordProcessing(Timer.Sample sample) { sample.stop(processingDuration); }
    public Timer.Sample startRedisLookup() { return Timer.start(registry); }
    public void recordRedisLookup(Timer.Sample sample) { sample.stop(redisLookupDuration); }
    public Timer.Sample startDbWrite() { return Timer.start(registry); }
    public void recordDbWrite(Timer.Sample sample) { sample.stop(dbWriteDuration); }
    public Timer.Sample startClaim() { return Timer.start(registry); }
    public void recordClaim(Timer.Sample sample) { sample.stop(claimDuration); }

    public void workerStarted() { activeWorkers.incrementAndGet(); }
    public void workerFinished() { activeWorkers.decrementAndGet(); }
    public void inFlightAdded(int count) { inFlight.addAndGet(count); }
    public void inFlightRemoved() { inFlight.decrementAndGet(); }

    public int activeWorkers() { return activeWorkers.get(); }
    public int inFlight() { return inFlight.get(); }
    public double processedCount() { return processed.count(); }
    public double errorCount() { return error.count(); }
    public double retryCount() { return retry.count(); }
    public double recoveredCount() { return recovered.count(); }
    public double duplicateSkippedCount() { return duplicateSkipped.count(); }
    public double ownershipLostCount() { return ownershipLost.count(); }
    public double backpressureCount() { return backpressure.count(); }
}
