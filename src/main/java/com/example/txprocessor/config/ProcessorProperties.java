package com.example.txprocessor.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * All tunables of the processing pipeline. Everything that influences correctness under
 * failure (timeouts, retry limits, queue bounds) is configuration, never a constant in code.
 */
@Validated
@ConfigurationProperties(prefix = "processor")
public class ProcessorProperties {

    /** Identity of this JVM. Must be unique across the cluster; defaults to the container hostname. */
    @NotBlank
    private String instanceId = "local";

    /** Master switch. Turned off in the data generator and in unit-style tests. */
    private boolean enabled = true;

    /** Upper bound on rows claimed per poll. The poller never claims more than the queue can hold. */
    @Min(1)
    private int batchSize = 1000;

    /** Number of worker threads in this instance. */
    @Min(1)
    private int workers = 8;

    /** Delay between poll cycles when the previous cycle found work. */
    @NotNull
    private Duration pollingDelay = Duration.ofMillis(500);

    /** Delay between poll cycles when the previous cycle found nothing (avoids hammering an idle DB). */
    @NotNull
    private Duration pollingIdleDelay = Duration.ofSeconds(2);

    /**
     * Lease duration. A PROCESSING row whose processing_started_at is older than this is
     * considered abandoned and becomes eligible for recovery. Must be comfortably larger
     * than {@link #leaseRenewalInterval} and than the realistic worst case processing time.
     */
    @NotNull
    private Duration processingTimeout = Duration.ofMinutes(10);

    /** How often a live worker extends the lease of the rows it currently owns. */
    @NotNull
    private Duration leaseRenewalInterval = Duration.ofSeconds(30);

    /** Capacity of the bounded hand-off queue between the poller and the workers. Bounds memory. */
    @Min(1)
    private int queueCapacity = 5000;

    /** How long the poller waits to hand a claimed row to the queue before releasing the claim. */
    @NotNull
    private Duration queueOfferTimeout = Duration.ofSeconds(5);

    /** Poller skips a cycle if fewer than this many queue slots are free (avoids single-row polls). */
    @Min(1)
    private int minClaimBatch = 1;

    /** Maximum number of *attempts* beyond the first before a transaction is failed permanently. */
    @Min(0)
    private int maxRetries = 3;

    /** First retry delay; subsequent delays multiply by {@link #retryMultiplier} up to {@link #retryMaxDelay}. */
    @NotNull
    private Duration retryInitialDelay = Duration.ofSeconds(1);

    @DecimalMin("1.0")
    private double retryMultiplier = 3.0;

    @NotNull
    private Duration retryMaxDelay = Duration.ofMinutes(5);

    /** How long a graceful shutdown waits for in-flight work before giving up and letting recovery handle it. */
    @NotNull
    private Duration shutdownGracePeriod = Duration.ofSeconds(30);

    @Valid
    private final Redis redis = new Redis();

    @Valid
    private final Recovery recovery = new Recovery();

    @Valid
    private final Outbox outbox = new Outbox();

    public static class Redis {
        /** Per-command timeout. Exceeding it is a *transient* failure and is retried. */
        @NotNull
        private Duration timeout = Duration.ofMillis(200);

        /** Immediate in-process retries for a Redis blip, before falling back to durable DB-backed retry. */
        @Min(0)
        private int immediateRetries = 1;

        @NotNull
        private Duration immediateRetryDelay = Duration.ofMillis(50);

        /**
         * Policy for a *deterministic* cache miss (key genuinely absent).
         * false (default): permanent error, the transaction goes straight to ERROR. Retrying a
         *                  key that is not there only burns capacity and delays the final status.
         * true:            treat as transient, so a miss caused by an incomplete cache warm-up
         *                  is retried until max-retries is exhausted.
         */
        private boolean retryOnCacheMiss = false;

        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration timeout) { this.timeout = timeout; }
        public int getImmediateRetries() { return immediateRetries; }
        public void setImmediateRetries(int immediateRetries) { this.immediateRetries = immediateRetries; }
        public Duration getImmediateRetryDelay() { return immediateRetryDelay; }
        public void setImmediateRetryDelay(Duration d) { this.immediateRetryDelay = d; }
        public boolean isRetryOnCacheMiss() { return retryOnCacheMiss; }
        public void setRetryOnCacheMiss(boolean retryOnCacheMiss) { this.retryOnCacheMiss = retryOnCacheMiss; }
    }

    public static class Recovery {
        private boolean enabled = true;

        /** How often this instance sweeps for expired leases. Every instance sweeps; SKIP LOCKED makes that safe. */
        @NotNull
        private Duration interval = Duration.ofMinutes(1);

        @Min(1)
        private int batchSize = 1000;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public Duration getInterval() { return interval; }
        public void setInterval(Duration interval) { this.interval = interval; }
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    }

    public static class Outbox {
        private boolean relayEnabled = true;

        @NotNull
        private Duration relayInterval = Duration.ofSeconds(1);

        @Min(1)
        private int relayBatchSize = 500;

        public boolean isRelayEnabled() { return relayEnabled; }
        public void setRelayEnabled(boolean relayEnabled) { this.relayEnabled = relayEnabled; }
        public Duration getRelayInterval() { return relayInterval; }
        public void setRelayInterval(Duration relayInterval) { this.relayInterval = relayInterval; }
        public int getRelayBatchSize() { return relayBatchSize; }
        public void setRelayBatchSize(int relayBatchSize) { this.relayBatchSize = relayBatchSize; }
    }

    public String getInstanceId() { return instanceId; }
    public void setInstanceId(String instanceId) { this.instanceId = instanceId; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    public int getWorkers() { return workers; }
    public void setWorkers(int workers) { this.workers = workers; }
    public Duration getPollingDelay() { return pollingDelay; }
    public void setPollingDelay(Duration pollingDelay) { this.pollingDelay = pollingDelay; }
    public Duration getPollingIdleDelay() { return pollingIdleDelay; }
    public void setPollingIdleDelay(Duration pollingIdleDelay) { this.pollingIdleDelay = pollingIdleDelay; }
    public Duration getProcessingTimeout() { return processingTimeout; }
    public void setProcessingTimeout(Duration processingTimeout) { this.processingTimeout = processingTimeout; }
    public Duration getLeaseRenewalInterval() { return leaseRenewalInterval; }
    public void setLeaseRenewalInterval(Duration d) { this.leaseRenewalInterval = d; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public Duration getQueueOfferTimeout() { return queueOfferTimeout; }
    public void setQueueOfferTimeout(Duration queueOfferTimeout) { this.queueOfferTimeout = queueOfferTimeout; }
    public int getMinClaimBatch() { return minClaimBatch; }
    public void setMinClaimBatch(int minClaimBatch) { this.minClaimBatch = minClaimBatch; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public Duration getRetryInitialDelay() { return retryInitialDelay; }
    public void setRetryInitialDelay(Duration d) { this.retryInitialDelay = d; }
    public double getRetryMultiplier() { return retryMultiplier; }
    public void setRetryMultiplier(double retryMultiplier) { this.retryMultiplier = retryMultiplier; }
    public Duration getRetryMaxDelay() { return retryMaxDelay; }
    public void setRetryMaxDelay(Duration retryMaxDelay) { this.retryMaxDelay = retryMaxDelay; }
    public Duration getShutdownGracePeriod() { return shutdownGracePeriod; }
    public void setShutdownGracePeriod(Duration d) { this.shutdownGracePeriod = d; }
    public Redis getRedis() { return redis; }
    public Recovery getRecovery() { return recovery; }
    public Outbox getOutbox() { return outbox; }
}
