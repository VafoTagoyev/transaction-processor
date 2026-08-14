package com.example.txprocessor.processing;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.error.ProcessingException;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Decides, for a failed attempt, whether there is another attempt and how long to wait.
 *
 * <p>Two hard guarantees, both required by acceptance criterion C8:
 * <ul>
 *   <li>a permanent failure is never retried;</li>
 *   <li>a transient failure is retried at most {@code max-retries} times, after which the
 *       transaction reaches the terminal ERROR status. The counter lives in the database
 *       ({@code transactions.retry_count}), so it survives a crash and cannot be reset by a
 *       restart. An infinite retry loop is therefore not expressible.</li>
 * </ul>
 */
@Component
public class RetryPolicy {

    private final int maxRetries;
    private final Duration initialDelay;
    private final double multiplier;
    private final Duration maxDelay;

    public RetryPolicy(ProcessorProperties properties) {
        this.maxRetries = properties.getMaxRetries();
        this.initialDelay = properties.getRetryInitialDelay();
        this.multiplier = properties.getRetryMultiplier();
        this.maxDelay = properties.getRetryMaxDelay();
    }

    public boolean shouldRetry(ProcessingException failure, int currentRetryCount) {
        return failure.transientFailure() && currentRetryCount < maxRetries;
    }

    /**
     * Exponential backoff, capped. {@code currentRetryCount} is the number of retries already
     * consumed, so the first retry waits {@code initialDelay}.
     */
    public Duration backoffFor(int currentRetryCount) {
        double seconds = initialDelay.toMillis() / 1000.0 * Math.pow(multiplier, currentRetryCount);
        double cappedSeconds = Math.min(seconds, maxDelay.toMillis() / 1000.0);
        return Duration.ofMillis(Math.round(cappedSeconds * 1000));
    }

    public int maxRetries() {
        return maxRetries;
    }
}
