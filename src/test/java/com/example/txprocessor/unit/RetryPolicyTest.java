package com.example.txprocessor.unit;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.error.CacheUnavailableException;
import com.example.txprocessor.error.CardNotFoundException;
import com.example.txprocessor.error.InvalidReferenceDataException;
import com.example.txprocessor.error.ProcessingException;
import com.example.txprocessor.error.TransientProcessingException;
import com.example.txprocessor.processing.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    @Test
    @DisplayName("Transient failures are retried until the budget is spent, then never again")
    void transientFailuresAreBoundedByMaxRetries() {
        RetryPolicy policy = policy(3, Duration.ofSeconds(1), 3.0, Duration.ofMinutes(5));
        ProcessingException failure = new CacheUnavailableException("timeout", null);

        assertThat(policy.shouldRetry(failure, 0)).isTrue();
        assertThat(policy.shouldRetry(failure, 1)).isTrue();
        assertThat(policy.shouldRetry(failure, 2)).isTrue();
        // The budget is exhausted: attempt 4 has happened, there is no attempt 5.
        assertThat(policy.shouldRetry(failure, 3)).isFalse();
        assertThat(policy.shouldRetry(failure, 99)).isFalse();
    }

    @Test
    @DisplayName("Permanent failures are never retried, not even on the first attempt")
    void permanentFailuresAreNeverRetried() {
        RetryPolicy policy = policy(3, Duration.ofSeconds(1), 3.0, Duration.ofMinutes(5));

        assertThat(policy.shouldRetry(new CardNotFoundException("card:1"), 0)).isFalse();
        assertThat(policy.shouldRetry(new InvalidReferenceDataException("bad json"), 0)).isFalse();
    }

    @Test
    @DisplayName("Backoff grows exponentially from the initial delay")
    void backoffIsExponential() {
        RetryPolicy policy = policy(5, Duration.ofSeconds(1), 3.0, Duration.ofMinutes(5));

        assertThat(policy.backoffFor(0)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.backoffFor(1)).isEqualTo(Duration.ofSeconds(3));
        assertThat(policy.backoffFor(2)).isEqualTo(Duration.ofSeconds(9));
        assertThat(policy.backoffFor(3)).isEqualTo(Duration.ofSeconds(27));
    }

    @Test
    @DisplayName("Backoff is capped, so a long lived failure does not schedule a retry next year")
    void backoffIsCapped() {
        RetryPolicy policy = policy(50, Duration.ofSeconds(1), 10.0, Duration.ofMinutes(5));

        assertThat(policy.backoffFor(20)).isEqualTo(Duration.ofMinutes(5));
        assertThat(policy.backoffFor(1000)).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    @DisplayName("max-retries=0 means exactly one attempt")
    void zeroRetriesMeansSingleAttempt() {
        RetryPolicy policy = policy(0, Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1));

        assertThat(policy.shouldRetry(new TransientProcessingException("X", "x"), 0)).isFalse();
    }

    private RetryPolicy policy(int maxRetries, Duration initial, double multiplier, Duration max) {
        ProcessorProperties properties = new ProcessorProperties();
        properties.setMaxRetries(maxRetries);
        properties.setRetryInitialDelay(initial);
        properties.setRetryMultiplier(multiplier);
        properties.setRetryMaxDelay(max);
        return new RetryPolicy(properties);
    }
}
