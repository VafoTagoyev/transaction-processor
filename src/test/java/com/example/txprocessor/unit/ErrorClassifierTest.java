package com.example.txprocessor.unit;

import com.example.txprocessor.error.CardNotFoundException;
import com.example.txprocessor.error.ErrorClassifier;
import com.example.txprocessor.error.ProcessingException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;

import java.io.IOException;
import java.net.SocketTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorClassifierTest {

    private final ErrorClassifier classifier = new ErrorClassifier();

    @Test
    @DisplayName("An already classified failure is passed through unchanged")
    void processingExceptionsArePassedThrough() {
        CardNotFoundException original = new CardNotFoundException("card:1");

        assertThat(classifier.classify(original)).isSameAs(original);
    }

    @Test
    @DisplayName("Database timeouts and lock contention are transient")
    void databaseProblemsAreTransient() {
        assertThat(classifier.classify(new QueryTimeoutException("timeout")).transientFailure()).isTrue();
        assertThat(classifier.classify(new CannotAcquireLockException("lock")).transientFailure()).isTrue();
    }

    @Test
    @DisplayName("Network failures are transient")
    void networkProblemsAreTransient() {
        assertThat(classifier.classify(new SocketTimeoutException("read timed out")).transientFailure()).isTrue();
        assertThat(classifier.classify(new IOException("connection reset")).transientFailure()).isTrue();
    }

    @Test
    @DisplayName("Constraint violations are permanent: the same data will violate it again")
    void integrityViolationsArePermanent() {
        ProcessingException classified = classifier.classify(new DataIntegrityViolationException("duplicate key"));

        assertThat(classified.transientFailure()).isFalse();
        assertThat(classified.errorCode()).isEqualTo("DATA_INTEGRITY");
    }

    @Test
    @DisplayName("An unrecognised failure defaults to transient, so a blip never silently drops work")
    void unknownFailuresDefaultToTransient() {
        ProcessingException classified = classifier.classify(new IllegalStateException("something odd"));

        assertThat(classified.transientFailure()).isTrue();
        assertThat(classified.errorCode()).isEqualTo("UNKNOWN");
        // Still bounded: RetryPolicy caps the number of attempts regardless of classification.
    }

    @Test
    @DisplayName("The classified message keeps the root cause, not just the wrapper")
    void messageCarriesRootCause() {
        ProcessingException classified = classifier.classify(
                new IllegalStateException("outer", new IllegalArgumentException("root cause here")));

        assertThat(classified.getMessage()).contains("root cause here");
    }
}
