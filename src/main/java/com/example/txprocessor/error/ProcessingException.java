package com.example.txprocessor.error;

/**
 * Root of the processing error hierarchy. The only thing the retry logic needs to know about
 * a failure is whether retrying it can plausibly succeed, so that decision is encoded in the
 * type rather than inspected with instanceof chains at the call site.
 */
public abstract class ProcessingException extends RuntimeException {

    protected ProcessingException(String message) {
        super(message);
    }

    protected ProcessingException(String message, Throwable cause) {
        super(message, cause);
    }

    /** True if another attempt may succeed without any external intervention. */
    public abstract boolean transientFailure();

    /** Short, stable code used as a metric tag and as the error_message prefix. */
    public abstract String errorCode();
}
