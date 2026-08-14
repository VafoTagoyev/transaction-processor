package com.example.txprocessor.error;

/** Infrastructure hiccup: cache timeout, connection reset, database timeout, deadlock victim. */
public class TransientProcessingException extends ProcessingException {

    private final String errorCode;

    public TransientProcessingException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public TransientProcessingException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    @Override
    public boolean transientFailure() {
        return true;
    }

    @Override
    public String errorCode() {
        return errorCode;
    }
}
