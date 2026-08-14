package com.example.txprocessor.error;

/** The input itself is unprocessable. Retrying identical input produces the identical failure. */
public class PermanentProcessingException extends ProcessingException {

    private final String errorCode;

    public PermanentProcessingException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public PermanentProcessingException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    @Override
    public boolean transientFailure() {
        return false;
    }

    @Override
    public String errorCode() {
        return errorCode;
    }
}
