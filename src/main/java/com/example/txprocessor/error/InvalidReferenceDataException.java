package com.example.txprocessor.error;

/** Reference data exists but is unusable: malformed JSON, or a mandatory field is absent. */
public class InvalidReferenceDataException extends PermanentProcessingException {

    public static final String CODE = "INVALID_REFERENCE_DATA";

    public InvalidReferenceDataException(String message) {
        super(CODE, message);
    }

    public InvalidReferenceDataException(String message, Throwable cause) {
        super(CODE, message, cause);
    }
}
