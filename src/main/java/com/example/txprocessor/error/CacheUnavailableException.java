package com.example.txprocessor.error;

/** Redis/Valkey timed out or refused the command. Always transient. */
public class CacheUnavailableException extends TransientProcessingException {

    public static final String CODE = "CACHE_UNAVAILABLE";

    public CacheUnavailableException(String message, Throwable cause) {
        super(CODE, message, cause);
    }
}
