package com.example.txprocessor.error;

/**
 * A cache miss when {@code processor.redis.retry-on-cache-miss=true}: the deployment expects
 * the cache to be filled asynchronously, so an absent key is treated as "not yet" rather than
 * "never". Bounded by max-retries like every other transient failure.
 */
public class CacheMissRetryableException extends TransientProcessingException {

    public static final String CODE = "CACHE_MISS_RETRYABLE";

    public CacheMissRetryableException(String key) {
        super(CODE, "Reference data not yet present for " + key);
    }
}
