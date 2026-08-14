package com.example.txprocessor.enrichment;

import java.util.List;

/**
 * Narrow port over the cache. Keeping it to one method makes the failure surface explicit and
 * lets unit tests drive timeouts and misses without a Redis instance.
 */
public interface ReferenceDataCache {

    /**
     * @return values positionally aligned with {@code keys}; a null element means the key is absent.
     * @throws com.example.txprocessor.error.CacheUnavailableException on timeout or connection failure
     */
    List<String> multiGet(List<String> keys);
}
