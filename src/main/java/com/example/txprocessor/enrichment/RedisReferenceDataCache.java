package com.example.txprocessor.enrichment;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.error.CacheUnavailableException;
import com.example.txprocessor.metrics.ProcessorMetrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Redis/Valkey implementation.
 *
 * <p><b>Key design.</b> Flat string keys {@code card:{cardId}} and {@code terminal:{terminalId}}
 * holding plain JSON. Flat keys are O(1), shard cleanly across a Redis Cluster (each key hashes
 * independently), and can be given a per-entity TTL. A single hash per entity type would defeat
 * clustering and make TTL per entity impossible.
 *
 * <p><b>Serialization.</b> JSON as UTF-8 strings, deserialized with Jackson ignoring unknown
 * fields. No Java-specific serializer, so reference data can be written by any producer and the
 * cache survives a refactoring of the Java records.
 *
 * <p><b>TTL.</b> Reference data is long lived and is treated as a source of truth for enrichment,
 * so the generator writes it without expiry by default; the TTL is configurable for deployments
 * where a separate system refreshes the cache. A short TTL combined with
 * {@code retry-on-cache-miss=false} would turn an expired key into a permanent ERROR, which is
 * why the two settings are documented together.
 *
 * <p><b>One round trip.</b> Card and terminal are fetched with a single MGET. This halves the
 * network latency per transaction versus two GETs and keeps failure isolation per transaction
 * (unlike pipelining a whole batch, where one slow response stalls every transaction in it).
 *
 * <p><b>Two-level retry.</b> A single fast in-process retry absorbs a sub-second blip without
 * touching the database. Anything worse falls through to the durable, DB-backed retry so the
 * worker is released and the backlog is not held in memory.
 */
@Component
public class RedisReferenceDataCache implements ReferenceDataCache {

    private static final Logger log = LoggerFactory.getLogger(RedisReferenceDataCache.class);

    private final StringRedisTemplate redisTemplate;
    private final ProcessorProperties.Redis properties;
    private final ProcessorMetrics metrics;

    public RedisReferenceDataCache(StringRedisTemplate redisTemplate,
                                   ProcessorProperties properties,
                                   ProcessorMetrics metrics) {
        this.redisTemplate = redisTemplate;
        this.properties = properties.getRedis();
        this.metrics = metrics;
    }

    @Override
    public List<String> multiGet(List<String> keys) {
        int attempts = properties.getImmediateRetries() + 1;
        RuntimeException lastFailure = null;

        for (int attempt = 1; attempt <= attempts; attempt++) {
            Timer.Sample sample = metrics.startRedisLookup();
            try {
                List<String> values = redisTemplate.opsForValue().multiGet(keys);
                metrics.recordRedisLookup(sample);
                return values == null ? Collections.nCopies(keys.size(), null) : values;
            } catch (QueryTimeoutException | RedisConnectionFailureException | RedisSystemException e) {
                metrics.recordRedisLookup(sample);
                lastFailure = e;
                metrics.cacheFailure();
                if (attempt < attempts) {
                    log.debug("Cache lookup attempt {}/{} failed, retrying immediately: {}",
                            attempt, attempts, e.toString());
                    sleepQuietly();
                }
            }
        }

        throw new CacheUnavailableException(
                "Cache lookup failed after " + attempts + " immediate attempts for keys " + keys, lastFailure);
    }

    private void sleepQuietly() {
        try {
            Thread.sleep(properties.getImmediateRetryDelay().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheUnavailableException("Interrupted while retrying cache lookup", e);
        }
    }
}
