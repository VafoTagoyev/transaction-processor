package com.example.txprocessor.config;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.TimeoutOptions;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class RedisConfig {

    /**
     * Lettuce is used with a single shared connection rather than a connection pool.
     * Lettuce multiplexes commands from many threads onto one Netty channel, so a pool would
     * add object churn and lock contention without adding throughput for simple GET/MGET
     * traffic. The relevant bound on Redis concurrency is the worker count, not a pool size.
     *
     * REJECT_COMMANDS: when the connection is down we want to fail *fast* with an exception
     * that we classify as transient, instead of silently buffering commands until they time
     * out. Fast failure means the transaction goes back to NEW with a backoff and the worker
     * is freed, rather than the whole worker pool blocking for the command timeout.
     */
    @Bean
    public LettuceClientConfigurationBuilderCustomizer lettuceCustomizer(ProcessorProperties properties) {
        return builder -> builder
                .commandTimeout(properties.getRedis().getTimeout())
                .clientOptions(ClientOptions.builder()
                        .autoReconnect(true)
                        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                        .timeoutOptions(TimeoutOptions.enabled(properties.getRedis().getTimeout()))
                        .build());
    }

    /** Reference data is stored as plain JSON strings; no Java-specific serialization. */
    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }
}
