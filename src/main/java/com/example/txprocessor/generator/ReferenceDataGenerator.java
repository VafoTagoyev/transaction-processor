package com.example.txprocessor.generator;

import com.example.txprocessor.domain.CardInfo;
import com.example.txprocessor.domain.TerminalInfo;
import com.example.txprocessor.enrichment.EnrichmentService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.function.LongFunction;

/**
 * Writes card and terminal reference data into Redis/Valkey with pipelining, so 110 000 keys
 * cost one round trip per batch instead of 110 000 round trips.
 *
 * <p>Bank codes are assigned round-robin over a small set, which gives the transaction generator
 * an exact and cheap way to hit the required 80/20 INTERNAL/EXTERNAL split: card {@code i} and
 * terminal {@code j} share a bank code if and only if {@code i % N == j % N}.
 */
@Component
public class ReferenceDataGenerator {

    private static final Logger log = LoggerFactory.getLogger(ReferenceDataGenerator.class);

    /** Card ids run from this base upwards, matching the assignment's card:100001 example. */
    public static final long CARD_ID_BASE = 100_000L;
    public static final long TERMINAL_ID_BASE = 50_000L;

    // Single source of truth for the key layout: whatever the reader expects, the writer writes.
    public static final String CARD_KEY_PREFIX = EnrichmentService.CARD_KEY_PREFIX;
    public static final String TERMINAL_KEY_PREFIX = EnrichmentService.TERMINAL_KEY_PREFIX;

    public static final String[] BANK_CODES = {"00444", "00445", "00446", "00447", "00448"};

    private static final String[] PRODUCTS = {"VISA_GOLD", "VISA_CLASSIC", "MC_STANDARD", "MC_WORLD", "MIR_CLASSIC"};
    private static final String[] TERMINAL_TYPES = {"POS", "ATM", "ECOM"};

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public ReferenceDataGenerator(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    public static String cardId(long index) {
        return Long.toString(CARD_ID_BASE + index);
    }

    public static String terminalId(long index) {
        return Long.toString(TERMINAL_ID_BASE + index);
    }

    public static String bankCodeFor(long index) {
        return BANK_CODES[(int) Math.floorMod(index, BANK_CODES.length)];
    }

    public static CardInfo cardInfo(long index) {
        return new CardInfo(
                Long.toString(10_000L + index),
                String.format("202080001%011d", index),
                PRODUCTS[(int) Math.floorMod(index, PRODUCTS.length)],
                bankCodeFor(index));
    }

    public static TerminalInfo terminalInfo(long index) {
        return new TerminalInfo(
                String.format("M%04d", 1000 + Math.floorMod(index, 9000)),
                String.format("%03d", Math.floorMod(index, 999) + 1),
                TERMINAL_TYPES[(int) Math.floorMod(index, TERMINAL_TYPES.length)],
                bankCodeFor(index));
    }

    public void generate(GeneratorProperties properties) {
        write(CARD_KEY_PREFIX, CARD_ID_BASE, properties.getCards(), properties,
                index -> serialize(cardInfo(index)));
        write(TERMINAL_KEY_PREFIX, TERMINAL_ID_BASE, properties.getTerminals(), properties,
                index -> serialize(terminalInfo(index)));
    }

    private void write(String keyPrefix, long idBase, int count,
                       GeneratorProperties properties, LongFunction<String> valueFactory) {
        long ttlSeconds = properties.getReferenceDataTtl().toSeconds();
        int batchSize = properties.getBatchSize();
        long start = System.nanoTime();

        for (int offset = 0; offset < count; offset += batchSize) {
            final int from = offset;
            final int to = Math.min(offset + batchSize, count);
            redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                for (int index = from; index < to; index++) {
                    byte[] key = (keyPrefix + (idBase + index)).getBytes(StandardCharsets.UTF_8);
                    byte[] value = valueFactory.apply(index).getBytes(StandardCharsets.UTF_8);
                    if (ttlSeconds > 0) {
                        connection.stringCommands().setEx(key, ttlSeconds, value);
                    } else {
                        connection.stringCommands().set(key, value);
                    }
                }
                return null;
            });
        }

        log.info("Generated {} '{}' keys in {} ms (ttl={})",
                count, keyPrefix, (System.nanoTime() - start) / 1_000_000,
                ttlSeconds == 0 ? "none" : ttlSeconds + "s");
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialise reference data", e);
        }
    }
}
