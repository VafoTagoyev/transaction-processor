package com.example.txprocessor.enrichment;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.CardInfo;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.EnrichedTransaction;
import com.example.txprocessor.domain.TerminalInfo;
import com.example.txprocessor.error.CacheMissRetryableException;
import com.example.txprocessor.error.CardNotFoundException;
import com.example.txprocessor.error.InvalidReferenceDataException;
import com.example.txprocessor.error.InvalidTransactionException;
import com.example.txprocessor.error.TerminalNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Resolves {@code card:{cardId}} and {@code terminal:{terminalId}} for a claimed transaction.
 *
 * <p><b>This runs entirely outside any database transaction.</b> A cache call is a network
 * round trip with a tail latency measured in hundreds of milliseconds under stress; holding a
 * PostgreSQL transaction open across it would pin a HikariCP connection, hold the row lock
 * taken by the claim, and — worse — hold back the transaction horizon so that vacuum cannot
 * clean up dead tuples on the hottest table in the system. The pipeline is therefore split
 * into three phases: short DB transaction (claim) -> no transaction (enrich + compute) ->
 * short DB transaction (persist). See docs/concurrency.md.
 */
@Service
public class EnrichmentService {

    public static final String CARD_KEY_PREFIX = "card:";
    public static final String TERMINAL_KEY_PREFIX = "terminal:";

    private final ReferenceDataCache cache;
    private final ObjectMapper objectMapper;
    private final boolean retryOnCacheMiss;

    public EnrichmentService(ReferenceDataCache cache, ObjectMapper objectMapper, ProcessorProperties properties) {
        this.cache = cache;
        this.objectMapper = objectMapper;
        this.retryOnCacheMiss = properties.getRedis().isRetryOnCacheMiss();
    }

    public EnrichedTransaction enrich(ClaimedTransaction transaction) {
        requireIdentifier(transaction.cardId(), "card_id");
        requireIdentifier(transaction.terminalId(), "terminal_id");

        String cardKey = CARD_KEY_PREFIX + transaction.cardId();
        String terminalKey = TERMINAL_KEY_PREFIX + transaction.terminalId();

        List<String> values = cache.multiGet(List.of(cardKey, terminalKey));
        String cardJson = values.get(0);
        String terminalJson = values.get(1);

        if (cardJson == null) {
            throw retryOnCacheMiss ? new CacheMissRetryableException(cardKey) : new CardNotFoundException(cardKey);
        }
        if (terminalJson == null) {
            throw retryOnCacheMiss ? new CacheMissRetryableException(terminalKey) : new TerminalNotFoundException(terminalKey);
        }

        CardInfo card = parse(cardJson, CardInfo.class, cardKey);
        TerminalInfo terminal = parse(terminalJson, TerminalInfo.class, terminalKey);

        return new EnrichedTransaction(transaction, card, terminal);
    }

    private <T> T parse(String json, Class<T> type, String key) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            // Malformed reference data will be malformed on every retry: permanent by construction.
            throw new InvalidReferenceDataException("Malformed reference data for " + key, e);
        }
    }

    private void requireIdentifier(String value, String column) {
        if (value == null || value.isBlank()) {
            throw new InvalidTransactionException("Transaction has no " + column);
        }
    }
}
