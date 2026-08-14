package com.example.txprocessor.unit;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.EnrichedTransaction;
import com.example.txprocessor.enrichment.EnrichmentService;
import com.example.txprocessor.enrichment.ReferenceDataCache;
import com.example.txprocessor.error.CacheMissRetryableException;
import com.example.txprocessor.error.CacheUnavailableException;
import com.example.txprocessor.error.CardNotFoundException;
import com.example.txprocessor.error.InvalidReferenceDataException;
import com.example.txprocessor.error.InvalidTransactionException;
import com.example.txprocessor.error.TerminalNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnrichmentServiceTest {

    private static final String CARD_JSON = """
            {"clientId":"12345","account":"20208000123456789001","productId":"VISA_GOLD","bankCode":"00444"}
            """;
    private static final String TERMINAL_JSON = """
            {"merchantId":"M1001","branchCode":"001","terminalType":"POS","bankCode":"00444"}
            """;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Both keys present: card and terminal are parsed and attached")
    void enrichesFromCache() {
        EnrichmentService service = service(cache(CARD_JSON, TERMINAL_JSON), false);

        EnrichedTransaction enriched = service.enrich(transaction("100001", "50001"));

        assertThat(enriched.card().bankCode()).isEqualTo("00444");
        assertThat(enriched.card().account()).isEqualTo("20208000123456789001");
        assertThat(enriched.terminal().merchantId()).isEqualTo("M1001");
        assertThat(enriched.terminal().branchCode()).isEqualTo("001");
    }

    @Test
    @DisplayName("Card and terminal are fetched in ONE round trip, in a stable key order")
    void usesASingleMultiGet() {
        RecordingCache cache = new RecordingCache(List.of(CARD_JSON, TERMINAL_JSON));
        service(cache, false).enrich(transaction("100001", "50001"));

        assertThat(cache.calls).isEqualTo(1);
        assertThat(cache.lastKeys).containsExactly("card:100001", "terminal:50001");
    }

    @Test
    @DisplayName("Missing card is permanent by default: retrying an absent key cannot help")
    void missingCardIsPermanentByDefault() {
        EnrichmentService service = service(cache(null, TERMINAL_JSON), false);

        assertThatThrownBy(() -> service.enrich(transaction("999999999", "50001")))
                .isInstanceOf(CardNotFoundException.class)
                .satisfies(e -> assertThat(((CardNotFoundException) e).transientFailure()).isFalse());
    }

    @Test
    @DisplayName("Missing terminal is permanent by default")
    void missingTerminalIsPermanentByDefault() {
        EnrichmentService service = service(cache(CARD_JSON, null), false);

        assertThatThrownBy(() -> service.enrich(transaction("100001", "888888888")))
                .isInstanceOf(TerminalNotFoundException.class);
    }

    @Test
    @DisplayName("With retry-on-cache-miss=true a miss becomes transient, for asynchronously warmed caches")
    void missingKeyIsRetryableWhenConfigured() {
        EnrichmentService service = service(cache(null, TERMINAL_JSON), true);

        assertThatThrownBy(() -> service.enrich(transaction("100001", "50001")))
                .isInstanceOf(CacheMissRetryableException.class)
                .satisfies(e -> assertThat(((CacheMissRetryableException) e).transientFailure()).isTrue());
    }

    @Test
    @DisplayName("A cache timeout propagates as a transient failure and is therefore retried")
    void cacheTimeoutIsTransient() {
        ReferenceDataCache failing = keys -> {
            throw new CacheUnavailableException("timeout", new RuntimeException("boom"));
        };

        assertThatThrownBy(() -> service(failing, false).enrich(transaction("100001", "50001")))
                .isInstanceOf(CacheUnavailableException.class)
                .satisfies(e -> assertThat(((CacheUnavailableException) e).transientFailure()).isTrue());
    }

    @Test
    @DisplayName("Malformed reference data is permanent: it will be malformed on every retry too")
    void malformedJsonIsPermanent() {
        EnrichmentService service = service(cache("{not json", TERMINAL_JSON), false);

        assertThatThrownBy(() -> service.enrich(transaction("100001", "50001")))
                .isInstanceOf(InvalidReferenceDataException.class);
    }

    @Test
    @DisplayName("Unknown JSON fields are tolerated, so the cache producer can evolve independently")
    void unknownFieldsAreIgnored() {
        String cardWithExtras = """
                {"clientId":"1","account":"a","productId":"p","bankCode":"00444","issuedAt":"2020-01-01"}
                """;
        EnrichmentService service = service(cache(cardWithExtras, TERMINAL_JSON), false);

        assertThat(service.enrich(transaction("100001", "50001")).card().bankCode()).isEqualTo("00444");
    }

    @Test
    @DisplayName("A transaction without card_id or terminal_id never reaches the cache")
    void missingIdentifiersFailFast() {
        RecordingCache cache = new RecordingCache(List.of(CARD_JSON, TERMINAL_JSON));
        EnrichmentService service = service(cache, false);

        assertThatThrownBy(() -> service.enrich(transaction(null, "50001")))
                .isInstanceOf(InvalidTransactionException.class);
        assertThatThrownBy(() -> service.enrich(transaction("100001", " ")))
                .isInstanceOf(InvalidTransactionException.class);
        assertThat(cache.calls).isZero();
    }

    private EnrichmentService service(ReferenceDataCache cache, boolean retryOnCacheMiss) {
        ProcessorProperties properties = new ProcessorProperties();
        properties.getRedis().setRetryOnCacheMiss(retryOnCacheMiss);
        return new EnrichmentService(cache, objectMapper, properties);
    }

    private ReferenceDataCache cache(String cardJson, String terminalJson) {
        return keys -> Arrays.asList(cardJson, terminalJson);
    }

    private ClaimedTransaction transaction(String cardId, String terminalId) {
        return new ClaimedTransaction(1L, "TX-1", cardId, terminalId,
                new BigDecimal("100.00"), "UZS", "PURCHASE", 0, UUID.randomUUID());
    }

    private static final class RecordingCache implements ReferenceDataCache {
        private final List<String> values;
        private int calls;
        private List<String> lastKeys;

        private RecordingCache(List<String> values) {
            this.values = values;
        }

        @Override
        public List<String> multiGet(List<String> keys) {
            calls++;
            lastKeys = keys;
            return values;
        }
    }
}
