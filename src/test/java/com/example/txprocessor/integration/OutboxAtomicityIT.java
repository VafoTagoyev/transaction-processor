package com.example.txprocessor.integration;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.ProcessingResult;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.outbox.TransactionProcessedEvent;
import com.example.txprocessor.processing.ResultPersistenceService;
import com.example.txprocessor.repository.AccountStatisticsRepository;
import com.example.txprocessor.support.IntegrationTestBase;
import com.example.txprocessor.support.ProcessorTestHarness;
import com.example.txprocessor.support.TestFixtures;
import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Transactional Outbox (assignment section 10).
 *
 * <p>The problem the pattern solves: a service that commits to the database and then publishes to
 * a broker has two independent commit points. Crash between them and the state changed but no
 * event was ever emitted; publish first and crash before the commit and consumers react to
 * something that never happened. There is no ordering of the two that is safe, because they are
 * not one atomic operation.
 *
 * <p>The outbox makes them one: the event is a row in the same database, written by the same
 * transaction as the state change. Either both are durable or neither is. A separate relay then
 * moves rows to the broker at its own pace, at-least-once, which consumers deduplicate by event id.
 */
class OutboxAtomicityIT extends IntegrationTestBase {

    @Test
    @DisplayName("The event is committed by the same transaction as the result, statistics and status")
    void eventIsCommittedWithTheStateChange() throws Exception {
        TestFixtures fixtures = fixtures();
        fixtures.seedExternalReferenceData();
        long id = fixtures.insertNewTransaction("100002", "50002", new BigDecimal("2000.00"));

        try (ProcessorTestHarness harness = harness("outbox-1")) {
            ClaimedTransaction claim = harness.claimRepository.claimBatch("outbox-1", 1).get(0);
            harness.persistenceService.persist(claim,
                    harness.classifier.classify(harness.enrichmentService.enrich(claim)));
        }

        Map<String, Object> event = jdbcTemplate.queryForMap("SELECT * FROM outbox_events");
        assertThat(event.get("event_type")).isEqualTo(TransactionProcessedEvent.TYPE);
        assertThat(((Number) event.get("aggregate_id")).longValue()).isEqualTo(id);
        assertThat(event.get("status")).isEqualTo("PENDING");

        JsonNode payload = OBJECT_MAPPER.readTree(event.get("payload").toString());
        assertThat(payload.get("transactionId").asLong()).isEqualTo(id);
        assertThat(payload.get("operationType").asText()).isEqualTo("EXTERNAL");
        assertThat(new BigDecimal(payload.get("commission").asText())).isEqualByComparingTo("20.00");
        assertThat(payload.get("processedBy").asText()).isEqualTo("outbox-1");
    }

    @Test
    @DisplayName("If any write in the unit fails, ALL of them roll back - result, status, statistics and event")
    void aFailureAnywhereRollsBackEverything() {
        TestFixtures fixtures = fixtures();
        fixtures.seedExternalReferenceData();
        long id = fixtures.insertNewTransaction("100002", "50002", new BigDecimal("2000.00"));

        try (ProcessorTestHarness harness = harness("outbox-2")) {
            ClaimedTransaction claim = harness.claimRepository.claimBatch("outbox-2", 1).get(0);
            ProcessingResult result = harness.classifier.classify(harness.enrichmentService.enrich(claim));

            // A persistence service whose statistics step fails *after* the result insert.
            ResultPersistenceService failing = failingAtStatistics(harness);

            assertThatThrownBy(() -> failing.persist(claim, result))
                    .isInstanceOf(DataIntegrityViolationException.class);

            // Nothing at all survived the rollback, including the status transition.
            assertThat(fixtures.countProcessedResults()).isZero();
            assertThat(fixtures.countOutboxEvents()).isZero();
            assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM account_statistics", Long.class)).isZero();
            assertThat(fixtures.statusOf(id)).isEqualTo("PROCESSING");

            // And the transaction is still owned by this worker, so a normal retry finishes it.
            assertThat(harness.persistenceService.persist(claim, result)).isTrue();
        }

        assertThat(fixtures.countProcessedResults()).isEqualTo(1);
        assertThat(fixtures.countOutboxEvents()).isEqualTo(1);
        assertThat(fixtures.statusOf(id)).isEqualTo("PROCESSED");
    }

    @Test
    @DisplayName("The relay publishes pending events and marks them PUBLISHED")
    void relayDrainsPendingEvents() {
        TestFixtures fixtures = fixtures();
        fixtures.seedInternalReferenceData();
        fixtures.insertNewTransactions(50, "100001", "50001", new BigDecimal("100.00"));

        try (ProcessorTestHarness harness = harness("outbox-3")) {
            harness.claimRepository.claimBatch("outbox-3", 50)
                    .forEach(claim -> harness.persistenceService.persist(claim,
                            harness.classifier.classify(harness.enrichmentService.enrich(claim))));

            assertThat(harness.outboxRepository.countByStatus("PENDING")).isEqualTo(50);

            harness.outboxRelay.relay();

            assertThat(harness.outboxRepository.countByStatus("PENDING")).isZero();
            assertThat(harness.outboxRepository.countByStatus("PUBLISHED")).isEqualTo(50);
        }

        Long unpublished = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE published_at IS NULL", Long.class);
        assertThat(unpublished).isZero();
        assertThat(fixtures.countOutboxEvents()).isEqualTo(50);
    }

    /** Replaces only the statistics step with one that fails, leaving the rest of the unit intact. */
    private ResultPersistenceService failingAtStatistics(ProcessorTestHarness harness) {
        AccountStatisticsRepository failingStatistics = new AccountStatisticsRepository(jdbcTemplate) {
            @Override
            public void addTransaction(String account, BigDecimal amount, BigDecimal commission) {
                throw new DataIntegrityViolationException("simulated failure after the result insert");
            }
        };
        ResultPersistenceService target = new ResultPersistenceService(
                harness.claimRepository,
                harness.processedRepository,
                failingStatistics,
                harness.outboxRepository,
                OBJECT_MAPPER,
                new ProcessorMetrics(new SimpleMeterRegistry()),
                harness.properties);

        ProxyFactory proxyFactory = new ProxyFactory(target);
        proxyFactory.setProxyTargetClass(true);
        proxyFactory.addAdvice(new TransactionInterceptor(transactionManager,
                new AnnotationTransactionAttributeSource()));
        return (ResultPersistenceService) proxyFactory.getProxy(getClass().getClassLoader());
    }

    private ProcessorTestHarness harness(String instanceId) {
        ProcessorProperties properties = ProcessorTestHarness.properties(instanceId);
        return new ProcessorTestHarness(jdbcTemplate, transactionManager, redisTemplate, OBJECT_MAPPER, properties);
    }
}
