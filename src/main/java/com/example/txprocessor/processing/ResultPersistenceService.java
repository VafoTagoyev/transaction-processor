package com.example.txprocessor.processing;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.ProcessingResult;
import com.example.txprocessor.error.OwnershipLostException;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.outbox.TransactionProcessedEvent;
import com.example.txprocessor.repository.AccountStatisticsRepository;
import com.example.txprocessor.repository.OutboxRepository;
import com.example.txprocessor.repository.ProcessedTransactionRepository;
import com.example.txprocessor.repository.TransactionClaimRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * The single atomic step of the pipeline. Four writes, one COMMIT:
 *
 * <ol>
 *   <li>{@code transactions} PROCESSING -> PROCESSED  (fenced by the processing token)</li>
 *   <li>{@code processed_transactions} INSERT ... ON CONFLICT DO NOTHING</li>
 *   <li>{@code account_statistics} atomic UPSERT with in-database addition</li>
 *   <li>{@code outbox_events} TRANSACTION_PROCESSED</li>
 * </ol>
 *
 * <h2>Why one transaction</h2>
 * If the status update and the result insert were separate transactions, a crash between them
 * would leave a result row with the source transaction still PROCESSING. Recovery would then
 * reclaim a transaction that is in fact already done — survivable thanks to the unique
 * constraint, but the account statistics and the outbox event would either be skipped or
 * duplicated depending on the order. Committing all four together removes the intermediate
 * state entirely: after the COMMIT the whole system is consistent, before it nothing happened.
 * That is what makes the process crash-safe at <em>every</em> instruction: there is exactly one
 * instant at which the outcome changes, and PostgreSQL makes that instant atomic and durable.
 *
 * <h2>Why the order matters</h2>
 * The fenced UPDATE goes first on purpose. It takes the row lock on {@code transactions},
 * which is the row every competing worker must also touch, so it doubles as the mutual
 * exclusion point. Under READ COMMITTED a second worker's UPDATE blocks on that lock, and when
 * the first transaction commits PostgreSQL re-evaluates the second UPDATE's WHERE clause
 * against the newly committed row version: the status is no longer PROCESSING and the token no
 * longer matches, so it updates zero rows and we raise {@link OwnershipLostException}. The
 * loser's whole transaction rolls back.
 *
 * <h2>Why steps 3 and 4 are conditional</h2>
 * The insert's return value distinguishes "we produced this result" from "the result already
 * existed". Statistics and the outbox event are applied only in the first case, so a replay
 * after a crash cannot double count an account or emit a second event, even though it is free
 * to recompute the business result.
 *
 * <h2>What is deliberately outside</h2>
 * The Redis lookup and the business computation. See {@code EnrichmentService} — a network call
 * inside this transaction would hold both a HikariCP connection and the row lock for the whole
 * cache round trip, and would hold back the vacuum horizon on the hottest table in the schema.
 */
@Service
public class ResultPersistenceService {

    private static final Logger log = LoggerFactory.getLogger(ResultPersistenceService.class);

    private final TransactionClaimRepository transactionRepository;
    private final ProcessedTransactionRepository processedRepository;
    private final AccountStatisticsRepository statisticsRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final ProcessorMetrics metrics;
    private final String instanceId;

    public ResultPersistenceService(TransactionClaimRepository transactionRepository,
                                    ProcessedTransactionRepository processedRepository,
                                    AccountStatisticsRepository statisticsRepository,
                                    OutboxRepository outboxRepository,
                                    ObjectMapper objectMapper,
                                    ProcessorMetrics metrics,
                                    ProcessorProperties properties) {
        this.transactionRepository = transactionRepository;
        this.processedRepository = processedRepository;
        this.statisticsRepository = statisticsRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.instanceId = properties.getInstanceId();
    }

    /**
     * @return true if this call produced the result, false if it was already there (idempotent replay)
     * @throws OwnershipLostException if the lease was reassigned while this worker was computing;
     *                                the caller must not treat that as a failure of the transaction
     */
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED)
    public boolean persist(ClaimedTransaction claimed, ProcessingResult result) {
        Timer.Sample sample = metrics.startDbWrite();
        try {
            // 1. Fence + mutual exclusion. Zero rows means somebody else owns this now.
            boolean stillOwned = transactionRepository.markProcessed(claimed.id(), claimed.processingToken());
            if (!stillOwned) {
                throw new OwnershipLostException(claimed.id(),
                        "processing token no longer matches; the lease was reassigned or the transaction is already terminal");
            }

            // 2. Unconditional idempotency barrier.
            boolean inserted = processedRepository.insertIfAbsent(result, instanceId);
            if (!inserted) {
                // Somebody produced this result in an earlier life of the transaction. The status
                // update above is still correct and still committed: it repairs a row that was
                // PROCESSING but already had a result. Nothing else may be applied twice.
                metrics.duplicateSkipped();
                log.info("Result already existed for transaction {}; skipping side effects", claimed.id());
                return false;
            }

            // 3 + 4. Exactly-once side effects, guarded by the insert above.
            if (result.account() != null && !result.account().isBlank()) {
                statisticsRepository.addTransaction(result.account(), result.amount(), result.commission());
            }
            outboxRepository.insert(UUID.randomUUID(), result.transactionId(),
                    TransactionProcessedEvent.TYPE, buildPayload(result));

            return true;
        } finally {
            metrics.recordDbWrite(sample);
        }
    }

    private String buildPayload(ProcessingResult result) {
        TransactionProcessedEvent event = new TransactionProcessedEvent(
                result.transactionId(),
                result.externalId(),
                result.account(),
                result.clientId(),
                result.amount(),
                result.commission(),
                result.operationType().name(),
                instanceId,
                Instant.now());
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            // Serialising a record of primitives cannot fail; if it somehow does, failing the
            // transaction is correct - an outbox that silently drops events is worse than a retry.
            throw new IllegalStateException("Failed to serialise outbox payload for " + result.transactionId(), e);
        }
    }
}
