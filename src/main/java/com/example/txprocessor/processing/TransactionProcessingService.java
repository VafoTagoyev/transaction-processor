package com.example.txprocessor.processing;

import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.EnrichedTransaction;
import com.example.txprocessor.domain.ProcessingResult;
import com.example.txprocessor.enrichment.EnrichmentService;
import com.example.txprocessor.error.ErrorClassifier;
import com.example.txprocessor.error.OwnershipLostException;
import com.example.txprocessor.error.ProcessingException;
import com.example.txprocessor.logging.LogContext;
import com.example.txprocessor.logging.LogMasking;
import com.example.txprocessor.metrics.ProcessorMetrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Processes exactly one claimed transaction, and never throws.
 *
 * <p>The three phases are deliberately separated by their transactional cost:
 * <pre>
 *   phase 1  enrich   network I/O, no database transaction, no locks held
 *   phase 2  classify pure computation
 *   phase 3  persist  one short database transaction, fenced, atomic
 * </pre>
 */
@Service
public class TransactionProcessingService {

    private static final Logger log = LoggerFactory.getLogger(TransactionProcessingService.class);

    private final EnrichmentService enrichmentService;
    private final BusinessClassifier classifier;
    private final ResultPersistenceService persistenceService;
    private final FailureHandler failureHandler;
    private final ErrorClassifier errorClassifier;
    private final OwnershipRegistry ownershipRegistry;
    private final ProcessorMetrics metrics;

    public TransactionProcessingService(EnrichmentService enrichmentService,
                                        BusinessClassifier classifier,
                                        ResultPersistenceService persistenceService,
                                        FailureHandler failureHandler,
                                        ErrorClassifier errorClassifier,
                                        OwnershipRegistry ownershipRegistry,
                                        ProcessorMetrics metrics) {
        this.enrichmentService = enrichmentService;
        this.classifier = classifier;
        this.persistenceService = persistenceService;
        this.failureHandler = failureHandler;
        this.errorClassifier = errorClassifier;
        this.ownershipRegistry = ownershipRegistry;
        this.metrics = metrics;
    }

    public void process(ClaimedTransaction transaction) {
        Timer.Sample sample = metrics.start();
        metrics.workerStarted();

        try (LogContext ignored = LogContext.forTransaction(transaction.id(), transaction.externalId())) {
            EnrichedTransaction enriched = enrichmentService.enrich(transaction);
            ProcessingResult result = classifier.classify(enriched);

            // Cheap pre-check: if the heartbeat already told us the lease is gone, don't bother
            // opening a database transaction. This is an optimisation only - the authoritative
            // check is the fenced UPDATE inside persist().
            if (ownershipRegistry.isRevoked(transaction.id())) {
                metrics.ownershipLost();
                log.warn("Abandoning transaction: lease revoked before persistence (card={})",
                        LogMasking.mask(transaction.cardId()));
                return;
            }

            boolean created = persistenceService.persist(transaction, result);
            if (created) {
                metrics.processed();
                log.debug("Processed as {} with commission {} (card={})",
                        result.operationType(), result.commission(), LogMasking.mask(transaction.cardId()));
            }
        } catch (OwnershipLostException e) {
            // Expected outcome of the slow-worker race. Not a failure: the current owner is
            // responsible for this transaction now, and nothing was written by us.
            metrics.ownershipLost();
            log.warn("Discarded result: {}", e.getMessage());
        } catch (Exception e) {
            ProcessingException classified = errorClassifier.classify(e);
            failureHandler.handle(transaction, classified);
        } finally {
            metrics.workerFinished();
            metrics.inFlightRemoved();
            metrics.recordProcessing(sample);
            ownershipRegistry.release(transaction.id());
        }
    }
}
