package com.example.txprocessor.support;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.enrichment.EnrichmentService;
import com.example.txprocessor.enrichment.RedisReferenceDataCache;
import com.example.txprocessor.error.ErrorClassifier;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.outbox.LoggingOutboxPublisher;
import com.example.txprocessor.outbox.OutboxRelay;
import com.example.txprocessor.processing.BusinessClassifier;
import com.example.txprocessor.processing.ClaimReleaser;
import com.example.txprocessor.processing.CommissionCalculator;
import com.example.txprocessor.processing.FailureHandler;
import com.example.txprocessor.processing.OwnershipRegistry;
import com.example.txprocessor.processing.ProcessingQueue;
import com.example.txprocessor.processing.ResultPersistenceService;
import com.example.txprocessor.processing.RetryPolicy;
import com.example.txprocessor.processing.TransactionPoller;
import com.example.txprocessor.processing.TransactionProcessingService;
import com.example.txprocessor.processing.WorkerPool;
import com.example.txprocessor.recovery.LeaseRenewalService;
import com.example.txprocessor.recovery.StaleProcessingRecoveryService;
import com.example.txprocessor.repository.AccountStatisticsRepository;
import com.example.txprocessor.repository.OutboxRepository;
import com.example.txprocessor.repository.ProcessedTransactionRepository;
import com.example.txprocessor.repository.TransactionClaimRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;

import java.time.Duration;

/**
 * One fully wired processor "instance", assembled by hand.
 *
 * <p>Because the production classes use constructor injection and hold no static state, a single
 * JVM can host several harnesses with different instance ids against one PostgreSQL, which is
 * exactly what the multi-instance and crash tests need. The only piece Spring normally supplies
 * that has to be recreated here is the {@code @Transactional} proxy around the persistence
 * service - built explicitly below so the tests exercise the same transaction boundaries as
 * production rather than a hand-rolled substitute.
 */
public class ProcessorTestHarness implements AutoCloseable {

    public final ProcessorProperties properties;
    public final ProcessorMetrics metrics;
    public final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    public final TransactionClaimRepository claimRepository;
    public final ProcessedTransactionRepository processedRepository;
    public final AccountStatisticsRepository statisticsRepository;
    public final OutboxRepository outboxRepository;

    public final EnrichmentService enrichmentService;
    public final BusinessClassifier classifier;
    public final ResultPersistenceService persistenceService;
    public final FailureHandler failureHandler;
    public final OwnershipRegistry ownershipRegistry;
    public final TransactionProcessingService processingService;
    public final ProcessingQueue queue;
    public final ClaimReleaser claimReleaser;
    public final WorkerPool workerPool;
    public final TransactionPoller poller;
    public final StaleProcessingRecoveryService recoveryService;
    public final LeaseRenewalService leaseRenewalService;
    public final OutboxRelay outboxRelay;

    public ProcessorTestHarness(JdbcTemplate jdbcTemplate,
                                PlatformTransactionManager transactionManager,
                                StringRedisTemplate redisTemplate,
                                ObjectMapper objectMapper,
                                ProcessorProperties properties) {
        this.properties = properties;
        this.metrics = new ProcessorMetrics(meterRegistry);

        this.claimRepository = new TransactionClaimRepository(jdbcTemplate);
        this.processedRepository = new ProcessedTransactionRepository(jdbcTemplate);
        this.statisticsRepository = new AccountStatisticsRepository(jdbcTemplate);
        this.outboxRepository = new OutboxRepository(jdbcTemplate);

        this.enrichmentService = new EnrichmentService(
                new RedisReferenceDataCache(redisTemplate, properties, metrics), objectMapper, properties);
        this.classifier = new BusinessClassifier(new CommissionCalculator());

        ResultPersistenceService target = new ResultPersistenceService(
                claimRepository, processedRepository, statisticsRepository, outboxRepository,
                objectMapper, metrics, properties);
        this.persistenceService = transactional(target, transactionManager, ResultPersistenceService.class);

        this.failureHandler = new FailureHandler(claimRepository, new RetryPolicy(properties), metrics);
        this.ownershipRegistry = new OwnershipRegistry();
        this.processingService = new TransactionProcessingService(
                enrichmentService, classifier, persistenceService, failureHandler,
                new ErrorClassifier(), ownershipRegistry, metrics);

        this.queue = new ProcessingQueue(properties, metrics);
        this.claimReleaser = new ClaimReleaser(claimRepository, ownershipRegistry, metrics);
        this.workerPool = new WorkerPool(queue, processingService, claimReleaser, properties);
        this.poller = new TransactionPoller(claimRepository, queue, ownershipRegistry, claimReleaser,
                metrics, properties);

        this.recoveryService = new StaleProcessingRecoveryService(claimRepository, metrics, properties);
        this.leaseRenewalService = new LeaseRenewalService(claimRepository, ownershipRegistry, properties);

        OutboxRelay relayTarget = new OutboxRelay(outboxRepository, new LoggingOutboxPublisher(), metrics, properties);
        this.outboxRelay = transactional(relayTarget, transactionManager, OutboxRelay.class);
    }

    /** Recreates Spring's {@code @Transactional} advice around a hand-constructed bean. */
    @SuppressWarnings("unchecked")
    private static <T> T transactional(T target, PlatformTransactionManager transactionManager, Class<T> type) {
        ProxyFactory proxyFactory = new ProxyFactory(target);
        proxyFactory.setProxyTargetClass(true);
        proxyFactory.addAdvice(new TransactionInterceptor(transactionManager,
                new AnnotationTransactionAttributeSource()));
        return (T) proxyFactory.getProxy(type.getClassLoader());
    }

    /** Defaults tuned for tests: short delays, short leases, small batches. */
    public static ProcessorProperties properties(String instanceId) {
        ProcessorProperties properties = new ProcessorProperties();
        properties.setInstanceId(instanceId);
        properties.setWorkers(4);
        properties.setBatchSize(100);
        properties.setQueueCapacity(500);
        properties.setPollingDelay(Duration.ofMillis(20));
        properties.setPollingIdleDelay(Duration.ofMillis(50));
        properties.setProcessingTimeout(Duration.ofSeconds(30));
        properties.setLeaseRenewalInterval(Duration.ofSeconds(5));
        properties.setMaxRetries(3);
        properties.setRetryInitialDelay(Duration.ofMillis(10));
        properties.setRetryMultiplier(2.0);
        properties.setRetryMaxDelay(Duration.ofMillis(100));
        properties.setShutdownGracePeriod(Duration.ofSeconds(10));
        properties.getRedis().setTimeout(Duration.ofSeconds(2));
        properties.getRedis().setImmediateRetries(0);
        return properties;
    }

    public void start() {
        workerPool.start();
        poller.start();
    }

    /**
      * Ordinary graceful shutdown: drain, then release whatever is still queued.
      * Deliberately unconditional - some tests start only the poller or only the workers, and
      * both threads are non-daemon, so close() must always be able to stop them.
      */
    public void stopGracefully() {
        poller.stop();
        workerPool.stop(properties.getShutdownGracePeriod());
    }

    /**
     * Simulates {@code kill -9} as closely as a single JVM allows.
     *
     * <p>The poller is stopped, everything queued is thrown away <em>without</em> being released,
     * and the workers are interrupted mid-flight with a zero grace period. What is left behind in
     * PostgreSQL is exactly what a killed container leaves behind: PROCESSING rows owned by an
     * instance that will never renew their leases and never finish them. Nothing in the database
     * is touched by this method - the point is that the cleanup has to come from recovery.
     *
     * @return how many claimed-but-unstarted transactions were abandoned
     */
    public int simulateHardCrash() {
        poller.stop();
        int abandoned = queue.drain().size();
        workerPool.stop(Duration.ZERO);
        return abandoned;
    }

    @Override
    public void close() {
        stopGracefully();
        meterRegistry.close();
    }
}
