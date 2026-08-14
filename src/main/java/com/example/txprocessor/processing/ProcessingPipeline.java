package com.example.txprocessor.processing;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.logging.LogContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Owns the start/stop order of the pipeline.
 *
 * <p>Start: workers first, then the poller, so no transaction is ever claimed before there is
 * somebody to process it. Stop: the mirror image — the poller stops claiming, then the workers
 * drain what is already claimed, then anything still queued is released back to NEW.
 *
 * <p>This makes a SIGTERM (docker stop, rolling deploy) cost nothing: no lease is left dangling
 * and no transaction waits for the processing timeout. A SIGKILL skips all of it, which is
 * precisely what the recovery mechanism exists for.
 */
@Component
@ConditionalOnProperty(prefix = "processor", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ProcessingPipeline implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ProcessingPipeline.class);

    private final TransactionPoller poller;
    private final WorkerPool workerPool;
    private final ProcessorProperties properties;

    private volatile boolean running;

    public ProcessingPipeline(TransactionPoller poller, WorkerPool workerPool, ProcessorProperties properties) {
        this.poller = poller;
        this.workerPool = workerPool;
        this.properties = properties;
    }

    @Override
    public void start() {
        LogContext.putInstanceId(properties.getInstanceId());
        log.info("Starting processing pipeline on instance {} (workers={}, batch-size={}, queue-capacity={}, "
                        + "processing-timeout={}, max-retries={})",
                properties.getInstanceId(), properties.getWorkers(), properties.getBatchSize(),
                properties.getQueueCapacity(), properties.getProcessingTimeout(), properties.getMaxRetries());
        workerPool.start();
        poller.start();
        running = true;
    }

    @Override
    public void stop() {
        log.info("Stopping processing pipeline on instance {}", properties.getInstanceId());
        poller.stop();
        workerPool.stop(properties.getShutdownGracePeriod());
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Start late and stop early relative to the infrastructure beans it depends on. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1000;
    }
}
