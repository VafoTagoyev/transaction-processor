package com.example.txprocessor.api;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.metrics.ProcessorMetrics;
import com.example.txprocessor.processing.ProcessingPipeline;
import com.example.txprocessor.processing.ProcessingQueue;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Liveness of the pipeline itself, in addition to the datasource and Redis indicators Spring
 * Boot contributes automatically. A container whose poller thread has died must fail its health
 * check, otherwise the orchestrator keeps routing it work it will never do.
 */
@Component
@ConditionalOnProperty(prefix = "processor", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ProcessorHealthIndicator implements HealthIndicator {

    private final ProcessingPipeline pipeline;
    private final ProcessingQueue queue;
    private final ProcessorMetrics metrics;
    private final ProcessorProperties properties;

    public ProcessorHealthIndicator(ProcessingPipeline pipeline,
                                    ProcessingQueue queue,
                                    ProcessorMetrics metrics,
                                    ProcessorProperties properties) {
        this.pipeline = pipeline;
        this.queue = queue;
        this.metrics = metrics;
        this.properties = properties;
    }

    @Override
    public Health health() {
        Health.Builder builder = pipeline.isRunning() ? Health.up() : Health.down();
        return builder
                .withDetail("instanceId", properties.getInstanceId())
                .withDetail("workers", properties.getWorkers())
                .withDetail("activeWorkers", metrics.activeWorkers())
                .withDetail("queueSize", queue.size())
                .withDetail("queueCapacity", queue.capacity())
                .withDetail("inFlight", metrics.inFlight())
                .withDetail("processed", metrics.processedCount())
                .withDetail("errors", metrics.errorCount())
                .withDetail("recovered", metrics.recoveredCount())
                .build();
    }
}
