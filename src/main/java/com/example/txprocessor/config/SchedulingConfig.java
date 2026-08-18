package com.example.txprocessor.config;

import com.example.txprocessor.outbox.OutboxRelay;
import com.example.txprocessor.recovery.LeaseRenewalService;
import com.example.txprocessor.recovery.StaleProcessingRecoveryService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.time.Duration;

/**
 * Registers the three periodic tasks programmatically instead of with {@code @Scheduled}.
 *
 * <p>This is forced by a format mismatch, not chosen as a style. Every duration in
 * application.yml is written in Spring Boot's relaxed form ({@code 500ms}, {@code 30s},
 * {@code 10m}), which is what {@code @ConfigurationProperties} binding into {@link Duration}
 * accepts. {@code @Scheduled(fixedDelayString = ...)} does not use that converter: it takes a
 * plain millisecond count or an ISO-8601 string ({@code PT30S}) and nothing else. So the very
 * same {@code 30s} that binds correctly into {@link ProcessorProperties} threw
 * NumberFormatException from the annotation and killed the context at startup.
 *
 * <p>Reading the already-bound {@link Duration} values here keeps one time format across the
 * whole configuration surface, keeps every {@code *_INTERVAL} environment override working, and
 * makes the intervals type-safe rather than strings re-parsed at startup.
 *
 * <p>All three beans are {@code @ConditionalOnProperty}, so each is resolved through an
 * {@link ObjectProvider} and contributes no task at all when it is switched off — a disabled
 * component must not leave a scheduled stub behind. Tasks run on the auto-configured
 * TaskScheduler ({@code spring.task.scheduling.pool.size: 3}), one thread available to each.
 */
@Configuration(proxyBeanMethods = false)
public class SchedulingConfig implements SchedulingConfigurer {

    private final ProcessorProperties properties;
    private final ObjectProvider<OutboxRelay> outboxRelay;
    private final ObjectProvider<LeaseRenewalService> leaseRenewalService;
    private final ObjectProvider<StaleProcessingRecoveryService> staleRecoveryService;

    public SchedulingConfig(ProcessorProperties properties,
                            ObjectProvider<OutboxRelay> outboxRelay,
                            ObjectProvider<LeaseRenewalService> leaseRenewalService,
                            ObjectProvider<StaleProcessingRecoveryService> staleRecoveryService) {
        this.properties = properties;
        this.outboxRelay = outboxRelay;
        this.leaseRenewalService = leaseRenewalService;
        this.staleRecoveryService = staleRecoveryService;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        // No initial delay: events left PENDING by a previous run should drain immediately,
        // matching the annotation this replaces, which specified no initialDelayString.
        outboxRelay.ifAvailable(relay -> registrar.addFixedDelayTask(
                new IntervalTask(
                        relay::relay,
                        properties.getOutbox().getRelayInterval(),
                        Duration.ZERO)));

        // One full interval before the first run on both sweeps, preserving the previous
        // initialDelayString: at t=0 this instance owns nothing, so an immediate pass is wasted
        // work, and for recovery an immediate sweep would also race the instances still starting.
        leaseRenewalService.ifAvailable(service -> registrar.addFixedDelayTask(
                new IntervalTask(
                        service::renewLeases,
                        properties.getLeaseRenewalInterval(),
                        properties.getLeaseRenewalInterval())));

        staleRecoveryService.ifAvailable(service -> registrar.addFixedDelayTask(
                new IntervalTask(
                        service::recoverStaleTransactions,
                        properties.getRecovery().getInterval(),
                        properties.getRecovery().getInterval())));
    }
}
