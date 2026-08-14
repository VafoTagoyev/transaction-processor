package com.example.txprocessor.recovery;

import com.example.txprocessor.config.ProcessorProperties;
import com.example.txprocessor.logging.LogContext;
import com.example.txprocessor.processing.OwnershipRegistry;
import com.example.txprocessor.repository.TransactionClaimRepository;
import com.example.txprocessor.repository.TransactionClaimRepository.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

/**
 * Heartbeat for owned transactions.
 *
 * <p>{@code processing_started_at} is not "when processing began" in the literal sense — it is
 * "when this lease was last confirmed alive". Pushing it forward at a fraction of the
 * processing-timeout means a healthy instance is never mistaken for a dead one, no matter how
 * long a particular transaction takes or how deep the queue is. That converts the stale-recovery
 * race from something that happens routinely under load into something that only happens when a
 * process really has stopped.
 *
 * <p>The renewal is fenced too: only rows whose token still matches are renewed. Anything the
 * database refuses to renew has been taken away, and the registry marks it revoked so the worker
 * holding it can abandon early instead of computing a result it will not be allowed to commit.
 *
 * <p>One statement renews the whole set, so the heartbeat costs one round trip per interval
 * regardless of how many transactions the instance owns.
 */
@Service
@ConditionalOnProperty(prefix = "processor", name = "enabled", havingValue = "true", matchIfMissing = true)
public class LeaseRenewalService {

    private static final Logger log = LoggerFactory.getLogger(LeaseRenewalService.class);

    private final TransactionClaimRepository repository;
    private final OwnershipRegistry ownershipRegistry;
    private final ProcessorProperties properties;

    public LeaseRenewalService(TransactionClaimRepository repository,
                               OwnershipRegistry ownershipRegistry,
                               ProcessorProperties properties) {
        this.repository = repository;
        this.ownershipRegistry = ownershipRegistry;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${processor.lease-renewal-interval:30s}",
            initialDelayString = "${processor.lease-renewal-interval:30s}")
    public void renewLeases() {
        try {
            renewOnce();
        } catch (RuntimeException e) {
            // Losing a heartbeat is survivable: the leases simply expire and recovery reclaims
            // them. Failing loudly here and stopping would be much worse.
            log.error("Lease renewal failed; leases may expire and be reclaimed", e);
        }
    }

    /** @return the number of leases successfully renewed. */
    public int renewOnce() {
        LogContext.putInstanceId(properties.getInstanceId());

        List<Lease> held = ownershipRegistry.snapshot();
        if (held.isEmpty()) {
            return 0;
        }

        Set<Long> renewed = repository.renewLeases(held);
        ownershipRegistry.revokeMissing(held, renewed);

        int lost = held.size() - renewed.size();
        if (lost > 0) {
            log.warn("{} of {} leases could not be renewed and were revoked; those transactions "
                    + "now belong to another instance and will be abandoned locally", lost, held.size());
        } else {
            log.debug("Renewed {} leases", renewed.size());
        }
        return renewed.size();
    }
}
