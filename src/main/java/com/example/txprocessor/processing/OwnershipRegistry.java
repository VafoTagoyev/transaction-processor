package com.example.txprocessor.processing;

import com.example.txprocessor.repository.TransactionClaimRepository.Lease;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory view of the leases this JVM currently holds.
 *
 * <p>It is <em>not</em> a source of truth — PostgreSQL is. Losing this map in a crash costs
 * nothing, because the durable state (status, processing_started_at, processing_token) is in the
 * database and the recovery sweep works purely from that. The registry exists for two
 * cheap optimisations:
 *
 * <ol>
 *   <li>the heartbeat knows which rows to renew;</li>
 *   <li>a worker can find out that its lease was revoked <em>before</em> it does useless work,
 *       instead of only discovering it when the fenced write matches zero rows.</li>
 * </ol>
 *
 * Correctness never depends on it: the database fence is authoritative in every case.
 */
@Component
public class OwnershipRegistry {

    private final Map<Long, Entry> leases = new ConcurrentHashMap<>();

    public void register(long transactionId, UUID token) {
        leases.put(transactionId, new Entry(token));
    }

    public void release(long transactionId) {
        leases.remove(transactionId);
    }

    /**
      * True only when we positively know the lease was taken away. An id this registry has never
      * seen returns false: the registry is an optimisation, and "I do not know" must fall through
      * to the authoritative check, which is the fenced UPDATE. Answering "revoked" for an unknown
      * id would abandon perfectly good work and leave the row waiting for its lease to expire.
      */
    public boolean isRevoked(long transactionId) {
        Entry entry = leases.get(transactionId);
        return entry != null && entry.revoked;
    }

    public void revoke(long transactionId) {
        Entry entry = leases.get(transactionId);
        if (entry != null) {
            entry.revoked = true;
        }
    }

    public List<Lease> snapshot() {
        return leases.entrySet().stream()
                .filter(e -> !e.getValue().revoked)
                .map(e -> new Lease(e.getKey(), e.getValue().token))
                .toList();
    }

    /** Marks every lease from {@code attempted} that is missing from {@code renewed} as revoked. */
    public void revokeMissing(List<Lease> attempted, Set<Long> renewed) {
        attempted.stream()
                .map(Lease::id)
                .filter(id -> !renewed.contains(id))
                .forEach(this::revoke);
    }

    public int size() {
        return leases.size();
    }

    private static final class Entry {
        private final UUID token;
        private volatile boolean revoked;

        private Entry(UUID token) {
            this.token = token;
        }
    }
}
