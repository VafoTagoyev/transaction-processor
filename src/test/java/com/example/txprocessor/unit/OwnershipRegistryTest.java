package com.example.txprocessor.unit;

import com.example.txprocessor.processing.OwnershipRegistry;
import com.example.txprocessor.repository.TransactionClaimRepository.Lease;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OwnershipRegistryTest {

    @Test
    @DisplayName("An unknown transaction is NOT reported as revoked: the DB fence decides, not this cache")
    void unknownTransactionsAreNotReportedAsRevoked() {
        OwnershipRegistry registry = new OwnershipRegistry();

        // The registry only ever answers "yes, it was definitely taken away". Anything it has not
        // seen must fall through to the fenced UPDATE, otherwise good work would be abandoned.
        assertThat(registry.isRevoked(42L)).isFalse();
    }

    @Test
    @DisplayName("Registered leases are live until explicitly revoked")
    void registeredLeasesAreLive() {
        OwnershipRegistry registry = new OwnershipRegistry();
        registry.register(42L, UUID.randomUUID());

        assertThat(registry.isRevoked(42L)).isFalse();

        registry.revoke(42L);
        assertThat(registry.isRevoked(42L)).isTrue();
    }

    @Test
    @DisplayName("Leases the database refused to renew are revoked locally")
    void unrenewedLeasesAreRevoked() {
        OwnershipRegistry registry = new OwnershipRegistry();
        UUID tokenA = UUID.randomUUID();
        UUID tokenB = UUID.randomUUID();
        registry.register(1L, tokenA);
        registry.register(2L, tokenB);

        List<Lease> attempted = registry.snapshot();
        registry.revokeMissing(attempted, Set.of(1L));

        assertThat(registry.isRevoked(1L)).isFalse();
        assertThat(registry.isRevoked(2L)).isTrue();
    }

    @Test
    @DisplayName("Revoked leases are excluded from the next heartbeat")
    void snapshotExcludesRevokedLeases() {
        OwnershipRegistry registry = new OwnershipRegistry();
        registry.register(1L, UUID.randomUUID());
        registry.register(2L, UUID.randomUUID());
        registry.revoke(2L);

        assertThat(registry.snapshot()).extracting(Lease::id).containsExactly(1L);
    }

    @Test
    @DisplayName("Release removes the lease entirely so the map cannot grow without bound")
    void releaseRemovesTheEntry() {
        OwnershipRegistry registry = new OwnershipRegistry();
        registry.register(1L, UUID.randomUUID());
        registry.release(1L);

        assertThat(registry.size()).isZero();
    }
}
