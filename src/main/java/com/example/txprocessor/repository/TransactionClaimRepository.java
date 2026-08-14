package com.example.txprocessor.repository;

import com.example.txprocessor.domain.ClaimedTransaction;
import com.example.txprocessor.domain.TransactionStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Every state transition of the {@code transactions} table lives here, and every transition
 * out of PROCESSING is <em>fenced</em>: it carries {@code AND processing_token = ?} so that a
 * worker whose lease was revoked cannot mutate a row that now belongs to someone else.
 *
 * <p>All statements are single round-trips. There is no read-then-write anywhere in this class,
 * which is what makes the operations safe without any application level locking.
 */
@Repository
public class TransactionClaimRepository {

    /**
     * Atomic claim. One statement does candidate selection, locking and the transition to
     * PROCESSING, so there is no window in which a row is selected but not yet owned.
     *
     * <p>FOR UPDATE SKIP LOCKED inside the CTE means concurrent claimers never block each other:
     * instance B walks past the rows instance A is currently locking instead of queueing behind
     * them. The outer UPDATE then re-locks exactly those rows (already held by this transaction)
     * and stamps ownership: status, timestamp, instance and a freshly generated fencing token.
     *
     * <p>RETURNING gives us the full payload in the same round trip, so claiming a batch of
     * 1000 rows costs one statement rather than 1 + 1000.
     */
    private static final String CLAIM_BATCH = """
            WITH candidates AS (
                SELECT id
                FROM transactions
                WHERE status = 'NEW'
                  AND next_attempt_at <= now()
                ORDER BY next_attempt_at, id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE transactions t
            SET status                = 'PROCESSING',
                processing_started_at = now(),
                processing_instance   = ?,
                processing_token      = gen_random_uuid(),
                updated_at            = now()
            FROM candidates c
            WHERE t.id = c.id
            RETURNING t.id, t.external_id, t.card_id, t.terminal_id, t.amount, t.currency,
                      t.transaction_type, t.retry_count, t.processing_token
            """;

    /** Fenced success transition. Executed inside the same DB transaction as the result insert. */
    private static final String MARK_PROCESSED = """
            UPDATE transactions
            SET status           = 'PROCESSED',
                processed_at     = now(),
                processing_token = NULL,
                error_message    = NULL,
                updated_at       = now()
            WHERE id = ?
              AND status = 'PROCESSING'
              AND processing_token = ?::uuid
            """;

    /** Fenced transient-failure transition: back to NEW, visible again only after the backoff. */
    private static final String SCHEDULE_RETRY = """
            UPDATE transactions
            SET status           = 'NEW',
                retry_count      = retry_count + 1,
                next_attempt_at  = now() + (?::double precision * INTERVAL '1 second'),
                processing_token = NULL,
                error_message    = ?,
                updated_at       = now()
            WHERE id = ?
              AND status = 'PROCESSING'
              AND processing_token = ?::uuid
            """;

    /** Fenced terminal-failure transition. */
    private static final String MARK_ERROR = """
            UPDATE transactions
            SET status           = 'ERROR',
                error_message    = ?,
                processed_at     = now(),
                processing_token = NULL,
                updated_at       = now()
            WHERE id = ?
              AND status = 'PROCESSING'
              AND processing_token = ?::uuid
            """;

    /**
     * Fenced release without penalty. Used when this instance claimed a row it then decided not
     * to process (queue full, graceful shutdown). The row becomes immediately claimable again and
     * its retry_count is untouched, because nothing was actually attempted.
     */
    private static final String RELEASE_CLAIM = """
            UPDATE transactions
            SET status           = 'NEW',
                next_attempt_at  = now(),
                processing_token = NULL,
                updated_at       = now()
            WHERE id = ?
              AND status = 'PROCESSING'
              AND processing_token = ?::uuid
            """;

    /**
     * Lease renewal ("heartbeat"). A live worker pushes processing_started_at forward for the
     * rows it still owns, so a slow-but-healthy worker is never mistaken for a dead one. The
     * (id, token) pairs are matched in lockstep by the two-argument form of unnest, and only
     * rows whose token still matches are renewed. RETURNING tells the caller which leases it
     * still holds; anything missing from the result has been taken away and must be abandoned.
     */
    private static final String RENEW_LEASES = """
            UPDATE transactions t
            SET processing_started_at = now(),
                updated_at            = now()
            FROM unnest(?::bigint[], ?::uuid[]) AS v(id, token)
            WHERE t.id = v.id
              AND t.processing_token = v.token
              AND t.status = 'PROCESSING'
            RETURNING t.id
            """;

    /**
     * Stale-lease recovery. Rows whose lease expired are handed back to the pool.
     *
     * <p>Two safety properties:
     * <ul>
     *   <li>SKIP LOCKED means a row that is <em>right now</em> inside another worker's commit is
     *       skipped rather than stolen mid-commit.</li>
     *   <li>Setting processing_token = NULL is the fence: the previous owner's token stops
     *       matching, so any late write from it affects zero rows.</li>
     * </ul>
     *
     * <p>A row that keeps expiring (a poison pill that kills whichever worker touches it) is
     * bounded by the same max-retries budget as any other failure and ends in ERROR, so recovery
     * can never become an infinite loop.
     */
    private static final String RECOVER_STALE = """
            WITH stale AS (
                SELECT id
                FROM transactions
                WHERE status = 'PROCESSING'
                  AND processing_started_at < now() - (?::double precision * INTERVAL '1 second')
                ORDER BY processing_started_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE transactions t
            SET status           = CASE WHEN t.retry_count >= ? THEN 'ERROR' ELSE 'NEW' END,
                retry_count      = t.retry_count + 1,
                recovery_count   = t.recovery_count + 1,
                processing_token = NULL,
                next_attempt_at  = now(),
                processed_at     = CASE WHEN t.retry_count >= ? THEN now() ELSE t.processed_at END,
                error_message    = CASE WHEN t.retry_count >= ?
                                        THEN 'LEASE_EXPIRED: exceeded retry budget after repeated lease expiry'
                                        ELSE t.error_message END,
                updated_at       = now()
            FROM stale s
            WHERE t.id = s.id
            RETURNING t.id, t.external_id, t.status, t.processing_instance
            """;

    private static final RowMapper<ClaimedTransaction> CLAIMED_MAPPER = (rs, rowNum) -> new ClaimedTransaction(
            rs.getLong("id"),
            rs.getString("external_id"),
            rs.getString("card_id"),
            rs.getString("terminal_id"),
            rs.getBigDecimal("amount"),
            rs.getString("currency"),
            rs.getString("transaction_type"),
            rs.getInt("retry_count"),
            readUuid(rs, "processing_token"));

    private final JdbcTemplate jdbcTemplate;

    public TransactionClaimRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<ClaimedTransaction> claimBatch(String instanceId, int limit) {
        return jdbcTemplate.query(CLAIM_BATCH, CLAIMED_MAPPER, limit, instanceId);
    }

    public boolean markProcessed(long id, UUID token) {
        return jdbcTemplate.update(MARK_PROCESSED, id, token.toString()) == 1;
    }

    public boolean scheduleRetry(long id, UUID token, double delaySeconds, String errorMessage) {
        return jdbcTemplate.update(SCHEDULE_RETRY, delaySeconds, errorMessage, id, token.toString()) == 1;
    }

    public boolean markError(long id, UUID token, String errorMessage) {
        return jdbcTemplate.update(MARK_ERROR, errorMessage, id, token.toString()) == 1;
    }

    public boolean releaseClaim(long id, UUID token) {
        return jdbcTemplate.update(RELEASE_CLAIM, id, token.toString()) == 1;
    }

    /** @return the ids whose lease was successfully renewed; anything absent has been revoked. */
    public Set<Long> renewLeases(Collection<Lease> leases) {
        if (leases.isEmpty()) {
            return Set.of();
        }
        String ids = toPgArray(leases.stream().map(l -> Long.toString(l.id())).toList());
        String tokens = toPgArray(leases.stream().map(l -> l.token().toString()).toList());
        List<Long> renewed = jdbcTemplate.queryForList(RENEW_LEASES, Long.class, ids, tokens);
        return new HashSet<>(renewed);
    }

    public List<RecoveredTransaction> recoverStale(double timeoutSeconds, int limit, int maxRetries) {
        return jdbcTemplate.query(RECOVER_STALE,
                (rs, rowNum) -> new RecoveredTransaction(
                        rs.getLong("id"),
                        rs.getString("external_id"),
                        TransactionStatus.valueOf(rs.getString("status")),
                        rs.getString("processing_instance")),
                timeoutSeconds, limit, maxRetries, maxRetries, maxRetries);
    }

    public long countByStatus(TransactionStatus status) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM transactions WHERE status = ?", Long.class, status.name());
        return count == null ? 0L : count;
    }

    public long countStuckProcessing(double timeoutSeconds) {
        Long count = jdbcTemplate.queryForObject("""
                SELECT count(*) FROM transactions
                WHERE status = 'PROCESSING'
                  AND processing_started_at < now() - (?::double precision * INTERVAL '1 second')
                """, Long.class, timeoutSeconds);
        return count == null ? 0L : count;
    }

    private static String toPgArray(List<String> values) {
        return values.stream().collect(Collectors.joining(",", "{", "}"));
    }

    private static UUID readUuid(ResultSet rs, String column) throws SQLException {
        String raw = rs.getString(column);
        return raw == null ? null : UUID.fromString(raw);
    }

    /** (transaction id, fencing token) pair held by a live worker. */
    public record Lease(long id, UUID token) {
    }

    /** Outcome of a stale-lease sweep for one row. */
    public record RecoveredTransaction(long id, String externalId, TransactionStatus newStatus, String previousInstance) {
    }
}
