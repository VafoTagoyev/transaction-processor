package com.example.txprocessor.repository;

import com.example.txprocessor.outbox.OutboxRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Repository
public class OutboxRepository {

    private static final String INSERT = """
            INSERT INTO outbox_events (id, aggregate_id, event_type, payload, status, created_at)
            VALUES (?::uuid, ?, ?, ?::jsonb, 'PENDING', now())
            """;

    /**
     * The relay claims a page of pending events the same way the poller claims transactions:
     * SKIP LOCKED so that every instance can run a relay without any of them blocking or
     * double-publishing.
     */
    private static final String CLAIM_PENDING = """
            SELECT id, aggregate_id, event_type, payload, attempts
            FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY created_at
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            """;

    private static final String MARK_PUBLISHED = """
            UPDATE outbox_events
            SET status = 'PUBLISHED', published_at = now(), attempts = attempts + 1
            WHERE id = ANY (?::uuid[])
            """;

    private static final String MARK_ATTEMPT_FAILED = """
            UPDATE outbox_events
            SET attempts = attempts + 1
            WHERE id = ANY (?::uuid[])
            """;

    private final JdbcTemplate jdbcTemplate;

    public OutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(UUID id, long aggregateId, String eventType, String payloadJson) {
        jdbcTemplate.update(INSERT, id.toString(), aggregateId, eventType, payloadJson);
    }

    public List<OutboxRecord> claimPending(int limit) {
        return jdbcTemplate.query(CLAIM_PENDING,
                (rs, rowNum) -> new OutboxRecord(
                        UUID.fromString(rs.getString("id")),
                        rs.getLong("aggregate_id"),
                        rs.getString("event_type"),
                        rs.getString("payload"),
                        rs.getInt("attempts")),
                limit);
    }

    public void markPublished(List<UUID> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(MARK_PUBLISHED, toPgArray(ids));
    }

    public void markAttemptFailed(List<UUID> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(MARK_ATTEMPT_FAILED, toPgArray(ids));
    }

    public long countByStatus(String status) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE status = ?", Long.class, status);
        return count == null ? 0L : count;
    }

    private static String toPgArray(List<UUID> ids) {
        return ids.stream().map(UUID::toString).collect(Collectors.joining(",", "{", "}"));
    }
}
