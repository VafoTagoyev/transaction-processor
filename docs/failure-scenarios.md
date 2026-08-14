# Failure scenarios

`docs/recovery.md` covers process and JVM crashes in depth. This document covers the *other*
failure modes — dependencies degrading, resources running out, and bad data — and states what the
service does in each case.

The rule everywhere below: **the fallback is always "the row stays PROCESSING and the lease
expires".** There is no failure path that loses a transaction; the worst case is that it takes one
processing timeout longer.

## 1. Redis / Valkey

| Failure | Detection | Behaviour | Configuration |
|---|---|---|---|
| Command timeout | Lettuce `commandTimeout` → `QueryTimeoutException` | `CacheUnavailableException` (transient) → one immediate retry → then NEW with backoff | `processor.redis.timeout`, `immediate-retries` |
| Connection refused / node down | `RedisConnectionFailureException`; `disconnectedBehavior=REJECT_COMMANDS` makes it fail fast instead of queueing | same as above; workers are released immediately rather than blocking for the timeout | `RedisConfig` |
| Total outage | every transaction fails transiently | all in-flight transactions return to NEW with exponentially growing backoff; the poller keeps claiming and keeps failing, but bounded by `max-retries`. After the budget is spent transactions become ERROR. | `max-retries`, `retry-max-delay` |
| Key missing (deterministic) | `null` from MGET | `CardNotFoundException` / `TerminalNotFoundException` — **permanent**, straight to ERROR. Retrying a key that is not there only burns capacity. | `retry-on-cache-miss=false` (default) |
| Key missing (cache still warming) | same | set `retry-on-cache-miss=true` and it becomes `CacheMissRetryableException` — transient, bounded by `max-retries` | `processor.redis.retry-on-cache-miss` |
| Malformed JSON | Jackson parse failure | `InvalidReferenceDataException` — permanent. It will be malformed on every retry too. | — |
| Missing `bankCode` | validated in `BusinessClassifier` | `InvalidReferenceDataException` — permanent. Deliberately *not* treated as "different banks", which would silently charge a wrong commission. | — |

**Design note.** A prolonged Redis outage will eventually push transactions to ERROR rather than
holding them forever. That is the honest reading of the assignment's retry policy, but it is an
operational trade-off: if the intended behaviour is "wait indefinitely for the cache to return",
raise `max-retries` and `retry-max-delay`. Both are configuration, not code.

## 2. PostgreSQL

| Failure | Behaviour |
|---|---|
| Unavailable at startup | Flyway fails and the application does not start. Correct: an instance that cannot reach its source of truth must not pretend to be healthy. The compose file gates the processors on `postgres: service_healthy`. |
| Goes away while running | The poller logs and backs off (`polling-idle-delay`) without dying; workers' persistence transactions fail and are classified transient. In-flight rows stay PROCESSING and are recovered when the database returns. |
| Connection pool exhausted | `connection-timeout` (10 s) → `CannotGetJdbcConnectionException` → transient → retry with backoff. Degrades, does not corrupt. See `docs/concurrency.md` §4. |
| Statement timeout / lock wait | `QueryTimeoutException` / `CannotAcquireLockException` → transient. |
| Deadlock (SQLSTATE 40P01) | Should be impossible by construction (§4 of `concurrency.md`). If one occurs, the victim is classified transient and retried. |
| Disk full / WAL full | Writes fail; everything is classified transient; the backlog stays in the database. No data is lost because nothing is acknowledged that was not committed. |
| Failover to a replica | In-flight transactions fail transiently and are retried. Committed results are durable if the replica was synchronous; with asynchronous replication, a window of committed results can be lost — a property of the deployment, not of this service. |

## 3. The processing pipeline

| Failure | Behaviour |
|---|---|
| Poison transaction that always throws | Bounded by `max-retries` → ERROR. `TransactionProcessingService.process` never propagates, so one bad row cannot kill a worker. |
| Poison transaction that kills its worker every time | Bounded by the same budget through `recovery_count` → ERROR with `LEASE_EXPIRED`. Cannot loop forever. |
| Worker thread dies unexpectedly | `UncaughtExceptionHandler` logs it; the transaction it held stays PROCESSING and is recovered. The pool does **not** self-heal by respawning threads — a dying worker is a bug that should be visible, and the health endpoint reports `activeWorkers`. |
| Queue full | The poller stops claiming (`processor_backpressure_total` increments). Memory stays bounded. |
| Queue refuses a claimed row (should not happen) | The claim is released back to NEW immediately rather than sat on. |
| OutOfMemoryError | `-XX:+ExitOnOutOfMemoryError` kills the JVM rather than leaving a zombie that holds leases without renewing them. Docker restarts it; recovery reclaims its rows. |
| Clock skew between instances | Irrelevant: every timestamp comparison (`next_attempt_at <= now()`, lease expiry, backoff arithmetic) is evaluated **by PostgreSQL**, using the database clock. No instance's local clock affects any decision. |

## 4. Data quality

| Case | Classification | Terminal status |
|---|---|---|
| `card_id` or `terminal_id` null/blank | `InvalidTransactionException` — permanent (fails before touching the cache) | ERROR |
| `amount` null | `InvalidTransactionException` — permanent | ERROR |
| `amount` negative | `InvalidTransactionException` — permanent (documented assumption; see README "ambiguities") | ERROR |
| Card not in cache | permanent by default | ERROR |
| Terminal not in cache | permanent by default | ERROR |
| Reference data missing `bankCode` | permanent | ERROR |
| `account` null on the card | processed normally, statistics skipped for that transaction | PROCESSED |

The generator produces ~1% card-not-found and ~1% terminal-not-found precisely so that this path
is exercised on every run rather than only in unit tests.

## 5. Outbox

| Failure | Behaviour |
|---|---|
| Publisher fails | Rows stay `PENDING`, `attempts` increments, the next sweep retries. Nothing is lost — the event is a committed database row. |
| Crash after publish, before the `PUBLISHED` commit | The event is republished on the next sweep. At-least-once by design; consumers deduplicate on the event UUID. |
| Relay never runs | Events accumulate as `PENDING`. Visible via `SELECT status, count(*) FROM outbox_events`. No effect on transaction processing. |
| Two instances relay simultaneously | `FOR UPDATE SKIP LOCKED` partitions the backlog; no event is published twice by the same sweep. |

## 6. Operational failure modes worth knowing about

| Symptom | Meaning | Action |
|---|---|---|
| `processor_recovered_total` rising during a healthy run | Leases are expiring on live workers — the heartbeat cannot keep up, or `processing-timeout` is too tight | Raise `processing-timeout` or lower `lease-renewal-interval` |
| `processor_ownership_lost_total` rising | Reclaims are happening and the fence is doing its job; work is being wasted | Same as above — the fence is a safety net, not a normal operating mode |
| `processor_duplicate_skipped_total` rising | Transactions are being reprocessed after already having a result | Look for an operator or a script pushing PROCESSED rows back to NEW |
| `processing_queue_size` pinned at capacity | The write side is the bottleneck | Expected under load; add instances or raise the pool. Working as designed. |
| Many ERROR rows with `CARD_NOT_FOUND` | The cache is not fully populated, or the generator was run without the reference data step | Re-run the generator; consider `retry-on-cache-miss=true` |
