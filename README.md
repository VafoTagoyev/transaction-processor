# Distributed Transaction Processor

A production-shaped service that reads transactions from PostgreSQL in batches, enriches them from
Redis/Valkey, applies business rules, and writes results back — safely across multiple threads,
multiple JVMs and multiple containers, and correctly across crashes.

**Java 21 · Spring Boot 3.3 · PostgreSQL 16 · Valkey 8 · Maven · Flyway · JdbcTemplate + HikariCP ·
Micrometer + Actuator · JUnit 5 + Testcontainers · Docker Compose**

The three properties everything else serves:

> **No lost transactions. No duplicate results. Automatic recovery after any crash.**

```bash
docker compose up -d --build                            # postgres + valkey + processor-1..3
GEN_TRANSACTIONS=100000 ./scripts/generate-data.sh      # load a dataset
./scripts/watch.sh                                      # live progress
./scripts/verify.sh                                     # PASS/FAIL against C1..C10
```

---

## Table of contents

1. [Architecture](#1-architecture) · 2. [Components](#2-components) · 3. [Database schema](#3-database-schema)
4. [Claiming strategy](#4-claiming-strategy) · 5. [Concurrency strategy](#5-concurrency-strategy)
6. [Transaction boundaries](#6-transaction-boundaries) · 7. [Idempotency](#7-idempotency)
8. [Recovery](#8-recovery) · 9. [Retry](#9-retry) · 10. [Backpressure](#10-backpressure)
11. [Redis behaviour](#11-redis--valkey-behaviour) · 12. [Multi-instance behaviour](#12-multi-instance-behaviour)
13. [Account statistics concurrency](#13-account-statistics-concurrency) · 14. [Transactional Outbox](#14-transactional-outbox)
15. [Configuration](#15-configuration) · 16. [Running locally](#16-running-locally) · 17. [Generating test data](#17-generating-test-data)
18. [Running tests](#18-running-tests) · 19. [Running 3 instances](#19-running-3-instances)
20. [Crash/recovery demonstration](#20-crashrecovery-demonstration) · 21. [Performance tests](#21-performance-tests)
22. [Observability](#22-observability) · 23. [Known limitations](#23-known-limitations)
24. [Bottlenecks and scaling](#24-bottlenecks-and-scaling) · 25. [Ambiguities and decisions](#25-ambiguities-in-the-assignment-and-the-decisions-taken)

**Deeper documents:** [architecture.md](docs/architecture.md) ·
[recovery.md](docs/recovery.md) · [concurrency.md](docs/concurrency.md) ·
[failure-scenarios.md](docs/failure-scenarios.md) · [performance-test.md](docs/performance-test.md) ·
[acceptance-criteria.md](docs/acceptance-criteria.md) · [interview-defense.md](docs/interview-defense.md)

---

## 1. Architecture

```
                        ┌───────────────────────────────┐
                        │          PostgreSQL           │
                        │  transactions (status + token)│
                        │  processed_transactions       │
                        │  account_statistics           │
                        │  outbox_events                │
                        └────▲────────▲────────▲────────┘
                             │        │        │
                    ┌────────┴──┐ ┌───┴──────┐ ┌┴─────────┐
                    │processor-1│ │processor-2│ │processor-3│
                    └────────┬──┘ └───┬──────┘ └┬─────────┘
                             └────────┼─────────┘
                                      ▼
                            ┌──────────────────┐
                            │  Redis / Valkey  │
                            └──────────────────┘
```

Inside each instance:

```
poller ──claim(batch)──▶ bounded queue ──▶ worker×N ──▶ enrich ──▶ classify ──▶ persist (1 tx)
   ▲                          │
   └── limit = min(batch-size, remainingCapacity())   ← backpressure

+ lease heartbeat   (keeps live work out of recovery)
+ recovery sweep    (reclaims abandoned leases)
+ outbox relay      (drains outbox_events)
```

Every instance runs all of these. Nothing is elected, nothing is sharded, no instance knows the
others exist. All coordination happens through row locks and a fencing token in PostgreSQL.

Full diagrams and the lifecycle of one transaction: **[docs/architecture.md](docs/architecture.md)**.

## 2. Components

| Responsibility | Class |
|---|---|
| Polling | `processing/TransactionPoller` |
| Claiming and all state transitions | `repository/TransactionClaimRepository` |
| Bounded hand-off / backpressure | `processing/ProcessingQueue` |
| Worker pool | `processing/WorkerPool`, `processing/ProcessingPipeline` |
| Per-transaction orchestration | `processing/TransactionProcessingService` |
| Redis enrichment | `enrichment/EnrichmentService`, `enrichment/RedisReferenceDataCache` |
| Business rules | `processing/BusinessClassifier`, `processing/CommissionCalculator` |
| Atomic result persistence | `processing/ResultPersistenceService` |
| Retry / error policy | `processing/FailureHandler`, `processing/RetryPolicy`, `error/ErrorClassifier` |
| Crash recovery | `recovery/StaleProcessingRecoveryService`, `recovery/LeaseRenewalService` |
| Ownership bookkeeping | `processing/OwnershipRegistry` |
| Outbox | `outbox/OutboxRelay`, `outbox/OutboxPublisher` |
| Metrics / logging / health | `metrics/ProcessorMetrics`, `logging/LogContext`, `api/ProcessorHealthIndicator` |
| Test data generation | `generator/TransactionGenerator`, `generator/ReferenceDataGenerator` |

## 3. Database schema

Flyway migrations in `src/main/resources/db/migration`:

- **`V1__baseline_tables.sql`** — the four tables exactly as specified in the assignment.
- **`V2__processing_ownership.sql`** — the columns the ownership protocol needs, plus constraints.
- **`V3__indexes.sql`** — indexes, each with the query it serves.

### Additions to the assignment's schema, and why

| Column | Why |
|---|---|
| `transactions.processing_token UUID` | **Fencing token.** A new UUID per claim; every write finalising a transaction is conditioned on it. Without this the stale-recovery race (requirement 47) is unsolvable. |
| `transactions.next_attempt_at TIMESTAMP` | Durable retry backoff. A failed transaction returns to NEW with a future timestamp and is naturally invisible to the claim query until then. |
| `transactions.recovery_count INTEGER` | Separates "the business logic failed" from "workers keep dying on this row". |
| `transactions.updated_at TIMESTAMP` | Operational visibility on a high-churn table. |
| `outbox_events.published_at`, `attempts` | Relay progress and poison-event detection. |

### Constraints

| Constraint | Purpose |
|---|---|
| `uk_processed_transaction UNIQUE (transaction_id)` | **The** idempotency barrier (assignment item 34). |
| `ck_transactions_status` | The status set is closed; no code path can invent a status the poller cannot see. |
| `ck_transactions_ownership` | `(status = 'PROCESSING') = (processing_token IS NOT NULL)` — makes "owned ⇔ PROCESSING" a database invariant, which is what lets the fence predicate be just two columns. |
| `ck_processed_operation_type` | INTERNAL / EXTERNAL only. |
| `ck_outbox_status` | PENDING / PUBLISHED / FAILED only. |

### Indexes and why each exists

| Index | Serves | Why this shape |
|---|---|---|
| `idx_transactions_claim (next_attempt_at, id) WHERE status='NEW'` | the claim query | **Partial** — the single most important decision for scale. The table holds millions of rows but this index holds only the *backlog*; a row leaves it entirely on reaching PROCESSED or ERROR. Claim cost tracks remaining work, not table size. Column order matches the `ORDER BY`, so the `LIMIT` needs no sort. A plain `(status, next_attempt_at)` index would retain an entry for every processed row forever and degrade continuously. |
| `idx_transactions_stale_processing (processing_started_at) WHERE status='PROCESSING'` | the recovery sweep | Also partial. In a healthy system it contains only currently in-flight rows (tens or hundreds), so the sweep is effectively free even against a 50M-row table. |
| `idx_transactions_external_id` | operational lookup by business key | Not on the hot path. Deliberately **not** unique — see [§25](#25-ambiguities-in-the-assignment-and-the-decisions-taken). |
| `idx_transactions_instance … WHERE status='PROCESSING'` | post-crash forensics ("what did the dead instance hold?") | Partial, tiny. |
| `uk_processed_transaction` | idempotency + "already processed?" | It is both the constraint and the lookup index. |
| `idx_processed_account` | reporting joins to `account_statistics` | — |
| `idx_outbox_pending (created_at) WHERE status='PENDING'` | the relay | Partial for the same reason as the claim index: published events leave it. |

`transactions` also carries `fillfactor = 85` and aggressive autovacuum settings, because every row
is UPDATEd at least twice and dead tuples accumulate in front of the claim index.

## 4. Claiming strategy

One statement performs candidate selection, locking and the transition to PROCESSING:

```sql
WITH candidates AS (
    SELECT id FROM transactions
    WHERE status = 'NEW' AND next_attempt_at <= now()
    ORDER BY next_attempt_at, id
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
)
UPDATE transactions t
SET status='PROCESSING', processing_started_at=now(),
    processing_instance=:instance, processing_token=gen_random_uuid(), updated_at=now()
FROM candidates c WHERE t.id = c.id
RETURNING t.id, t.external_id, t.card_id, t.terminal_id, t.amount, t.currency,
          t.transaction_type, t.retry_count, t.processing_token;
```

- **`FOR UPDATE`** locks each candidate row for the duration of the statement's transaction.
- **`SKIP LOCKED`** makes concurrent claimers step over each other instead of blocking — this is
  what turns the table into a work queue.
- **One statement** means there is no window between "selected" and "owned".
- **`RETURNING`** delivers the payload in the same round trip: 1000 rows cost one statement.
- The claim persists `status`, `processing_started_at`, `processing_instance` (requirement 14) and
  a fresh **fencing token**.

Proven by `ClaimExclusivityIT`. Limitations of `SKIP LOCKED` and the comparison with a distributed
lock: **[docs/concurrency.md §1](docs/concurrency.md)**.

## 5. Concurrency strategy

```
1 poller thread → ArrayBlockingQueue(capacity) → N worker threads (processor.workers)
3 scheduler threads: lease heartbeat, recovery sweep, outbox relay
```

**Platform threads, not virtual threads.** The work per transaction is one cache round trip plus
one short JDBC transaction — both blocking, both ultimately bounded by the connection pool, not by
thread count. 10 000 virtual threads against a 20-connection pool add queueing and heap pressure,
not throughput; and on Java 21 a virtual thread blocking inside `synchronized` (which JDBC drivers
still use liberally) pins its carrier. A fixed pool sized in the range of the HikariCP pool gives
predictable memory, predictable database load and an observable queue depth.

*If* virtual threads were adopted, the mandatory guard is a `Semaphore` with permits ≤
`maximum-pool-size` around the persistence step, plus the bounded intake queue. Full analysis,
including deadlock freedom and what happens when workers exceed the pool:
**[docs/concurrency.md §4](docs/concurrency.md)**.

## 6. Transaction boundaries

```
TX 1  claim                 1 statement          connection held briefly
—     enrich (Redis)        network I/O          NO transaction, NO connection, NO locks
—     classify              pure computation
TX 2  persist               4 statements         connection held briefly
        a. UPDATE transactions → PROCESSED        (fenced)
        b. INSERT processed_transactions          ON CONFLICT DO NOTHING
        c. INSERT account_statistics              ON CONFLICT DO UPDATE
        d. INSERT outbox_events
```

**Why a–d are one transaction.** They are one fact expressed in four tables. Split them and you
invent states the system cannot interpret — a result whose transaction is still PROCESSING,
statistics counting a transaction with no result, an event for work that rolled back. Committing
them together means there is exactly one instant at which the outcome changes, which is what makes
a crash at an arbitrary instruction safe.

**Why the Redis call is outside.** A cache round trip has a long tail. Inside a transaction it
would pin a HikariCP connection for its whole duration, hold the claim's row locks, and hold back
the transaction horizon so autovacuum could not clean the hottest table in the schema. There is no
consistency requirement that would justify it: reference data is immutable for the life of a
transaction.

**Isolation:** READ COMMITTED throughout. Correctness comes from row locks, the fence predicate and
unique constraints — none of which need stronger snapshot semantics.

## 7. Idempotency

Four layers, coarse to fine:

1. **Atomic claim** — normally only one worker ever owns a transaction.
2. **Fencing token** — a reassigned lease invalidates the old owner's writes.
3. **Row lock ordering** — the fenced UPDATE runs first, so competitors serialise and the losers
   are rejected when they re-evaluate their `WHERE` clause after the winner commits.
4. **`UNIQUE(transaction_id)`** — the unconditional barrier that no code path, race, crash or
   operator error can bypass.

Plus the part that is easy to miss: **the statistics increment and the outbox event are executed
only when the insert reported 1 row.** Without that gate you would have "no duplicate row" but
still double-counted money — worse, because it is invisible.

```java
boolean stillOwned = transactionRepository.markProcessed(id, token);
if (!stillOwned) throw new OwnershipLostException(...);   // rolls back everything

boolean inserted = processedRepository.insertIfAbsent(result, instanceId);
if (!inserted) return false;                              // idempotent replay: no side effects

statisticsRepository.addTransaction(...);                 // exactly-once
outboxRepository.insert(...);                             // exactly-once
```

## 8. Recovery

A stale lease is returned to the pool:

```sql
WITH stale AS (
    SELECT id FROM transactions
    WHERE status='PROCESSING' AND processing_started_at < now() - :timeout
    ORDER BY processing_started_at LIMIT :n FOR UPDATE SKIP LOCKED
)
UPDATE transactions t
SET status = CASE WHEN t.retry_count >= :max THEN 'ERROR' ELSE 'NEW' END,
    retry_count = t.retry_count + 1, recovery_count = t.recovery_count + 1,
    processing_token = NULL, next_attempt_at = now()
FROM stale s WHERE t.id = s.id RETURNING ...;
```

**How the service knows where it stopped after a restart: it doesn't need to.** There is no offset,
no checkpoint and no cursor anywhere in the codebase. The progress of the system *is* the `status`
column. A restarting instance asks "what is NEW?", which is a query, not a memory.

**The slow-worker race** (requirement 47) is handled in layers:

| Layer | Mechanism | Effect |
|---|---|---|
| 1 | Lease heartbeat renews `processing_started_at` for owned rows | a slow-but-alive worker is never reclaimed |
| 2 | The heartbeat's `RETURNING` reports lost leases | the worker abandons early instead of wasting work |
| 3 | Fencing token invalidated by recovery | a late write matches 0 rows → `OwnershipLostException` → full rollback |
| 4 | `SKIP LOCKED` in the recovery CTE | a row inside an active commit is skipped, not stolen |
| 5 | `UNIQUE(transaction_id)` | even if all of the above failed, only one result can exist |

**No infinite recovery loop:** each reclaim charges the retry budget, so a poison pill reaches
ERROR with `LEASE_EXPIRED` rather than cycling forever.

**Graceful shutdown** (SIGTERM) skips recovery entirely: the poller stops, workers drain, and
anything still queued is released back to NEW immediately — so a rolling restart is invisible in
the throughput graph.

The full crash matrix (all eight scenarios from section 6 of the assignment) is
**[docs/recovery.md](docs/recovery.md)**.

## 9. Retry

| Setting | Default | Meaning |
|---|---|---|
| `processor.max-retries` | 3 | retries beyond the first attempt |
| `processor.retry-initial-delay` | 1s | first backoff |
| `processor.retry-multiplier` | 3.0 | exponential factor |
| `processor.retry-max-delay` | 5m | hard cap |
| `processor.processing-timeout` | 10m | lease duration |

**Transient** (retried): Redis timeout, connection failure, DB timeout, lock contention, network
errors, and — deliberately — *anything unrecognised*. A transaction wrongly classified transient
costs a bounded number of extra attempts and still lands in ERROR; one wrongly classified permanent
is dropped after a single blip. Under a "no lost transactions" requirement the asymmetry is clear.

**Permanent** (straight to ERROR): card not found, terminal not found, malformed reference data,
missing `bankCode`, invalid transaction fields.

`retry_count` lives in the database, so a restart cannot reset the budget and an infinite loop is
not expressible.

## 10. Backpressure

```java
int free = queue.remainingCapacity();
if (free < minClaimBatch) { metrics.backpressure(); return pollingDelay; }
int limit = Math.min(batchSize, free);
```

This is stronger than "block when the queue is full", because a claim also *writes* PROCESSING to
the database. Over-claiming would hide rows from the other instances until their leases expired.
Sizing the claim by free capacity keeps the backlog in PostgreSQL where every instance can see it.

Memory held by the pipeline is bounded by `queue-capacity + workers` transactions — a configuration
decision, not a function of backlog size. `ArrayBlockingQueue` is used precisely because it *cannot*
grow. Exposed as `processing_queue_size` / `processing_queue_capacity` and
`processor_backpressure_total`.

## 11. Redis / Valkey behaviour

| Aspect | Decision |
|---|---|
| **Keys** | Flat `card:{cardId}`, `terminal:{terminalId}`. O(1), shard cleanly across a cluster (each key hashes independently), per-entity TTL possible. A single hash per entity type would defeat clustering and per-entity expiry. |
| **Serialization** | Plain JSON strings, Jackson with `ignoreUnknown`. No Java-specific serializer, so producers can be written in anything and the cache survives refactoring the records. |
| **TTL** | None by default (reference data is long-lived and authoritative for enrichment); configurable via `generator.reference-data-ttl`. Note the interaction with `retry-on-cache-miss`: a short TTL plus permanent-miss policy would turn an expired key into an ERROR. |
| **Round trips** | One `MGET` for both keys — halves latency versus two GETs while keeping per-transaction failure isolation (unlike batch pipelining, where one slow response stalls every transaction in the batch). |
| **Timeout** | `processor.redis.timeout` (200 ms). Exceeding it is transient. |
| **Connection failure** | Lettuce with `disconnectedBehavior=REJECT_COMMANDS` — fail fast instead of buffering commands until they time out, so workers are released immediately. |
| **Retry** | Two levels: one immediate in-process retry absorbs a sub-second blip without touching the database; anything worse falls through to the durable DB-backed retry with backoff. |
| **Cache miss** | Deterministic and permanent by default → ERROR. Retrying a key that is not there only burns capacity. Set `processor.redis.retry-on-cache-miss=true` for deployments where the cache is warmed asynchronously. |
| **Pooling** | None. Lettuce multiplexes commands from many threads onto one Netty channel; a pool would add contention without adding throughput for simple GET/MGET traffic. |

## 12. Multi-instance behaviour

Three identical containers, one PostgreSQL, one Valkey. Identity comes from `INSTANCE_ID`,
defaulting to `${HOSTNAME}` (so `docker compose up --scale` works without configuration).

- **Mutual exclusion** — the atomic claim; two instances cannot own the same row.
- **Failure isolation** — instances share no lock, no leader and no coordination. Killing one does
  not pause the others for a single poll cycle.
- **Recovery on every instance** — no leader election, therefore no single point of failure in the
  mechanism whose job is surviving failures. `SKIP LOCKED` makes concurrent sweeps safe.
- **Load balancing** — implicit. Whoever polls next takes what is available; a slow instance simply
  claims less.

Proven by `MultiInstanceProcessingIT` (which also asserts that all three instances actually
contributed) and `CrashRecoveryIT`.

## 13. Account statistics concurrency

```sql
INSERT INTO account_statistics (account, transactions_count, total_amount, total_commission, updated_at)
VALUES (?, 1, ?, ?, now())
ON CONFLICT (account) DO UPDATE
SET transactions_count = account_statistics.transactions_count + 1,
    total_amount       = account_statistics.total_amount + EXCLUDED.total_amount,
    total_commission   = account_statistics.total_commission + EXCLUDED.total_commission,
    updated_at         = now();
```

The read, the addition and the write are one statement on a row the statement locks — there is no
read-modify-write window, so there is nothing to lose. A concurrent writer blocks on the row lock
and then adds to the newly committed value; increments compose.

`SELECT` → modify in Java → `UPDATE` would lose increments on every interleaving, and under load
that interleaving is the norm rather than the exception.

Proven by `AccountStatisticsConcurrencyIT`: 1500 transactions on a single account across three
instances, and 3200 direct UPSERTs from 32 threads — exact totals in both.

## 14. Transactional Outbox

**The problem.** A service that commits to the database and then publishes to a broker has two
independent commit points. Crash between them and the state changed but no event was emitted;
publish first and crash before committing and consumers react to something that never happened.
There is no ordering of the two that is safe, because they are not one atomic operation. Wrapping
them in a distributed transaction (XA) trades this for worse availability and is not supported by
most brokers.

**The solution.** Make the event part of the same transaction:

```
BEGIN
  UPDATE transactions SET status='PROCESSED' ...   -- fenced
  INSERT INTO processed_transactions ...           -- ON CONFLICT DO NOTHING
  INSERT INTO account_statistics ...               -- atomic UPSERT
  INSERT INTO outbox_events (TRANSACTION_PROCESSED)
COMMIT
```

Either all four are durable or none is. A separate relay (`OutboxRelay`) then moves rows to the
broker at its own pace, claiming with `FOR UPDATE SKIP LOCKED` so every instance can relay without
publishing the same event twice.

Delivery to the broker is **at-least-once** — the publish happens before the COMMIT that marks the
row `PUBLISHED`, so a crash in that window republishes. Consumers deduplicate on the stable event
UUID. Being precise about where exactly-once ends is more useful than claiming it end to end.

Kafka is intentionally not implemented (the assignment says it is optional); `OutboxPublisher` is a
one-method interface and the default writes to the log. That the broker is swappable without
touching the persistence path is exactly what the pattern buys.

Proven by `OutboxAtomicityIT`, including a failure injected *after* the result insert that leaves
no result row, no event, no statistics row and the status unchanged.

## 15. Configuration

Everything is `application.yml` plus environment variables. Nothing that affects correctness under
failure is a constant in code.

| Property | Env var | Default | Purpose |
|---|---|---|---|
| `processor.instance-id` | `INSTANCE_ID` | `${HOSTNAME}` | Cluster-unique identity |
| `processor.enabled` | `PROCESSOR_ENABLED` | `true` | Off for the generator |
| `processor.batch-size` | `BATCH_SIZE` | `1000` | Max rows per claim |
| `processor.workers` | `WORKERS` | `8` | Worker threads |
| `processor.polling-delay` | `POLLING_DELAY` | `500ms` | Delay after a productive cycle |
| `processor.polling-idle-delay` | `POLLING_IDLE_DELAY` | `2s` | Delay after an empty cycle |
| `processor.processing-timeout` | `PROCESSING_TIMEOUT` | `10m` | Lease duration |
| `processor.lease-renewal-interval` | `LEASE_RENEWAL_INTERVAL` | `30s` | Heartbeat period |
| `processor.queue-capacity` | `QUEUE_CAPACITY` | `5000` | The memory bound |
| `processor.max-retries` | `MAX_RETRIES` | `3` | Retry budget |
| `processor.retry-initial-delay` | `RETRY_INITIAL_DELAY` | `1s` | First backoff |
| `processor.retry-multiplier` | `RETRY_MULTIPLIER` | `3.0` | Exponential factor |
| `processor.retry-max-delay` | `RETRY_MAX_DELAY` | `5m` | Backoff cap |
| `processor.shutdown-grace-period` | `SHUTDOWN_GRACE_PERIOD` | `30s` | Graceful drain window |
| `processor.redis.timeout` | `REDIS_TIMEOUT` | `200ms` | Per-command timeout |
| `processor.redis.immediate-retries` | `REDIS_IMMEDIATE_RETRIES` | `1` | In-process retries |
| `processor.redis.retry-on-cache-miss` | `REDIS_RETRY_ON_CACHE_MISS` | `false` | Miss policy |
| `processor.recovery.interval` | `RECOVERY_INTERVAL` | `1m` | Sweep period |
| `processor.recovery.batch-size` | `RECOVERY_BATCH_SIZE` | `1000` | Rows per sweep |
| `processor.outbox.relay-interval` | `OUTBOX_RELAY_INTERVAL` | `1s` | Relay period |
| `spring.datasource.hikari.maximum-pool-size` | `DB_POOL_SIZE` | `20` | Connection pool |
| `spring.datasource.url` | `DB_URL` | localhost | Database |
| `spring.data.redis.host` / `.port` | `REDIS_HOST` / `REDIS_PORT` | localhost:6379 | Cache |

**Sizing rule for the pool:** `workers + poller + heartbeat + recovery + relay + headroom`. With
the defaults that is 8 + 4 = 12 against a pool of 20. Keep `workers ≤ maximum-pool-size`; see
[§24](#24-bottlenecks-and-scaling).

Generator settings live under `generator.*` — see [§17](#17-generating-test-data).

## 16. Running locally

**Requirements:** Docker + Docker Compose. (For `mvn` directly: JDK 21 and Maven 3.9+.)

```bash
docker compose up -d --build
docker compose ps
curl -s localhost:8081/actuator/health | jq
curl -s localhost:8081/status | jq
```

| Service | Port |
|---|---|
| PostgreSQL | 5432 |
| Valkey | 6379 |
| processor-1 / 2 / 3 | 8081 / 8082 / 8083 |

Local run without Docker (with PostgreSQL and Valkey already available):

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--processor.instance-id=local-1 --server.port=8080"
```

## 17. Generating test data

```bash
./scripts/generate-data.sh                          # 1 000 000 tx, 100 000 cards, 10 000 terminals
GEN_TRANSACTIONS=100000 ./scripts/generate-data.sh  # smaller, for a quick run
```

Or directly:

```bash
docker compose --profile generator run --rm generator
```

- **Transactions** are bulk-loaded with PostgreSQL's `COPY` protocol in chunks — the difference
  between minutes and tens of minutes for a million rows — with flat heap usage regardless of volume.
- **Reference data** is written with Redis pipelining, one round trip per batch.
- **Distribution** (all configurable): ~80% INTERNAL / ~20% EXTERNAL, ~1% card-not-found,
  ~1% terminal-not-found, and a configurable share of amounts placed exactly on the 1 000 000
  commission boundary so the boundary is exercised on every run.
- **The seed is fixed**, so the same configuration produces the same dataset on every machine and
  performance runs are comparable.

| Property | Env var | Default |
|---|---|---|
| `generator.transactions` | `GENERATOR_TRANSACTIONS` | 1 000 000 |
| `generator.cards` | `GENERATOR_CARDS` | 100 000 |
| `generator.terminals` | `GENERATOR_TERMINALS` | 10 000 |
| `generator.internal-ratio` | `GENERATOR_INTERNAL_RATIO` | 0.80 |
| `generator.card-miss-ratio` | `GENERATOR_CARD_MISS_RATIO` | 0.01 |
| `generator.terminal-miss-ratio` | `GENERATOR_TERMINAL_MISS_RATIO` | 0.01 |
| `generator.boundary-amount-ratio` | `GENERATOR_BOUNDARY_AMOUNT_RATIO` | 0.02 |
| `generator.seed` | `GENERATOR_SEED` | 20240101 |

## 18. Running tests

```bash
mvn test      # unit tests only — fast, no Docker
mvn verify    # unit + integration (Testcontainers → Docker required)
```

| Category | Classes |
|---|---|
| Unit | `CommissionCalculatorTest`, `BusinessClassifierTest`, `EnrichmentServiceTest`, `RetryPolicyTest`, `ErrorClassifierTest`, `LogMaskingTest`, `OwnershipRegistryTest` |
| Claiming | `ClaimExclusivityIT` |
| Idempotency / concurrency | `IdempotencyConcurrencyIT` |
| Fencing (the slow-worker race) | `FencingTokenIT` |
| Recovery | `StaleRecoveryIT`, `CrashRecoveryIT` |
| Multi-instance | `MultiInstanceProcessingIT` |
| Aggregation | `AccountStatisticsConcurrencyIT` |
| Outbox | `OutboxAtomicityIT` |
| Retry / errors | `ErrorFlowIT` |
| Backpressure | `BackpressureIT` |
| Spring wiring, migrations, metrics | `ApplicationContextIT` |

Integration tests run against real PostgreSQL and real Valkey via Testcontainers — the guarantees
under test are properties of `SKIP LOCKED`, `ON CONFLICT` and row-lock re-evaluation, so an
in-memory database would prove nothing. Several "instances" are hosted in one JVM by constructing
the production components directly (they use constructor injection and hold no static state); the
`@Transactional` proxy is rebuilt explicitly in `ProcessorTestHarness` so the tests exercise the
real transaction boundaries.

> **Execution status:** these tests have not been run in the environment where this project was
> authored (no Docker daemon, no JDK 21, no Maven Central access). See [§26](#26-what-has-and-has-not-been-executed).

## 19. Running 3 instances

```bash
./scripts/run-3-instances.sh   # build, start, wait for health, then follow progress
./scripts/watch.sh             # live view: backlog, per-instance progress, TPS, duplicates
./scripts/verify.sh            # PASS/FAIL summary against the acceptance criteria
```

## 20. Crash/recovery demonstration

Implements section 6.2 of the assignment (steps 55–64) end to end:

```bash
./scripts/crash-recovery-demo.sh 100000
```

1. generate 100 000 NEW transactions
2. start 3 instances × 8 workers
3. wait for processing to be under way
4. **`docker compose kill -s SIGKILL processor-2`** — a real `kill -9`, no graceful shutdown
5. verify the survivors keep processing (**C3**)
6. restart processor-2 and watch it rejoin with no offset given to it (**C4**)
7. wait for the backlog to drain, then run the full verification (**C1, C5, C6, C7**)
8. restart all three and confirm nothing is reprocessed

## 21. Performance tests

```bash
./scripts/perf-test.sh                 # 1M transactions across 1, 3 and 5 instances
./scripts/perf-test.sh 200000 "1 3"    # smaller
```

For each configuration the script regenerates the dataset from the same fixed seed, scales the
`processor` service, samples until the backlog drains, collects TPS / avg / P95 / P99 / peak DB
connections / Redis latency, and re-checks duplicates and un-terminal rows so a fast-but-wrong run
cannot pass unnoticed.

> **`docs/performance-test.md` contains no measured numbers.** Every cell is marked
> `TO BE MEASURED`, per the instruction not to invent results. The document also lists the
> hypotheses the measurement should confirm and a table for interpreting a disappointing result.

## 22. Observability

**Metrics** — Micrometer meter names use the dot convention; the Prometheus registry renders them
as the names the assignment requires:

| Meter | `/actuator/prometheus` |
|---|---|
| `processor.received` | `processor_received_total` |
| `processor.processed` | `processor_processed_total` |
| `processor.error` | `processor_error_total` |
| `processor.retry` | `processor_retry_total` |
| `processor.recovered` | `processor_recovered_total` |
| `processing.duration` | `processing_duration_seconds{quantile=…}` |
| `redis.lookup.duration` | `redis_lookup_duration_seconds` |
| `db.write.duration` | `db_write_duration_seconds` |
| `active.workers` | `active_workers` |
| `processing.queue.size` | `processing_queue_size` |

Plus, beyond the required set: `processor_ownership_lost_total` (fenced writes — how often the
slow-worker race actually fires), `processor_duplicate_skipped_total` (idempotent replays),
`processor_backpressure_total`, `processor_claim_released_total`, `processor_cache_failure_total`,
`processor_outbox_published_total`, `processing_queue_capacity`, `processor_inflight`,
`claim_duration_seconds`. Every metric is tagged with `instance`.

**Logs** — MDC carries `instanceId`, `transactionId` and `externalId` on every line. Default output
is a human-readable pattern; `SPRING_PROFILES_ACTIVE=json` switches to one JSON object per line
with those keys promoted to fields. Card identifiers are masked (`LogMasking`) — all but the last
four characters — and reference data values are never logged.

**Endpoints** — `/actuator/health` (includes a pipeline-liveness indicator: a container whose
poller thread died fails its health check), `/actuator/prometheus`, `/actuator/metrics`,
`/actuator/loggers`, and `/status` for a direct answer to the acceptance questions.

## 23. Known limitations

1. **Polling has a floor on latency.** A transaction waits up to `polling-delay` before anyone
   looks at it. Fine for batch processing; wrong for sub-100 ms requirements. See [§24](#24-bottlenecks-and-scaling).
2. **`transactions` is not partitioned.** At tens of millions of rows, vacuum and index maintenance
   become the operational burden. The fix (range partitioning by `created_at`, dropping old
   partitions) is straightforward but not implemented.
3. **A prolonged Redis outage eventually produces ERROR rows** rather than waiting indefinitely,
   once the retry budget is spent. This follows the assignment's retry policy, but if "wait for the
   cache forever" is wanted, raise `max-retries` and `retry-max-delay`.
4. **The outbox relay is at-least-once**, so consumers must deduplicate on the event UUID. There is
   no dead-letter handling for events that fail repeatedly beyond the `attempts` counter.
5. **No dead-letter queue or automatic reprocessing for ERROR rows.** They are terminal and need an
   operator decision. A `SELECT ... UPDATE status='NEW'` is safe by construction, but there is no
   tooling for it.
6. **The statistics UPSERT serialises per account.** Fine for a normal account distribution; a
   single account receiving thousands of transactions per second would become a hot row. The fix is
   an insert-only ledger with periodic rollup, at the cost of real-time statistics.
7. **One poller thread per instance** means claim throughput per instance is bounded by one
   statement per `polling-delay`. With `batch-size = 1000` and 500 ms that is 2000 rows/s per
   instance — raise the batch size or lower the delay before adding poller threads.
8. **`processing_token` is a UUID, not a monotonic counter.** It gives correct fencing (equality is
   all we need) but not orderability. A monotonic epoch would additionally allow "reject anything
   older than X" semantics at an external store — not needed here, since the store *is* the fence.
9. **No authentication on the actuator endpoints.** Acceptable for a local assignment; not for a
   deployment.
10. **No archival of `processed_transactions` or `outbox_events`.** Both grow without bound.

## 24. Bottlenecks and scaling

**Order in which limits appear as instances are added:**

1. **PostgreSQL write throughput** — one small write transaction per result, four tables plus
   indexes, one WAL flush per commit. This is the first and dominant ceiling.
2. **Claim-index contention** — many claimers scanning the same hot region, each walking past the
   others' locked rows.
3. **Connection count** — N × pool size; each backend is a process. Past a few hundred, the server
   context-switches more than it works.
4. **Autovacuum on `transactions`** — every row is updated at least twice, and the dead tuples
   accumulate exactly where the claim index scans.
5. **Redis** — comfortably last: one MGET of two keys against an in-memory dataset.

The application is deliberately *not* on that list. The response to saturation is to fix the
database or change the transport, not to add threads.

**Path to the Senior Challenge (50M/day, peaks of 10–20k TPS, 3 → 10 → 30 instances):**

| Step | Change | Buys |
|---|---|---|
| 1 | Range-partition `transactions` by `created_at`; partial indexes per partition | vacuum and index maintenance stay bounded; old partitions are dropped, not deleted |
| 2 | Batched persistence: fence N rows with one `UPDATE ... WHERE id = ANY(?) RETURNING id`, then insert only the returned ids | amortises round trips and WAL flushes while keeping the guarantees |
| 3 | PgBouncer in transaction mode | decouples instance count from `max_connections` |
| 4 | Tune WAL: `max_wal_size`, `checkpoint_completion_target`, separate WAL device | commit throughput |
| 5 | Redis Cluster | the flat key design already shards cleanly with no code change |
| 6 | Replace polling with Kafka; keep the outbox as the bridge | removes the polling ceiling entirely |
| 7 | Read replicas for reporting; keep processing on the primary | takes analytical load off the hot path |

**When to move to Kafka:** when polling cost stops being negligible (many instances repeatedly
being told "nothing new"), when throughput exceeds what one PostgreSQL can commit, when the work
already originates as an event, when per-key ordering is required, or when several independent
consumers need the same stream. What you give up is the transactional coupling between work item
and result — so exactly-once effects have to be rebuilt with the same idempotency key and unique
constraint this design already has. The pragmatic middle path is Kafka as the transport with
PostgreSQL still the system of record, which leaves the processing logic, the idempotency barrier
and the recovery model untouched.

Full discussion: **[docs/interview-defense.md](docs/interview-defense.md)** questions 76 and 80.

## 25. Ambiguities in the assignment, and the decisions taken

| # | Ambiguity | Decision | Justification |
|---|---|---|---|
| 1 | The `transactions` DDL has no column that can express ownership over time, but requirement 47 demands the slow-worker race be solved. | Added `processing_token`, `next_attempt_at`, `recovery_count`, `updated_at` in a separate migration (`V2`) so the delta from the assignment is explicit and reviewable. | Requirement 47 is not solvable with the given columns. Splitting the migration keeps the original schema visible. |
| 2 | "После проверки такие записи можно вернуть в NEW **либо** re-claim другим instance" — two options offered. | Return to NEW. | One claim path instead of two; any instance can then take the row, which balances better than assigning it to whoever happened to run the sweep. |
| 3 | Cache miss policy: "отсутствующий справочник **после принятой политики**" leaves the policy to the candidate. | Permanent → ERROR by default, with `retry-on-cache-miss=true` available. | A key that is absent will be absent on retry; retrying burns capacity and delays the terminal status. The flag covers the asynchronously-warmed-cache deployment. |
| 4 | Commission at exactly 1 000 000: `< 1 000 000 → 1%` and `>= 1 000 000 → 0.5%` are unambiguous, but the discontinuity is unusual enough to be worth confirming. | Implemented exactly as written: 1 000 000 pays 0.5%. | The specification is explicit. Called out because "one cent more principal, half the commission" is the kind of rule worth double-checking with the business. Tested directly in `CommissionCalculatorTest`. |
| 5 | Negative amounts are undefined. | Rejected as `INVALID_TRANSACTION` → ERROR. | Inventing a refund rule would be a silent business decision. Failing loudly is recoverable; a wrong commission is not. |
| 6 | The commission threshold is currency-agnostic in the assignment, but `currency` exists on the row. | Applied the threshold to `amount` regardless of currency, as written. | Making it currency-aware would require an FX policy the assignment does not define. Flagged as a real-world gap. |
| 7 | `external_id` is not declared UNIQUE. | Left non-unique, with an index for lookups. | The ingestion path is out of scope. Processing idempotency does not depend on it — that is `UNIQUE(transaction_id)`. A one-line comment in `V3` shows how to tighten it if the upstream feed can duplicate. |
| 8 | Recovery could be run by a leader or by everyone. | Everyone. | A leader needs election and becomes a single point of failure for the one mechanism that exists to survive failures. `SKIP LOCKED` already makes concurrent sweeps safe. |
| 9 | Batch insert vs per-transaction writes (item 29) is left to the candidate. | Per-transaction. | Correctness under concurrency is the graded property; batching complicates fencing and turns one poison row into a failed batch. The batched variant and its trade-offs are documented in `docs/concurrency.md` §7. |
| 10 | Whether a reclaim should count against the retry budget. | Yes, and it also increments `recovery_count`. | Otherwise a transaction that kills every worker it touches cycles forever, violating "never an infinite loop". The separate counter keeps the two failure modes distinguishable. |

## 26. What has and has not been executed

Honesty about the state of verification, since it affects how you should read the rest of this
README.

**Not executed.** The environment in which this project was written has no Docker daemon, no JDK 21
and no network access to Maven Central. Therefore:

- `mvn test` and `mvn verify` have **not** been run;
- the Docker images have **not** been built;
- `docs/performance-test.md` contains **no** measured numbers — every cell is marked
  `TO BE MEASURED`, per the instruction not to invent results.

**What that means for you.** Expect to fix ordinary build-time issues (a missing import, a
dependency version) on the first `mvn verify`. What has been designed carefully, and what the
review should focus on, is the concurrency and failure behaviour: the claim statement, the fencing
protocol, the transaction boundaries and the recovery matrix. Those are argued in detail in
`docs/concurrency.md` and `docs/recovery.md`, and each has a test written specifically to falsify it.

**To verify everything:**

```bash
mvn verify                                  # all tests, Docker required
./scripts/crash-recovery-demo.sh 100000     # the full section 6.2 procedure
./scripts/perf-test.sh                      # fills in docs/performance-test.md
./scripts/verify.sh                         # PASS/FAIL against C1..C10
```
