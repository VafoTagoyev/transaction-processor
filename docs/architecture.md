# Architecture

## 1. System topology

```
                     ┌──────────────────────────────────────────────┐
                     │                PostgreSQL                    │
                     │  transactions  (NEW / PROCESSING / PROCESSED │
                     │                 / ERROR + fencing token)     │
                     │  processed_transactions  UNIQUE(transaction_id)
                     │  account_statistics      (atomic UPSERT)     │
                     │  outbox_events           (TRANSACTION_PROCESSED)
                     └───────▲──────────────▲──────────────▲────────┘
                             │              │              │
              claim / fence  │              │              │
              persist        │              │              │
                     ┌───────┴────┐  ┌──────┴─────┐  ┌─────┴──────┐
                     │ processor-1│  │ processor-2│  │ processor-3│
                     │            │  │            │  │            │
                     │  poller    │  │  poller    │  │  poller    │
                     │  queue     │  │  queue     │  │  queue     │
                     │  workers×8 │  │  workers×8 │  │  workers×8 │
                     │  recovery  │  │  recovery  │  │  recovery  │
                     │  heartbeat │  │  heartbeat │  │  heartbeat │
                     │  relay     │  │  relay     │  │  relay     │
                     └───────┬────┘  └──────┬─────┘  └─────┬──────┘
                             │              │              │
                     enrich  │              │              │
                             └──────────────┼──────────────┘
                                            ▼
                              ┌──────────────────────────┐
                              │      Redis / Valkey      │
                              │  card:{cardId}     JSON  │
                              │  terminal:{terminalId}   │
                              └──────────────────────────┘
```

Every instance is the same image and runs the same five background activities. Nothing is
elected, nothing is sharded, and no instance knows that the others exist — coordination happens
entirely through row locks and a fencing token in PostgreSQL.

## 2. Inside one instance

```
  ┌────────────┐   claim batch     ┌──────────────────┐   take    ┌──────────────┐
  │            │  (1 statement,    │  ArrayBlocking   │──────────▶│  worker 1    │
  │  poller    │──FOR UPDATE──────▶│  Queue           │──────────▶│  worker 2    │
  │ (1 thread) │   SKIP LOCKED)    │  bounded,        │──────────▶│    ...       │
  │            │                   │  capacity=N      │──────────▶│  worker 8    │
  └─────┬──────┘                   └──────────────────┘           └──────┬───────┘
        │                                   ▲                            │
        │ limit = min(batch-size,           │ remainingCapacity()        │
        │             remainingCapacity())  │                            │
        │                                   └── backpressure ────────────┘
        │                                                                │
        ▼                                                                ▼
   PostgreSQL                                          ┌─────────────────────────────┐
                                                       │ 1. enrich   (Valkey, no tx) │
  ┌──────────────┐  every lease-renewal-interval       │ 2. classify (pure)          │
  │  heartbeat   │──renew owned leases (fenced)───────▶│ 3. persist  (1 tx, fenced)  │
  └──────────────┘                                     └─────────────────────────────┘
  ┌──────────────┐  every recovery.interval
  │  recovery    │──reclaim expired leases (SKIP LOCKED)
  └──────────────┘
  ┌──────────────┐  every outbox.relay-interval
  │  outbox relay│──publish PENDING events (SKIP LOCKED)
  └──────────────┘
```

| Component | Class | Responsibility |
|---|---|---|
| Polling | `TransactionPoller` | One thread. Sizes each claim by free queue capacity. |
| Claiming | `TransactionClaimRepository` | All SQL for state transitions; every exit from PROCESSING is fenced. |
| Queueing / backpressure | `ProcessingQueue` | Fixed-capacity `ArrayBlockingQueue`; the memory bound of the service. |
| Worker pool | `WorkerPool` | Fixed platform threads, take-and-process loop. |
| Orchestration | `TransactionProcessingService` | The three phases; converts every failure into a durable transition. |
| Enrichment | `EnrichmentService`, `RedisReferenceDataCache` | One MGET per transaction; timeout, miss and outage policy. |
| Business rules | `BusinessClassifier`, `CommissionCalculator` | Pure functions. |
| Persistence | `ResultPersistenceService` | The single atomic unit: status + result + statistics + outbox. |
| Failure policy | `FailureHandler`, `RetryPolicy`, `ErrorClassifier` | Transient vs permanent, bounded backoff. |
| Recovery | `StaleProcessingRecoveryService`, `LeaseRenewalService` | Lease expiry sweep and heartbeat. |
| Outbox | `OutboxRelay`, `OutboxPublisher` | Drains `outbox_events` at-least-once. |
| Observability | `ProcessorMetrics`, `LogContext`, `ProcessorHealthIndicator` | Metrics, MDC, health. |
| Test data | `TransactionGenerator`, `ReferenceDataGenerator` | COPY and pipelined Redis writes. |

## 3. Lifecycle of one transaction

```
   ┌─────┐
   │ NEW │◀──────────────────────────────────────────┐
   └──┬──┘                                           │
      │                                              │
      │ (1) CLAIM  — one statement:                  │ (5) transient failure,
      │     WITH candidates AS (                     │     attempts remaining:
      │       SELECT id FROM transactions            │     status=NEW
      │        WHERE status='NEW'                    │     retry_count+1
      │          AND next_attempt_at<=now()          │     next_attempt_at=now()+backoff
      │        ORDER BY next_attempt_at, id          │     processing_token=NULL
      │        LIMIT :batch FOR UPDATE SKIP LOCKED)  │
      │     UPDATE ... SET status='PROCESSING',      │ (6) lease expired
      │       processing_started_at=now(),           │     (recovery sweep):
      │       processing_instance=:id,               │     status=NEW, retry_count+1,
      │       processing_token=gen_random_uuid()     │     recovery_count+1, token=NULL
      │     RETURNING ...                            │
      ▼                                              │
 ┌────────────┐                                      │
 │ PROCESSING │──────────────────────────────────────┘
 └─────┬──────┘   heartbeat renews processing_started_at while the owner lives
      │
      │ (2) ENRICH   MGET card:{id} terminal:{id}      ← no DB transaction open
      │ (3) CLASSIFY INTERNAL/EXTERNAL + commission    ← pure computation
      │ (4) PERSIST  one DB transaction:
      │       a. UPDATE transactions SET status='PROCESSED', token=NULL
      │            WHERE id=? AND status='PROCESSING' AND processing_token=?
      │          → 0 rows  ⇒ OwnershipLostException ⇒ ROLLBACK everything
      │       b. INSERT INTO processed_transactions ... ON CONFLICT DO NOTHING
      │          → 0 rows  ⇒ already done; skip c and d (exactly-once side effects)
      │       c. INSERT INTO account_statistics ... ON CONFLICT DO UPDATE (in-DB addition)
      │       d. INSERT INTO outbox_events (TRANSACTION_PROCESSED)
      │       COMMIT   ← the single instant at which the outcome becomes real
      ▼
 ┌───────────┐                          ┌───────┐
 │ PROCESSED │  (terminal)              │ ERROR │  (terminal)
 └───────────┘                          └───────┘
                                            ▲
                                            │ permanent failure (card/terminal not found,
                                            │ invalid data), or the retry budget is spent
```

## 4. Why the pipeline is split into three phases

| Phase | DB transaction | Connection held | Locks held |
|---|---|---|---|
| Claim | short, one statement | one, briefly | row locks on the claimed batch, released at commit |
| Enrich + classify | **none** | **none** | **none** |
| Persist | short, four statements | one, briefly | the transaction row + one statistics row |

Keeping the cache round trip outside a transaction is the difference between a connection pool
that is 5% busy and one that is saturated. A cache call has a tail latency in the hundreds of
milliseconds under stress; holding a transaction across it would pin a HikariCP connection for
that whole time, keep the claim's row locks held, and hold back PostgreSQL's transaction horizon
so autovacuum could not clean dead tuples on the hottest table in the schema.

## 5. Data flow of the fencing token

```
  claim        token = gen_random_uuid()  ──────────┐ written to the row,
                                                    │ returned to the worker
  worker holds the token in memory  ◀───────────────┘

  heartbeat    UPDATE ... WHERE processing_token = <token>   → renews or reports loss
  success      UPDATE ... WHERE processing_token = <token>   → 1 row or OwnershipLost
  retry        UPDATE ... WHERE processing_token = <token>   → 1 row or no-op
  error        UPDATE ... WHERE processing_token = <token>   → 1 row or no-op
  release      UPDATE ... WHERE processing_token = <token>   → 1 row or no-op

  recovery     UPDATE ... SET processing_token = NULL        → every old token is now invalid
```

Every write that can change the fate of a transaction is conditioned on the token. Recovery
invalidates the token in one statement, which atomically revokes the previous owner's ability to
affect the row — without needing to reach, stop, or even know about that owner.

See `docs/concurrency.md` for the locking analysis and `docs/recovery.md` for the crash matrix.
