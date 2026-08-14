# Acceptance criteria C1–C10

For each criterion: where it is implemented, which test proves it, and what the expected behaviour
is.

> **Execution status.** The tests below were written to run, not to look good in a listing. They
> have **not been executed in the authoring environment**, which has no Docker daemon, no JDK 21
> and no access to Maven Central. Run `mvn verify` (Docker required for the `*IT` tests) to turn
> the "expected" column into observed results.

---

## C1 — 1 000 000 NEW transactions reach terminal statuses with no manual intervention

**Implementation**
- `TransactionPoller.pollOnce` — continuous claiming, no operator input, no id ranges.
- `TransactionClaimRepository.CLAIM_BATCH` — the claim query; its `WHERE status = 'NEW'` predicate
  is the entire resume logic.
- `FailureHandler.handle` — every failure ends in `NEW` (retry) or `ERROR` (terminal).
- `StaleProcessingRecoveryService` — anything stuck in PROCESSING is returned to the pool.
- `V2__processing_ownership.sql` — `ck_transactions_status` makes the status set closed, so no row
  can end up in a state the poller cannot see.

**Tests**
- `MultiInstanceProcessingIT.threeInstancesProcessEverythingExactlyOnce` — 1580 mixed transactions
  across three instances; asserts `PROCESSED + ERROR == total` and `NEW == PROCESSING == 0`.
- `ErrorFlowIT.missingReferenceDataEndsInError` — the failing 2% also reach a terminal status.
- At full scale: `./scripts/generate-data.sh && ./scripts/run-3-instances.sh && ./scripts/verify.sh`.

**Expected** every row is `PROCESSED` or `ERROR`; the SQL check
`SELECT count(*) FROM transactions WHERE status NOT IN ('PROCESSED','ERROR')` returns 0.

---

## C2 — with 3 instances, one transaction never creates more than one processed record

**Implementation**
- `TransactionClaimRepository.CLAIM_BATCH` — atomic claim; `FOR UPDATE SKIP LOCKED` plus the
  transition to PROCESSING in one statement.
- `ResultPersistenceService.persist` step 1 — fenced UPDATE, which is also the mutual-exclusion
  point (row lock on `transactions`).
- `ProcessedTransactionRepository.INSERT_IF_ABSENT` — `ON CONFLICT (transaction_id) DO NOTHING`.
- `uk_processed_transaction` in `V1__baseline_tables.sql` — the constraint itself.

**Tests**
- `ClaimExclusivityIT.concurrentClaimersPartitionTheBacklog` — 12 concurrent claimers, 2000 rows,
  zero overlap.
- `IdempotencyConcurrencyIT.concurrentPersistsProduceOneResult` — 16 threads persisting the same
  transaction: exactly 1 success, 15 `OwnershipLostException`.
- `IdempotencyConcurrencyIT.concurrentInstancesProcessingOneTransaction` — 16 independent
  instances racing over a single transaction.
- `MultiInstanceProcessingIT` — end to end across three instances.

**Expected** `countDuplicateResults() == 0` in every case; `processed_transactions` row count
equals the number of successful transactions.

---

## C3 — after `kill -9` of one instance, the system keeps working

**Implementation**
- Instances share no lock, no leader and no coordination — only row locks in PostgreSQL.
- `TransactionPoller` keeps claiming; the dead instance's rows are simply invisible until their
  leases expire.
- `StaleProcessingRecoveryService` runs on **every** instance (no leader election, therefore no
  single point of failure for the recovery mechanism itself).

**Tests**
- `CrashRecoveryIT.survivorsFinishTheWorkAfterOneInstanceDies` — asserts the result count strictly
  increases after the crash.
- `scripts/crash-recovery-demo.sh` step 5 — a real `docker compose kill -s SIGKILL processor-2`,
  then asserts progress 15 s later.

**Expected** `count(processed_transactions)` keeps rising after the kill; the surviving instances
show no errors related to the departure.

---

## C4 — a restarted instance automatically rejoins the work

**Implementation**
- No resume state exists to restore. `ProcessingPipeline.start` starts the poller and workers; the
  first claim query finds whatever is NEW.
- `processor.instance-id` defaults to `${HOSTNAME}`, so a restarted container gets a consistent
  identity without configuration.

**Tests**
- `CrashRecoveryIT.restartedInstanceResumesFromDatabaseStateAlone` — a second harness with no
  memory of the first drains everything, given only a database connection.
- `CrashRecoveryIT.restartingEverythingDoesNotReprocessCompletedWork` — three successive
  generations of instances; the result count never changes after work is done.
- `scripts/crash-recovery-demo.sh` step 6.

**Expected** the restarted instance starts processing within one poll cycle, with no offset, id
list or checkpoint supplied to it.

---

## C5 — stale PROCESSING rows are recovered automatically after the timeout

**Implementation**
- `TransactionClaimRepository.RECOVER_STALE` — `processing_started_at < now() - timeout`, selected
  with `FOR UPDATE SKIP LOCKED`, returned to NEW with the token invalidated.
- `StaleProcessingRecoveryService` — scheduled every `processor.recovery.interval`.
- `LeaseRenewalService` — keeps live workers out of the sweep, so only genuinely abandoned rows
  are reclaimed.
- `idx_transactions_stale_processing` — partial index making the sweep cheap at any table size.

**Tests**
- `StaleRecoveryIT.onlyExpiredLeasesAreReclaimed` — expired reclaimed, healthy untouched.
- `StaleRecoveryIT.heartbeatPreventsSpuriousRecovery` — a renewed lease is not stale.
- `StaleRecoveryIT.rowsLockedByAnActiveCommitAreSkipped` — a row inside a commit is skipped.
- `CrashRecoveryIT` — reclaim after a crash, end to end.

**Expected** `SELECT count(*) FROM transactions WHERE status='PROCESSING' AND processing_started_at
< now() - <timeout>` returns 0 once the system is idle.

---

## C6 — after any crash/restart, lost successful transactions = 0

**Implementation**
- All four writes in one transaction (`ResultPersistenceService.persist`), so there is no partial
  state to lose.
- Recovery returns anything unfinished to the pool.
- `FailureHandler` failing to write is itself safe: the row stays PROCESSING and the lease expires.

**Tests**
- `CrashRecoveryIT` (all three tests) — final counts always equal the input.
- `OutboxAtomicityIT.aFailureAnywhereRollsBackEverything` — a failure mid-unit leaves *nothing*
  behind, so there is no half-processed transaction to lose.
- `scripts/verify.sh` — checks both directions:
  PROCESSED without a result row, and result rows whose transaction is not PROCESSED.

**Expected** both cross-checks return 0; `count(processed_transactions)` equals the number of
transactions with `status = 'PROCESSED'`.

---

## C7 — duplicate `transaction_id` = 0

**Implementation**
- `uk_processed_transaction UNIQUE (transaction_id)` — the unconditional barrier.
- `INSERT ... ON CONFLICT DO NOTHING` — turns a duplicate into a return value instead of an error.
- Statistics and outbox writes are gated on that return value, so "no duplicate row" also means
  "no double counting".

**Tests**
- `IdempotencyConcurrencyIT` (all three tests).
- `FencingTokenIT` (all three tests).
- `MultiInstanceProcessingIT`, `CrashRecoveryIT`, `BackpressureIT` — all assert
  `countDuplicateResults() == 0` as a standing invariant.

**Expected** 0, always, under every combination of concurrency and failure.

---

## C8 — retry is bounded and never becomes an infinite loop

**Implementation**
- `RetryPolicy.shouldRetry` — permanent failures are never retried; transient failures only while
  `retry_count < max-retries`.
- `transactions.retry_count` lives in the database, so a restart cannot reset the budget.
- `RECOVER_STALE` charges the same budget, so repeated lease expiry also terminates.
- `RetryPolicy.backoffFor` — exponential with a hard cap (`retry-max-delay`).

**Tests**
- `RetryPolicyTest` — bounds, exponential growth, cap, `max-retries=0`.
- `ErrorFlowIT.retriesAreBounded` — ends in ERROR and then cannot be claimed again.
- `ErrorFlowIT.permanentFailureSkipsRetries` — permanent failures do not consume the budget.
- `ErrorFlowIT.backoffDelaysTheNextClaim` — the delay is real, not a spin.
- `StaleRecoveryIT.repeatedLeaseExpiryIsBoundedByTheRetryBudget` — poison-pill protection.

**Expected** `max(retry_count) <= max-retries + 1`; every failing transaction reaches ERROR.

---

## C9 — backpressure prevents uncontrolled memory growth

**Implementation**
- `ProcessingQueue` — `ArrayBlockingQueue` with a fixed capacity, allocated up front.
- `TransactionPoller.pollOnce` — `limit = min(batch-size, remainingCapacity())`; skips the cycle
  entirely when there is no room and increments `processor_backpressure_total`.
- `ClaimReleaser` — a claim that cannot be honoured is handed straight back.
- `-XX:+ExitOnOutOfMemoryError` in the Dockerfile — a memory failure becomes a restart, not a
  zombie holding leases.

**Tests**
- `BackpressureIT.pollerNeverClaimsMoreThanTheQueueCanHold` — with workers stopped, PROCESSING
  stops exactly at `queue-capacity` while thousands stay NEW; the backpressure counter rises.
- `BackpressureIT.queueDrainsWithSlowConsumers` — one worker, capacity 16, 800 transactions:
  the queue never exceeds capacity and everything still completes.

**Expected** memory held by the pipeline ≤ `queue-capacity + workers` transactions, independent of
backlog size; `processing_queue_size` never exceeds `processing_queue_capacity`.

---

## C10 — all key scenarios are covered by automated tests

| Scenario | Test |
|---|---|
| INTERNAL / EXTERNAL classification | `BusinessClassifierTest` |
| Commission, including exactly 1 000 000 | `CommissionCalculatorTest` |
| Enrichment, cache miss, timeout, malformed data | `EnrichmentServiceTest` |
| Retry decision and backoff | `RetryPolicyTest` |
| Error classification | `ErrorClassifierTest` |
| Sensitive-data masking | `LogMaskingTest` |
| Lease bookkeeping | `OwnershipRegistryTest` |
| Claim exclusivity under contention | `ClaimExclusivityIT` |
| Idempotency under concurrency | `IdempotencyConcurrencyIT` |
| Fencing / slow-worker race | `FencingTokenIT` |
| Stale recovery, heartbeat, poison pill | `StaleRecoveryIT` |
| Multi-instance end to end | `MultiInstanceProcessingIT` |
| Crash and restart | `CrashRecoveryIT` |
| Account aggregation concurrency | `AccountStatisticsConcurrencyIT` |
| Outbox atomicity and relay | `OutboxAtomicityIT` |
| Retry and error flow end to end | `ErrorFlowIT` |
| Backpressure | `BackpressureIT` |
| Spring wiring, migrations, metrics, actuator | `ApplicationContextIT` |

**Commands**

```bash
mvn test       # unit tests only, no Docker required
mvn verify     # unit + integration tests (Testcontainers; Docker required)
```
