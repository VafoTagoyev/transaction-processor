# Recovery

> **The question this document answers:** *how can the system guarantee no data loss and no
> duplicate processed result when the JVM dies at an arbitrary instruction?*

## 1. The short answer

Three properties, in this order of importance.

**1. There is no in-memory progress state to lose.** The service has no offset, no checkpoint, no
cursor and no "last processed id" anywhere. The progress of the whole system *is* the `status`
column of the `transactions` table. A process that dies takes nothing with it that anyone needs.
A process that starts does not ask "where was I?" — it asks "what is still NEW?", which is a
query, not a memory.

**2. There is exactly one instant at which a transaction's outcome changes: the COMMIT of the
persistence transaction.** Status change, result row, statistics increment and outbox event are
all written by that one transaction. Before the COMMIT, nothing happened. After it, everything
happened. PostgreSQL makes that instant atomic and durable, and there is no code path that can
observe a half state. So "the JVM died at an arbitrary instruction" reduces to a single binary
question: *did the COMMIT land or not?*

**3. Both answers to that question are safe.**
- *It did not land.* The row is still PROCESSING with a lease nobody renews. The recovery sweep
  returns it to NEW after the processing timeout, and it is processed again from scratch. The
  business computation is a pure function of immutable input, so recomputing it produces exactly
  the same result. Nothing lost.
- *It did land.* The row is PROCESSED and a `processed_transactions` row exists. Even if
  something later pushed that transaction back into the pool, the reprocessing hits
  `ON CONFLICT (transaction_id) DO NOTHING`, gets a row count of 0, and *skips the statistics
  increment and the outbox event*. Nothing duplicated.

Everything below is the detail behind those three sentences.

## 2. The two barriers

The design does not rely on a single mechanism. It has an outer barrier that makes duplicates
*rare* and an inner barrier that makes them *impossible*.

| | Mechanism | What it prevents | What happens if it fails |
|---|---|---|---|
| Outer | Atomic claim + fencing token + lease heartbeat | Two workers owning the same transaction at the same time | The inner barrier still holds |
| Inner | `UNIQUE(transaction_id)` on `processed_transactions` | A second result row, ever, for any reason | Nothing — this one cannot fail; it is a database constraint |

The inner barrier is deliberately not clever. It does not depend on the token being correct, on
the clock being accurate, on the heartbeat running, or on the code being right. That is the point:
the property "at most one result per transaction" is enforced by something that has no bugs to
have.

## 3. Crash scenario matrix (assignment section 6)

Notation: `T` = the transaction row, `P` = the `processed_transactions` row.

### Scenario 1 — crash before the claim

| | |
|---|---|
| **State left behind** | `T.status = NEW`. Nothing was written. |
| **Who notices** | Nobody has to. The row was never invisible to anyone. |
| **What happens** | The next poll on any instance — including a restart of the dead one — claims it normally. |
| **Result** | No loss. `processed = 1` eventually. |
| **Proven by** | `CrashRecoveryIT.survivorsFinishTheWorkAfterOneInstanceDies` |

### Scenario 2 — crash after the PROCESSING claim, before enrichment

| | |
|---|---|
| **State left behind** | `T.status = PROCESSING`, `processing_started_at` set, `processing_instance` set, `processing_token` set. Invisible to the claim query. |
| **Who notices** | The recovery sweep on any surviving instance. Nobody renews this lease, so `processing_started_at` stops moving. |
| **What happens** | After `processing-timeout`, `recoverStale` sets `status = NEW`, `retry_count + 1`, `recovery_count + 1`, `processing_token = NULL`. Any instance then claims it. |
| **Result** | The row does not hang forever. That is criterion **C5**. |
| **Proven by** | `StaleRecoveryIT.onlyExpiredLeasesAreReclaimed`, `CrashRecoveryIT.restartedInstanceResumesFromDatabaseStateAlone` |

### Scenario 3 — crash during Redis enrichment

Identical to scenario 2 from the database's point of view: no database write had happened yet.
The cache lookup has no side effects, so there is nothing to undo. Recovery reclaims and the new
owner performs the lookup again.

### Scenario 4 — crash after enrichment, before the result insert

Also identical to scenario 2. The enriched data and the computed commission existed only in the
dead JVM's heap. The assignment explicitly permits repeating the computation here
("Допускается повтор вычисления, но не дубль"), and the recomputation is guaranteed to produce
the same output because `BusinessClassifier` is a pure function of the transaction row and the
cached reference data — no clock, no randomness, no accumulated state.
Proven by `BusinessClassifierTest.classificationIsDeterministic`.

### Scenario 5 — crash after the result INSERT but before the COMMIT

| | |
|---|---|
| **State left behind** | Nothing. PostgreSQL rolls the transaction back when the connection dies. `P` does not exist, `T.status` is still `PROCESSING`. |
| **Why** | The INSERT was never visible to any other transaction — uncommitted rows are invisible under READ COMMITTED, and the row is discarded at rollback. This is not something the application arranges; it is what a database *is*. |
| **What happens** | Exactly scenario 2. Recovery reclaims, the new owner recomputes and commits. |
| **Result** | Consistency preserved, no partial write, no duplicate. |
| **Proven by** | `OutboxAtomicityIT.aFailureAnywhereRollsBackEverything` — a failure injected after the result insert leaves *no* result row, *no* outbox event, *no* statistics row, and the status still `PROCESSING`. |

### Scenario 6 — crash immediately after the COMMIT

| | |
|---|---|
| **State left behind** | Fully consistent: `T.status = PROCESSED`, `P` exists, statistics incremented once, outbox event written. |
| **Risk** | The dead instance never got to "finish"; an operator or an unusual sequence could push the row back into the pool. |
| **What happens if it is reprocessed** | The claim succeeds, enrichment and classification rerun, and then `insertIfAbsent` returns **false**. The service treats that as an idempotent replay: it commits the status repair but skips the statistics increment and the outbox event. |
| **Result** | `duplicates = 0`. That is criterion **C7**. |
| **Proven by** | `IdempotencyConcurrencyIT.replayAfterRecoveryIsIdempotent` |

### Scenario 7 — one of three instances is killed

| | |
|---|---|
| **State left behind** | Its in-flight transactions are PROCESSING with expired-soon leases; everything else is untouched. |
| **What happens** | The other two never even pause: they are not coordinated with the dead one, share no lock, and hold no reference to it. They keep claiming NEW rows throughout. Within one processing timeout, their recovery sweeps reclaim the orphaned rows. |
| **Result** | Processing continues (**C3**) and nothing is lost (**C6**). |
| **Proven by** | `CrashRecoveryIT.survivorsFinishTheWorkAfterOneInstanceDies` and `scripts/crash-recovery-demo.sh` (a real `kill -9` on a container) |

### Scenario 8 — every instance is restarted

| | |
|---|---|
| **State left behind** | All in-memory state is gone on all three. |
| **What happens** | Each instance starts, connects, and begins claiming NEW rows. It is given no offset, no id range and no checkpoint. Orphaned PROCESSING rows are reclaimed by the timeout sweep. Already PROCESSED rows are simply not selected by the claim query — its `WHERE status = 'NEW'` predicate is the whole resume logic. |
| **Result** | Automatic continuation (**C4**), and no reprocessing of completed work. |
| **Proven by** | `CrashRecoveryIT.restartingEverythingDoesNotReprocessCompletedWork` |

## 4. The hard part: the slow-worker race

This is assignment requirement 47 and defence question 74, and it is the only genuinely subtle
problem in the whole design.

**The race.** Worker A on instance 1 claims transaction 42 and starts working. It is slow — a
long GC pause, a stalled cache connection, a frozen container. Its lease expires. Instance 2's
recovery sweep returns 42 to NEW. Instance 2 claims it, processes it and commits. Worker A now
wakes up and tries to commit its own result.

**Why this is not solvable by tuning.** Any timeout-based liveness detection can be wrong: it is
impossible in general to distinguish a process that is slow from one that is dead. Making the
timeout longer only makes the race rarer and recovery slower — it does not remove it. So the
design does not try to detect liveness perfectly. It makes being wrong harmless.

### Layer 1 — prevention: the lease heartbeat

`processing_started_at` is not really "when processing began". It is "when this lease was last
confirmed alive". Every `lease-renewal-interval`, `LeaseRenewalService` pushes it forward for
every transaction this instance owns, in one statement:

```sql
UPDATE transactions t
   SET processing_started_at = now(), updated_at = now()
  FROM unnest(?::bigint[], ?::uuid[]) AS v(id, token)
 WHERE t.id = v.id AND t.processing_token = v.token AND t.status = 'PROCESSING'
RETURNING t.id;
```

A worker that is merely slow keeps its lease indefinitely, no matter how long a single
transaction takes or how deep the queue is. Only a worker that has genuinely stopped executing —
dead JVM, frozen container, partitioned from the database — lets the lease lapse. With
`lease-renewal-interval` at a small fraction of `processing-timeout` (20 s versus 2 min in the
compose file), several consecutive heartbeats must fail before a reclaim is even considered.

The renewal is itself fenced, and its `RETURNING` clause is a free liveness check in the other
direction: any id the database declines to renew has been taken away, so `OwnershipRegistry`
marks it revoked and the worker abandons it *before* wasting a cache lookup and a database
transaction on work it will not be allowed to commit.

Proven by `StaleRecoveryIT.heartbeatPreventsSpuriousRecovery` and
`StaleRecoveryIT.heartbeatDetectsRevokedLeases`.

### Layer 2 — containment: the fencing token

If a reclaim happens anyway, it must not matter. Every claim generates a **new** UUID into
`processing_token`, and the worker keeps that value in memory. Every write that finalises a
transaction carries it:

```sql
UPDATE transactions
   SET status = 'PROCESSED', processed_at = now(), processing_token = NULL, ...
 WHERE id = ? AND status = 'PROCESSING' AND processing_token = ?::uuid;
```

Recovery sets `processing_token = NULL`. From that moment the old worker's token matches nothing.
Its UPDATE reports **0 rows affected**, `ResultPersistenceService` throws `OwnershipLostException`,
and — because that happens *inside* the `@Transactional` method — the entire persistence
transaction rolls back: no result row, no statistics increment, no outbox event, no status change.

Two workers may briefly *compute* the same transaction. Only one can ever *commit* it.

Proven by `FencingTokenIT`, which reproduces the full sequence: slow worker computes → lease
expires → recovery reclaims → new owner commits → slow worker attempts to commit and is rejected,
with assertions that the result count, the outbox count and the statistics count are all exactly 1.

### Layer 3 — mutual exclusion for free: the fenced UPDATE goes first

The order of statements inside the persistence transaction is not arbitrary. The fenced UPDATE on
`transactions` runs **first**, which means it takes the row lock on the one row every competing
worker must also touch. Under READ COMMITTED, a second worker's UPDATE blocks on that lock, and
when the first transaction commits, PostgreSQL re-evaluates the second UPDATE's `WHERE` clause
against the newly committed row version. The status is no longer `PROCESSING` and the token no
longer matches, so it affects zero rows. The row lock is the mutex and the token is the fence, and
we get both from one statement.

Proven by `IdempotencyConcurrencyIT.concurrentPersistsProduceOneResult`: 16 threads persisting the
same transaction simultaneously produce exactly 1 success and exactly 15 `OwnershipLostException`.

### Layer 4 — recovery cannot steal a row mid-commit

The recovery sweep selects candidates with `FOR UPDATE SKIP LOCKED`. A row that is *at this very
moment* inside another worker's persistence transaction is locked by that transaction, so the
sweep skips it rather than reclaiming it out from under a commit that is about to succeed.

Proven by `StaleRecoveryIT.rowsLockedByAnActiveCommitAreSkipped`.

## 5. Why recovery cannot loop forever

A transaction whose processing reliably kills whichever worker touches it (a "poison pill") would,
with a naive design, be reclaimed and re-killed indefinitely. Each reclaim therefore charges the
same retry budget as an ordinary failure:

```sql
SET status = CASE WHEN t.retry_count >= :max_retries THEN 'ERROR' ELSE 'NEW' END,
    retry_count = t.retry_count + 1,
    recovery_count = t.recovery_count + 1
```

After `max-retries` reclaims the transaction becomes `ERROR` with
`error_message = 'LEASE_EXPIRED: ...'` — a terminal status the claim query will never select
again. `recovery_count` is tracked separately so operators can tell "the business logic failed"
apart from "workers keep dying on this row".

Proven by `StaleRecoveryIT.repeatedLeaseExpiryIsBoundedByTheRetryBudget`, which asserts the
transition to ERROR on the fourth expiry and then asserts that a fresh instance cannot claim it.

## 6. Graceful shutdown: the case that should not need recovery

`kill -9` is what recovery is for. A normal SIGTERM (`docker stop`, a rolling deploy) should cost
nothing, so `ProcessingPipeline` implements `SmartLifecycle`:

1. the poller stops claiming;
2. workers keep draining the queue until it is empty or the grace period expires;
3. anything still queued is **released back to NEW immediately** (`ClaimReleaser`), with
   `retry_count` untouched because nothing was attempted.

The work is available to the other instances on their very next poll, rather than after a full
processing timeout. A rolling restart is therefore invisible in the throughput graph, and
recovery is reserved for actual failures.

## 7. Operational verification

`scripts/verify.sql` and `scripts/verify.sh` implement the assignment's checks 61-64 as SQL:

```sql
-- nothing left un-terminal                       (C1)
SELECT count(*) FROM transactions WHERE status NOT IN ('PROCESSED','ERROR');
-- duplicate results                              (C7)
SELECT count(*) FROM (SELECT transaction_id FROM processed_transactions
                      GROUP BY transaction_id HAVING count(*) > 1) d;
-- PROCESSED without a result row, and vice versa (C6)
-- PROCESSING older than the processing timeout   (C5)
```

`scripts/crash-recovery-demo.sh` runs the full section 6.2 procedure end to end against real
containers, including a real `SIGKILL`.
