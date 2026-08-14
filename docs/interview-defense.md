# Defence questions 65–80

Answers to section 17 of the assignment, each tied to the code that implements it.

---

### 65. What happens if two instances run `SELECT ... NEW LIMIT 1000` at the same time without locking?

They get **the same 1000 rows**. A plain `SELECT` under READ COMMITTED takes no row locks, and both
instances read the same committed snapshot. Both then enrich all 1000 (double the cache load), both
compute the same results, and both try to insert. The unique constraint keeps the *data* correct,
but the *system* is broken: half the work is thrown away, the database absorbs a stream of conflicts,
and with N instances you do N times the work for 1× the throughput. Adding instances would make it
slower, not faster.

The fix is that selection and ownership must be a single atomic operation — you cannot "look then
take", you must "take". That is what the CTE + `FOR UPDATE SKIP LOCKED` + `UPDATE ... RETURNING`
does in `TransactionClaimRepository.CLAIM_BATCH`.

Proven empirically by `ClaimExclusivityIT.concurrentClaimersPartitionTheBacklog`.

---

### 66. How does `FOR UPDATE SKIP LOCKED` work, and what are its limitations?

**How.** `FOR UPDATE` acquires a row-level exclusive lock on each row the query returns, held until
the transaction ends. Normally, a second transaction that reaches a locked row *blocks* until the
lock is released. `SKIP LOCKED` changes that: rather than waiting, the scan silently ignores rows
that are currently locked and moves on to the next candidate. The result is that N concurrent
consumers partition the table between themselves without ever waiting for each other — it turns a
table into a work queue.

**Limitations:**

1. **No ordering guarantee.** You get "roughly the oldest available", not "the oldest". Fine when
   items are independent, wrong if you need per-key ordering.
2. **Contention at the head of the index.** Every consumer scans the same hot region and has to
   walk past everyone else's locked rows. The cost of skipping grows with consumer count; this is
   the first scaling limit of the pattern.
3. **Sensitivity to bloat.** The scan also walks dead tuples, so a high-churn table needs
   aggressive autovacuum and a suitable `fillfactor` — both configured in
   `V2__processing_ownership.sql`.
4. **The lock lasts only as long as the transaction.** Once the claim commits, the row is
   unprotected for the entire processing duration. `SKIP LOCKED` gives you a safe *handover*, not
   safe *ownership over time*. That gap is exactly what the fencing token covers.
5. **It requires a real row to lock**, so it does not compose with statement-level batching
   patterns that avoid touching rows.

---

### 67. Why is a distributed lock not always better than a database row lock?

Because a distributed lock puts ownership in a *different system* from the data, which creates
problems that a row lock simply does not have:

- **Two systems can disagree.** Redis says you own it; PostgreSQL says the row has already been
  processed. Reconciling that is application logic that can be wrong.
- **You cannot commit the lock and the data together.** The lock release and the data write are two
  separate commits, so there is always a window.
- **A crash leaks the lock** until its TTL expires, and choosing that TTL is the same impossible
  liveness guess as choosing a processing timeout — except that now a *third* system's availability
  is on the critical path.
- **A GC pause or clock skew can expire a lock while its holder is still working.** This is the
  classic distributed-lock failure, and the standard fix is… a fencing token. If you need the token
  anyway, the lock is doing much less work than it appears to.
- **Extra failure domain and extra latency**: the lock service can be down or partitioned while the
  database is perfectly healthy, and every claim costs an extra round trip.

The row lock, by contrast, is free (part of a statement we already run), is released automatically
when the connection dies, and lives in the same transaction as the data.

A distributed lock is the right tool when the protected resource is *not* in a transactional store
— coordinating access to an external API, a file system, a device. Here the resource is a row in
the database that is deciding ownership. Use the database.

---

### 68. Where should the transaction boundaries be, and why is a Redis call undesirable inside a long DB transaction?

**Boundaries** (see `docs/concurrency.md` §2):

```
TX 1: claim    (1 statement)
      — no transaction — enrich (Redis) + classify
TX 2: persist  (4 statements: status, result, statistics, outbox)
```

TX 2 must contain all four writes, because they are one fact expressed in four tables. Splitting
them creates uninterpretable intermediate states — a result whose transaction is still PROCESSING,
statistics counting something with no result, an event for work that rolled back — and each of
those states is a bug waiting for a crash to expose it.

**Why the Redis call must be outside.** A cache round trip is network I/O with a long tail
(hundreds of milliseconds under stress). Holding a database transaction across it would:

1. **Pin a HikariCP connection** for that whole time. With 8 workers and a 20-connection pool,
   the pool stops being a resource and becomes a queue.
2. **Hold the row locks** taken by the claim, blocking anything that touches those rows.
3. **Hold back the transaction horizon**, so autovacuum cannot remove dead tuples — on the highest
   churn table in the schema, which is also the one the claim index lives on. This one is the most
   damaging and the least obvious: it degrades slowly and looks like "the database got slower".

The only reason to put it inside would be needing cache-and-database consistency in one snapshot.
We do not: reference data is immutable for the life of a transaction, and the business result is a
pure function of it.

---

### 69. How is double processing excluded?

Four layers, coarse to fine:

1. **Atomic claim.** Selection and ownership in one statement, so two workers cannot both come to
   believe they own a transaction.
2. **Fencing token.** A fresh UUID per claim, checked by every write that finalises the
   transaction. If recovery reassigned the row, the old owner's writes match zero rows.
3. **Row lock ordering.** The fenced UPDATE runs first in the persistence transaction, so it takes
   the row lock every competitor must also take. Under READ COMMITTED the loser blocks, then
   re-evaluates its `WHERE` against the newly committed row and finds no match.
4. **`UNIQUE(transaction_id)`.** The unconditional barrier: no code path, race, crash or operator
   error can produce a second result row.

Layers 1–3 make double processing rare and cheap. Layer 4 makes a duplicate *result* impossible.
The distinction matters: the system tolerates double *computation* (which is wasteful but
harmless, and which the assignment explicitly permits) while forbidding a double *outcome*.

---

### 70. How is idempotency achieved?

- **The barrier**: `INSERT ... ON CONFLICT (transaction_id) DO NOTHING`. Its return value — 1 or 0
  — tells us whether we produced the result or someone already had.
- **Conditional side effects**: the statistics increment and the outbox event are executed **only**
  when the insert returned 1. This is the part people usually miss. Without it you have "no
  duplicate row" but still double-counted money, which is worse than a duplicate row because it is
  invisible.
- **Determinism**: `BusinessClassifier` is a pure function — no clock, no randomness, no state — so
  a replay computes an identical result and there is nothing to reconcile.
- **Stable identity**: `transaction_id` is the idempotency key, and it comes from the source row,
  not from anything the worker generates.

Note the outbox is at-least-once *to the broker* — it publishes before committing the `PUBLISHED`
flag — so consumers deduplicate on the event UUID. That is a deliberate, documented boundary of
where exactly-once ends.

---

### 71. How does the service know where it stopped after a restart?

It does not need to know, and that is the design.

There is no offset, no checkpoint, no cursor and no "last processed id" anywhere in the codebase.
The progress of the entire system is the `status` column:

- `NEW` — waiting;
- `PROCESSING` — owned by a lease that is either being renewed or is about to expire;
- `PROCESSED` / `ERROR` — done.

A restarting instance does not ask "where was I?". It asks "what is `NEW`?" — which is a query,
not a memory. That is the whole resume mechanism, and it is why no manual intervention, id range or
offset is ever required (assignment requirement 6.1).

---

### 72. Why can a single checkpoint not live only in JVM memory?

Because process memory does not survive the failure it would be protecting against.

- **A crash loses it.** `kill -9`, OOM, a hardware failure — the checkpoint is gone and you cannot
  tell what was in flight. You must then either reprocess an unknown amount of work or skip an
  unknown amount.
- **It cannot be shared.** Three instances would have three checkpoints describing three different
  views of one dataset. There is no consistent global position to write down.
- **It cannot be committed with the data.** A checkpoint in memory and a result in the database are
  two separate updates, so they can disagree at any moment.
- **It assumes ordered, contiguous progress.** With N instances claiming concurrently and failures
  causing out-of-order retries, "position 12345" does not describe the state of the system.

The durable equivalent — per-row status in the same database as the data, updated in the same
transaction as the result — has none of these problems, and costs one column.

---

### 73. How is a PROCESSING row safely recovered after a crash?

```sql
WITH stale AS (
    SELECT id FROM transactions
    WHERE status = 'PROCESSING'
      AND processing_started_at < now() - :timeout
    ORDER BY processing_started_at
    LIMIT :n
    FOR UPDATE SKIP LOCKED
)
UPDATE transactions t
SET status           = CASE WHEN t.retry_count >= :max THEN 'ERROR' ELSE 'NEW' END,
    retry_count      = t.retry_count + 1,
    recovery_count   = t.recovery_count + 1,
    processing_token = NULL,          -- ← this is the fence
    next_attempt_at  = now()
FROM stale s WHERE t.id = s.id
RETURNING t.id, t.external_id, t.status, t.processing_instance;
```

Four safety properties:

1. **`SKIP LOCKED`** — a row that is at this instant inside another worker's commit is locked, so
   it is skipped rather than stolen mid-commit.
2. **`processing_token = NULL`** — atomically revokes the previous owner's ability to affect the
   row, without needing to reach it, stop it, or even know it exists.
3. **Bounded** — each reclaim charges the retry budget, so a row that keeps killing workers ends
   in ERROR instead of cycling forever.
4. **Runs on every instance** — no leader election, therefore no single point of failure in the
   one mechanism whose job is surviving failures. `SKIP LOCKED` makes concurrent sweeps safe.

---

### 74. How do you avoid recovery taking a transaction that a slow worker is still genuinely processing?

This is the hardest question in the assignment. The honest starting point: **you cannot reliably
distinguish "slow" from "dead"** — that is a fundamental result, not an engineering gap. So the
design does not try to be right; it makes being wrong harmless.

**Layer 1 — prevention (make it rare): the lease heartbeat.**
`processing_started_at` does not mean "when processing began", it means "when this lease was last
confirmed alive". `LeaseRenewalService` pushes it forward every `lease-renewal-interval` for every
transaction the instance owns, in one statement. A worker that is merely slow keeps its lease
indefinitely; only a worker that has actually stopped executing lets it lapse. With renewal at
20 s against a 2 min timeout, six consecutive heartbeats must fail before a reclaim is considered.

**Layer 2 — early detection.** The renewal statement's `RETURNING` clause reports which leases were
*not* renewed. Those have been taken away, so `OwnershipRegistry` marks them revoked and the worker
abandons them before wasting a cache lookup and a database transaction.

**Layer 3 — containment (make it harmless): the fencing token.** If a reclaim happens anyway, the
old worker's token no longer matches. Its finalising UPDATE affects 0 rows,
`ResultPersistenceService` throws `OwnershipLostException` *inside* the transaction, and the whole
unit rolls back: no result, no statistics increment, no outbox event, no status change. Two workers
may compute the same transaction; only one can commit it.

**Layer 4 — no mid-commit theft.** `SKIP LOCKED` in the recovery CTE means a row inside an active
commit is skipped.

**Layer 5 — the constraint.** Even if every layer above failed, `UNIQUE(transaction_id)` still
permits only one result row.

`FencingTokenIT` reproduces the exact sequence end to end and asserts that the result count, the
outbox count and the statistics count are all exactly 1.

---

### 75. What is the difference between at-most-once, at-least-once and exactly-once?

- **At-most-once** — mark the work done *before* doing it. A crash loses it silently. Zero
  duplicates, possible loss. Acceptable for metrics; never for money.
- **At-least-once** — do the work, then mark it done. A crash between the two repeats the work.
  Zero loss, possible duplicates. This is what claim-then-process fundamentally gives you.
- **Exactly-once** — as a *delivery* guarantee across a network, impossible: the two-generals
  problem means the acknowledgement itself can be lost. As an *effect*, entirely achievable.

This service is **at-least-once processing with exactly-once effects**. A transaction may be
attempted many times — after a crash, after a lease expiry, after a retry — but the observable
outcome (one result row, one statistics increment, one outbox event) happens exactly once, because
the effects are idempotent and the four writes are atomic.

The outbox → broker hop is explicitly at-least-once, which is why the event carries a stable UUID
for consumer-side deduplication. Being precise about *where* exactly-once ends is more useful than
claiming it end to end.

---

### 76. Which bottleneck appears first as instances are added?

In order:

1. **PostgreSQL write throughput** — this is the first and dominant limit. Every transaction costs
   one small write transaction touching four tables plus their indexes, and every commit needs a
   WAL flush. Instances scale linearly until the database's commit rate saturates.
2. **Contention at the head of the claim index** — with many claimers, `SKIP LOCKED` scans spend
   increasing time walking past each other's locked rows. Mitigated by fewer, larger claims.
3. **Connection count** — N × `maximum-pool-size` connections. Each PostgreSQL backend is a process
   with real memory cost; past a few hundred, the server spends its time context-switching. This is
   where a connection pooler (PgBouncer, transaction mode) becomes necessary.
4. **Autovacuum on `transactions`** — every row is updated at least twice, so dead tuples
   accumulate on the exact table the claim query scans. If vacuum falls behind, claim latency rises
   and the whole system slows in a way that looks like nothing in particular.
5. **Redis** — last, and comfortably so: a single MGET of two keys against an in-memory dataset.

Notably, the *application* is not on this list. The correct response to saturation is to fix the
database (partitioning, batching, a pooler) or change the transport (Kafka), not to add threads.

---

### 77. What happens to the Hikari connection pool if the worker count grows sharply?

Progressive degradation, in this order:

1. While `workers ≤ maximum-pool-size`, nothing: every worker gets a connection on demand.
2. Past that, workers queue in `getConnection()`. Throughput stops improving — the added threads
   only add queueing — while latency percentiles start to climb.
3. At `connection-timeout` (10 s), `getConnection()` starts throwing
   `CannotGetJdbcConnectionException`. `ErrorClassifier` classifies it transient, so those
   transactions go back to NEW with backoff and are retried later.
4. That retry traffic adds *more* load, which is the classic retry-storm feedback loop. It is
   bounded here only because `max-retries` is finite — transactions eventually land in ERROR.
5. Meanwhile the database itself is degrading: more backends, more context switching, more lock
   contention.

So the system degrades rather than corrupts, but the throughput curve is not merely flat — it
bends downward. The right responses are: keep `workers ≤ maximum-pool-size`; raise the pool *and*
PostgreSQL's `max_connections` together; or add instances instead of threads. This is also the
central argument against virtual threads for this workload (`docs/concurrency.md` §4).

The service ships with `leak-detection-threshold: 60000`, which turns a transaction-boundary bug
into a loud log line rather than a slow pool leak.

---

### 78. What is a deadlock and how do you diagnose it?

A deadlock is a cycle in the waits-for graph: transaction A holds lock X and waits for Y while B
holds Y and waits for X. Neither can proceed. PostgreSQL detects the cycle after `deadlock_timeout`
(default 1 s) and aborts one participant with SQLSTATE 40P01.

**Why this design cannot produce one:**

- One persistence transaction touches exactly one `transactions` row, one
  `processed_transactions` row and one `account_statistics` row.
- The table order is identical in every worker: transactions → processed_transactions →
  account_statistics → outbox_events.
- No transaction ever holds two rows of the same table, so there is no pair of rows two workers
  could grab in opposite orders. Without that, no cycle can form.

**Diagnosis if one ever appeared:**
- `log_lock_waits = on` with a suitable `deadlock_timeout`; PostgreSQL logs both statements and the
  cycle it detected.
- `pg_locks` joined to `pg_stat_activity` for the live picture of who holds and who waits.
- `pg_stat_database.deadlocks` as a metric to alert on.

And operationally: a deadlock victim surfaces here as `CannotAcquireLockException`, which is
classified transient and retried with backoff — a survivable event, not an outage.

---

### 79. How do you protect `account_statistics` from lost updates?

By never reading the value into Java at all.

The naive version — `SELECT`, add in Java, `UPDATE` — loses increments whenever two workers
interleave, and under load that interleaving is the norm, not the exception: both read 100, both
compute 150, both write 150, one increment vanishes silently.

The implementation does the arithmetic **inside the database, in one statement, on a row the
statement itself locks**:

```sql
INSERT INTO account_statistics (account, transactions_count, total_amount, total_commission, updated_at)
VALUES (?, 1, ?, ?, now())
ON CONFLICT (account) DO UPDATE
SET transactions_count = account_statistics.transactions_count + 1,
    total_amount       = account_statistics.total_amount + EXCLUDED.total_amount,
    total_commission   = account_statistics.total_commission + EXCLUDED.total_commission,
    updated_at         = now();
```

There is no read-modify-write window, so there is nothing to lose. A concurrent writer blocks on the
row lock and, when it proceeds, re-reads the committed value and adds to *that* — increments
compose. It also handles the first transaction for an account without a separate "create row" path.

Alternatives and why not: `SELECT ... FOR UPDATE` + `UPDATE` is correct but costs two round trips
and a longer lock hold; an optimistic version column is correct but produces retry storms exactly
on hot accounts; an insert-only ledger with periodic rollup is the right answer at extreme write
rates on a single account, at the cost of losing real-time statistics.

`AccountStatisticsConcurrencyIT` proves exactness with 1500 transactions on one account across
three instances, and with 3200 direct UPSERTs from 32 threads.

---

### 80. When should PostgreSQL polling be replaced by a Kafka consumer group?

Polling is the right choice when the data is **already in the database**, when you need
**transactional coupling** between the work item and its result, and when the throughput is within
the database's comfortable write range. That is precisely this assignment, and it buys enormous
simplicity: no extra infrastructure, exactly-once effects for free, and the ability to answer "what
is the state of transaction X?" with a single query.

**Switch to Kafka when:**

1. **Polling cost stops being negligible.** At high instance counts, every instance repeatedly
   scanning the head of the same index is pure overhead — you are paying the database to tell you
   "nothing new" thousands of times per minute. Kafka pushes instead.
2. **Throughput exceeds what one PostgreSQL can commit.** Around the assignment's Senior Challenge
   figures — 50M/day, peaks of 10–20k TPS — a single instance is at its limit. Kafka partitions
   horizontally; a single PostgreSQL does not.
3. **The work originates as an event.** If an upstream system already emits transactions, writing
   them to a table just to poll them back out adds a hop and a failure mode for nothing.
4. **You need ordering per key.** Kafka gives per-partition ordering natively; `SKIP LOCKED`
   explicitly does not.
5. **You need multiple independent consumers** of the same stream. In the database model, each new
   consumer needs its own status column or its own table.
6. **You want offset-based replay.** Reprocessing a time range in Kafka is an offset reset; in the
   database it is a bulk UPDATE that interacts with everything else.

**What you give up:** the work item and its result are no longer in one transactional store, so
exactly-once effects have to be rebuilt — which in practice means the same idempotency key and the
same unique constraint we already have, plus an outbox for anything you emit. The consumer becomes
at-least-once and you deduplicate at the destination.

**The pragmatic middle path**, and the one I would actually propose first for this system: keep
PostgreSQL as the system of record, add Kafka *in front* as the transport, and use the existing
outbox to publish downstream. The processing logic, the idempotency barrier and the recovery model
survive the change untouched — which is a reasonable argument that this design is already the right
shape for that migration.
