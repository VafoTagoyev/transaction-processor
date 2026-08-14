# Concurrency

## 1. The claim: what happens without locking, and why this works

Two instances running the naive query at the same time:

```sql
SELECT * FROM transactions WHERE status = 'NEW' ORDER BY id LIMIT 1000;
```

both get **the same 1000 rows**. `SELECT` under READ COMMITTED takes no row locks and both see the
same committed snapshot. Both then enrich and process all 1000, and both try to write results. The
unique constraint saves the data, but the system does twice the work, doubles the load on the
cache and the database, and produces a stream of constraint conflicts. With three instances it is
three times the work. Nothing about it scales.

The fix is that selection and ownership must be **one atomic operation**:

```sql
WITH candidates AS (
    SELECT id
    FROM transactions
    WHERE status = 'NEW' AND next_attempt_at <= now()
    ORDER BY next_attempt_at, id
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
)
UPDATE transactions t
SET status                = 'PROCESSING',
    processing_started_at = now(),
    processing_instance   = :instance,
    processing_token      = gen_random_uuid(),
    updated_at            = now()
FROM candidates c
WHERE t.id = c.id
RETURNING t.id, t.external_id, t.card_id, t.terminal_id, t.amount, t.currency,
          t.transaction_type, t.retry_count, t.processing_token;
```

Properties:

- **`FOR UPDATE`** takes a row-level exclusive lock on each candidate, held until the statement's
  transaction commits. No other transaction can update those rows in the meantime.
- **`SKIP LOCKED`** makes concurrent claimers *step over* rows another claimer already holds
  instead of blocking behind them. Without it, three instances polling simultaneously would
  serialise: two would block on the first instance's locks and then find the rows no longer NEW.
- **One statement** means there is no window between "I selected it" and "I own it". A crash
  between the two is not possible, because there is no "between".
- **`RETURNING`** delivers the payload in the same round trip: claiming 1000 rows costs one
  statement, not 1 + 1000.
- **`next_attempt_at <= now()`** implements retry backoff for free — a transaction waiting out its
  backoff is simply not a candidate.

Proven by `ClaimExclusivityIT`: 12 threads claiming concurrently from a 2000 row backlog produce
2000 claims with zero overlaps and zero losses.

### Limitations of SKIP LOCKED (defence question 66)

1. **No ordering guarantee.** Rows are processed in roughly, not exactly, index order. For this
   workload transactions are independent, so it does not matter. If per-key ordering were
   required, SKIP LOCKED alone would be the wrong tool and you would partition by key.
2. **Contention at the head of the index.** All claimers scan the same "oldest NEW rows" region
   and must walk over each other's locked rows. Cost grows with the number of concurrent
   claimers. Mitigations: fewer, larger claims (one poller thread per instance, batch of 1000);
   and at very high instance counts, partitioning or a randomised starting offset.
3. **Bloat sensitivity.** The scan walks dead tuples too. Hence `fillfactor = 85` and aggressive
   autovacuum settings on `transactions` in `V2__processing_ownership.sql`.
4. **It only protects rows the statement actually locks.** It is not a substitute for a fence:
   once the claiming transaction commits, the lock is gone and the row is unprotected. That gap —
   which lasts for the entire processing duration — is what `processing_token` covers.

### Why a database row lock beats a distributed lock (defence question 67)

| | Redis/Zookeeper lock | PostgreSQL row lock + fenced UPDATE |
|---|---|---|
| Ownership and data | separate systems that can disagree | the same row; ownership *is* a column |
| Crash of the holder | lock leaks until TTL expiry; TTL guessing | row lock released by the database at disconnect |
| Clock skew / GC pause | expired lock while the holder still works — the classic distributed-lock failure | the same risk, but the fenced UPDATE makes it harmless |
| Extra failure domain | yes: the lock service can be down or partitioned while the DB is fine | no: if PostgreSQL is down nothing can proceed anyway |
| Cost per transaction | at least one extra network round trip | zero — it is part of a statement we already run |
| Atomicity with the write | impossible: two systems, two commits | the same transaction |

A distributed lock is the right tool when the resource is *not* in a transactional store. Here it
is. Adding one would add a dependency, a failure mode and a round trip, and would still need the
fencing token — so it would strictly increase risk. (Note that even Redlock's own documentation
concedes that a fencing token is required for correctness; if you need the token anyway, the lock
is doing much less than it appears to.)

## 2. Transaction boundaries (defence question 68)

```
┌───────────────────────────────────────────────────────────────────────────┐
│ TX 1  claim            1 statement           ~1 ms       connection held  │
├───────────────────────────────────────────────────────────────────────────┤
│  no transaction        MGET card + terminal  ~0.3–200 ms  NO connection   │
│  no transaction        classify + commission  ~microseconds               │
├───────────────────────────────────────────────────────────────────────────┤
│ TX 2  persist          4 statements          ~1–3 ms     connection held  │
│         a. UPDATE transactions  (fenced)                                  │
│         b. INSERT processed_transactions ON CONFLICT DO NOTHING           │
│         c. INSERT account_statistics ON CONFLICT DO UPDATE                │
│         d. INSERT outbox_events                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

**Why a, b, c and d are one transaction.** They are one fact — "this transaction was processed" —
expressed in four tables. Split them and you invent states the system cannot interpret: a result
row whose transaction is still PROCESSING, statistics that count a transaction with no result, an
event for something that was rolled back. Committing them together means there is exactly one
instant at which the outcome changes, which is what makes a crash at an arbitrary point safe (see
`docs/recovery.md`).

**Why the cache call is outside.** A cache round trip is network I/O with a long tail. Inside a
transaction it would:
- pin a HikariCP connection for its whole duration — with 8 workers and a 200 ms tail, the pool
  spends its time waiting rather than working;
- hold the row locks taken by the claim for the whole enrichment;
- hold back the transaction horizon, so autovacuum cannot remove dead tuples on the highest-churn
  table in the schema, degrading exactly the index the claim query depends on.

The only reason to put it inside would be needing the cache value to be consistent with a database
read in the same snapshot. It is not: reference data is immutable for the life of a transaction
and the business result is a pure function of it.

**Isolation level.** READ COMMITTED (the PostgreSQL default) throughout. No step needs repeatable
reads: correctness comes from row locks, the fence predicate and unique constraints, none of which
depend on snapshot semantics. REPEATABLE READ would add serialisation-failure retries for no
benefit; SERIALIZABLE would add substantial overhead and predicate-lock conflicts on a hot table.

## 3. Idempotency (defence questions 69 and 70)

Layered, from the coarsest to the finest:

1. **Atomic claim** — normally only one worker ever holds a transaction.
2. **Fencing token** — if a lease is reassigned, the old owner's writes match zero rows.
3. **Row lock on `transactions`** — concurrent finalisers serialise; the losers are rejected by
   the fence when they re-evaluate.
4. **`UNIQUE(transaction_id)` on `processed_transactions`** — the unconditional barrier. Not
   bypassable by any code path, any race, any crash, or any operator error.
5. **Conditional side effects** — statistics and outbox are applied only when the insert reported
   a row count of 1, which turns them from at-least-once into exactly-once.

Layer 4 is the one that matters most, because it is the only one that cannot be undermined by a
bug in layers 1–3. Layer 5 is what makes idempotency mean "no double counting", not merely "no
second row".

### at-most-once / at-least-once / exactly-once (defence question 75)

- **At-most-once**: mark the work done before doing it. A crash loses it silently. Never
  acceptable for financial data.
- **At-least-once**: do the work, then mark it done. A crash between the two repeats the work.
  This is what claim-then-process gives you, and it is the honest description of *processing* here.
- **Exactly-once delivery** across a network is impossible. **Exactly-once *effect*** is not, and
  it is what this service provides: processing may be attempted many times, but the observable
  outcome — one result row, one statistics increment, one outbox event — occurs exactly once,
  because the effect is idempotent and the four writes are atomic.

The outbox relay is explicitly at-least-once to the broker: it publishes before committing the
`PUBLISHED` flag, so a crash in that window republishes. Consumers deduplicate on the stable
event UUID.

## 4. Threading model (defence question 77)

```
1 poller thread  ──▶ ArrayBlockingQueue(capacity) ──▶ N worker threads
                                                       (N = processor.workers)
3 scheduler threads: lease heartbeat, recovery sweep, outbox relay
```

### Why platform threads and not virtual threads

Per transaction the work is: one cache round trip (blocking), one short JDBC transaction
(blocking, requires a pooled connection). Analysis:

1. **The connection pool is the real ceiling.** Useful concurrency at the database is bounded by
   `maximum-pool-size`, not by thread count. 10 000 virtual threads against a 20-connection pool
   do not produce more throughput — they produce 10 000 objects waiting on a semaphore, plus
   10 000 sets of in-flight enrichment data on the heap, plus much worse latency percentiles.
2. **Pinning.** On Java 21 a virtual thread that blocks inside a `synchronized` block pins its
   carrier. JDBC drivers and connection pools still use `synchronized` liberally. The scalability
   argument for virtual threads is therefore weakest exactly where this workload spends its time.
   (This is improved in later JDKs, but the assignment specifies Java 21.)
3. **The blocking is short and bounded.** Virtual threads shine when tasks block for a long time
   on many independent I/O channels. Here each task blocks for single-digit milliseconds on two
   resources that are both already pool-limited.

**Decision: a fixed pool of platform threads, sized in the same range as the HikariCP pool.**
Memory is predictable, database load is predictable, and queue depth is directly observable.

**If virtual threads were adopted anyway**, the mandatory guard would be a `Semaphore` with
permits ≤ `maximum-pool-size` around the persistence step, plus keeping the bounded intake queue.
Without that, the connection pool becomes the failure point: every virtual thread blocks in
`getConnection()`, `connection-timeout` starts firing, and the resulting exceptions are classified
as transient and retried — turning a capacity problem into a retry storm.

### Starvation and pool exhaustion

- Workers never wait on each other and never hold a database connection during I/O, so no worker
  can block another.
- A connection is held only for the duration of the persistence transaction. With
  `workers ≤ maximum-pool-size` no worker ever waits for a connection.
- Budget for the default configuration: 8 workers + 1 poller + 1 heartbeat + 1 recovery +
  1 relay = 12 concurrent users against `maximum-pool-size: 20`. Comfortable headroom, and
  `leak-detection-threshold: 60000` will complain loudly if a connection is ever held longer than
  a minute, which would mean a transaction-boundary bug.
- **What happens if workers are raised sharply** (question 77): once `workers > pool size`,
  threads queue in `getConnection()`. Latency rises first; then `connection-timeout` (10 s) starts
  firing `CannotGetJdbcConnectionException`, which `ErrorClassifier` treats as transient, so the
  transactions go back to NEW with backoff and are retried. The system degrades rather than
  corrupts — but throughput *falls*, because the added threads only add queueing. The correct
  response is to raise the pool (and the database's `max_connections`) or add instances, not
  threads.

### Deadlocks (defence question 78)

A deadlock is a cycle in the waits-for graph: A holds X and wants Y, B holds Y and wants X.
PostgreSQL detects it after `deadlock_timeout` and kills one transaction with SQLSTATE 40P01.

This design cannot form such a cycle:

- One persistence transaction touches **one** `transactions` row, **one** `processed_transactions`
  row and **one** `account_statistics` row.
- The order across tables is identical in every worker: transactions → processed_transactions →
  account_statistics → outbox_events.
- No transaction ever holds two rows of the same table, so there is no pair of rows that two
  workers could acquire in opposite orders.

Diagnosis if one ever appeared: `log_lock_waits = on` plus `deadlock_timeout`; the PostgreSQL log
prints both statements and the cycle; `pg_locks` joined to `pg_stat_activity` shows the live
picture. And in this service, a deadlock victim surfaces as `CannotAcquireLockException`, which is
classified transient and retried with backoff — a survivable event, not an outage.

## 5. account_statistics: preventing lost updates (defence question 79)

The wrong way, and why:

```java
var current = jdbc.queryForObject("SELECT total_amount FROM account_statistics WHERE account = ?", ...);
var updated = current.add(amount);
jdbc.update("UPDATE account_statistics SET total_amount = ? WHERE account = ?", updated, ...);
```

Two workers read 100, both compute 150, both write 150. One increment vanished. Under READ
COMMITTED this is not a rare interleaving — it is the *normal* one under load.

The way this service does it:

```sql
INSERT INTO account_statistics (account, transactions_count, total_amount, total_commission, updated_at)
VALUES (?, 1, ?, ?, now())
ON CONFLICT (account) DO UPDATE
SET transactions_count = account_statistics.transactions_count + 1,
    total_amount       = account_statistics.total_amount + EXCLUDED.total_amount,
    total_commission   = account_statistics.total_commission + EXCLUDED.total_commission,
    updated_at         = now();
```

The read, the addition and the write are one statement on a row the statement locks. There is no
window to lose anything in. A concurrent writer blocks on the row lock, and when it proceeds it
re-reads the committed value and adds to *that*. Increments compose.

Alternatives considered:
- `SELECT ... FOR UPDATE` then `UPDATE`: also correct, but two round trips and a longer lock hold.
- Optimistic version column: correct, but generates retry storms precisely on hot accounts.
- Insert-only ledger plus periodic rollup: the right answer at very high write rates on a single
  account, at the cost of statistics no longer being real-time. Noted in "known limitations".

Proven by `AccountStatisticsConcurrencyIT`: 1500 transactions on one account across three
instances, and 3200 direct UPSERTs from 32 threads — exact totals in both.

## 6. Backpressure

```
limit = min(batch-size, queue.remainingCapacity())
if (queue.remainingCapacity() < min-claim-batch) { record backpressure; skip this cycle; }
```

This is stronger than "block when the queue is full", because a claim is not free: it writes
PROCESSING to the database. Over-claiming would mark rows as owned by an instance that has no
capacity to work on them, hiding them from the other instances until the lease expired. Sizing the
claim by free capacity means the backlog stays in PostgreSQL, where every instance can see it.

Memory held by the pipeline is bounded by `queue-capacity + workers` claimed transactions —
a configuration decision, not a function of how far behind the service is. `ArrayBlockingQueue` is
chosen precisely because its capacity is fixed at construction: it *cannot* grow.

Proven by `BackpressureIT`: with the workers stopped, the number of PROCESSING rows stops exactly
at `queue-capacity` while thousands remain NEW, and the backpressure counter increments.

## 7. Parallel writes: batch or per transaction? (assignment item 29)

Per-transaction writes were chosen. Trade-offs:

| | Per transaction (chosen) | Batched multi-row insert |
|---|---|---|
| Round trips | one small transaction per result | one per N results |
| Throughput ceiling | lower per worker, scales by adding workers/instances | higher per worker |
| Fencing | natural: one row, one token, one check | needs a per-row fence check before the batch, or the batch commits work the instance no longer owns |
| Poison row | isolated: only that transaction fails | the whole batch fails and must be split and retried |
| Failure attribution | exact | requires per-row unpicking |

Correctness under concurrency is the graded property here, so the simple form wins. If the write
path became the bottleneck, the batched variant is still available: fence all N rows with a single
`UPDATE ... WHERE id = ANY(?) AND processing_token = ANY(?) RETURNING id`, then insert only the
returned ids. That keeps the guarantees and amortises the round trips — at the cost of the
poison-row behaviour above.
