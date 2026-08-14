# Performance test

> **Status of the numbers below: NOT MEASURED.**
> The environment in which this project was written has no Docker daemon, no JDK 21 and no access
> to Maven Central, so the suite could not be executed. Every cell in the results tables is a
> placeholder marked `TO BE MEASURED`. Nothing here is estimated, extrapolated or invented — run
> `./scripts/perf-test.sh` and paste in the real output.

## 1. What is measured

| Metric | Source |
|---|---|
| TPS | `count(processed_transactions)` ÷ wall-clock seconds from first claim to empty backlog |
| Avg latency | `processing_duration_seconds_sum ÷ processing_duration_seconds_count`, summed over instances |
| P95 / P99 | `processing_duration_seconds{quantile="0.95"/"0.99"}` from `/actuator/prometheus` |
| DB connections | peak `SELECT count(*) FROM pg_stat_activity WHERE datname='txprocessor'`, sampled every 5 s |
| Redis latency | `redis_lookup_duration_seconds_sum ÷ _count` |

`processing_duration` covers the full per-transaction path: enrichment, classification and the
persistence transaction. It does not include time spent waiting in the bounded queue, which is
reported separately by `processing_queue_size`.

## 2. Procedure

Fully automated:

```bash
./scripts/perf-test.sh                 # 1 000 000 transactions; 1, 3 and 5 instances
./scripts/perf-test.sh 200000 "1 3"    # smaller dataset, fewer configurations
```

For each configuration the script regenerates the dataset from the same fixed seed (so every run
sees byte-identical input), scales the `processor` service, samples until the backlog is drained,
collects the metrics above, and appends one row to `docs/performance-results-<timestamp>.md`. It
also re-checks duplicates and un-terminal rows after every configuration, so a fast run that was
also wrong cannot pass unnoticed.

Manual equivalent:

```bash
docker compose -f docker-compose.perf.yml --profile generator run --rm generator
docker compose -f docker-compose.perf.yml up -d --build --scale processor=1
# ...wait for the backlog to drain, collect metrics, then:
docker compose -f docker-compose.perf.yml up -d --scale processor=3
docker compose -f docker-compose.perf.yml up -d --scale processor=5
```

### Environment to record with the results

| Item | Value |
|---|---|
| Host CPU / cores | TO BE MEASURED |
| Host RAM | TO BE MEASURED |
| Disk (NVMe / SSD / network) | TO BE MEASURED |
| Docker version / platform | TO BE MEASURED |
| PostgreSQL image and settings | `postgres:16-alpine`, see `docker-compose.perf.yml` |
| Valkey image | `valkey/valkey:8-alpine` |
| Dataset | 1 000 000 transactions, 100 000 cards, 10 000 terminals, seed 20240101 |
| Per-instance config | `WORKERS=8`, `BATCH_SIZE=1000`, `QUEUE_CAPACITY=5000`, `DB_POOL_SIZE=20` |

## 3. Results

### 3.1 Scaling (the assignment's table, section 12)

| Instances | TPS | Avg (ms) | P95 (ms) | P99 (ms) | DB connections | Redis latency (ms) |
|---|---|---|---|---|---|---|
| 1 | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED |
| 3 | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED |
| 5 | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED |

### 3.2 Extended view

| Instances | Workers/instance | Wall clock (s) | TPS | Scaling vs 1 instance | Peak DB connections | Duplicates | Un-terminal rows |
|---|---|---|---|---|---|---|---|
| 1 | 8 | TO BE MEASURED | TO BE MEASURED | 1.00× | TO BE MEASURED | must be 0 | must be 0 |
| 3 | 8 | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | must be 0 | must be 0 |
| 5 | 8 | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | TO BE MEASURED | must be 0 | must be 0 |

### 3.3 Data generation (record separately)

| Step | Volume | Duration |
|---|---|---|
| `COPY` into transactions | 1 000 000 rows | TO BE MEASURED |
| Pipelined card writes | 100 000 keys | TO BE MEASURED |
| Pipelined terminal writes | 10 000 keys | TO BE MEASURED |

## 4. What the numbers should be read against

These are the hypotheses the measurement is meant to confirm or refute. They are predictions, not
results.

1. **1 → 3 instances should scale close to linearly.** The instances share no lock and no
   coordination; `SKIP LOCKED` lets their claims pass each other. Expected sub-linearity comes
   only from added contention at the head of the claim index and from PostgreSQL write bandwidth.
2. **3 → 5 should scale visibly less than linearly.** By then the bottleneck should have moved to
   PostgreSQL: 5 × 20 = 100 connections, all doing small write transactions, plus WAL flush and
   checkpoint pressure. If throughput is flat from 3 to 5, that confirms the database is the
   ceiling — which is the expected and correct outcome for a polling architecture.
3. **P99 should be dominated by the cache tail, not by the database**, unless the pool is
   saturated. If P99 tracks `db_write_duration` instead, the connection pool or WAL is the limit.
4. **Peak DB connections should be ≈ instances × (workers + 4)**, capped by `DB_POOL_SIZE`. If it
   sits pinned at the cap, the pool is the bottleneck and raising workers will make things worse,
   not better (see `docs/concurrency.md` §4).
5. **Redis latency should stay flat across all three configurations.** A 110 000-key dataset fits
   in memory trivially and the access pattern is a single MGET of two keys.

## 5. Interpreting a disappointing result

| Symptom | Likely cause | Where to look |
|---|---|---|
| TPS flat from 3 → 5 instances | PostgreSQL write bandwidth or connection saturation | `pg_stat_activity`, WAL settings, `db_write_duration` |
| P99 ≫ P95 | cache tail latency or GC pauses | `redis_lookup_duration`, JVM GC logs |
| `processing_queue_size` pinned at capacity | the write side is the bottleneck; backpressure working as designed | `processor_backpressure_total` |
| `processing_queue_size` near zero and TPS low | the *claim* is the bottleneck, not the workers | `claim_duration`, index bloat, `autovacuum` activity |
| `processor_retry_total` climbing | transient failures under load — usually pool exhaustion | Hikari metrics, `connection-timeout` |
| `processor_recovered_total` > 0 during a clean run | leases are expiring on live workers | raise `processing-timeout` or lower `lease-renewal-interval` |

## 6. Correctness must hold at every configuration

A performance run that produced duplicates or lost transactions is a failed run regardless of its
TPS. `scripts/perf-test.sh` asserts both after every configuration; `scripts/verify.sh` can be run
at any time for the full acceptance summary.
