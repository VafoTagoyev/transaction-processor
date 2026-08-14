#!/usr/bin/env bash
#
# Reproducible performance run for 1, 3 and 5 instances over the identical dataset.
# Fills in the table in docs/performance-test.md.
#
#   ./scripts/perf-test.sh                 1 000 000 transactions, instance counts 1 3 5
#   ./scripts/perf-test.sh 200000 "1 3"    smaller dataset, fewer configurations
#
# For each configuration the script:
#   1. regenerates the dataset from the same fixed seed, so every run sees identical input
#   2. scales the processor service
#   3. samples progress until the backlog is drained
#   4. records wall-clock TPS, latency percentiles from Micrometer, DB connections and Redis latency
set -euo pipefail
cd "$(dirname "$0")/.."

COUNT=${1:-1000000}
INSTANCE_COUNTS=${2:-"1 3 5"}
COMPOSE="docker compose -f docker-compose.perf.yml"
PSQL="${COMPOSE} exec -T postgres psql -U txprocessor -d txprocessor"
RESULTS_FILE="docs/performance-results-$(date +%Y%m%d-%H%M%S).md"

q() { ${PSQL} -tAc "$1" | tr -d '[:space:]'; }

# Micrometer exposes the timer as processing_duration_seconds{quantile="0.95"} etc.
metric() { # port  metric-name-with-labels
  curl -fs "http://localhost:$1/actuator/prometheus" 2>/dev/null | grep -E "^$2" | awk '{print $2}' | head -1
}

processor_ports() {
  ${COMPOSE} ps --format '{{.Name}} {{.Ports}}' 2>/dev/null \
    | grep processor \
    | grep -oE '0\.0\.0\.0:[0-9]+->8080' \
    | grep -oE ':[0-9]+' | tr -d ':'
}

echo "| Instances | Workers/instance | Transactions | Wall clock (s) | TPS | Avg latency (ms) | P95 (ms) | P99 (ms) | DB connections | Redis avg (ms) |" > "${RESULTS_FILE}"
echo "|---|---|---|---|---|---|---|---|---|---|" >> "${RESULTS_FILE}"

${COMPOSE} up -d postgres valkey

for N in ${INSTANCE_COUNTS}; do
  echo
  echo "################ configuration: ${N} instance(s) ################"

  ${COMPOSE} down --remove-orphans >/dev/null 2>&1 || true
  ${COMPOSE} up -d postgres valkey
  sleep 5

  echo ">> regenerating the dataset (fixed seed, identical for every configuration)"
  GEN_TRANSACTIONS=${COUNT} ${COMPOSE} --profile generator run --rm generator >/dev/null

  echo ">> starting ${N} processor instance(s)"
  ${COMPOSE} up -d --build --scale processor="${N}" processor
  sleep 20

  PORTS=$(processor_ports)
  echo ">> processor actuator ports: ${PORTS}"

  START_EPOCH=$(date +%s)
  MAX_DB_CONNECTIONS=0
  while true; do
    REMAINING=$(q "SELECT count(*) FROM transactions WHERE status IN ('NEW','PROCESSING')")
    CONNECTIONS=$(q "SELECT count(*) FROM pg_stat_activity WHERE datname='txprocessor'")
    [[ "${CONNECTIONS}" -gt "${MAX_DB_CONNECTIONS}" ]] && MAX_DB_CONNECTIONS=${CONNECTIONS}
    printf '\r   remaining=%-10s db_connections=%-4s elapsed=%ss   ' \
      "${REMAINING}" "${CONNECTIONS}" "$(( $(date +%s) - START_EPOCH ))"
    [[ "${REMAINING}" == "0" ]] && break
    sleep 5
  done
  END_EPOCH=$(date +%s)
  echo

  ELAPSED=$(( END_EPOCH - START_EPOCH ))
  [[ ${ELAPSED} -eq 0 ]] && ELAPSED=1
  RESULTS=$(q "SELECT count(*) FROM processed_transactions")
  TPS=$(( RESULTS / ELAPSED ))

  SUM=0; CNT=0; P95=0; P99=0; RSUM=0; RCNT=0
  for PORT in ${PORTS}; do
    S=$(metric "${PORT}" 'processing_duration_seconds_sum'); SUM=$(awk -v a="${SUM}" -v b="${S:-0}" 'BEGIN{print a+b}')
    C=$(metric "${PORT}" 'processing_duration_seconds_count'); CNT=$(awk -v a="${CNT}" -v b="${C:-0}" 'BEGIN{print a+b}')
    V=$(curl -fs "http://localhost:${PORT}/actuator/prometheus" | grep 'processing_duration_seconds{' | grep 'quantile="0.95"' | awk '{print $2}' | head -1)
    P95=$(awk -v a="${P95}" -v b="${V:-0}" 'BEGIN{print (b>a)?b:a}')
    V=$(curl -fs "http://localhost:${PORT}/actuator/prometheus" | grep 'processing_duration_seconds{' | grep 'quantile="0.99"' | awk '{print $2}' | head -1)
    P99=$(awk -v a="${P99}" -v b="${V:-0}" 'BEGIN{print (b>a)?b:a}')
    R=$(metric "${PORT}" 'redis_lookup_duration_seconds_sum'); RSUM=$(awk -v a="${RSUM}" -v b="${R:-0}" 'BEGIN{print a+b}')
    R=$(metric "${PORT}" 'redis_lookup_duration_seconds_count'); RCNT=$(awk -v a="${RCNT}" -v b="${R:-0}" 'BEGIN{print a+b}')
  done

  AVG_MS=$(awk -v s="${SUM}" -v c="${CNT}" 'BEGIN{printf "%.2f", (c>0)? s/c*1000 : 0}')
  P95_MS=$(awk -v v="${P95}" 'BEGIN{printf "%.2f", v*1000}')
  P99_MS=$(awk -v v="${P99}" 'BEGIN{printf "%.2f", v*1000}')
  REDIS_MS=$(awk -v s="${RSUM}" -v c="${RCNT}" 'BEGIN{printf "%.2f", (c>0)? s/c*1000 : 0}')
  WORKERS=${WORKERS:-8}

  echo "| ${N} | ${WORKERS} | ${RESULTS} | ${ELAPSED} | ${TPS} | ${AVG_MS} | ${P95_MS} | ${P99_MS} | ${MAX_DB_CONNECTIONS} | ${REDIS_MS} |" >> "${RESULTS_FILE}"

  echo ">> ${N} instance(s): ${RESULTS} results in ${ELAPSED}s = ${TPS} TPS, avg ${AVG_MS}ms, p95 ${P95_MS}ms, p99 ${P99_MS}ms"

  echo ">> correctness check for this configuration"
  echo "   duplicates: $(q "SELECT count(*) FROM (SELECT transaction_id FROM processed_transactions GROUP BY transaction_id HAVING count(*)>1) d")"
  echo "   un-terminal: $(q "SELECT count(*) FROM transactions WHERE status NOT IN ('PROCESSED','ERROR')")"
done

${COMPOSE} down --remove-orphans >/dev/null 2>&1 || true

echo
echo "Results written to ${RESULTS_FILE}:"
cat "${RESULTS_FILE}"
echo
echo "Copy this table into docs/performance-test.md, replacing the TO BE MEASURED placeholders."
