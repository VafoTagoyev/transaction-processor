#!/usr/bin/env bash
# Live view of the backlog, the per-instance progress and the queue depth.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE_FILE=${COMPOSE_FILE:-docker-compose.yml}
PREV_PROCESSED=0
PREV_TS=$(date +%s)

while true; do
  ROW=$(docker compose -f "${COMPOSE_FILE}" exec -T postgres psql -U txprocessor -d txprocessor -tAc "
    SELECT (SELECT count(*) FROM transactions WHERE status='NEW'),
           (SELECT count(*) FROM transactions WHERE status='PROCESSING'),
           (SELECT count(*) FROM transactions WHERE status='PROCESSED'),
           (SELECT count(*) FROM transactions WHERE status='ERROR'),
           (SELECT count(*) FROM processed_transactions),
           (SELECT count(*) FROM (SELECT transaction_id FROM processed_transactions
                                  GROUP BY transaction_id HAVING count(*)>1) d)" 2>/dev/null || echo "|||||")

  IFS='|' read -r NEW PROCESSING PROCESSED ERRORS RESULTS DUPES <<< "${ROW}"
  NOW=$(date +%s)
  ELAPSED=$(( NOW - PREV_TS ))
  TPS=0
  if [[ ${ELAPSED} -gt 0 && -n "${PROCESSED}" ]]; then
    TPS=$(( (PROCESSED - PREV_PROCESSED) / ELAPSED ))
  fi
  PREV_PROCESSED=${PROCESSED:-0}
  PREV_TS=${NOW}

  printf '\n%s  NEW=%-9s PROCESSING=%-6s PROCESSED=%-9s ERROR=%-6s results=%-9s duplicates=%-3s ~%s tps\n' \
    "$(date +%H:%M:%S)" "${NEW}" "${PROCESSING}" "${PROCESSED}" "${ERRORS}" "${RESULTS}" "${DUPES}" "${TPS}"

  for port in 8081 8082 8083; do
    STATUS=$(curl -fs --max-time 2 "http://localhost:${port}/status" 2>/dev/null || echo '')
    if [[ -n "${STATUS}" ]]; then
      printf '   :%s  %s\n' "${port}" \
        "$(echo "${STATUS}" | tr ',' '\n' | grep -E 'instanceId|localProcessed|localErrors|localRecovered|localBackpressure' | tr '\n' ' ')"
    else
      printf '   :%s  (not responding)\n' "${port}"
    fi
  done
  sleep 5
done
