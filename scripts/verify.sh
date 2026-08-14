#!/usr/bin/env bash
# Runs scripts/verify.sql against the running compose stack and prints a PASS/FAIL summary.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE_FILE=${COMPOSE_FILE:-docker-compose.yml}
PSQL="docker compose -f ${COMPOSE_FILE} exec -T postgres psql -U txprocessor -d txprocessor"

${PSQL} -f - < scripts/verify.sql

echo
echo "=============================== ACCEPTANCE SUMMARY ==============================="

check() { # name  sql  expected
  local name=$1 sql=$2 expected=$3
  local actual
  actual=$(${PSQL} -tAc "${sql}" | tr -d '[:space:]')
  if [[ "${actual}" == "${expected}" ]]; then
    printf '  PASS  %-58s (%s)\n' "${name}" "${actual}"
  else
    printf '  FAIL  %-58s (got %s, expected %s)\n' "${name}" "${actual}" "${expected}"
  fi
}

check "C1  no transaction left in NEW or PROCESSING" \
      "SELECT count(*) FROM transactions WHERE status NOT IN ('PROCESSED','ERROR')" 0
check "C7  duplicate transaction_id in processed_transactions" \
      "SELECT count(*) FROM (SELECT transaction_id FROM processed_transactions GROUP BY transaction_id HAVING count(*)>1) d" 0
check "C6  PROCESSED transactions with no result row" \
      "SELECT count(*) FROM transactions t LEFT JOIN processed_transactions p ON p.transaction_id=t.id WHERE t.status='PROCESSED' AND p.id IS NULL" 0
check "C6  result rows whose transaction is not PROCESSED" \
      "SELECT count(*) FROM processed_transactions p JOIN transactions t ON t.id=p.transaction_id WHERE t.status<>'PROCESSED'" 0
check "C5  PROCESSING rows older than the processing timeout" \
      "SELECT count(*) FROM transactions WHERE status='PROCESSING' AND processing_started_at < now() - INTERVAL '2 minutes'" 0
check "     account_statistics count matches processed results" \
      "SELECT CASE WHEN (SELECT coalesce(sum(transactions_count),0) FROM account_statistics) = (SELECT count(*) FROM processed_transactions WHERE account IS NOT NULL) THEN 0 ELSE 1 END" 0
check "     outbox event count matches processed results" \
      "SELECT CASE WHEN (SELECT count(*) FROM outbox_events) = (SELECT count(*) FROM processed_transactions) THEN 0 ELSE 1 END" 0
echo "================================================================================="
