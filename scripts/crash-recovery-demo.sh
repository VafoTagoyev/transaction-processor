#!/usr/bin/env bash
#
# The crash-recovery demonstration required by section 6.2 of the assignment.
#
#   55. create 100 000 NEW transactions          -> step 1
#   56. run 3 instances x 8 workers              -> step 2
#   57. wait for processing to start             -> step 3
#   58. kill -9 one instance (no graceful stop)  -> step 4
#   59. verify the other two keep going          -> step 5
#   60. start the killed instance again          -> step 6
#   61. COUNT(processed_transactions) = 100 000  -> step 7
#   62. no duplicate transaction_id              -> step 7
#   63. no eternal PROCESSING past the timeout   -> step 7
#   64. restart everything, nothing reprocessed  -> step 8
#
# Usage: ./scripts/crash-recovery-demo.sh [transaction_count]
set -euo pipefail
cd "$(dirname "$0")/.."

COUNT=${1:-100000}
PSQL="docker compose exec -T postgres psql -U txprocessor -d txprocessor"

q() { ${PSQL} -tAc "$1" | tr -d '[:space:]'; }
banner() { echo; echo "=================================================================="; echo "$1"; echo "=================================================================="; }

banner "STEP 1/8  Generating ${COUNT} NEW transactions"
docker compose up -d --build postgres valkey
GEN_TRANSACTIONS=${COUNT} GEN_CARDS=${GEN_CARDS:-100000} GEN_TERMINALS=${GEN_TERMINALS:-10000} \
  docker compose --profile generator run --rm generator
echo "NEW transactions: $(q "SELECT count(*) FROM transactions WHERE status='NEW'")"

banner "STEP 2/8  Starting 3 instances x 8 workers"
docker compose up -d --build processor-1 processor-2 processor-3
for port in 8081 8082 8083; do
  until curl -fs "http://localhost:${port}/actuator/health" | grep -q '"status":"UP"'; do printf '.'; sleep 2; done
done
echo " all three are UP"

banner "STEP 3/8  Waiting for processing to be under way"
until [[ "$(q "SELECT count(*) FROM processed_transactions")" -gt 2000 ]]; do printf '.'; sleep 2; done
echo
echo "processed so far: $(q "SELECT count(*) FROM processed_transactions")"
echo "owned by processor-2 right now: $(q "SELECT count(*) FROM transactions WHERE status='PROCESSING' AND processing_instance LIKE '%processor-2%' OR processing_instance IS NOT NULL AND status='PROCESSING'")"

banner "STEP 4/8  kill -9 processor-2 (no graceful shutdown, no SIGTERM handler)"
BEFORE_KILL=$(q "SELECT count(*) FROM processed_transactions")
docker compose kill -s SIGKILL processor-2
echo "processor-2 killed. Results at time of kill: ${BEFORE_KILL}"
echo "PROCESSING rows left orphaned: $(q "SELECT count(*) FROM transactions WHERE status='PROCESSING'")"

banner "STEP 5/8  Verifying the survivors keep processing (criterion C3)"
sleep 15
AFTER_KILL=$(q "SELECT count(*) FROM processed_transactions")
echo "results after 15s: ${AFTER_KILL} (was ${BEFORE_KILL})"
if [[ "${AFTER_KILL}" -gt "${BEFORE_KILL}" ]]; then
  echo "  PASS  C3: processing continued without the dead instance"
else
  echo "  FAIL  C3: no progress after the kill"
fi

banner "STEP 6/8  Restarting processor-2 (criterion C4)"
docker compose up -d processor-2
until curl -fs "http://localhost:8082/actuator/health" | grep -q '"status":"UP"'; do printf '.'; sleep 2; done
echo " processor-2 is back, with no offset or id list handed to it"

banner "STEP 7/8  Waiting for the backlog to drain, then verifying"
# PROCESSING_TIMEOUT is 2m in docker-compose.yml, so orphaned leases are reclaimed within ~2.5m.
while true; do
  REMAINING=$(q "SELECT count(*) FROM transactions WHERE status IN ('NEW','PROCESSING')")
  echo "  remaining: ${REMAINING}"
  [[ "${REMAINING}" == "0" ]] && break
  sleep 10
done
./scripts/verify.sh

banner "STEP 8/8  Restarting every instance; already PROCESSED work must not be redone"
RESULTS_BEFORE=$(q "SELECT count(*) FROM processed_transactions")
docker compose restart processor-1 processor-2 processor-3
sleep 30
RESULTS_AFTER=$(q "SELECT count(*) FROM processed_transactions")
echo "results before restart: ${RESULTS_BEFORE}"
echo "results after  restart: ${RESULTS_AFTER}"
if [[ "${RESULTS_BEFORE}" == "${RESULTS_AFTER}" ]]; then
  echo "  PASS  nothing was reprocessed after restarting all instances"
else
  echo "  FAIL  the result count changed after a restart"
fi

banner "DONE"
echo "expected result count: ${COUNT} minus the ~2% deliberately unresolvable reference data"
q "SELECT status, count(*) FROM transactions GROUP BY status" >/dev/null
${PSQL} -c "SELECT status, count(*) FROM transactions GROUP BY status ORDER BY status;"
