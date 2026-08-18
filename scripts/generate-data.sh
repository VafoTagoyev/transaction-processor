#!/usr/bin/env bash
# Loads the dataset: transactions into PostgreSQL (COPY) and reference data into Valkey (pipelined).
#
#   ./scripts/generate-data.sh                    1 000 000 transactions, 100 000 cards, 10 000 terminals
#   GEN_TRANSACTIONS=50000 ./scripts/generate-data.sh     a smaller set for a quick run
set -euo pipefail
cd "$(dirname "$0")/.."

export GEN_TRANSACTIONS=${GEN_TRANSACTIONS:-1000000}
export GEN_CARDS=${GEN_CARDS:-100000}
export GEN_TERMINALS=${GEN_TERMINALS:-10000}
export GEN_TRUNCATE=${GEN_TRUNCATE:-true}
COMPOSE_FILE=${COMPOSE_FILE:-docker-compose.yml}

echo ">> Ensuring PostgreSQL and Valkey are up"
docker compose -f "${COMPOSE_FILE}" up -d postgres valkey

echo ">> Generating ${GEN_TRANSACTIONS} transactions, ${GEN_CARDS} cards, ${GEN_TERMINALS} terminals"
# --build is not optional: the compose file pins `image: transaction-processor:latest`, so once
# that tag exists `run` reuses it and silently ignores every source change since it was built.
time docker compose -f "${COMPOSE_FILE}" --profile generator run --build --rm generator

echo
echo ">> Result"
docker compose -f "${COMPOSE_FILE}" exec -T postgres psql -U txprocessor -d txprocessor -c \
  "SELECT status, count(*) FROM transactions GROUP BY status;"
docker compose -f "${COMPOSE_FILE}" exec -T valkey valkey-cli DBSIZE
