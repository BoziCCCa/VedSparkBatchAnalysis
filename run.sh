#!/usr/bin/env bash
# Runs the Spark application container against the running cluster.
# Usage: ./run.sh                                   smoke test
#        ./run.sh /app/app.py filter --where ...    script path + its arguments
set -euo pipefail
cd "$(dirname "$0")"

docker compose -f docker-compose.app.yml run --rm --service-ports spark-app "$@"
