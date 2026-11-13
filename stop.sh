#!/usr/bin/env bash
# Stops the Spark cluster and HDFS. Data in HDFS is kept.
# Usage: ./stop.sh            stop containers, keep data
#        ./stop.sh --purge    also delete HDFS data, event logs and the network
set -euo pipefail
cd "$(dirname "$0")"

if [ "${1:-}" = "--purge" ]; then
  docker compose -f docker-compose.spark.yml --profile scale down -v
  docker compose -f docker-compose.hdfs.yml down -v
  docker network rm bigdata >/dev/null 2>&1 || true
  echo "Stopped. HDFS data, event logs and the network were deleted."
else
  docker compose -f docker-compose.spark.yml --profile scale down
  docker compose -f docker-compose.hdfs.yml down
  echo "Stopped. HDFS data is kept."
fi
