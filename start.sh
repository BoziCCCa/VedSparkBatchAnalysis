#!/usr/bin/env bash
# Starts HDFS and the Spark cluster.
# Usage: ./start.sh [workers]     workers = 1, 2 (default) or 3
set -euo pipefail
cd "$(dirname "$0")"

WORKERS="${1:-2}"
case "$WORKERS" in
  1|2|3) ;;
  *) echo "Usage: ./start.sh [1|2|3]" >&2; exit 1 ;;
esac

docker network inspect bigdata >/dev/null 2>&1 || docker network create bigdata

echo "Starting HDFS..."
docker compose -f docker-compose.hdfs.yml up -d

echo "Starting Spark with $WORKERS worker(s)..."
services=(spark-master spark-history-server)
for i in 1 2 3; do
  if [ "$i" -le "$WORKERS" ]; then
    services+=("spark-worker-$i")
  else
    # Remove workers left over from a run with more workers.
    docker rm -f "spark-worker-$i" >/dev/null 2>&1 || true
  fi
done
docker compose -f docker-compose.spark.yml up -d "${services[@]}"

echo "Waiting for HDFS to leave safe mode..."
until docker exec namenode hdfs dfsadmin -safemode wait >/dev/null 2>&1; do
  sleep 2
done

echo
echo "Cluster is ready."
echo "  HDFS            http://localhost:9870"
echo "  Spark master    http://localhost:8080"
echo "  History server  http://localhost:18081"
