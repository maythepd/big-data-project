#!/usr/bin/env bash
# Starts and stops the Docker stacks in order.
#   lite     MongoDB, Kafka 2/3 brokers, HDFS NameNode + 2/4 DataNodes, consumer, producers; small heaps
#   full     every service, Kafka UI included
#   serving  MongoDB only
#   stop     stop everything (volumes are kept)
#   status   container memory, free RAM, local jobs
# Lite keeps 2 brokers (KRaft majority, min.insync.replicas=2) and 2 DataNodes (every
# block has a copy on any 2 of the 4), so it reuses the full stack's data.
set -euo pipefail
cd "$(dirname "$0")/.."

KAFKA="docker compose -f docker-compose.yml"
HDFS="docker compose -f docker-compose.hdfs.yml"
MONGO="docker compose -f docker-compose.serving.yml"
SPARK="docker compose -f docker-compose.spark.yml"
PRODUCERS="docker compose -f docker-compose.producers.yml"

# lite: small heaps for the small streaming load
lite_env() {
  export OPENAQ_KAFKA_HEAP="-Xmx256m -Xms256m"
  export OPENAQ_HDFS_HEAP=256m
  export OPENAQ_MONGO_CACHE_GB=0.5
  export OPENAQ_MONGO_MEM=1.2g
}

wait_for() {   # wait_for <what> <seconds> <command...>
  local what=$1 secs=$2 i; shift 2
  for ((i = 0; i < secs; i += 3)); do
    if "$@" >/dev/null 2>&1; then echo "  $what: ok"; return 0; fi
    sleep 3
  done
  echo "  $what: not ready after ${secs}s" >&2
  return 1
}

# never stop or recreate MongoDB during a load_mongo.py run
guard_mongo() {
  if pgrep -f "src/serving/load_mongo.py" >/dev/null; then
    echo "load_mongo.py is running: MongoDB is left as it is (wait for the load to finish)" >&2
    return 1
  fi
}

mem_check() {   # warn when the machine is already short of memory
  local avail; avail=$(awk '/MemAvailable/ {print int($2 / 1024)}' /proc/meminfo)
  echo "free memory before start: ${avail} MB"
  if [ "$avail" -lt "$1" ]; then
    echo "warning: less than $1 MB available; stop Spark jobs or the dashboard first" >&2
  fi
}

start_mongo() {
  $MONGO up -d
  wait_for "MongoDB" 60 docker exec mongo mongosh --quiet --eval 'db.adminCommand({ping: 1}).ok'
}

start_kafka() {   # start_kafka [services...]; no argument = all, Kafka UI included
  $KAFKA up -d "$@"
  wait_for "Kafka topic air-quality" 120 docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka1:29092 --describe --topic air-quality
}

start_hdfs() {    # start_hdfs [services...]; no argument = NameNode + 4 DataNodes
  $HDFS up -d "$@"
  wait_for "HDFS out of safe mode" 180 docker exec namenode sh -c 'hdfs dfsadmin -safemode get | grep -q OFF'
}

start_consumer() {
  $SPARK up -d spark-consumer
  echo "  consumer started: docker logs -f spark-consumer (Spark UI http://localhost:4040)"
}

start_producers() {   # only sources with a station catalog; without one a producer exits and restarts forever
  local source services=()
  for source in openaq waqi openweather; do
    if [ -f "data/dim/${source}_stations.parquet" ]; then
      services+=("producer-$source")
    else
      echo "  producer-$source skipped: run python -m src.ingestion.$source.fetch_dimensions first" >&2
    fi
  done
  if [ ${#services[@]} -gt 0 ]; then
    $PRODUCERS up -d --build "${services[@]}"
    echo "  producers started: docker logs -f producer-openaq (or -waqi, -openweather)"
  fi
}

case "${1:-}" in
  lite)
    guard_mongo
    mem_check 4000
    lite_env
    # services of the full stack that lite leaves out, in case they are running
    docker stop kafka-ui kafka3 datanode3 datanode4 >/dev/null 2>&1 || true
    start_mongo
    start_kafka kafka1 kafka2
    start_hdfs namenode datanode1 datanode2
    start_consumer
    start_producers
    ;;
  full)
    guard_mongo
    mem_check 6000
    start_mongo
    start_kafka
    start_hdfs
    start_consumer
    start_producers
    ;;
  serving)
    guard_mongo
    start_mongo
    ;;
  stop)
    $PRODUCERS stop
    docker stop spark-consumer >/dev/null 2>&1 || true
    $HDFS stop
    $KAFKA stop
    if guard_mongo; then $MONGO stop; fi
    ;;
  status)
    docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}'
    echo
    free -h | head -2
    echo
    pgrep -af "streamlit run|src.ingestion|load_mongo.py|spark-submit" | grep -v pgrep || echo "no local producer, dashboard or Spark job"
    ;;
  *)
    sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
