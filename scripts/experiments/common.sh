#!/usr/bin/env bash
# Shared helpers for the big-data experiments (docs/bigdata-experiments.md). Source it:
#   . scripts/experiments/common.sh
# Each run has its own topic (replay-<run>), HDFS root (/replay/<run>) and folder
# (openaq_lake/replay/<run>: manifest, progress log), so runs never mix.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: pass container paths unchanged

KAFKA="docker compose -f docker-compose.yml"
HDFS="docker compose -f docker-compose.hdfs.yml"
SPARK="docker compose -f docker-compose.spark.yml"
PRODUCERS="docker compose -f docker-compose.producers.yml"

say() { echo "[$(date +%H:%M:%S)] $*"; }

infra_up() {   # 3 Kafka brokers and HDFS (NameNode + 4 DataNodes); no MongoDB, no live producers
  $KAFKA up -d kafka1 kafka2 kafka3
  $HDFS up -d
  local i
  for i in $(seq 1 60); do
    docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092 --list >/dev/null 2>&1 && break
    sleep 3
  done
  for i in $(seq 1 60); do
    docker exec namenode sh -c 'hdfs dfsadmin -safemode get | grep -q OFF' >/dev/null 2>&1 && break
    sleep 3
  done
  say "Kafka and HDFS ready"
}

replay() {     # replay RUN [replay.py options]: sends to topic replay-RUN
  local run=$1; shift
  $PRODUCERS run --rm replay --run-id "$run" --topic "replay-$run" "$@"
}

replay_bg() {  # the same in the background; prints the PID
  replay "$@" > "openaq_lake/replay/$1.replay.log" 2>&1 &
  echo $!
}

consumer_up() {   # consumer_up RUN [VAR=value ...]: start the consumer under test on replay-RUN
  local run=$1; shift
  mkdir -p "openaq_lake/replay/$run"
  env REPLAY_RUN="$run" REPLAY_TOPIC="replay-$run" "$@" $SPARK up -d --force-recreate spark-replay
}

consumer_down() {  # consumer_down [RUN]: save the consumer's log to the run folder, then remove it
  [ -n "${1:-}" ] && docker logs spark-replay >> "openaq_lake/replay/$1/consumer.log" 2>&1 || true
  $SPARK rm -sf spark-replay >/dev/null 2>&1 || true
}

wait_drained() {  # wait_drained RUN [timeout_s]: every query idle and 0 offsets behind
  python scripts/experiments/wait_drained.py "openaq_lake/replay/$1" "${2:-1800}"
}

reconcile() {     # reconcile RUN: manifest vs archive and streaming Silver
  $SPARK run --rm spark-job /opt/spark/bin/spark-submit src/chaos/reconcile_stream.py --run-id "$1"
}

mem_sampler() {   # mem_sampler RUN CONTAINER: sample memory every 2 s into the run folder; prints PID
  local out="openaq_lake/replay/$1/memory_$2.csv"
  ( echo "time,mem"; while docker inspect "$2" >/dev/null 2>&1; do
      docker stats --no-stream --format "{{.MemUsage}}" "$2" 2>/dev/null | sed "s|^|$(date +%s),|"
      sleep 2
    done ) > "$out" &
  echo $!
}
