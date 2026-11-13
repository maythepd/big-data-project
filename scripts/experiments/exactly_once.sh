#!/usr/bin/env bash
# Experiment 2, exactly-once under failure (docs/bigdata-experiments.md).
# LIMIT messages at RATE msg/s, 1% of them sent twice (producer retries), while the consumer is
# killed twice (kill -9, no clean shutdown), a Kafka broker and an HDFS DataNode are stopped.
# Afterwards the manifest is reconciled with the archive and Silver: nothing may be lost, and no
# key may be stored twice.
# Usage: scripts/experiments/exactly_once.sh    (LIMIT, RATE, TAG override)
. "$(dirname "$0")/common.sh"
infra_up
TAG=${TAG:-$(date +%m%d%H%M)}
run="eo_${TAG}"

consumer_up "$run"
replay "$run" --hours 24 --limit "${LIMIT:-600000}" --rate "${RATE:-3000}" --dup-share 0.01   > "openaq_lake/replay/$run.replay.log" 2>&1 &
pid=$!
say "$run: replay running (pid $pid)"
sleep 45; say "kill -9 the consumer";      docker kill spark-replay >/dev/null
sleep 15; say "consumer back";             consumer_up "$run"
sleep 30; say "stop broker kafka2";        docker stop kafka2 >/dev/null
sleep 30; say "stop DataNode datanode2";   docker stop datanode2 >/dev/null
sleep 30; say "kill -9 the consumer again"; docker kill spark-replay >/dev/null
sleep 10; say "consumer back";             consumer_up "$run"
sleep 20; say "broker kafka2 back";        docker start kafka2 >/dev/null
wait "$pid" && say "replay finished" || say "replay exited with an error"
wait_drained "$run" 1800 || say "$run: not drained"
say "DataNode datanode2 back";             docker start datanode2 >/dev/null
consumer_down "$run"
reconcile "$run"
