#!/usr/bin/env bash
# Live fault-tolerance demo, streaming part (docs/fault-tolerance-report.md, "Live demo").
# Locust puts load on the pipeline; you trigger one fault at a time with Enter; the audience
# watches Locust's charts (http://localhost:8089) and this terminal: the consumer's lag and,
# per Silver batch, where the bad rows went.
#   1 Kafka (3 brokers), HDFS (NameNode + 4 DataNodes), the consumer under test, Locust
#   2 Enter: bad data for 60 s     -> each kind lands in quarantine / a flag / the dead-letter store
#   3 Enter: broker kafka2 down    -> writes go on (2 copies still in sync), latency blips
#   4 Enter: broker kafka3 down    -> writes REFUSED (NotEnoughReplicas): no unsafe write is accepted
#   5 Enter: both brokers back     -> failures stop
#   6 Enter: kill -9 the consumer  -> lag grows; Enter: restart it -> it resumes from its checkpoint
#   7 Enter: DataNode datanode2 down -> the consumer keeps writing (3 copies, 3 nodes left); Enter: back
#   8 Enter: stop everything the demo started
# Batch faults (crash during a rebuild, out of memory, a bad rebuild rolled back): demo_batch.sh.
# Usage: scripts/experiments/demo.sh   (~6 GB of RAM while it runs)
. "$(dirname "$0")/common.sh"
run="demo_$(date +%m%d%H%M)"
LOADTEST="docker compose -f docker-compose.loadtest.yml"
step() { echo; read -r -p ">>> $* [Enter] " _; }

infra_up
$HDFS up -d datanode3 datanode4 >/dev/null 2>&1
consumer_up "$run" REPLAY_TRIGGER="10 seconds" >/dev/null
export LOADTEST_TOPIC="replay-$run"
$LOADTEST up -d --build >/dev/null
say "Locust: http://localhost:8089  (e.g. 20 users, spawn rate 5, Start)"
say "Consumer Spark UI: http://localhost:4041  -> Structured Streaming tab"

python -u scripts/experiments/watch_demo.py "openaq_lake/replay/$run" spark-replay &
watcher=$!
trap 'kill $watcher 2>/dev/null' EXIT

step "Start the load in Locust, then: send bad data for 60 s"
say "bad data: what each kind should become (archive / Silver):"
sed -n '/^BAD_KINDS = {/,/^}/p' loadtest/locustfile.py | grep '": ' | tr -d '",' | sed 's/^ */    /'
$LOADTEST run --rm --no-deps locust -f loadtest/locustfile.py --headless -u 3 -r 3 -t 60s --only-summary \
  BadDataSender 2>&1 | grep -E "bad data |Aggregated" || true
step "Stop broker kafka2"
docker stop kafka2 >/dev/null; say "kafka2 down: 2 of 3 replicas left, min.insync 2 -> writes continue"
step "Stop broker kafka3 too"
docker stop kafka3 >/dev/null; say "kafka3 down: 1 replica left -> producers get NotEnoughReplicas (see Locust failures)"
step "Bring both brokers back"
docker start kafka2 kafka3 >/dev/null; say "brokers back: failures stop, in-sync replicas recover"
step "kill -9 the consumer"
docker kill spark-replay >/dev/null; say "consumer gone: messages pile up in Kafka (Locust keeps sending)"
step "Restart the consumer"
consumer_up "$run" REPLAY_TRIGGER="10 seconds" >/dev/null; say "consumer back: resumes from its HDFS checkpoint, lag drains"
step "Stop DataNode datanode2"
docker stop datanode2 >/dev/null; say "datanode2 down: writes continue on the other DataNodes"
step "Bring datanode2 back"
docker start datanode2 >/dev/null; say "datanode2 back"
step "Stop the demo (Locust, consumer; Kafka and HDFS stay up)"
$LOADTEST down >/dev/null 2>&1
consumer_down "$run"
say "done. Consumer log and progress: openaq_lake/replay/$run/. Stop Kafka/HDFS: scripts/stack.sh stop"
