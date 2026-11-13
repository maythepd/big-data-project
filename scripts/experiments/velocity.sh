#!/usr/bin/env bash
# Experiment 1, velocity and backpressure (docs/bigdata-experiments.md).
#  A. Steady rates: LIMIT messages replayed at each rate in RATES (msg/s, 0 = as fast as the
#     producer goes) into a running consumer: throughput, batch duration, lag.
#  B. Catch-up: BACKLOG messages queue up while the consumer is down (an outage), then it
#     starts with and without maxOffsetsPerTrigger: first batch size, batch durations, memory,
#     time to drain.
# Usage: scripts/experiments/velocity.sh        (RATES, LIMIT, BACKLOG, CAP, TAG override)
. "$(dirname "$0")/common.sh"
infra_up
TAG=${TAG:-$(date +%m%d%H%M)}

for rate in ${RATES:-2000 5000 10000 0}; do
  run="vel_${TAG}_rate${rate}"
  say "$run: consumer up, replay ${LIMIT:-300000} messages at ${rate} msg/s"
  consumer_up "$run"
  replay "$run" --hours 12 --limit "${LIMIT:-300000}" --rate "$rate"
  wait_drained "$run" 1800 || say "$run: not drained"
  consumer_down "$run"
  reconcile "$run"
done

for cap in none "${CAP:-50000}"; do
  run="vel_${TAG}_backlog_${cap}"
  say "$run: consumer down, ${BACKLOG:-1000000} messages queue up"
  mkdir -p "openaq_lake/replay/$run"
  replay "$run" --hours 48 --limit "${BACKLOG:-1000000}"
  if [ "$cap" = none ]; then consumer_up "$run"; else consumer_up "$run" REPLAY_MAX_OFFSETS="$cap"; fi
  sampler=$(mem_sampler "$run" spark-replay)
  wait_drained "$run" 3600 || say "$run: not drained"
  kill "$sampler" 2>/dev/null || true
  consumer_down "$run"
  reconcile "$run"
done
say "done: openaq_lake/reports/replay_vel_${TAG}_*"
