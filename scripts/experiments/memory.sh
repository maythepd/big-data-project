#!/usr/bin/env bash
# Experiment 3, memory pressure (docs/bigdata-experiments.md).
#  A. Batch: the full-month Silver build (33M rows, the control copy) with the Spark driver at
#     each size in MEMS. In local mode every task runs in the driver JVM, so this caps the whole
#     job. Per size: finished or not, time, spill to memory and disk, GC share, peak memory.
#  B. R14 (a window over each sensor's month) at R14_MEM with --rows-per-task 500000, the
#     job's memory knob, and 10x that: bigger tasks need more memory each.
#  C. Streaming: BACKLOG messages queue up during an outage, then a 1g consumer starts
#     without and with maxOffsetsPerTrigger (CAP).
# Usage: scripts/experiments/memory.sh [A|B|C ...]   (default: all; MEMS, R14_MEM, BACKLOG, CAP, TAG)
. "$(dirname "$0")/common.sh"
TAG=${TAG:-$(date +%m%d%H%M)}
EV=openaq_lake/spark-events
PARTS=${*:-A B C}

lake_job() {   # lake_job LABEL MEMORY script args...: event log in $EV/LABEL, output in $EV/LABEL.log
  local label=$1 mem=$2 start; shift 2
  mkdir -p "$EV/$label"
  start=$(date +%s)
  if $SPARK run --rm spark-lake /opt/spark/bin/spark-submit --driver-memory "$mem" \
      --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:///app/$EV/$label" \
      "$@" > "$EV/$label.log" 2>&1; then
    say "$label: finished in $(( $(date +%s) - start ))s"
  else
    say "$label: FAILED (exit $?) after $(( $(date +%s) - start ))s; see $EV/$label.log"
  fi
}

if [[ $PARTS == *A* ]]; then
  for mem in ${MEMS:-1g 2g 4g 8g}; do
    label="mem_${TAG}_silver_$mem"
    rm -rf "openaq_lake/scratch/$label"
    lake_job "$label" "$mem" src/silver/build_silver.py --input openaq_lake/chaos/control/near_silver_compact \
      --output "openaq_lake/scratch/$label" --run-id "$label" --months 2025-01
  done
fi

if [[ $PARTS == *B* ]]; then
  for rpt in 500000 5000000; do
    label="mem_${TAG}_r14_${R14_MEM:-2g}_rpt$rpt"
    rm -rf "openaq_lake/scratch/$label"
    mkdir -p "openaq_lake/scratch/$label"
    cp -r openaq_lake/chaos/control/silver/measurements "openaq_lake/scratch/$label/"
    lake_job "$label" "${R14_MEM:-2g}" src/silver/flag_temporal_outliers.py \
      --silver "openaq_lake/scratch/$label/measurements" --run-id "$label" --months 2025-01 \
      --vn-locations none --rows-per-task "$rpt"
  done
fi

if [[ $PARTS == *C* ]]; then
  infra_up
  for cap in none "${CAP:-50000}"; do
    run="mem_${TAG}_stream_1g_$cap"
    mkdir -p "openaq_lake/replay/$run"
    replay "$run" --hours 48 --limit "${BACKLOG:-1000000}"
    if [ "$cap" = none ]; then consumer_up "$run" REPLAY_DRIVER_MEMORY=1g
    else consumer_up "$run" REPLAY_DRIVER_MEMORY=1g REPLAY_MAX_OFFSETS="$cap"; fi
    sampler=$(mem_sampler "$run" spark-replay)
    wait_drained "$run" 1200 || say "$run: not drained in 20 min"
    kill "$sampler" 2>/dev/null || true
    consumer_down "$run"
    say "$run: $(grep -c 'FAILED' "openaq_lake/replay/$run/consumer.log" || true) query failures, " \
        "$(grep -ci 'OutOfMemoryError' "openaq_lake/replay/$run/consumer.log" || true) OutOfMemoryError lines"
    reconcile "$run"
  done
fi

python src/chaos/spark_events.py "$EV"/mem_"${TAG}"_* --csv "openaq_lake/reports/memory_${TAG}.csv" 2>/dev/null || true
say "done: openaq_lake/reports/memory_${TAG}.csv, openaq_lake/reports/replay_mem_${TAG}_*"
