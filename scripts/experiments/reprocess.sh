#!/usr/bin/env bash
# Experiment 4, reprocessing safety (docs/bigdata-experiments.md).
#  Plain Parquet Silver (the current design: staging, then move files into the month folder):
#   1 a month built before (copy of the control Silver)
#   2 a rebuild crashes in the middle of moving files (failpoint silver.mid_move): what readers see
#   3 the same run id rerun: the month is whole again, nothing stored twice
#   4 a crash after the move, before the done marker (silver.before_done), and a rerun
#  Delta Lake:
#   5 load the month, overwrite it with a bad rebuild (the chaos_01 Silver), quality gate, restore
#   6 kill an overwrite while it writes: readers keep the last committed version
# Usage: scripts/experiments/reprocess.sh [plain|delta]   (default both; TAG, KILL_AFTER override)
. "$(dirname "$0")/common.sh"
TAG=${TAG:-$(date +%m%d%H%M)}
R="openaq_lake/scratch/reproc_${TAG}"
DELTA_PKG=io.delta:delta-spark_2.12:3.2.1
PARTS=${*:-plain delta}

silver() {   # silver RUN_ID [CRASH_AT]: rebuild the month into $R/silver
  $SPARK run --rm -e CHAOS_CRASH_AT="${2:-}" spark-lake /opt/spark/bin/spark-submit --driver-memory 8g \
    src/silver/build_silver.py --input openaq_lake/chaos/control/near_silver_compact \
    --output "$R/silver" --run-id "$1" --months 2025-01 > "$R/$1${2:+.$2}.log" 2>&1
}
check() { python src/chaos/check_month.py "$R/silver/measurements" 2025-01 --expect "$EXPECT"; }
delta() {
  $SPARK run --rm "$@"
}
delta_step() {
  delta spark-lake /opt/spark/bin/spark-submit --driver-memory 6g --packages "$DELTA_PKG" \
    --conf spark.jars.ivy=/tmp/.ivy2 src/chaos/delta_reprocess.py "$@" 2>&1 | grep "delta_reprocess:"
}

EXPECT=$(python src/chaos/check_month.py openaq_lake/chaos/control/silver/measurements 2025-01 \
         | python -c "import json, sys; print(json.load(sys.stdin)['rows'])")
mkdir -p "$R"
say "expected rows in 2025-01: $EXPECT"

if [[ $PARTS == *plain* ]]; then
  mkdir -p "$R/silver"
  cp -r openaq_lake/chaos/control/silver/measurements "$R/silver/"
  say "1 month built before:";                         check
  say "2 rebuild crashing mid-move";                  silver "reproc_${TAG}_a" silver.mid_move || true
  say "  what readers see now:";                      check || true
  say "3 rerun of the same run id";                   silver "reproc_${TAG}_a"
  say "  after the rerun:";                           check
  say "4 rebuild crashing before its done marker";    silver "reproc_${TAG}_b" silver.before_done || true
  check || true
  say "  rerun:";                                     silver "reproc_${TAG}_b"
  check
fi

if [[ $PARTS == *delta* ]]; then
  rm -rf openaq_lake/scratch/delta
  say "5 Delta: load the month";                      delta_step load
  say "  bad rebuild (chaos_01 Silver) overwrites it"; delta_step overwrite --source openaq_lake/chaos/chaos_01/silver
  say "  quality gate";
  if delta_step gate; then say "  gate passed"; else say "  gate FAILED: restore"; delta_step restore; fi
  say "6 Delta: kill an overwrite after ${KILL_AFTER:-45}s"
  delta --name delta-overwrite spark-lake /opt/spark/bin/spark-submit --driver-memory 6g --packages "$DELTA_PKG" \
    --conf spark.jars.ivy=/tmp/.ivy2 src/chaos/delta_reprocess.py overwrite \
    --source openaq_lake/chaos/control/silver > "$R/delta_killed.log" 2>&1 &
  sleep "${KILL_AFTER:-45}"
  docker kill delta-overwrite >/dev/null 2>&1 && say "  killed" || say "  the overwrite had already finished"
  wait || true
  say "  what readers see:";                          delta_step status
fi
