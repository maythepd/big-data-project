#!/usr/bin/env bash
# Live fault-tolerance demo, batch part (docs/fault-tolerance-report.md, "Live demo"). Uses the
# small Southeast Asia month (930k rows, openaq_lake/chaos/control_sea and chaos_sea) so steps 1
# and 3 take under a minute each; step 2 needs the full month. Enter starts each step.
#   1 a Silver rebuild crashes after deleting the old files (failpoint silver.after_delete)
#     -> a reader finds the month EMPTY; the rerun of the same run id makes it whole, nothing twice
#   2 R14 on the full month (33M rows) in 2 GB: tasks of 5M rows -> OutOfMemoryError in ~20 s;
#     the default 500k rows per task fits (~3 min)
#   3 Delta Lake: load the month, a bad rebuild (the faulty chaos month) overwrites it, the
#     quality gate compares the versions and fails, RESTORE brings the good version back
# Usage: scripts/experiments/demo_batch.sh   (Spark only: ~3 GB, no Kafka or HDFS)
. "$(dirname "$0")/common.sh"
D="openaq_lake/scratch/demo_batch"
step() { echo; read -r -p ">>> $* [Enter] " _; }
spark() {   # spark MEMORY script args...: one Spark job in the container, its errors summarized
  local mem=$1; shift
  $SPARK run --rm -e CHAOS_CRASH_AT="${CRASH:-}" spark-lake /opt/spark/bin/spark-submit --driver-memory "$mem" \
    "$@" > "$D/last_job.log" 2>&1 && say "job finished" || {
    say "job FAILED (exit $?):"; grep -m3 -E "OutOfMemoryError|failpoint|Exception:" "$D/last_job.log" | sed 's/^/    /'; }
}
check() { python src/chaos/check_month.py "$D/silver/measurements" 2025-01 --expect "$EXPECT" || true; }
delta() {
  $SPARK run --rm spark-lake /opt/spark/bin/spark-submit --driver-memory 2g --packages io.delta:delta-spark_2.12:3.2.1 \
    --conf spark.jars.ivy=/tmp/.ivy2 src/chaos/delta_reprocess.py --table "/app/$D/delta" "$@" 2>&1 \
    | grep "delta_reprocess:" | sed 's/^delta_reprocess: /    /'
}

rm -rf "$D"; mkdir -p "$D/silver"
cp -r openaq_lake/chaos/control_sea/silver/measurements "$D/silver/"
EXPECT=$(python src/chaos/check_month.py "$D/silver/measurements" 2025-01 | python -c "import json,sys; print(json.load(sys.stdin)['rows'])")
say "the month as built before (what readers see):"; check

step "1. Rebuild the month, crashing right after the old files are deleted"
CRASH=silver.after_delete spark 2g src/silver/build_silver.py --input openaq_lake/chaos/control_sea/near_silver_compact \
  --output "$D/silver" --run-id demo_batch_rebuild --months 2025-01
say "what a reader sees now:"; check
step "Rerun the same rebuild (same run id)"
spark 2g src/silver/build_silver.py --input openaq_lake/chaos/control_sea/near_silver_compact \
  --output "$D/silver" --run-id demo_batch_rebuild --months 2025-01
say "after the rerun:"; check

step "2. R14 on the full month (33M rows) in 2 GB, with tasks of 5M rows"
rm -rf "$D/full" openaq_lake/reports/demo_batch_r14_big openaq_lake/reports/demo_batch_r14_default
mkdir -p "$D/full"; cp -r openaq_lake/chaos/control/silver/measurements "$D/full/"
spark 2g src/silver/flag_temporal_outliers.py --silver "$D/full/measurements" --run-id demo_batch_r14_big \
  --months 2025-01 --vn-locations none --rows-per-task 5000000
step "The same with the default 500k rows per task (about 3 minutes)"
spark 2g src/silver/flag_temporal_outliers.py --silver "$D/full/measurements" --run-id demo_batch_r14_default \
  --months 2025-01 --vn-locations none

step "3. Delta Lake: load the good month as version 0"
delta load --source openaq_lake/chaos/control_sea/silver
step "A bad rebuild (the faulty chaos month) overwrites it"
delta overwrite --source openaq_lake/chaos/chaos_sea/silver
step "Quality gate: compare the new version with the previous one"
if delta gate | tee "$D/gate.txt" && grep -q '"passed": true' "$D/gate.txt"; then
  say "gate passed"
else
  say "gate FAILED -> restore the previous version"; delta restore
fi
say "done: $D"
