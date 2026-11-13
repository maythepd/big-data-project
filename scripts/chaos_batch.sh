#!/usr/bin/env bash
# Chaos run on one month of the local lake (docs/fault-injection.md), every step in the Spark container:
#   1 inject faults into Near-Silver   2 build Silver on it   3 R14 temporal flags   4 evaluate
#
# Usage: scripts/chaos_batch.sh RUN_ID YYYY-MM [inject_batch.py options]
#   scripts/chaos_batch.sh control 2025-01 --scale 0     # untouched copy: the baseline
#   scripts/chaos_batch.sh chaos_01 2025-01 --seed 42    # evaluated against openaq_lake/chaos/control
# CONTROL=<run> picks another baseline run (it must cover the same month and --bbox).
# DRIVER_MEMORY (default 8g) sets the Spark driver memory; Docker needs a few GB more.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=$1 MONTH=$2
shift 2
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: pass container paths unchanged

spark() {
  docker compose -f docker-compose.spark.yml run --rm spark-lake \
    /opt/spark/bin/spark-submit --driver-memory "${DRIVER_MEMORY:-8g}" "$@"
}

spark src/chaos/inject_batch.py --run-id "$RUN" --month "$MONTH" "$@"
spark src/silver/build_silver.py --input "openaq_lake/chaos/$RUN/near_silver_compact" \
  --output "openaq_lake/chaos/$RUN/silver" --run-id "${RUN}_silver" --months "$MONTH"
spark src/silver/flag_temporal_outliers.py --silver "openaq_lake/chaos/$RUN/silver/measurements" \
  --run-id "${RUN}_r14" --months "$MONTH" --vn-locations none

CONTROL=${CONTROL:-control}
baseline="openaq_lake/chaos/$CONTROL/silver/measurements"
if [ "$RUN" != "$CONTROL" ] && [ -d "$baseline" ]; then
  spark src/chaos/evaluate.py --run-id "$RUN" --month "$MONTH" --baseline "$baseline"
else
  spark src/chaos/evaluate.py --run-id "$RUN" --month "$MONTH"
fi
