#!/usr/bin/env bash
# History lake for the forecast models (docs/forecasting.md): a slice of the team's Near-Silver on
# Hugging Face, through the same Silver and Gold jobs as the main lake, then the models.
#   1 fetch: Vietnam + a 5% sample of the world's stations (src/ingestion/hf_near_silver.py)
#   2 Silver (R01-R13)   3 R14 temporal flags   4 Gold station_hourly   5 Gold AQI and daily
#   6 forecast and early-warning models (src/ml/aqi_forecast.py)   7 XGBoost and LSTM (src/ml/compare_models.py)
# Everything goes to $LAKE (default openaq_lake_hist/, git-ignored), never to openaq_lake/.
# Every step keeps done markers per month, so a rerun resumes where it stopped.
#
# Usage: scripts/history_pipeline.sh            (FROM, TO, LAKE, WORLD_SHARE, DRIVER_MEMORY, ML_DRIVER_MEMORY, ML_ARGS)
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: pass container paths unchanged
LAKE=${LAKE:-openaq_lake_hist} FROM=${FROM:-2024-01} TO=${TO:-2025-12}

months=$(python - "$FROM" "$TO" <<'EOF'
import sys
y, m = map(int, sys.argv[1].split("-")); ly, lm = map(int, sys.argv[2].split("-"))
out = []
while (y, m) <= (ly, lm):
    out.append(f"{y:04d}-{m:02d}"); y, m = y + (m == 12), m % 12 + 1
print(" ".join(out))
EOF
)
spark() {
  docker compose -f docker-compose.spark.yml run --rm -e LAKE_DIR="/app/$LAKE" spark-lake \
    /opt/spark/bin/spark-submit --driver-memory "${DRIVER_MEMORY:-4g}"     --conf spark.sql.legacy.parquet.nanosAsLong=true "$@"   # data/dim/*.parquet (pandas) has ns timestamps
}
step() { echo; echo "== $* ($(date +%H:%M:%S))"; }

# dimensions Silver and Gold join with: copied once from the main lake
mkdir -p "$LAKE/silver" "$LAKE/reports"
for d in dim_location dim_sensor dim_sensor_unit; do
  [ -d "$LAKE/silver/$d" ] || cp -r "openaq_lake/silver/$d" "$LAKE/silver/"
done

step "1 fetch $FROM..$TO"
LAKE_DIR="$LAKE" python -u -m src.ingestion.hf_near_silver --from "$FROM" --to "$TO" --world-share "${WORLD_SHARE:-0.05}"
step "2 Silver"
spark src/silver/build_silver.py --run-id hist_silver --months $months
step "3 R14 temporal flags"
spark src/silver/flag_temporal_outliers.py --run-id hist_r14 --months $months
step "4 Gold station_hourly"
spark src/gold/build_gold.py --run-id hist_gold --stream none --months $months
step "5 Gold AQI and daily"
spark src/gold/build_aqi_daily.py --run-id hist_aqi --months $months
step "6 models"
# two years: test on the last 4 weeks, threshold on the week before; the world is already a 5% sample
# 8 GB: caching two years of test predictions ran a 4 GB driver out of heap
RUN="ml_hist_$(date +%Y%m%d_%H%M)"
DRIVER_MEMORY=${ML_DRIVER_MEMORY:-8g} spark src/ml/aqi_forecast.py --run-id "$RUN" \
  ${ML_ARGS:---test-days 28 --val-days 7 --sample 0.2 --test-sample 0.2}
step "7 XGBoost and LSTM comparison (host Python, GPU when there is one)"
if python -c "import xgboost, torch" 2>/dev/null; then
  LAKE_DIR="$LAKE" python -u -m src.ml.compare_models --run "$RUN"
else
  echo "skipped: pip install xgboost torch"
fi
step "done: $LAKE/gold/ml/$RUN (load with: LAKE_DIR=$LAKE python -m src.serving.load_ml)"
