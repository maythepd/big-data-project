# AQI forecasting and early warning

The dashboard page "Dự báo & cảnh báo" answers two questions for every Vietnamese station:

1. **Forecasting:** what will the AQI be over the next 24 hours?
2. **Risk classification and early warning:** will the AQI go above 150 (VN "Xấu", US "Unhealthy") at any point in the next 24 hours?

The production models are Spark MLlib, in [src/ml/aqi_forecast.py](../src/ml/aqi_forecast.py). [src/ml/compare_models.py](../src/ml/compare_models.py) trains XGBoost and a small LSTM on the same data and split, to compare them. Each AQI standard (VN_AQI and US EPA) gets its own models.

## Data: two years in a separate lake

[scripts/history_pipeline.sh](../scripts/history_pipeline.sh) builds `openaq_lake_hist/` (git-ignored) from the team's Near-Silver on Hugging Face (`maythepd/big-data-near-silver`). It uses the same Silver and Gold jobs as the main lake:

1. **Fetch** ([src/ingestion/hf_near_silver.py](../src/ingestion/hf_near_silver.py)), January 2024 to December 2025. Each file is read in memory, and the script keeps:
   - every Vietnamese station
   - anything inside the Vietnam bounding box
   - a fixed 5% sample of the world's other stations, chosen by a hash of the station id, so each kept station's timeline stays whole

   It kept 62.4M of about 1 billion readings (525 MB), in about 12 minutes.
2. **Silver** (rules R01–R13), **R14** temporal flags, **Gold** `station_hourly`, then **Gold AQI and daily tables**. All four steps took about 30 minutes on Docker Spark.
3. **The result:** 35.2M Gold station-hours from 2,220 stations, 42 of them in Vietnam (621k Vietnam station-hours). Of those, 9.9M station-hours have an AQI value; these are what the models use.

Two quirks of the source, and how the pipeline handles them:
- **Inherited schema metadata.** The Near-Silver files carry Spark's schema note. When rewritten with an extra column, Spark trusts the note and hides the new column, so the fetch drops the note. It also stores microsecond timestamps, because Spark 3.5 cannot read nanosecond ones.
- **An oversized month.** March 2024 holds 155M source rows, 5 times a normal month: some sensors report every few seconds. Silver keeps them (they are not duplicates), and Gold averages them by hour.

## Features

The features are computed by one Spark window-function pass over every station's timeline. Every feature comes from the past, as seen at the hour the forecast is made:

- the AQI now, and 1, 2, 3, 6, 12 and 24 hours ago
- the mean over the last 6 and 24 hours, and the minimum and maximum over the last 24 hours
- the change over the last 3 hours and over the last 24 hours
- the hourly concentrations of PM2.5, PM10, O₃, NO₂, CO and SO₂ (missing ones are set to -1)
- the local hour (derived from longitude), the day of the week, latitude and longitude
- for the forecast models only: how many hours ahead (h), and the local hour of the target

The lags use range windows on time, not row offsets. When an hour is missing, its lag stays missing and is filled with the current value. It never borrows an older row. The job writes the feature table to `<run>/features`, and the comparison script reads that same table.

## Models

| | Forecast | Warning |
|---|---|---|
| Spark MLlib | `GBTRegressor`, 60 trees, depth 6: the change in AQI h hours ahead, one model for every h | `RandomForestClassifier`, 40 trees, depth 10: the AQI band of the worst hour in the next 24 hours; warns when P(band above 150) ≥ the threshold |
| XGBoost (GPU) | the same rows and features as the Spark GBT; up to 2,000 trees, depth 8, with early stopping | a binary classifier: is the worst hour of the next 24 hours above 150? |
| LSTM (GPU) | 64 units; reads the last 24 hours and outputs all 24 next hours at once | warns when the highest of its 24 predicted hours is above 150 |

Training data is every Vietnam row plus 20% of the other stations. The Spark and XGBoost runs sample 20% of the other stations' hours; the LSTM samples 20% of the other stations themselves, using a differently salted hash from the fetch, so the two samples stay independent.

For the trees, each training hour is paired with 3 random horizons. The tree models' warning threshold is chosen on the validation window by F2, which counts a missed episode as worse than a false alarm.

## Honest evaluation

The data is split by time, never by row:

- **Train:** up to 26 November 2025.
- **Validation:** 27 November – 3 December 2025, used only to choose the warning threshold and for early stopping.
- **Test:** 4–31 December 2025, which no model saw during training.

All results below are measured on Vietnam's test hours, about 3,500 station-hours at each horizon. They are compared with these baselines:

- **Persistence:** the AQI stays as it is now.
- **The warning rule:** "the last 24 hours already went above 150".

## Results (run `ml_hist_2y`)

Mean absolute error of the forecast, in AQI points, on Vietnam's test hours:

| VN_AQI | 1 h | 6 h | 24 h | | US AQI | 1 h | 6 h | 24 h |
|---|---|---|---|---|---|---|---|---|
| Persistence | 6.3 | 23.7 | 33.5 | | Persistence | 6.2 | 22.2 | 31.2 |
| Spark GBT | 8.7 | 22.7 | 33.6 | | Spark GBT | 9.5 | 22.6 | 31.5 |
| XGBoost | 9.7 | 23.9 | 33.9 | | XGBoost | 10.4 | 23.3 | 34.3 |
| LSTM | **4.8** | **22.5** | **33.5** | | LSTM | **5.2** | **21.4** | **31.0** |

Early warning (AQI above 150 within 24 hours), on Vietnam's test hours:

| VN_AQI | Precision | Recall | F1 | Episodes warned | | US AQI | Precision | Recall | F1 | Episodes warned |
|---|---|---|---|---|---|---|---|---|---|---|
| Rule: last 24 h already above | 0.76 | 0.77 | 0.76 | 55/70 | | Rule | 0.85 | 0.86 | 0.85 | 75/86 |
| Spark random forest | 0.63 | **0.96** | 0.76 | **70/70** | | Spark random forest | 0.82 | **0.91** | **0.87** | **85/86** |
| XGBoost | 0.67 | 0.93 | **0.78** | **70/70** | | XGBoost | 0.85 | 0.90 | **0.87** | **85/86** |
| LSTM (predicted peak) | **0.96** | 0.58 | 0.73 | 56/70 | | LSTM | **0.97** | 0.67 | 0.79 | 69/86 |

What this shows:

- **The LSTM is the best forecaster at every horizon.** It is the only model clearly better than persistence at 1 hour (4.8 against 6.3). It reads the hour-by-hour shape of the last day directly, where the trees see only a handful of summaries.
- **The tree models are worse than persistence at 1 hour.** One tree model has to cover all 24 horizons, and at h = 1 the true change is small. From 6 hours onward they roughly match persistence. One model per horizon, or blending with persistence at short horizons, would be the next step.
- **Nothing beats persistence much at 24 hours.** A day ahead depends on weather, which the models do not see.
- **For the warning, the classifiers win on recall.** Spark and XGBoost warned all 70 episodes in VN_AQI, where the rule warned 55. That is the property an early warning needs. Their precision is lower: about a third of the alarms are false.
- **The LSTM's "predicted peak above 150" warning is the opposite trade-off.** Almost every alarm is right (precision 0.96), but it misses 4 hours in 10. A forecast tends to smooth the peaks, so a classifier trained on the event itself is the better warning.
- **Every warned episode had its warning up a full 24 hours ahead** (the median lead is 24 hours for all models). December pollution in northern Vietnam builds up over days, so the lead time saturates at the 24-hour window.
- **Training time:** Spark trained on CPU in Docker, in 6 minutes for both standards including evaluation. XGBoost took about 13 seconds per model and the LSTM 15–18 seconds, both on the RTX 5070 Ti GPU.

## Run

```bash
scripts/history_pipeline.sh                                        # fetch, Silver, R14, Gold, Spark models
LAKE_DIR=openaq_lake_hist python -m src.ml.compare_models          # XGBoost and LSTM (GPU), comparison.json
LAKE_DIR=openaq_lake_hist python -m src.serving.load_ml            # into MongoDB (ml_* collections)
streamlit run src/dashboard/app.py                                 # page "Dự báo & cảnh báo"
```

The Spark step needs an 8 GB driver for two years of data (`DRIVER_MEMORY=8g`). With 4 GB, it ran out of memory while caching the test predictions.

The outputs go to `openaq_lake_hist/gold/ml/<run>/` (`forecast`, `warning`, `backtest`, `warning_backtest`, `recent`, `features`, `metrics.json`, `comparison.json`) and the Spark models to `openaq_lake_hist/models/<run>/`.

Tests: `tests/test_ml_scoring.py` (no Spark) and `tests/test_ml_features.py` (in the `spark-lake` container).

## Limits

- **No weather features.** Wind, rain and temperature inversions drive the 24-hour error. The OpenWeather source in Silver could supply them.
- **The forecast starts from each station's last hour in Gold** (31 December 2025). It is not live; feeding it the stream's latest hours is the next step.
- **A short Vietnam test.** The test month has about 3,500 Vietnam station-hours per horizon, from the few stations active in December 2025.
