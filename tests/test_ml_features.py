"""Features, labels and the time split of src/ml/aqi_forecast.py on a tiny station (Spark)."""
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from src.ml.aqi_forecast import (DAY, HOUR, POLLUTANTS, every_horizon, horizon_rows, split_bounds, windows,
                                 with_features)

T0 = datetime(2025, 1, 1)


def station(spark):
    """Hours 0..40 of one VN station with AQI = hour number (VN) and 2 x hour (US); hour 5 missing."""
    rows = [(1, T0 + timedelta(hours=i), 21.0, 105.8, True, float(i), 2.0 * i) + (None,) * len(POLLUTANTS)
            for i in range(41) if i != 5]
    cols = "location_id int, hour_utc timestamp, lat double, lon double, is_vn boolean, aqi_vn double, aqi_us double, " + \
           ", ".join(f"{p} double" for p in POLLUTANTS)
    return spark.createDataFrame(rows, cols)


def at(df, hour):
    return df.filter(F.col("hour_utc") == T0 + timedelta(hours=hour)).first()


def test_lags_use_the_exact_hour_and_labels_look_ahead_only(spark):
    f = with_features(station(spark)).persist()
    r6, r7, r30 = at(f, 6), at(f, 7), at(f, 30)
    assert r6["vn_lag1"] == 6                  # hour 5 is missing: the current value, not hour 4's
    assert r7["vn_lag1"] == 6 and r7["vn_lag2"] == 7   # lag 2 lands on the missing hour 5
    assert r30["vn_lag24"] == 6 and r30["us_lag24"] == 12
    assert r6["vn_mean6"] == (1 + 2 + 3 + 4 + 6) / 5   # hours 1..6 that exist
    assert r6["vn_d3"] == 6 - 3
    r0 = at(f, 0)
    assert r0["vn_next24_max"] == 24 and r0["vn_next24_n"] == 23   # hours 1..24 without 5, not hour 0
    assert r0["pm25_1h"] == -1.0               # missing concentrations become -1 for the trees
    assert r6["hour_local"] == (6 + 7) % 24    # longitude 105.8: UTC+7 solar time
    f.unpersist()


def test_horizon_rows_join_the_exact_target_hour(spark):
    f = with_features(station(spark))
    anchor = f.filter(F.col("hour_utc") == T0 + timedelta(hours=3))
    rows = {r["h"]: r for r in horizon_rows(anchor, f, "vn", every_horizon([1, 2, 3])).collect()}
    assert sorted(rows) == [1, 3]              # 3 + 2 = hour 5 is missing: no row
    assert rows[3]["y_aqi"] == 6 and rows[3]["y"] == 6 - 3
    assert rows[1]["target_hour_local"] == (3 + 7 + 1) % 24


def test_training_labels_end_before_validation(spark):
    f = with_features(station(spark))
    t_end = int((T0 + timedelta(hours=40)).timestamp())
    val_start, test_start = split_bounds(t_end, 0, 0)
    assert test_start == t_end + HOUR and val_start == test_start
    val_start, test_start = t_end - 10 * HOUR, t_end - 4 * HOUR
    train, val, test = windows(f, val_start, test_start)
    assert train.agg(F.max("ts")).first()[0] + 24 * HOUR < val_start
    assert test.agg(F.min("ts")).first()[0] >= test_start
    assert val.count() == 0                    # 6 h of validation cannot hold a 24 h label
    assert DAY == 24 * HOUR
