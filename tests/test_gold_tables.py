"""Gold table logic: usable Silver rows, station hours, rolling means, local days."""

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from src.gold.build_aqi_daily import HW_COLUMNS, station_daily, with_rolling
from src.gold.build_gold import station_hourly
from src.gold.silver_union import use_reason

T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
VN_OFFSET = 420  # UTC+7, minutes


def hourly_rows(spark, rows):
    """station_hourly rows (location, parameter, hour offset from T0, value) with Vietnam's offset."""
    data = [(loc, p, "x", T0 + timedelta(hours=h), v, v, v, 1, VN_OFFSET) for loc, p, h, v in rows]
    schema = ("location_id string, parameter string, unit string, hour_utc timestamp, value_mean double, "
              "value_min double, value_max double, n_readings long, utc_offset_min int")
    return spark.createDataFrame(data, schema).select(*HW_COLUMNS)


def by_hour(df, location, parameter, column):
    # hour offset computed in Spark: collected timestamps are naive local times of this machine
    hour = ((F.unix_timestamp("hour_utc") - int(T0.timestamp())) / 3600).cast("int")
    rows = (df.filter((F.col("location_id") == location) & (F.col("parameter") == parameter))
            .select(hour.alias("h"), column).collect())
    return {r.h: r[column] for r in rows}


def test_use_reason(spark, rules):
    df = spark.createDataFrame([
        ("valid", []), ("suspect", ["unit_relabeled"]), ("suspect", ["negative_small", "unit_relabeled"]),
        ("suspect", ["spike"]), ("suspect", ["unit_relabeled", "flatline"]), ("suspect", ["unit_suspect"]),
        ("invalid", ["unit_relabeled"]),
    ], "dq_status string, dq_flags array<string>")
    got = [r[0] for r in df.select(use_reason(rules)).collect()]
    assert got == ["valid", "suspect_allowed", "suspect_allowed", "suspect_excluded", "suspect_excluded",
                   "suspect_excluded", "invalid"]


def test_station_hourly_counts_twin_channels_once(spark, rules):
    h = T0
    sensor_hours = spark.createDataFrame([
        ("L1", "pm25", h, 2, 10.0, 9.0, 11.0, 0, ["batch"]),
        ("L1", "pm25", h, 1, 10.0000000001, 10.0, 10.0, 0, ["stream"]),   # same value at 6 decimals
        ("L1", "no2", h, 1, 0.01, 0.01, 0.01, 0, ["batch"]),
        ("L1", "no2", h, 1, 0.02, 0.02, 0.02, 0, ["batch"]),
        ("L1", "no2", h, 0, None, None, None, None, []),                  # no usable reading
    ], "location_id string, parameter string, hour_utc timestamp, n_usable long, mean double, min double, "
       "max double, utc_offset_min int, origins array<string>").withColumn("unit", F.lit("x"))
    dim = spark.createDataFrame([("L1", "Hà Nội", 21.0, 105.8)],
                                "location_id string, location_name string, lat double, lon double")
    vn = spark.createDataFrame([("L1", True)], "location_id string, is_vn boolean")
    out = {r.parameter: r for r in station_hourly(sensor_hours, rules, dim, vn, "test").collect()}
    pm = out["pm25"]
    assert (pm.sensor_count, pm.value_count, pm.value_mean) == (2, 1, 10.0)
    assert (pm.value_min, pm.value_max, pm.n_readings, pm.origin) == (9.0, 11.0, 3, "batch+stream")
    no2 = out["no2"]
    assert (no2.sensor_count, no2.value_count) == (2, 2)
    assert no2.value_mean == pytest.approx(0.015)
    assert (no2.is_vn, no2.location_name) == (True, "Hà Nội")


def test_rolling_means_need_enough_hours_in_the_window(spark, rules):
    rows = ([("L1", "o3", h, float(h + 1)) for h in range(6)] + [("L1", "o3", 13, 1.0)]
            + [("L1", "pm25", h, 10.0) for h in range(18)]
            + [("L1", "no2", 0, 1.0)])
    hw = with_rolling(hourly_rows(spark, rows), rules)
    o3 = by_hour(hw, "L1", "o3", "avg_8h")
    assert o3[4] is None                      # 5 of 8 hours
    assert o3[5] == pytest.approx(3.5)        # 6 of 8 hours: mean of 1..6
    assert o3[13] is None                     # range window: hours 6..12 are missing, not skipped
    pm24 = by_hour(hw, "L1", "pm25", "avg_24h")
    assert pm24[16] is None and pm24[17] == pytest.approx(10.0)   # 17 vs 18 of 24 hours
    nowcast = by_hour(hw, "L1", "pm25", "nowcast")
    assert nowcast[0] is None and nowcast[1] == pytest.approx(10.0)
    assert set(by_hour(hw, "L1", "no2", "avg_8h").values()) == {None}


def test_local_day_and_75_percent_rule(spark, rules):
    # Hour 17 UTC = 00:00 local, labelled as the END of the previous local day (01:00 .. 00:00).
    rows = ([("L24", "pm25", h, 10.0) for h in range(17, 42)]    # 1 h on Jan 1, 24 h on Jan 2
            + [("L18", "pm25", h, 20.0) for h in range(18, 36)]  # 18 h on Jan 2
            + [("L17", "pm25", h, 30.0) for h in range(18, 35)])  # 17 h on Jan 2
    hw = with_rolling(hourly_rows(spark, rows), rules)
    daily = station_daily(hw, rules, T0, T0 + timedelta(days=31))
    got = {(r.location_id, r.local_date.day): (r.hours, r.day_valid, r.value_mean) for r in daily.collect()}
    assert got == {("L24", 1): (1, False, 10.0), ("L24", 2): (24, True, 10.0),
                   ("L18", 2): (18, True, 20.0), ("L17", 2): (17, False, 30.0)}
    assert {r.max_8h for r in daily.collect()} == {None}         # only O3 and CO keep max_8h
