"""Fault injection (src/chaos/faults.py): mutations, deterministic selection, and which Silver rules
catch which fault. A strict xfail marks a known gap: it starts failing once a rule closes it."""

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F

from src.chaos.faults import BY_NAME, FAULTS, apply_faults, fault_label, select_faults
from src.silver.rules import apply_rules, dedup, finalize

T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
SCHEMA = ("location_id string, sensors_id string, location string, datetime string, lat string, lon string, "
          "parameter string, units string, value string, value_num double, lat_num double, lon_num double, "
          "datetime_ts timestamp, source_block string, year int, month int, _want string")


def row(sensor="1", hour=0, parameter="pm25", value=20.5, units="µg/m³", offset_h=7, want=None, block="blk_1"):
    ts = T0 + timedelta(hours=hour)
    wall = (ts + timedelta(hours=offset_h)).strftime("%Y-%m-%dT%H:%M:%S")
    return ("10", sensor, "Hanoi", f"{wall}+{offset_h:02d}:00", "21.0", "105.8", parameter, units,
            repr(value), value, 21.0, 105.8, ts, block, 2025, 1, want)


def with_faults(spark, rows):
    """Apply the faults named in _want, bypassing the random selection."""
    df = spark.createDataFrame(rows, SCHEMA)
    return apply_faults(select_faults(df, seed=1, scale=0).withColumn("_fault", F.col("_want")))


def by_tag(df):
    return {r["source_block"]: r for r in df.collect()}


def test_value_and_text_faults(spark):
    names = ["sentinel", "out_of_range", "spike", "bad_number", "unknown_parameter", "unit_alias",
             "missing_key", "shifted_row", "unit_x1000", "unit_div1000", "calibration_bias"]
    out = by_tag(with_faults(spark, [row(sensor=str(i), want=n) for i, n in enumerate(names)]
                             + [row(sensor="99", parameter="pm10", want="spike", block="blk_2")]))
    get = lambda n, block="blk_1": out[f"chaos-{n}-{block}"]
    assert get("sentinel").value_num == -999.0 and get("sentinel").value == "-999.0"
    assert get("out_of_range").value_num == 987654.0
    assert get("spike").value_num == 20.5 * 10 + 600
    assert get("spike", "blk_2").value_num == 20.5 * 10 + 1200          # pm10 needs a larger jump
    assert get("bad_number").value == "20,5" and get("bad_number").value_num is None
    assert get("unknown_parameter").parameter == "pm2.5"
    assert get("unit_alias").units == "ug/m3"
    assert get("missing_key").sensors_id == ""
    assert get("shifted_row").parameter == "20.5"
    assert get("unit_x1000").value_num == pytest.approx(20500)
    assert get("unit_div1000").value_num == pytest.approx(0.0205)
    assert get("calibration_bias").value_num == pytest.approx(32.8)


def test_time_and_coordinate_faults(spark):
    out = by_tag(with_faults(spark, [row(sensor="1", want="future_ts"), row(sensor="2", want="tz_shift"),
                                     row(sensor="3", want="coord_swap"), row(sensor="4", want="null_island")]))
    # sessionTimeZone is UTC; collected timestamps are naive local times of the machine
    fut, tz = out["chaos-future_ts-blk_1"], out["chaos-tz_shift-blk_1"]
    assert fut.datetime == "2027-01-01T07:00:00+07:00"
    assert tz.datetime == "2025-01-01T07:00:00+00:00"          # wall clock kept, offset lost
    swap, island = out["chaos-coord_swap-blk_1"], out["chaos-null_island-blk_1"]
    assert (swap.lat, swap.lon, swap.lat_num, swap.lon_num) == ("105.8", "21.0", 105.8, 21.0)
    assert (island.lat_num, island.lon_num, island.lat) == (0.0, 0.0, "0")
    shifted = (with_faults(spark, [row(want="future_ts"), row(sensor="2", want="tz_shift")])
               .select("source_block", ((F.unix_timestamp("datetime_ts") - int(T0.timestamp())) / 3600)
                       .alias("h")).collect())
    hours = {r.source_block: r.h for r in shifted}
    assert hours["chaos-future_ts-blk_1"] == 730 * 24 and hours["chaos-tz_shift-blk_1"] == 7


def test_stuck_repeats_the_first_value_of_the_window(spark):
    out = with_faults(spark, [row(hour=h, value=10.0 + h, want="stuck") for h in range(5)])
    assert {r.value_num for r in out.collect()} == {10.0}


def test_duplicates_keep_a_clean_original(spark):
    out = with_faults(spark, [row(sensor="1", want="duplicate"), row(sensor="2", value=10.0, want="dup_conflict")])
    rows = sorted((r.sensors_id, r.source_block, r.value_num) for r in out.collect())
    assert rows == [("1", "blk_1", 20.5), ("1", "chaos-duplicate-blk_1", 20.5),
                    ("2", "blk_1", 10.0), ("2", "chaos-dup_conflict-blk_1", 15.0)]


def test_selection_is_deterministic_and_one_cause_per_row(spark):
    rows = [row(sensor=str(s), hour=h, value=5.0 + h % 7) for s in range(8) for h in range(24 * 28)]
    df = spark.createDataFrame(rows, SCHEMA).drop("_want")
    pick = lambda: {(r.sensors_id, r.datetime): r._fault
                    for r in select_faults(df, seed=7, scale=30).collect() if r._fault}
    first = pick()
    assert first == pick() and first
    level = {f.name: f.level for f in FAULTS}
    by_sensor = {}
    for (sensor, _), name in first.items():
        by_sensor.setdefault(sensor, set()).add(level[name])
    assert all(len(levels) == 1 for levels in by_sensor.values())   # a sensor fault excludes row faults
    windows = {}
    for (sensor, dt), name in first.items():
        if level[name] == "sensor":
            windows.setdefault((sensor, name), []).append(dt)
    assert windows
    for (sensor, name), times in windows.items():
        assert len(times) <= BY_NAME[name].window_days * 24


def test_fault_label(spark):
    df = spark.createDataFrame([("chaos-unit_x1000-blk_5",), ("blk_5",), (None,)], "source_block string")
    assert [r.f for r in df.select(fault_label().alias("f")).collect()] == ["unit_x1000", "clean", "clean"]


EXPECTED_ROW_CATCHES = [f.name for f in FAULTS
                        if f.expected and f.expected not in ("dedup", "spike", "flatline")]


@pytest.fixture(scope="module")
def silver_on_faults(spark, silver_rules):
    rows = [row(sensor=str(i), want=n) for i, n in enumerate(EXPECTED_ROW_CATCHES + ["bad_number"])]
    dim = spark.createDataFrame([("10", 21.0, 105.8, "fixed")],
                                "location_id string, lat_ref double, lon_ref double, location_kind string")
    measured, quarantine = apply_rules(with_faults(spark, rows), silver_rules, dim, "2026-11-12T00:00:00Z")
    deduped, keep = dedup(measured)
    flags = {fault_label_of(r.source_block): (r.dq_flags, r.dq_status)
             for r in finalize(deduped.filter(keep)).collect()}
    reasons = {fault_label_of(r.source_block): r.quarantine_reason for r in quarantine.collect()}
    return flags, reasons


def fault_label_of(block):
    return block.split("-")[1]


@pytest.mark.parametrize("name", EXPECTED_ROW_CATCHES)
def test_silver_rules_catch_row_faults(silver_on_faults, name):
    flags, reasons = silver_on_faults
    expected = BY_NAME[name].expected
    if expected.startswith("quarantine:"):
        assert reasons[name] == expected.split(":", 1)[1]
    else:
        assert expected in flags[name][0]


@pytest.mark.xfail(strict=True, reason="gap: no rule flags an unparseable value; the row stays valid with a "
                                       "null value (docs/fault-injection.md, R15)")
def test_unparseable_value_is_flagged(silver_on_faults):
    flags, _ = silver_on_faults
    assert flags["bad_number"][1] != "valid"
