"""Single implementation of Silver rules as pure DataFrame transformations.

Parameters live in config/silver_rules_v1.yaml. values/units checks and rule finalization EDA call these
functions to measure impact; the Silver build calls the same ones, so the
measured numbers match the data that is written.
"""

from pathlib import Path

import yaml
from pyspark.sql import functions as F

DEFAULT_RULES = Path(__file__).resolve().parents[2] / "config" / "silver_rules_v1.yaml"


def load_rules(path=DEFAULT_RULES):
    rules = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    # YAML 1.1 reads unquoted `no:` (nitric oxide) as False: reject non-string keys.
    for section in ["canonical_units", "molecular_weight", "bounds"]:
        bad = [k for k in rules[section] if not isinstance(k, str)]
        if bad:
            raise ValueError(f"{section}: non-string keys {bad}; quote them in YAML")
    return rules


def _mapped(column, mapping):
    out = column
    for raw, std in mapping.items():
        out = F.when(column == F.lit(raw), F.lit(std)).otherwise(out)
    return out


def conversion_table(spark, rules):
    """Rules as data: one row per known (parameter_std, units_n).

    Columns: unit_std, factor (value_std = value * factor), temp_f (°F -> °C),
    bounds b_min/b_max/b_negmin in the canonical unit, invalid_pair.
    Joining this small table keeps every later expression a plain column
    comparison. The first version inlined CASE chains of ~26 branches into each
    flag; after Spark collapsed the projections the expression tree blew up and
    the driver ran out of heap (values/units checks pilot, 24/10/2026).
    """
    mv = float(rules["molar_volume_l"])
    rows = []
    for parameter, canonical in rules["canonical_units"].items():
        b = rules["bounds"].get(parameter, {})
        bounds = (float(b["min"]) if "min" in b else None, float(b["max"]) if "max" in b else None,
                  float(b["negative_small_min"]) if "negative_small_min" in b else None)
        rows.append((parameter, canonical, canonical, 1.0, False, *bounds, False))
        if canonical == "ppm":
            rows.append((parameter, "ppb", canonical, 0.001, False, *bounds, False))
        if parameter in rules["molecular_weight"]:
            factor = mv / (float(rules["molecular_weight"][parameter]) * 1000.0)
            rows.append((parameter, "µg/m³", canonical, factor, False, *bounds, False))
        if parameter == "temperature":
            rows.append((parameter, "f", canonical, None, True, *bounds, False))
    for parameter, units in rules["invalid_pairs"]:
        rows.append((parameter, units, units, None, False, None, None, None, True))
    schema = ("parameter_std string, units_n string, unit_std string, factor double, temp_f boolean, "
              "b_min double, b_max double, b_negmin double, invalid_pair boolean")
    return spark.createDataFrame(rows, schema)


def normalize_units(df, rules, sensor_units=None):
    """Add parameter_std, unit_std, value_std, unit_unconverted and the bound columns.

    value_std is value_num converted to the canonical unit when the conversion
    is defined; otherwise value_std = value_num and unit_std keeps the
    normalized raw unit (flag `unit_unconverted`).
    sensor_units (silver/dim_sensor_unit, R06b) overrides the declared unit per
    (sensors_id, parameter, units): relabel_ppm -> "ppm", relabel_f -> "f"
    (flag `unit_relabeled`); action suspect sets `unit_suspect`.
    """
    norm = rules["normalization"]
    param = _mapped(F.lower(F.trim(F.col("parameter"))), norm["parameter_aliases"])
    units = _mapped(F.lower(F.trim(F.col("units"))), norm["units_aliases"])
    table = conversion_table(df.sparkSession, rules)
    if sensor_units is not None:
        df = df.join(F.broadcast(sensor_units.select("sensors_id", "parameter", "units", "action")),
                     ["sensors_id", "parameter", "units"], "left")
    else:
        df = df.withColumn("action", F.lit(None).cast("string"))
    units = (F.when(F.col("action") == "relabel_ppm", F.lit("ppm"))
              .when(F.col("action") == "relabel_f", F.lit("f")).otherwise(units))
    df = (df.withColumn("parameter_std", param).withColumn("units_n", units)
            .withColumn("unit_relabeled", F.coalesce(F.col("action").startswith("relabel"), F.lit(False)))
            .withColumn("unit_suspect", F.coalesce(F.col("action") == "suspect", F.lit(False)))
            .drop("action")
            .join(F.broadcast(table), ["parameter_std", "units_n"], "left"))
    value = F.col("value_num")
    converted = F.when(F.col("temp_f"), (value - 32.0) * 5.0 / 9.0).otherwise(value * F.col("factor"))
    has_conv = F.coalesce(F.col("temp_f") | F.col("factor").isNotNull(), F.lit(False))
    return (df.withColumn("unit_unconverted", ~has_conv & ~F.coalesce(F.col("invalid_pair"), F.lit(False)))
              .withColumn("value_std", F.when(has_conv, converted).otherwise(value))
              .withColumn("unit_std", F.coalesce(F.col("unit_std"), F.col("units_n")))
              .withColumn("invalid_pair", F.coalesce(F.col("invalid_pair"), F.lit(False)))
              .drop("units_n", "factor", "temp_f"))


def value_flags(df, rules):
    """Sentinel / extreme / hard-bound / small-negative flags on value_std.

    Sentinels are matched on the raw value_num (before conversion); bounds come
    from conversion_table() columns. Requires normalize_units() first.
    """
    raw = F.col("value_num")
    finite = raw.isNotNull() & ~F.isnan(raw) & (F.abs(raw) != float("inf"))
    sentinel = finite & raw.isin(*[float(v) for v in rules["sentinels"]])
    extreme = finite & ~sentinel & (F.abs(raw) >= float(rules["extreme_abs"]))
    usable = finite & ~sentinel & ~extreme
    v = F.col("value_std")
    false = F.lit(False)
    return (df.withColumn("v_sentinel", F.coalesce(sentinel, false))
              .withColumn("v_extreme", F.coalesce(extreme, false))
              .withColumn("v_invalid_pair", F.col("invalid_pair"))
              .withColumn("v_below_min", F.coalesce(usable & (v < F.col("b_min")), false))
              .withColumn("v_above_max", F.coalesce(usable & (v > F.col("b_max")), false))
              .withColumn("v_negative_small", F.coalesce(
                  usable & (v < 0) & (v >= F.col("b_negmin")), false))
              .withColumn("v_unknown_parameter", ~F.col("parameter_std").isin(*rules["canonical_units"])
                          | F.col("parameter_std").isNull())
              .drop("invalid_pair", "b_min", "b_max", "b_negmin"))


# ---------------------------------------------------------------------------
# Full rule set R01-R14 (SILVER_PLAN.md §8). Order matters and is fixed here.
# ---------------------------------------------------------------------------

HARD_FLAGS = ["ts_out_of_range", "invalid_pair", "sentinel", "extreme",
              "below_min", "above_max", "unknown_parameter"]
SOFT_FLAGS = ["negative_small", "unit_unconverted", "unit_suspect", "unit_relabeled", "spike", "flatline"]
INFO_FLAGS = ["datetime_offset_repaired", "coord_invalid", "coord_filled", "dup_resolved"]
NUMERIC_PATTERN = r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"


def _nonblank(name):
    return F.col(name).isNotNull() & (F.length(F.trim(F.col(name))) > 0)


def quarantine_reason():
    """R01 / R03: rows that cannot be represented as a measurement."""
    split_head = (_nonblank("location_id") & ~_nonblank("datetime") & ~_nonblank("parameter")
                  & ~_nonblank("value_raw"))
    shifted = (F.trim(F.col("parameter")).rlike(NUMERIC_PATTERN)
               | F.lower(F.trim(F.col("units"))).isin("pm25", "pm10", "pm1", "no2", "o3", "co", "so2"))
    no_key = (F.col("sensors_id").cast("long").isNull() | F.col("location_id").cast("long").isNull()
              | F.col("datetime_utc").isNull())
    return (F.when(split_head | shifted, F.lit("multiline_split_record"))
             .when(no_key, F.lit("no_key")))


def repaired_datetime():
    """R02: "+HH:-MM" -> "+HH:MM" (Newfoundland offsets, verified against DST in structure checks)."""
    fixed = F.regexp_replace(F.col("datetime"), r"([+-]\d{2}):-(\d{2})$", "$1:$2")
    return F.to_timestamp(fixed)


def valid_coordinate():
    """R11: same definition as build_location_reference.py."""
    lat, lon = F.col("lat_num"), F.col("lon_num")
    fin = lambda c: c.isNotNull() & ~F.isnan(c) & (F.abs(c) != float("inf"))
    return (fin(lat) & fin(lon) & lat.between(-90, 90) & lon.between(-180, 180)
            & (lat != lon) & ~((lat == 0) & (lon == 0)))


def prepare(df, rules, build_utc):
    """R01-R04: time repair, key checks, quarantine reason, time range.

    The raw string `value` is renamed to `value_raw`; `value` becomes the cleaned number.
    """
    df = df.withColumnRenamed("value", "value_raw")
    df = df.withColumn("datetime_utc", F.coalesce(F.col("datetime_ts"), repaired_datetime()))
    df = df.withColumn("datetime_offset_repaired",
                       F.col("datetime_ts").isNull() & F.col("datetime_utc").isNotNull())
    df = df.withColumn("quarantine_reason", quarantine_reason())
    lo = F.to_timestamp(F.lit(rules["time_range"]["min_utc"]))
    return df.withColumn("ts_out_of_range", F.coalesce(
        (F.col("datetime_utc") < lo) | (F.col("datetime_utc") > F.lit(build_utc).cast("timestamp")),
        F.lit(False)))


def coordinates(df, dim_location):
    """R11: keep valid row coordinates, else fixed-location reference, else null."""
    ref = dim_location.filter(F.col("location_kind") == "fixed").select(
        "location_id", "lat_ref", "lon_ref")
    df = df.join(F.broadcast(ref), "location_id", "left")
    ok = valid_coordinate()
    return (df.withColumn("coord_invalid", ~ok)
              .withColumn("coord_filled", ~ok & F.col("lat_ref").isNotNull())
              .withColumn("lat", F.when(ok, F.col("lat_num")).otherwise(F.col("lat_ref")))
              .withColumn("lon", F.when(ok, F.col("lon_num")).otherwise(F.col("lon_ref")))
              .withColumn("coord_source", F.when(ok, "row")
                          .when(F.col("lat_ref").isNotNull(), "location_ref").otherwise("none"))
              .drop("lat_ref", "lon_ref"))


def soft_outlier_flags(df, rules):
    """R14: spike / flatline per sensor (window by sensor and time). Flag only."""
    so = rules["soft_outliers"]
    from pyspark.sql import Window
    w = Window.partitionBy("sensors_id").orderBy("datetime_utc")
    v = F.col("value")
    prev, nxt = F.lag("value").over(w), F.lead("value").over(w)
    same = F.coalesce(v == prev, F.lit(False))
    df = df.withColumn("_run", F.sum(F.when(same, 0).otherwise(1)).over(w))
    df = df.withColumn("_run_len", F.count("*").over(Window.partitionBy("sensors_id", "_run")))
    min_value = F.lit(None).cast("double")
    for parameter, threshold in so["spike_min_value"].items():
        min_value = F.when(F.col("parameter_std") == parameter, F.lit(float(threshold))).otherwise(min_value)
    spike = (v >= min_value) & (v > float(so["spike_ratio"]) * F.greatest(prev, nxt))
    return (df.withColumn("spike", F.coalesce(spike, F.lit(False)))
              .withColumn("flatline", F.coalesce((F.col("_run_len") >= int(so["flatline_min_run"]))
                                                 & v.isNotNull() & (v != 0), F.lit(False)))
              .drop("_run", "_run_len"))


def apply_rules(df, rules, dim_location, build_utc, sensor_units=None, soft_outliers=False):
    """Return (measurements, quarantine) DataFrames before dedup.

    measurements carries dq flag columns + dq_flags + dq_status + value.
    Dedup (R13) is applied by dedup() on a sensor-partitioned frame.
    """
    df = prepare(df, rules, build_utc)
    quarantine = df.filter(F.col("quarantine_reason").isNotNull())
    df = df.filter(F.col("quarantine_reason").isNull())
    df = value_flags(normalize_units(df, rules, sensor_units), rules)
    df = (df.withColumnRenamed("v_sentinel", "sentinel").withColumnRenamed("v_extreme", "extreme")
            .withColumnRenamed("v_invalid_pair", "invalid_pair")
            .withColumnRenamed("v_below_min", "below_min").withColumnRenamed("v_above_max", "above_max")
            .withColumnRenamed("v_negative_small", "negative_small")
            .withColumnRenamed("v_unknown_parameter", "unknown_parameter"))
    hard = reduce_or([F.col(f) for f in HARD_FLAGS])
    df = df.withColumn("value", F.when(hard, F.lit(None).cast("double")).otherwise(F.col("value_std")))
    if soft_outliers:
        df = soft_outlier_flags(df, rules)
    else:
        df = df.withColumn("spike", F.lit(False)).withColumn("flatline", F.lit(False))
    df = coordinates(df, dim_location)
    return df, quarantine


def reduce_or(columns):
    out = F.lit(False)
    for c in columns:
        out = out | F.coalesce(c, F.lit(False))
    return out


def dedup(df):
    """R13: one row per (sensors_id, datetime_utc).

    Tie-break (deterministic): non-hard rows first, then rows with a value,
    then the smallest source_block. Requires df partitioned by sensors_id.
    """
    from pyspark.sql import Window
    hard = reduce_or([F.col(f) for f in HARD_FLAGS])
    key = Window.partitionBy("sensors_id", "datetime_utc")
    order = key.orderBy(hard.asc(), F.col("value").isNull().asc(), F.col("source_block").asc())
    df = df.withColumn("dup_count", F.count("*").over(key)).withColumn("_rn", F.row_number().over(order))
    return df.withColumn("dup_resolved", F.col("dup_count") > 1), F.col("_rn") == 1


def finalize(df):
    """dq_flags array + dq_status (invalid > suspect > valid)."""
    flags = HARD_FLAGS + SOFT_FLAGS + INFO_FLAGS
    arr = F.filter(F.array(*[F.when(F.col(f), F.lit(f)) for f in flags]), lambda x: x.isNotNull())
    status = (F.when(reduce_or([F.col(f) for f in HARD_FLAGS]), "invalid")
               .when(reduce_or([F.col(f) for f in SOFT_FLAGS]), "suspect").otherwise("valid"))
    return df.withColumn("dq_flags", arr).withColumn("dq_status", status)
