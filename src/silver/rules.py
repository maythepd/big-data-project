"""Silver rules R01-R14 as DataFrame transformations (config/silver_rules_v1.yaml).

Shared by the EDA checks and the Silver build, so measured impact matches the output.
"""

from pathlib import Path

import yaml
from pyspark.sql import functions as F

DEFAULT_RULES = Path(__file__).resolve().parents[2] / "config" / "silver_rules_v1.yaml"


def load_rules(path=DEFAULT_RULES):
    rules = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    # YAML 1.1 reads `no:` (nitric oxide) as False
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
    """One row per (parameter_std, units_n): unit_std, factor, temp_f, bounds, invalid_pair.

    A join instead of long CASE chains, which blew up the driver's expression tree.
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
    """Convert to the canonical unit (value_std, unit_std) and add the bound columns.

    sensor_units (dim_sensor_unit, R06b) relabels or flags units per sensor.
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
              .when(F.col("action") == "relabel_f", F.lit("f"))
              .when(F.col("action") == "relabel_c", F.lit("c")).otherwise(units))
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
    """Sentinel, extreme, hard-bound and small-negative flags (after normalize_units)."""
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


# ---------- full rule set R01-R14, applied in this order ----------

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
    """R02: "+HH:-MM" -> "+HH:MM" (Newfoundland offsets)."""
    fixed = F.regexp_replace(F.col("datetime"), r"([+-]\d{2}):-(\d{2})$", "$1:$2")
    return F.to_timestamp(fixed)


def valid_coordinate():
    """R11: same definition as build_location_reference.py."""
    lat, lon = F.col("lat_num"), F.col("lon_num")
    fin = lambda c: c.isNotNull() & ~F.isnan(c) & (F.abs(c) != float("inf"))
    return (fin(lat) & fin(lon) & lat.between(-90, 90) & lon.between(-180, 180)
            & (lat != lon) & ~((lat == 0) & (lon == 0)))


def prepare(df, rules, build_utc):
    """R01-R04: time repair, key checks, quarantine reason, time range."""
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


def _per_parameter(mapping):
    out = F.lit(None).cast("double")
    for parameter, value in mapping.items():
        out = F.when(F.col("parameter") == parameter, F.lit(float(value))).otherwise(out)
    return out


def flatline_kind(df, rules):
    """R14 flatline: a sensor repeating one value; adds flatline_kind (negative, at_floor, held).

    df holds a month plus context_hours on each side, partitioned by sensors_id.
    """
    from pyspark.sql import Window
    fl = rules["soft_outliers"]["flatline"]
    w = Window.partitionBy("sensors_id").orderBy("datetime_utc")
    new = F.coalesce(F.col("value") != F.lag("value").over(w), F.lit(True))
    df = df.withColumn("_run", F.sum(new.cast("int")).over(w))
    run = Window.partitionBy("sensors_id", "_run")
    df = (df.withColumn("_readings", F.count("*").over(run))
            .withColumn("_hours", (F.max("datetime_utc").over(run).cast("long")
                                   - F.min("datetime_utc").over(run).cast("long")) / 3600.0))
    v, hours = F.col("value"), F.col("_hours")
    floor = _per_parameter(fl["floor"])
    # only parameters with a small-negative band use negative "no data" codes
    no_negative = [k for k, b in rules["bounds"].items() if "negative_small_min" in b]
    kind = (F.when(v < 0, F.when(F.col("parameter").isin(*no_negative),
                                 F.when(hours >= fl["negative_min_hours"], F.lit("negative")))
                          .when(hours >= fl["min_hours"], F.lit("held")))
             .when(floor.isNotNull() & (v <= floor), F.when(hours >= fl["floor_min_hours"], F.lit("at_floor")))
             .when(hours >= fl["min_hours"], F.lit("held")))
    kind = F.when(F.col("parameter").isin(*fl["parameters"]) & (F.col("_readings") >= fl["min_readings"]), kind)
    return df.withColumn("flatline_kind", kind).drop("_run", "_readings", "_hours")


def spike_kind(df, rules):
    """R14 spike: a reading far above its sensor-hour, or an hour far above its neighbours.

    "Far" needs both a ratio and a minimum difference (diff only for temperature).
    """
    from pyspark.sql import Window
    sp = rules["soft_outliers"]["spike"]
    w = Window.partitionBy("sensors_id").orderBy("datetime_utc")
    sh = Window.partitionBy("sensors_id", "_hour")
    df = (df.withColumn("_hour", F.date_trunc("hour", "datetime_utc").cast("long"))
            .withColumn("_hour_median", F.median("value").over(sh))
            .withColumn("_hour_n", F.count("*").over(sh)))
    # one entry per hour in the neighbour lists
    first = F.coalesce(F.lag("_hour").over(w) != F.col("_hour"), F.lit(True))
    df = df.withColumn("_hour_entry", F.when(first, F.col("_hour_median")))
    t = Window.partitionBy("sensors_id").orderBy("_hour")
    df = (df.withColumn("_nb", F.array_sort(F.concat(
                F.collect_list("_hour_entry").over(t.rangeBetween(-3 * 3600, -1)),
                F.collect_list("_hour_entry").over(t.rangeBetween(1, 3 * 3600)))))
            .withColumn("_k", F.size("_nb"))
            .withColumn("_local", F.expr(
                "CASE WHEN _k < 3 THEN NULL WHEN _k % 2 = 1 THEN element_at(_nb, CAST(_k DIV 2 + 1 AS INT)) "
                "ELSE (element_at(_nb, CAST(_k DIV 2 AS INT)) + element_at(_nb, CAST(_k DIV 2 + 1 AS INT))) / 2 END")))
    min_diff = _per_parameter(sp["min_diff"])
    diff_only = F.col("parameter").isin(*sp["diff_only"])

    def far(x, ref):
        above = (x >= float(sp["ratio"]) * ref) & (x - ref >= min_diff)
        return F.coalesce(F.when(diff_only, F.abs(x - ref) >= min_diff).otherwise(above), F.lit(False))

    reading = (F.col("_hour_n") >= sp["min_hour_readings"]) & far(F.col("value"), F.col("_hour_median"))
    kind = F.when(reading, F.lit("reading")).when(far(F.col("_hour_median"), F.col("_local")), F.lit("hour"))
    return (df.withColumn("spike_kind", F.when(min_diff.isNotNull(), kind))
              .drop("_hour", "_hour_median", "_hour_n", "_hour_entry", "_nb", "_k", "_local"))


def with_temporal_flags(df):
    """Replace the R14 flags in dq_flags and recompute dq_status (idempotent)."""
    r14 = {f: F.coalesce(F.col(f), F.lit(False)) for f in ["spike", "flatline"]}
    flags = HARD_FLAGS + SOFT_FLAGS + INFO_FLAGS
    arr = F.filter(F.array(*[F.when(r14[f] if f in r14 else F.array_contains("dq_flags", f), F.lit(f))
                             for f in flags]), lambda x: x.isNotNull())
    status = (F.when(F.exists("dq_flags", lambda x: x.isin(*HARD_FLAGS)), "invalid")
               .when(F.exists("dq_flags", lambda x: x.isin(*SOFT_FLAGS)), "suspect").otherwise("valid"))
    return df.withColumn("dq_flags", arr).withColumn("dq_status", status).drop("spike", "flatline")


def apply_rules(df, rules, dim_location, build_utc, sensor_units=None):
    """Return (measurements, quarantine) before dedup (R13)."""
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
    # R14 runs later on the built Silver (flag_temporal_outliers.py)
    df = df.withColumn("spike", F.lit(False)).withColumn("flatline", F.lit(False))
    df = coordinates(df, dim_location)
    return df, quarantine


def reduce_or(columns):
    out = F.lit(False)
    for c in columns:
        out = out | F.coalesce(c, F.lit(False))
    return out


def dedup(df):
    """R13: one row per (sensors_id, datetime_utc); prefer non-hard, valued, oldest block."""
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
