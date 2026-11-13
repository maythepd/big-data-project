"""Fault catalogue for chaos runs on Near-Silver (docs/fault-injection.md).

Each fault breaks rows the way real feeds break. Selection is deterministic: xxhash64 of the
seed and the row (row faults) or the sensor (sensor faults), so the same seed injects the same
faults. A changed or added row gets source_block "chaos-<fault>-<original block>". Silver keeps
source_block unchanged and no rule reads it, except R13 dedup as its last tie-breaker, where
"chaos-" sorts after "blk_": an original row always wins over its injected copy. The evaluation
reads the label back from source_block.
"""

from dataclasses import dataclass

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.silver.build_silver import offset_minutes

TAG = "chaos-"
PM = ("pm1", "pm25", "pm10")
GASES = ("o3", "no2", "so2", "co", "no")
MICROGRAMS = ("µg/m³", "μg/m³")       # U+00B5 and U+03BC spellings
DAY = 86_400


@dataclass(frozen=True)
class Fault:
    name: str
    level: str                # "row": single readings; "sensor": every reading of a sensor in a window
    rate: float               # share of rows (row) or of sensors (sensor) selected
    expected: str | None      # what should catch it: a dq flag, "quarantine:<reason>", "dedup"; None = no rule
    what: str
    parameters: tuple = ()    # eligible raw parameters (lower-case); empty = all
    window_days: int = 0      # sensor faults: length of the faulty window inside the month


FAULTS = [
    Fault("sentinel", "row", 5e-4, "sentinel", "value replaced by the -999 no-data code"),
    Fault("out_of_range", "row", 5e-4, "above_max", "value far above any physical bound", PM + GASES),
    Fault("spike", "row", 5e-4, "spike", "one reading 10x its level plus a jump (within the hard bound)", PM),
    Fault("bad_number", "row", 5e-4, None, "decimal comma: '12.5' sent as '12,5', unparseable"),
    Fault("unknown_parameter", "row", 5e-4, "unknown_parameter", "parameter spelled 'pm2.5'", ("pm25",)),
    Fault("unit_alias", "row", 5e-4, "unit_unconverted", "unit spelled 'ug/m3'"),
    Fault("future_ts", "row", 5e-4, "ts_out_of_range", "timestamp two years ahead"),
    Fault("coord_swap", "row", 5e-4, "coord_invalid", "lat and lon swapped"),
    Fault("null_island", "row", 5e-4, "coord_invalid", "coordinates (0, 0)"),
    Fault("missing_key", "row", 5e-4, "quarantine:no_key", "sensors_id blank"),
    Fault("shifted_row", "row", 5e-4, "quarantine:multiline_split_record",
          "columns shifted: the value lands in parameter"),
    Fault("duplicate", "row", 5e-4, "dedup", "the same record delivered twice"),
    Fault("dup_conflict", "row", 5e-4, None, "a second record for the same sensor and time, 1.5x the value"),
    Fault("stuck", "sensor", 4e-3, "flatline", "sensor repeats its first value for 3 days",
          PM + GASES + ("temperature", "humidity", "relativehumidity"), 3),
    Fault("unit_x1000", "sensor", 4e-3, "above_max", "values 1000x for 5 days (unit change upstream)",
          PM + GASES, 5),
    Fault("unit_div1000", "sensor", 4e-3, None, "values /1000 for 5 days (unit change upstream)", PM + GASES, 5),
    Fault("calibration_bias", "sensor", 4e-3, None, "values 1.6x for 7 days (calibration drift)",
          PM + ("no2", "o3"), 7),
    Fault("tz_shift", "sensor", 4e-3, None, "local time sent as UTC for 7 days", (), 7),
]
BY_NAME = {f.name: f for f in FAULTS}


def _uniform(*cols):
    """Deterministic uniform number in [0, 1) from a hash of the columns."""
    return F.pmod(F.xxhash64(*cols), F.lit(1 << 31)).cast("double") / float(1 << 31)


def _assign(u, faults, scale):
    """Fault name for u in consecutive [lo, lo + rate * scale) intervals, else null."""
    expr, lo = F.lit(None).cast("string"), 0.0
    for f in faults:
        hi = lo + f.rate * scale
        expr = F.when((u >= lo) & (u < hi), F.lit(f.name)).otherwise(expr)
        lo = hi
    if lo >= 1:
        raise ValueError(f"fault rates add up to {lo:.3f} >= 1 at scale {scale}")
    return expr


def _condition(f):
    """Row-level eligibility of fault f (beyond its parameter list)."""
    v = F.col("value_num")
    extra = {
        "spike": v.between(1, 80),
        "bad_number": F.col("value").contains("."),
        "unit_alias": F.lower(F.trim("units")).isin(*MICROGRAMS),
        "future_ts": F.col("datetime_ts").isNotNull() & F.col("_off").isNotNull(),
        "coord_swap": F.col("lat_num").isNotNull() & F.col("lon_num").isNotNull()
                      & (F.col("lat_num") != F.col("lon_num")),
        "shifted_row": v.isNotNull(),
        "dup_conflict": v.isNotNull() & (v != 0),
        "stuck": v.isNotNull(),
        "unit_x1000": v.isNotNull(),
        "unit_div1000": v.isNotNull(),
        "calibration_bias": v.isNotNull(),
        "tz_shift": F.col("datetime_ts").isNotNull() & F.col("_off").isNotNull() & (F.col("_off") != 0),
    }.get(f.name, F.lit(True))
    if f.parameters:
        extra = extra & F.col("_p").isin(*f.parameters)
    return extra


def _eligible(fault_col, faults):
    out = F.lit(False)
    for f in faults:
        out = F.when(fault_col == f.name, _condition(f)).otherwise(out)
    return F.coalesce(out, F.lit(False))


def _by_fault(fault_col, mapping, default=None):
    out = F.lit(default)
    for name, value in mapping.items():
        out = F.when(fault_col == name, F.lit(value)).otherwise(out)
    return out


def select_faults(df, seed, faults=FAULTS, scale=1.0):
    """Add `_fault`: at most one fault per row, or null.

    A sensor picked for a sensor fault is faulty only inside its window and gets no row faults,
    so every changed row has exactly one cause.
    """
    row_faults = [f for f in faults if f.level == "row"]
    sensor_faults = [f for f in faults if f.level == "sensor"]
    seed = F.lit(int(seed))
    df = df.withColumn("_p", F.lower(F.trim("parameter"))).withColumn("_off", offset_minutes())

    # hashes as their own columns: inlined in every CASE branch they overflow Spark's 64 KB codegen limit
    df = (df.withColumn("_us", _uniform(seed, F.lit("sensor"), "sensors_id"))
            .withColumn("_ur", _uniform(seed, F.lit("row"), "sensors_id", "datetime", "source_block", "value"))
            .withColumn("_sf", _assign(F.col("_us"), sensor_faults, scale))
            .withColumn("_rf", _assign(F.col("_ur"), row_faults, scale)))
    # the sensor's window starts on a hashed day of the row's month
    days = _by_fault(F.col("_sf"), {f.name: f.window_days for f in sensor_faults}, 0)
    month_start = F.unix_timestamp(F.to_timestamp(F.format_string("%04d-%02d-01", "year", "month")))
    start = month_start + F.pmod(F.xxhash64(seed, F.lit("window"), "sensors_id"),
                                 F.greatest(F.lit(1), 28 - days)) * DAY
    t = F.unix_timestamp("datetime_ts")
    in_window = (t >= start) & (t < start + days * DAY)
    sensor_hit = F.when(in_window & _eligible(F.col("_sf"), sensor_faults), F.col("_sf"))
    row_hit = F.when(F.col("_sf").isNull() & _eligible(F.col("_rf"), row_faults), F.col("_rf"))
    return df.withColumn("_fault", F.coalesce(sensor_hit, row_hit)).drop("_us", "_ur", "_sf", "_rf")


def _wall_clock():
    """The local date-time part of the original string, without its offset."""
    return F.regexp_extract("datetime", r"^(.*?)(Z|[+-]\d{2}:-?\d{2})?$", 1)


def _suffix():
    return F.regexp_extract("datetime", r"(Z|[+-]\d{2}:-?\d{2})$", 1)


def _setters():
    """{column: {fault: new value}}, every expression on the unchanged input row."""
    v, off = F.col("value_num"), F.col("_off")
    num = {"sentinel": F.lit(-999.0), "out_of_range": F.lit(987654.0),
           "spike": v * 10 + F.when(F.col("_p") == "pm10", 1200).otherwise(600),
           "bad_number": F.lit(None).cast("double"), "stuck": F.col("_stuck_num"),
           "unit_x1000": v * 1000, "unit_div1000": v / 1000, "calibration_bias": v * 1.6,
           "dup_conflict": v * 1.5}
    text = {name: e.cast("string") for name, e in num.items()}
    text.update(bad_number=F.regexp_replace("value", r"\.", ","), stuck=F.col("_stuck_str"))
    seconds = F.unix_timestamp("datetime_ts")
    later = seconds + 730 * DAY
    wall = F.date_format(F.timestamp_seconds(later + off * 60), "yyyy-MM-dd'T'HH:mm:ss")
    return {
        "value_num": num,
        "value": text,
        # local wall time read as UTC: the instant moves by the offset, the string keeps the wall clock
        "datetime_ts": {"future_ts": F.timestamp_seconds(later), "tz_shift": F.timestamp_seconds(seconds + off * 60)},
        "datetime": {"future_ts": F.concat(wall, _suffix()),
                     "tz_shift": F.concat(_wall_clock(), F.lit("+00:00"))},
        "lat": {"coord_swap": F.col("lon"), "null_island": F.lit("0")},
        "lon": {"coord_swap": F.col("lat"), "null_island": F.lit("0")},
        "lat_num": {"coord_swap": F.col("lon_num"), "null_island": F.lit(0.0)},
        "lon_num": {"coord_swap": F.col("lat_num"), "null_island": F.lit(0.0)},
        "sensors_id": {"missing_key": F.lit("")},
        "parameter": {"unknown_parameter": F.lit("pm2.5"), "shifted_row": F.col("value")},
        "units": {"unit_alias": F.lit("ug/m3")},
    }


def apply_faults(df):
    """Mutate rows by `_fault`, add duplicate copies, tag source_block; drop helper columns.

    Input needs the Near-Silver columns plus `_fault`, `_p` and `_off` (select_faults). The
    original of a duplicated row stays clean; its copy carries the fault.
    """
    columns = [c for c in df.columns if not c.startswith("_")]
    fault = F.col("_fault")
    # read three times below (stuck aggregate, rows, duplicate copies): compute the selection once,
    # which also stops Spark from inlining the whole selection expression into each filter
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    # stuck: the sensor's first value of its window, from a small aggregate (no full-table window)
    stuck = (df.filter(fault == "stuck").groupBy("sensors_id")
               .agg(F.min_by("value_num", "datetime_ts").alias("_stuck_num"),
                    F.min_by("value", "datetime_ts").alias("_stuck_str")))
    df = df.join(F.broadcast(stuck), "sensors_id", "left")
    is_dup = fault.isin("duplicate", "dup_conflict")
    df = df.withColumn("_fault", F.when(is_dup, F.lit(None)).otherwise(fault)).unionByName(
        df.filter(is_dup))

    setters = _setters()
    out = []
    for c in columns:
        expr = F.col(c)
        for name, new in setters.get(c, {}).items():
            expr = F.when(F.col("_fault") == name, new).otherwise(expr)
        out.append(expr.alias(c))
    tag = F.concat(F.lit(TAG), F.col("_fault"), F.lit("-"), F.coalesce("source_block", F.lit("none")))
    df = df.select(*out, "_fault")
    return df.withColumn("source_block", F.coalesce(tag, F.col("source_block"))).select(*columns)


def inject(df, seed, faults=FAULTS, scale=1.0):
    """Near-Silver in, Near-Silver with faults out (same columns)."""
    return apply_faults(select_faults(df, seed, faults, scale))


def fault_label(column="source_block"):
    """Fault name from a tagged source_block, or 'clean'."""
    name = F.regexp_extract(F.coalesce(F.col(column), F.lit("")), r"^chaos-([a-z0-9_]+)-", 1)
    return F.when(name == "", F.lit("clean")).otherwise(name)
