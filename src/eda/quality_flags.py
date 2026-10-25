"""structure checks diagnostic flags. Flags overlap and never modify measurements."""

from pyspark.sql import functions as F

from .common import finite, nonblank

NUMERIC_PATTERN = r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"
BAD_OFFSET_PATTERN = r"[+-]\d{2}:-\d{2}$"


def repaired_datetime():
    """Diagnostic repair of "+HH:-MM" offsets; Silver decides whether to apply it."""
    fixed = F.regexp_replace(F.col("datetime"), r"([+-]\d{2}):-(\d{2})$", "$1:$2")
    return F.to_timestamp(fixed)


def quality_flags():
    flags = {}
    for raw, typed in [
        ("datetime", "datetime_ts"), ("value", "value_num"),
        ("lat", "lat_num"), ("lon", "lon_num"),
    ]:
        flags[f"{raw}_missing"] = ~nonblank(raw)
        flags[f"{raw}_cast_failed"] = nonblank(raw) & F.col(typed).isNull()
    for name in ["location_id", "sensors_id", "parameter", "units"]:
        flags[f"{name}_missing"] = ~nonblank(name)
    for name in ["value_num", "lat_num", "lon_num"]:
        flags[f"{name}_nonfinite"] = F.col(name).isNotNull() & ~finite(name)
    lat, lon = F.col("lat_num"), F.col("lon_num")
    flags["latitude_out_of_range"] = finite("lat_num") & ~lat.between(-90, 90)
    flags["longitude_out_of_range"] = finite("lon_num") & ~lon.between(-180, 180)
    flags["swap_candidate"] = (
        finite("lat_num") & finite("lon_num")
        & (~lat.between(-90, 90) | ~lon.between(-180, 180))
        & lon.between(-90, 90) & lat.between(-180, 180)
    )
    # structure checks findings on near_silver_compact (see BLOCK_RECOVERY_NOTES.md §1.3-1.4):
    # "-02:-30" style offsets (Newfoundland) and coordinate sentinels.
    flags["datetime_bad_offset"] = F.col("datetime").rlike(BAD_OFFSET_PATTERN)
    flags["datetime_offset_repairable"] = (
        flags["datetime_bad_offset"] & repaired_datetime().isNotNull()
    )
    # Head of a CSV record split by a newline inside the quoted location name:
    # ids and the first part of the name only, everything after is empty.
    flags["split_record_head"] = (
        nonblank("location_id") & ~nonblank("datetime") & ~nonblank("parameter")
        & ~nonblank("value")
    )
    both = finite("lat_num") & finite("lon_num")
    flags["coord_sentinel_200"] = both & (lat == 200) & (lon == 200)
    flags["coord_zero"] = both & (lat == 0) & (lon == 0)
    flags["coord_lat_eq_lon"] = both & (lat == lon) & (lat != 200) & (lat != 0)
    flags["numeric_parameter"] = F.trim(F.col("parameter")).rlike(NUMERIC_PATTERN)
    # Heuristic only: units containing a familiar parameter token.
    flags["parameter_token_in_units"] = F.lower(F.trim(F.col("units"))).isin(
        "pm25", "pm10", "pm1", "pm4", "no2", "no", "nox", "o3", "so2", "co", "co2", "bc"
    )
    flags["suspected_column_shift"] = (
        flags["numeric_parameter"] | flags["parameter_token_in_units"]
    )
    return {name: F.coalesce(expr, F.lit(False)) for name, expr in flags.items()}


def with_quality_flags(df):
    flags = quality_flags()
    df = df.select("*", *[expr.alias(name) for name, expr in flags.items()])
    return df.withColumn(
        "issues", F.filter(
            F.array(*[F.when(F.col(name), F.lit(name)) for name in flags]),
            lambda item: item.isNotNull(),
        ),
    )


def datetime_pattern():
    """Lexical classification; not a date repair or validity claim."""
    value = F.trim(F.col("datetime"))
    return (
        F.when(~nonblank("datetime"), "missing")
        .when(value.rlike(r"^\d{4}-\d{2}-\d{2}[T ]"), "iso_like")
        .when(value.rlike(r"^\d{4}-\d{2}-\d{2}$"), "date_only")
        .when(value.rlike(r"^[+-]?\d+(\.\d+)?$"), "numeric")
        .when(value.rlike(r"^\d{1,2}[/\-]\d{1,2}[/\-]\d{4}"), "day_or_month_first")
        .otherwise("other")
    )
