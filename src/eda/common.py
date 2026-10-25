"""Shared Spark expressions and local Parquet input handling."""

from pathlib import Path

from pyspark.sql import functions as F, types as T

RAW_COLUMNS = [
    "location_id", "sensors_id", "location", "datetime", "lat", "lon",
    "parameter", "units", "value",
]
TYPED_COLUMNS = ["value_num", "lat_num", "lon_num", "datetime_ts"]


def nonblank(name):
    return F.col(name).isNotNull() & (F.length(F.trim(F.col(name))) > 0)


def finite(name):
    col = F.col(name)
    return col.isNotNull() & ~F.isnan(col) & (F.abs(col) != float("inf"))


def count_if(condition, name):
    return F.count(F.when(condition, 1)).alias(name)


def input_files(source, limit=None):
    root = Path(source).expanduser().absolute()
    files = sorted(root.rglob("*.parquet"))
    if limit is not None:
        files = files[:limit]
    if not files:
        raise ValueError(f"No Parquet files found: {root}")
    broken = [str(p) for p in files if not p.is_file()]
    if broken:
        raise ValueError(f"Unreadable Parquet paths (including broken links): {broken[:5]}")
    return root, files


def read_near_silver(spark, files):
    # Explicit file list supports both flat exports and year/month directories.
    df = spark.read.parquet(*[p.as_uri() for p in files])
    required = RAW_COLUMNS + TYPED_COLUMNS
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"Missing Near-Silver columns: {missing}")
    if isinstance(df.schema["datetime_ts"].dataType, T.LongType):
        df = df.withColumn(
            "datetime_ts",
            F.expr("timestamp_micros(CAST(FLOOR(datetime_ts / 1000) AS BIGINT))"),
        )
    # Capture before any shuffle/cache; this is NOT the original CSV path.
    return df.select(*required, F.input_file_name().alias("source_block"))


def read_compact(spark, root, include_null_ts=True):
    """Read block recovery output: year=/month= partitions plus the _null_ts folder.

    Rows keep their provenance in `source_block`; `_null_ts` rows get
    year/month = null, like datetime_ts.
    """
    root = Path(root).expanduser().absolute()
    columns = RAW_COLUMNS + TYPED_COLUMNS + ["source_block", "year", "month"]
    # Path.as_uri() escapes "=", which Spark does not decode for partitions.
    df = spark.read.parquet("file://" + str(root)).select(*columns)
    null_ts = root / "_null_ts"
    if include_null_ts and null_ts.is_dir():
        df = df.unionByName(
            spark.read.parquet("file://" + str(null_ts))
            .withColumn("year", F.lit(None).cast("int"))
            .withColumn("month", F.lit(None).cast("int"))
            .select(*columns)
        )
    return df


def parquet_row_count(root):
    """Row count from Parquet footers only (no Spark scan)."""
    import pyarrow.parquet as pq
    root = Path(root).expanduser().absolute()
    return sum(pq.ParquetFile(p).metadata.num_rows for p in root.rglob("*.parquet"))


def save_csv(frame, output, name):
    print(f"Writing {name}...", flush=True)
    # escape='"' doubles embedded quotes (RFC 4180) so Python's csv module reads
    # values such as '-244365"' from split records correctly.
    frame.coalesce(1).write.mode("errorifexists").option("header", True).option("escape", '"').csv(
        (output / name).as_uri()
    )
