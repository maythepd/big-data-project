"""Silver: one cleaned table of air-quality readings for Vietnam, from the OpenAQ
historical archive (Near-Silver) and the live stream archive written by
src/streaming/consumer.py.

- keeps Vietnam only: archive rows from fixed OpenAQ locations in Vietnam
  (data/dim/openaq_locations.parquet, written by openaq.fetch_dimensions)
- keeps the parameters in schema.UNITS and converts ppm/ppb to µg/m³, like the producers
- drops timestamps in the future and values outside VALID_RANGE
- one row per station, parameter and timestamp: the archive replaces stream readings
  with the same timestamp, since it is OpenAQ's final record, and co-located sensors
  measuring the same parameter are averaged
- writes Parquet partitioned by year/month (UTC) and sorted by station and time, so a
  dashboard can load one month at a time

Only the months it writes are replaced, so a daily run can rebuild just the recent ones:
    spark-submit src/batch/build_silver.py --since 2026-09
"""
import argparse
from datetime import datetime

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, functions as F

from src.common.config import DIM_DIR
from src.common.schema import MOLAR_VOLUME, MOLECULAR_WEIGHT, NAME_MAP, UNITS

HDFS = "hdfs://namenode:8020"
DEFAULT_NEAR_SILVER = f"{HDFS}/openaq/near_silver"
DEFAULT_STREAM = f"{HDFS}/openaq/stream/readings"
DEFAULT_OUTPUT = f"{HDFS}/openaq/silver/readings"
LOCATIONS = DIM_DIR / "openaq_locations.parquet"

# First-pass plausibility limits, set before profiling the data: values outside them
# are sensor faults or fill values such as -999. One entry per parameter in
# schema.UNITS, in its stored unit. Tighten them after EDA.
VALID_RANGE = {
    "pm1": (0, 2000), "pm25": (0, 2000), "pm10": (0, 3000),
    "no": (0, 2000), "no2": (0, 2000), "o3": (0, 2000), "so2": (0, 2000), "nh3": (0, 2000),
    "co": (0, 50000),                    # µg/m³, about 44 ppm
    "temperature": (-30, 60), "relativehumidity": (0, 100),
    # WAQI's US AQI sub-indices go past 500 during extreme events
    **{p: (0, 999) for p, unit in UNITS.items() if unit == "us_aqi"},
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--near-silver", default=DEFAULT_NEAR_SILVER)
    p.add_argument("--stream", default=DEFAULT_STREAM)
    p.add_argument("--output", default=DEFAULT_OUTPUT)
    p.add_argument("--since", type=lambda s: datetime.strptime(s, "%Y-%m"),
                   help="rebuild only the months from YYYY-MM on (UTC); default: all months")
    return p.parse_args()


def literal_map(mapping):
    """Python dict -> Spark map column, so lookups stay inside the query."""
    return F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])


def read_history(spark, path, since):
    """Near-Silver rows from fixed OpenAQ locations in Vietnam, in the shared columns."""
    if not LOCATIONS.is_file():
        raise SystemExit(f"{LOCATIONS} is missing: run python -m src.ingestion.openaq.fetch_dimensions")
    vietnam = [row[0] for row in spark.read.parquet(str(LOCATIONS))
               .select(F.col("station_id").cast("string")).collect()]

    df = spark.read.parquet(path)
    if since:
        # Near-Silver months follow the time zone of the machine that built it, so
        # read one month earlier too; rows are cut on UTC time in main()
        year, month = (since.year, since.month - 1) if since.month > 1 else (since.year - 1, 12)
        df = df.where(F.col("year") * 100 + F.col("month") >= year * 100 + month)
    return (df.where(F.col("location_id").isin(vietnam))
            .select(F.lit("openaq").alias("source"),
                    F.concat(F.lit("openaq-"), "location_id").alias("station_id"),
                    F.col("lat_num").alias("lat"), F.col("lon_num").alias("lon"),
                    "parameter", F.col("units").alias("unit"), F.col("value_num").alias("value"),
                    F.col("datetime_ts").alias("event_time"),
                    F.lit("archive").alias("origin")))


def read_stream(spark, path, since):
    """Readings archived by the streaming consumer: Vietnam only, deduplicated, in µg/m³."""
    try:
        df = spark.read.parquet(path)
    except AnalysisException as error:
        if error.getErrorClass() not in ("PATH_NOT_FOUND", "UNABLE_TO_INFER_SCHEMA"):
            raise
        print(f"No stream archive at {path} yet; using history only")
        return None
    if since:
        df = df.where(F.col("date") >= F.lit(f"{since:%Y-%m}-01").cast("date"))
    return df.select("source", "station_id", "lat", "lon", "parameter", "unit", "value",
                     "event_time", F.lit("stream").alias("origin"))


def tag(readings):
    """Standardise parameter names and units like schema.make_message, and add
    drop_reason: null for rows Silver keeps, otherwise why the row is dropped."""
    parameter = F.coalesce(literal_map(NAME_MAP)[F.col("parameter")], F.col("parameter"))
    df = (readings
          .withColumn("parameter", parameter)
          # the micro sign used in schema.UNITS, not the Greek letter mu
          .withColumn("unit", F.regexp_replace(F.lower(F.trim("unit")), "μ", "µ"))
          .withColumn("target_unit", literal_map(UNITS)[F.col("parameter")]))

    weight = literal_map(MOLECULAR_WEIGHT)[F.col("parameter")]
    df = df.withColumn("value_std",
                       F.when(F.col("unit") == F.col("target_unit"), F.col("value"))
                       .when(F.col("unit") == "ppm", F.col("value") * weight * 1000 / MOLAR_VOLUME)
                       .when(F.col("unit") == "ppb", F.col("value") * weight / MOLAR_VOLUME))

    low = literal_map({p: low for p, (low, _) in VALID_RANGE.items()})[F.col("parameter")]
    high = literal_map({p: high for p, (_, high) in VALID_RANGE.items()})[F.col("parameter")]
    reason = (F.when(F.col("event_time").isNull() | (F.col("event_time") > F.current_timestamp()),
                     "bad timestamp")
              .when(F.col("target_unit").isNull(), "parameter not kept")
              .when(F.col("value").isNull(), "no value")
              .when(F.col("value_std").isNull(), "unit not convertible")
              .when(~F.col("value_std").between(low, high), "out of range"))   # NaN included
    return df.withColumn("drop_reason", reason)


def combine(kept):
    """One row per station, parameter and timestamp. The archive is OpenAQ's final
    record, so it replaces stream readings with the same timestamp; co-located
    sensors measuring the same parameter are averaged."""
    archive = F.col("origin") == "archive"
    return (kept.groupBy("station_id", "parameter", "event_time")
            .agg(F.first("source").alias("source"),
                 F.first("lat", ignorenulls=True).alias("lat"),
                 F.first("lon", ignorenulls=True).alias("lon"),
                 F.first("target_unit").alias("unit"),
                 F.round(F.coalesce(F.avg(F.when(archive, F.col("value_std"))),
                                    F.avg("value_std")), 3).alias("value"),
                 F.when(F.max(archive.cast("int")) == 1, "archive").otherwise("stream").alias("origin"))
            .select("source", "station_id", "lat", "lon", "parameter", "value", "unit",
                    "event_time", "origin"))


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("OpenAQ-Silver")
        .config("spark.sql.session.timeZone", "UTC")
        # replace only the year/month partitions this run writes
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    readings = read_history(spark, args.near_silver, args.since)
    stream = read_stream(spark, args.stream, args.since)
    if stream is not None:
        readings = readings.unionByName(stream)
    if args.since:
        readings = readings.where(F.col("event_time") >= F.lit(f"{args.since:%Y-%m}-01").cast("timestamp"))

    tagged = tag(readings).cache()
    print("\nRows kept (drop_reason null) and dropped, by origin:")
    tagged.groupBy("origin", "drop_reason").count().orderBy("origin", "drop_reason").show(truncate=False)
    print("Parameter/unit pairs dropped most often (a needed one here means"
          " schema.UNITS or the unit clean-up in tag() is missing it):")
    (tagged.where(F.col("drop_reason").isin("parameter not kept", "unit not convertible"))
     .groupBy("parameter", "unit").count().orderBy(F.desc("count")).show(20, truncate=False))

    silver = (combine(tagged.where(F.col("drop_reason").isNull()))
              .withColumn("year", F.year("event_time"))
              .withColumn("month", F.month("event_time")))

    # one file per month, sorted so a station's time series is read in order
    (
        silver.repartition("year", "month")
        .sortWithinPartitions("station_id", "parameter", "event_time")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    print("\nSilver complete")
    print(f"Near-Silver: {args.near_silver}")
    print(f"Stream archive: {args.stream}")
    print(f"Output: {args.output}")
    print(f"Months rebuilt: {f'from {args.since:%Y-%m}' if args.since else 'all'}")

    spark.stop()


if __name__ == "__main__":
    main()
