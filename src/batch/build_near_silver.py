import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = Path.home() / "openaq_near_silver_staging"
DEFAULT_OUTPUT = ROOT / "data" / "near_silver"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=str(DEFAULT_INPUT))
    p.add_argument("--output", default=str(DEFAULT_OUTPUT))

    # Kept only for compatibility with the old command.
    # This version intentionally does NOT call repartition(),
    # so this value is not used.
    p.add_argument("--output-partitions", type=int, default=None)

    p.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    return p.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("OpenAQ-Near-Silver")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.maxRecordsPerFile", "2000000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = T.StructType([
        T.StructField("location_id", T.StringType(), True),
        T.StructField("sensors_id", T.StringType(), True),
        T.StructField("location", T.StringType(), True),
        T.StructField("datetime", T.StringType(), True),
        T.StructField("lat", T.StringType(), True),
        T.StructField("lon", T.StringType(), True),
        T.StructField("parameter", T.StringType(), True),
        T.StructField("units", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
    ])

    input_glob = str(Path(args.input).expanduser() / "*.csv.gz")

    df = (
        spark.read
        .option("header", True)
        .option("enforceSchema", True)
        .schema(schema)
        .csv(input_glob)
    )

    # Compaction concatenates original gzip members for speed, so every
    # original source header is still present inside the merged files.
    # All known schemas start with "location_id", making this filter exact.
    df = df.filter(
        F.col("location_id").isNotNull()
        & (F.col("location_id") != "location_id")
    )

    # Near-Silver only: preserve raw strings and add typed shadow columns.
    df = (
        df
        .withColumn("value_num", F.col("value").cast("double"))
        .withColumn("lat_num", F.col("lat").cast("double"))
        .withColumn("lon_num", F.col("lon").cast("double"))
        .withColumn("datetime_ts", F.to_timestamp("datetime"))
        .withColumn("year", F.year("datetime_ts"))
        .withColumn("month", F.month("datetime_ts"))
    )

    # No explicit repartition here.
    # partitionBy() still creates year=YYYY/month=M directories,
    # but avoids the full-dataset shuffle caused by repartition(year, month).
    (
        df.write
        .mode(args.mode)
        .option("compression", "snappy")
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    print("\nNear-Silver complete")
    print(f"Input: {args.input}")
    print(f"Output: {args.output}")
    print("Explicit repartition: disabled")
    print("Output partitioning: year/month")

    spark.stop()


if __name__ == "__main__":
    main()
