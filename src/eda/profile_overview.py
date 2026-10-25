import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit-files", type=int, default=None)
    args = parser.parse_args()

    source = Path(args.input).expanduser().absolute()
    output = Path(args.output).expanduser().absolute()

    files = sorted(source.glob("*.parquet"))
    if args.limit_files is not None:
        if args.limit_files < 1:
            parser.error("--limit-files must be positive")
        files = files[:args.limit_files]
    if not files:
        raise SystemExit(f"No Parquet files found: {source}")

    # A separate output directory prevents accidental report replacement.
    output.mkdir(parents=True, exist_ok=False)

    spark = (
        SparkSession.builder
        .appName("OpenAQ-Near-Silver-EDA")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(*[p.as_uri() for p in files])

        if isinstance(df.schema["datetime_ts"].dataType, T.LongType):
            df = df.withColumn(
                "datetime_ts",
                F.expr(
                    "timestamp_micros("
                    "CAST(FLOOR(datetime_ts / 1000) AS BIGINT))"
                ),
            )

        (output / "schema.json").write_text(
            df.schema.json(), encoding="utf-8"
        )
        (output / "run.txt").write_text(
            f"input={source}\n"
            f"files={len(files)}\n"
            f"limit_files={args.limit_files}\n"
            "timezone=UTC\n",
            encoding="utf-8",
        )

        def count_if(condition, name):
            return F.count(F.when(condition, 1)).alias(name)

        def nonblank(name):
            return F.col(name).isNotNull() & (
                F.length(F.trim(F.col(name))) > 0
            )

        def finite(name):
            col = F.col(name)
            return (
                col.isNotNull()
                & ~F.isnan(col)
                & (F.abs(col) != F.lit(float("inf")))
            )

        def save(frame, name):
            print(f"Writing {name}...", flush=True)
            (
                frame.coalesce(1).write
                .mode("errorifexists")
                .option("header", True)
                .csv((output / name).as_uri())
            )

        # One aggregation for overall missingness and quality flags.
        metrics = [F.count("*").alias("rows")]
        for field in df.schema.fields:
            col = F.col(field.name)
            metrics.append(
                count_if(col.isNull(), f"{field.name}__null")
            )
            if isinstance(field.dataType, T.StringType):
                metrics.append(count_if(
                    col.isNotNull() & (F.length(F.trim(col)) == 0),
                    f"{field.name}__blank",
                ))

        for raw, typed in [
            ("value", "value_num"),
            ("lat", "lat_num"),
            ("lon", "lon_num"),
            ("datetime", "datetime_ts"),
        ]:
            metrics.append(count_if(
                nonblank(raw) & F.col(typed).isNull(),
                f"{typed}__cast_failed",
            ))

        for name in ["value_num", "lat_num", "lon_num"]:
            metrics.extend([
                count_if(F.isnan(name), f"{name}__nan"),
                count_if(
                    F.abs(F.col(name)) == F.lit(float("inf")),
                    f"{name}__infinite",
                ),
            ])

        metrics.extend([
            count_if(
                finite("value_num") & (F.col("value_num") < 0),
                "negative_values",
            ),
            count_if(
                finite("lat_num")
                & ~F.col("lat_num").between(-90, 90),
                "latitude_out_of_range",
            ),
            count_if(
                finite("lon_num")
                & ~F.col("lon_num").between(-180, 180),
                "longitude_out_of_range",
            ),
            F.min("datetime_ts").alias("first_timestamp_utc"),
            F.max("datetime_ts").alias("last_timestamp_utc"),
        ])
        save(df.agg(*metrics), "overview")

        # Preserve original parameter/unit spelling to expose inconsistencies.
        value = F.when(finite("value_num"), F.col("value_num"))
        grouped = df.groupBy("parameter", "units").agg(
            F.count("*").alias("rows"),
            F.count(value).alias("finite_values"),
            count_if(F.col("value_num").isNull(), "null_values"),
            count_if(
                finite("value_num") & (F.col("value_num") < 0),
                "negative_values",
            ),
            F.min(value).alias("min_value"),
            F.max(value).alias("max_value"),
            F.avg(value).alias("mean_value"),
            F.stddev_samp(value).alias("stddev_value"),
            F.percentile_approx(
                value, [0.01, 0.25, 0.5, 0.75, 0.99], 1000
            ).alias("quantiles"),
        )

        # CSV does not support array columns.
        grouped = grouped.select(
            "*",
            *[
                F.col("quantiles")[i].alias(name)
                for i, name in enumerate(["p01", "p25", "p50", "p75", "p99"])
            ],
        ).drop("quantiles")
        save(grouped, "parameter_units")

        monthly = (
            df.groupBy(
                F.date_format("datetime_ts", "yyyy-MM").alias("month_utc")
            )
            .agg(F.count("*").alias("rows"))
        )
        save(monthly, "monthly_coverage")

        print(f"EDA complete: {output}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()