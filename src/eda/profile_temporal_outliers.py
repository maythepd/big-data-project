"""Profile frozen values and jumps in deduped Silver to set the R14 thresholds.

Outputs per parameter (and Vietnam): flat_runs, flat_runs_top, reading_jumps, hour_jumps.
    Input : silver/measurements    Output: reports/<run>/ (resumable per month)
"""

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sys

import pyarrow.parquet as pq
from pyspark.sql import SparkSession, Window, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.common import count_if, save_csv

LAKE = Path.home() / "openaq_lake"
VN_LOCATIONS = Path(__file__).resolve().parents[2] / "data" / "dim" / "openaq_locations.parquet"
DURATION_EDGES_H = [1, 3, 6, 12, 24, 48, 168]
RATIO_EDGES = [2, 3, 5, 10]
QUANTILES = {0.5: "p50", 0.99: "p99", 0.999: "p999"}


def uri(path):
    return "file://" + str(path)


def bucket(col, edges, unit):
    """Label a value by the edges it reaches: '<1h', '1-3h', ..., '>=168h'."""
    expr = F.lit(f">={edges[-1]}{unit}")
    for lo, hi in reversed(list(zip([None] + edges[:-1], edges))):
        label = f"<{hi}{unit}" if lo is None else f"{lo}-{hi}{unit}"
        expr = F.when(col < hi, F.lit(label)).otherwise(expr)
    return expr


def month_rows(silver, y, m):
    files = list((silver / f"year={y}" / f"month={m}").glob("*.parquet"))
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def month_frame(spark, silver, y, m, lookback_h, vn):
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    py, pm = (y, m - 1) if m > 1 else (y - 1, 12)
    cols = ["sensors_id", "location_id", "parameter", "datetime_utc", "value"]
    s = spark.read.parquet(uri(silver))
    cur = s.filter((F.col("year") == y) & (F.col("month") == m))
    prev = s.filter((F.col("year") == py) & (F.col("month") == pm)
                    & (F.col("datetime_utc") >= F.lit(start.replace(tzinfo=None)) - F.expr(f"INTERVAL {lookback_h} HOURS")))
    df = cur.select(*cols).unionByName(prev.select(*cols)).filter(F.col("value").isNotNull())
    df = df.withColumn("in_month", F.col("datetime_utc") >= F.lit(start.replace(tzinfo=None)))
    return df.join(F.broadcast(vn), "location_id", "left").fillna(False, ["vn"])


def flat_runs(df):
    w = Window.partitionBy("sensors_id").orderBy("datetime_utc")
    new = F.coalesce(F.col("value") != F.lag("value").over(w), F.lit(True))
    runs = (df.withColumn("run", F.sum(new.cast("int")).over(w))
              .groupBy("sensors_id", "run")
              .agg(F.first("parameter").alias("parameter"), F.first("location_id").alias("location_id"),
                   F.first("vn").alias("vn"), F.first("value").alias("value"),
                   F.count("*").alias("readings"), F.sum(F.col("in_month").cast("int")).alias("rows_in_month"),
                   F.min("datetime_utc").alias("start"), F.max("datetime_utc").alias("end"),
                   F.min(F.col("in_month").cast("int")).alias("starts_in_month")))
    runs = runs.filter(F.col("rows_in_month") > 0).withColumn(
        "hours", (F.col("end").cast("long") - F.col("start").cast("long")) / 3600.0)
    return runs.withColumn("zero", F.col("value") == 0).withColumn("duration", bucket(F.col("hours"), DURATION_EDGES_H, "h"))


def jumps(df):
    """reading vs its sensor-hour median; sensor-hour median vs the 3 hours on each side."""
    df = df.filter(F.col("in_month")).withColumn("hour", F.date_trunc("hour", "datetime_utc"))
    hourly = (df.groupBy("sensors_id", "parameter", "vn", "hour")
                .agg(F.median("value").alias("hour_median"), F.count("*").alias("n")))
    reading = (df.join(hourly.select("sensors_id", "hour", "hour_median", "n"), ["sensors_id", "hour"])
                 .filter((F.col("n") >= 3) & (F.col("hour_median") > 0))
                 .withColumn("ratio", F.col("value") / F.col("hour_median")))
    t = Window.partitionBy("sensors_id").orderBy(F.col("hour").cast("long"))
    before, after = t.rangeBetween(-3 * 3600, -1), t.rangeBetween(1, 3 * 3600)
    hour = (hourly.withColumn("nb", F.array_sort(F.concat(F.collect_list("hour_median").over(before),
                                                          F.collect_list("hour_median").over(after))))
                  .withColumn("k", F.size("nb"))
                  .filter(F.col("k") >= 3)
                  .withColumn("local", F.expr(
                      "CASE WHEN k % 2 = 1 THEN element_at(nb, CAST(k DIV 2 + 1 AS INT)) "
                      "ELSE (element_at(nb, CAST(k DIV 2 AS INT)) + element_at(nb, CAST(k DIV 2 + 1 AS INT))) / 2 END"))
                  .filter(F.col("local") > 0)
                  .withColumn("ratio", F.col("hour_median") / F.col("local"))
                  .withColumn("diff", F.col("hour_median") - F.col("local")))
    return reading, hour


def ratio_summary(frame, keys):
    return frame.groupBy(*keys).agg(
        F.count("*").alias("n"),
        *[F.expr(f"percentile_approx(ratio, {q}, 1000)").alias(f"ratio_{name}") for q, name in QUANTILES.items()],
        *[count_if((F.col("ratio") >= r) | (F.col("ratio") <= 1.0 / r), f"ratio_ge_{r}x") for r in RATIO_EDGES])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver", default=str(LAKE / "silver" / "measurements"))
    p.add_argument("--run-id", default="temporal_outliers_01")
    p.add_argument("--months", nargs="+", default=["2019-01", "2023-10", "2024-11", "2026-01"])
    p.add_argument("--lookback-h", type=int, default=48)
    # smaller tasks than build_silver: the sensor-window sort needs ~220 MB each
    p.add_argument("--rows-per-task", type=int, default=500_000)
    args = p.parse_args()
    silver = Path(args.silver).expanduser().absolute()
    report = LAKE / "reports" / args.run_id
    parts = report / "_parts"
    parts.mkdir(parents=True, exist_ok=True)
    meta = {"status": "running", "args": vars(args), "started_utc": datetime.now(timezone.utc).isoformat(), "months": {}}
    (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    spark = (SparkSession.builder.appName("OpenAQ-EDA-TemporalOutliers")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.codegen.hugeMethodLimit", "8000")
             .config("spark.sql.files.maxPartitionBytes", str(256 << 20))
             # keep the row-count partitions (AQE would merge them and run out of memory)
             .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    vn = (spark.read.parquet(uri(VN_LOCATIONS)).select(F.col("station_id").cast("long").alias("location_id"))
          .distinct().withColumn("vn", F.lit(True)))
    try:
        for key in args.months:
            y, m = (int(x) for x in key.split("-"))
            if (parts / "_done" / key).exists():
                continue
            t0 = datetime.now(timezone.utc)
            rows = month_rows(silver, y, m)
            n = max(40, math.ceil(rows / args.rows_per_task))
            spark.conf.set("spark.sql.shuffle.partitions", str(n))
            df = month_frame(spark, silver, y, m, args.lookback_h, vn).repartition(n, "sensors_id")

            # written to disk once: too many runs to cache
            flat_runs(df).withColumn("ym", F.lit(key)).write.mode("overwrite").parquet(uri(parts / "_runs" / f"ym={key}"))
            runs = spark.read.parquet(uri(parts / "_runs" / f"ym={key}"))
            (runs.groupBy("ym", "parameter", "vn", "zero", "duration")
                 .agg(F.count("*").alias("runs"), F.sum("rows_in_month").alias("rows_in_month"),
                      count_if(F.col("starts_in_month") == 0, "crosses_month_start"),
                      F.sum(F.when(F.col("starts_in_month") == 0, F.col("rows_in_month"))).alias("rows_crossing"))
                 .write.mode("overwrite").parquet(uri(parts / "flat_runs" / f"ym={key}")))
            (runs.filter(F.col("hours") >= 24).orderBy(F.desc("hours")).limit(200)
                 .drop("run", "duration").write.mode("overwrite").parquet(uri(parts / "flat_runs_top" / f"ym={key}")))

            reading, hour = jumps(df)
            ratio_summary(reading.withColumn("ym", F.lit(key)), ["ym", "parameter", "vn"]) \
                .write.mode("overwrite").parquet(uri(parts / "reading_jumps" / f"ym={key}"))
            ratio_summary(hour.withColumn("ym", F.lit(key)), ["ym", "parameter", "vn"]).join(
                hour.groupBy("parameter", "vn").agg(
                    *[F.expr(f"percentile_approx(abs(diff), {q}, 1000)").alias(f"abs_diff_{name}")
                      for q, name in QUANTILES.items()]), ["parameter", "vn"]) \
                .write.mode("overwrite").parquet(uri(parts / "hour_jumps" / f"ym={key}"))

            (parts / "_done").mkdir(exist_ok=True)
            (parts / "_done" / key).touch()
            meta["months"][key] = {"rows": rows, "partitions": n,
                                   "seconds": round((datetime.now(timezone.utc) - t0).total_seconds(), 1)}
            (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
            print(f"{key} rows={rows} partitions={n} {meta['months'][key]['seconds']}s", flush=True)

        for name, order in [("flat_runs", ["ym", "parameter", "vn", "zero", "duration"]),
                            ("flat_runs_top", ["ym", F.desc("hours")]),
                            ("reading_jumps", ["ym", "parameter", "vn"]),
                            ("hour_jumps", ["ym", "parameter", "vn"])]:
            if not (report / name / "_SUCCESS").exists():
                save_csv(spark.read.parquet(uri(parts / name)).orderBy(*order), report, name)
        meta.update(status="complete", finished_utc=datetime.now(timezone.utc).isoformat())
        (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
