"""Gold pass 1: station_hourly, one row per (location_id, parameter, hour_utc), per month.

Sensor-hour means of the usable Silver rows, then location-hour (twin channels counted
once), joined with dim_location. Staging + move; counts reconcile with Silver.
"""

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import shutil
import sys
import time

import pyarrow.parquet as pq
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.gold.silver_union import (DEFAULT_GOLD_RULES, LAKE, ROOT, load_gold_rules, month_range,
                                   union_silver, uri)

REASONS = ["valid", "suspect_allowed", "suspect_excluded", "invalid"]
MAX_ROWS_OUTPUT_FILE = 4_000_000     # ~100 MB of station_hourly per file
MIN_SHUFFLE_PARTITIONS = 64
ROWS_PER_SHUFFLE_PARTITION = 1_000_000


def plan(df):
    return df.sparkSession._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")


def sensor_hour(union):
    usable = F.col("use_reason").isin("valid", "suspect_allowed")
    v = F.when(usable, F.col("value"))
    return (union.groupBy("location_id", "sensors_id", "parameter", "unit",
                          F.date_trunc("hour", "datetime_utc").alias("hour_utc"))
            .agg(F.count("*").alias("n_rows"),
                 *[F.count(F.when(F.col("use_reason") == r, 1)).alias(f"n_{r}") for r in REASONS],
                 F.count(v).alias("n_usable"),
                 F.avg(v).alias("mean"), F.min(v).alias("min"), F.max(v).alias("max"),
                 F.max(F.when(usable, F.col("utc_offset_min"))).alias("utc_offset_min"),
                 F.array_sort(F.collect_set(F.when(usable, F.col("origin")))).alias("origins")))


def station_hourly(sensor_hours, rules, dim, vn, run_id):
    hr = rules["hour"]
    kept = sensor_hours.filter(F.col("n_usable") >= hr["min_usable_readings"])
    rounded = F.round("mean", hr["twin_round_decimals"])
    station = (kept.groupBy("location_id", "parameter", "unit", "hour_utc")
               .agg(F.count("*").alias("sensor_count"),
                    F.array_distinct(F.collect_list(rounded)).alias("_values"),
                    F.min("min").alias("value_min"), F.max("max").alias("value_max"),
                    F.sum("n_usable").alias("n_readings"),
                    F.max("utc_offset_min").alias("utc_offset_min"),
                    F.array_sort(F.array_distinct(F.flatten(F.collect_list("origins")))).alias("_origins")))
    station = (station
               .withColumn("value_count", F.size("_values"))
               .withColumn("value_mean", F.aggregate("_values", F.lit(0.0), lambda a, x: a + x)
                           / F.col("value_count"))
               .withColumn("origin", F.array_join("_origins", "+"))
               .drop("_values", "_origins"))
    station = (station.join(F.broadcast(dim), "location_id", "left")
               .join(F.broadcast(vn), "location_id", "left")
               .withColumn("is_vn", F.coalesce("is_vn", F.lit(False))))
    return station.select(
        "location_id", "parameter", "unit", "hour_utc", "value_mean", "value_min", "value_max",
        "sensor_count", "value_count", "n_readings", "origin", "utc_offset_min", "is_vn",
        "location_name", "lat", "lon",
        F.lit(rules["version"]).alias("gold_version"), F.lit(run_id).alias("build_run_id"))


def move_month(staged, target):
    target.mkdir(parents=True, exist_ok=True)
    for old in target.glob("*.parquet"):
        old.unlink()
    for f in sorted(staged.glob("*.parquet")):
        f.rename(target / f.name)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver", default=str(LAKE / "silver" / "measurements"))
    p.add_argument("--stream", default=str(LAKE / "stream_copy"), help="'none' to skip")
    p.add_argument("--output", default=str(LAKE / "gold"))
    p.add_argument("--rules", default=str(DEFAULT_GOLD_RULES))
    p.add_argument("--run-id", default="gold_v1_01")
    p.add_argument("--months", nargs="+", required=True, help="YYYY-MM")
    p.add_argument("--dry-run", action="store_true", help="counts only, no Gold files")
    args = p.parse_args()

    rules = load_gold_rules(args.rules)
    silver = Path(args.silver).expanduser().absolute()
    stream = None if args.stream == "none" else Path(args.stream).expanduser().absolute()
    out_root = Path(args.output).expanduser().absolute() / "station_hourly"
    report = LAKE / "reports" / args.run_id
    staging = LAKE / "_staging" / "gold" / args.run_id
    for d in [report / "done", report / "plans", staging]:
        d.mkdir(parents=True, exist_ok=True)
    meta = {"status": "running", "run_id": args.run_id, "gold_version": rules["version"],
            "dry_run": args.dry_run, "silver": str(silver), "stream": str(stream),
            "months": args.months, "started_utc": datetime.now(timezone.utc).isoformat()}
    (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    spark = (SparkSession.builder.appName("OpenAQ-Gold-Build")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.parquet.compression.codec", "zstd")
             # 64 MB row groups: 8 writers x 128 MB ran a 3 GB driver out of heap
             .config("spark.hadoop.parquet.block.size", str(64 * 1024 * 1024))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        dim = (spark.read.parquet(uri(LAKE / "silver" / "dim_location"))
               .select(F.col("location_id_num").alias("location_id"), "location_name",
                       F.col("lat_ref").alias("lat"), F.col("lon_ref").alias("lon")))
        vn = (spark.read.parquet(uri(ROOT / rules["vn_locations"]))
              .select(F.col("station_id").cast("long").alias("location_id")).distinct()
              .withColumn("is_vn", F.lit(True)))
        log = (report / "job_log.csv").open("a", encoding="utf-8")
        for key in args.months:
            done = report / "done" / key
            if done.exists():
                continue
            t0 = time.time()
            start, end = month_range(key)
            union = union_silver(spark, silver, stream, start, end, rules)
            if union is None:
                print(f"{key}: no input", flush=True)
                continue
            # footer row count sizes the shuffle
            month_dir = silver / f"year={start.year}" / f"month={start.month}"
            silver_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in month_dir.glob("*.parquet")) \
                if month_dir.is_dir() else 0
            spark.conf.set("spark.sql.shuffle.partitions",
                           str(max(MIN_SHUFFLE_PARTITIONS, math.ceil(silver_rows / ROWS_PER_SHUFFLE_PARTITION))))

            # DISK_ONLY leaves the heap to the Parquet writers
            hours = sensor_hour(union).persist(StorageLevel.DISK_ONLY)
            totals = hours.agg(F.sum("n_rows").alias("rows"),
                               *[F.sum(f"n_{r}").alias(r) for r in REASONS],
                               F.count("*").alias("sensor_hours"),
                               F.count(F.when(F.col("n_usable") >= rules["hour"]["min_usable_readings"], 1))
                               .alias("sensor_hours_kept"),
                               F.sum(F.when(F.col("n_usable") >= rules["hour"]["min_usable_readings"],
                                            F.col("n_usable"))).alias("usable_rows_kept")).first().asDict()
            totals = {k: int(v or 0) for k, v in totals.items()}
            t_hours = time.time() - t0
            if totals["rows"] != sum(totals[r] for r in REASONS):
                raise RuntimeError(f"{key}: use_reason counts do not add up: {totals}")

            station = station_hourly(hours, rules, dim, vn, args.run_id)
            if not (report / "plans" / "station_hourly.txt").exists():
                (report / "plans" / "station_hourly.txt").write_text(plan(station), encoding="utf-8")
            parts = max(1, math.ceil(totals["sensor_hours_kept"] / MAX_ROWS_OUTPUT_FILE))
            station = station.repartition(parts, "location_id").sortWithinPartitions(
                "location_id", "parameter", "hour_utc")
            target = staging / key
            if target.exists():
                shutil.rmtree(target)
            if args.dry_run:
                stats = station.agg(F.count("*").alias("station_hours"), F.sum("n_readings").alias("n_readings"),
                                    F.sum(F.col("is_vn").cast("int")).alias("vn_station_hours")).first()
            else:
                station.write.parquet(uri(target))
                stats = (spark.read.parquet(uri(target))
                         .agg(F.count("*").alias("station_hours"), F.sum("n_readings").alias("n_readings"),
                              F.sum(F.col("is_vn").cast("int")).alias("vn_station_hours")).first())
            hours.unpersist()
            result = dict(totals, station_hours=int(stats["station_hours"]),
                          n_readings=int(stats["n_readings"] or 0),
                          vn_station_hours=int(stats["vn_station_hours"] or 0),
                          silver_rows_in_month_files=silver_rows,
                          seconds_sensor_hour=round(t_hours, 1))
            if result["n_readings"] != totals["usable_rows_kept"]:
                raise RuntimeError(f"{key}: n_readings {result['n_readings']} != usable rows {totals['usable_rows_kept']}")
            if not args.dry_run:
                move_month(target, out_root / f"year={start.year}" / f"month={start.month}")
                shutil.rmtree(target)
            result["seconds"] = round(time.time() - t0, 1)
            log.write(f"{key},{json.dumps(result)}\n")
            log.flush()
            print(f"{key} {json.dumps(result)}", flush=True)
            done.write_text(json.dumps(result, indent=2), encoding="utf-8")
        log.close()
        meta.update(status="complete")
    except Exception as exc:
        meta.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        meta["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
