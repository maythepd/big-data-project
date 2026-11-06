"""R14: flag spike and flatline rows on the built Silver, one month at a time.

Each month is read with context_hours of its neighbours, flagged (rules.py), then
rewritten in place via staging with dq_flags/dq_status recomputed. Values never change.
    Input : silver/measurements, config/silver_rules_v1.yaml (soft_outliers)
    Output: silver/measurements (in place), reports/<run>/
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import shutil
import sys

import pyarrow.parquet as pq
from pyspark.sql import SparkSession, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.common import save_csv
from src.silver.build_silver import MAX_ROWS_COMPUTE_TASK, MIN_COMPUTE_PARTITIONS
from src.silver.rules import (DEFAULT_RULES, SOFT_FLAGS, flatline_kind, load_rules, spike_kind,
                              with_temporal_flags)

LAKE = Path.home() / "openaq_lake"
VN_LOCATIONS = Path(__file__).resolve().parents[2] / "data" / "dim" / "openaq_locations.parquet"
R14 = ["spike", "flatline"]


def uri(path):
    return "file://" + str(path)


def rows_in(files):
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def month_dir(silver, y, m):
    return silver / f"year={y}" / f"month={m}"


def context_frame(spark, silver, y, m, hours, vn):
    """Non-invalid rows of the month (in_month) plus `hours` of the months around it."""
    # tz-aware: naive literals are read in the machine's zone
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    end = datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc)
    dirs = [d for d in (month_dir(silver, t.year, t.month) for t in [start - timedelta(days=1), start, end])
            if d.exists()]
    other_soft = [f for f in SOFT_FLAGS if f not in R14]
    df = (spark.read.option("basePath", uri(silver)).parquet(*[uri(d) for d in dirs])
               .filter((F.col("dq_status") != "invalid") & F.col("value").isNotNull()
                       & (F.col("datetime_utc") >= F.lit(start - timedelta(hours=hours)))
                       & (F.col("datetime_utc") < F.lit(end + timedelta(hours=hours))))
               .select("sensors_id", "location_id", "parameter", "datetime_utc", "value",
                       (~F.exists("dq_flags", lambda x: x.isin(*other_soft))).alias("was_valid")))
    df = df.withColumn("in_month", (F.col("datetime_utc") >= F.lit(start)) & (F.col("datetime_utc") < F.lit(end)))
    return df.join(F.broadcast(vn), "location_id", "left").fillna(False, ["vn"])


def rewrite(spark, files, flags, parts, target, version):
    """The month's files with the R14 flags joined in, same columns and layout."""
    df = spark.read.parquet(*[uri(f) for f in files])
    columns = df.columns
    compute = max(parts, MIN_COMPUTE_PARTITIONS, math.ceil(rows_in(files) / MAX_ROWS_COMPUTE_TASK))
    keys = flags.select("sensors_id", "datetime_utc",
                        F.col("flatline_kind").isNotNull().alias("flatline"),
                        F.col("spike_kind").isNotNull().alias("spike"))
    out = (df.repartition(compute, "sensors_id")
             .join(keys.repartition(compute, "sensors_id"), ["sensors_id", "datetime_utc"], "left"))
    out = with_temporal_flags(out).withColumn("rules_version", F.lit(version)).select(*columns)
    if compute != parts:
        out = out.repartition(parts, "sensors_id")
    out.sortWithinPartitions("sensors_id", "datetime_utc").write.parquet(uri(target))
    return sorted(target.glob("*.parquet"))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver", default=str(LAKE / "silver" / "measurements"))
    p.add_argument("--rules", default=str(DEFAULT_RULES))
    p.add_argument("--run-id", default="temporal_flags_01")
    p.add_argument("--months", nargs="*", help="YYYY-MM (default: every month of Silver)")
    p.add_argument("--dry-run", action="store_true", help="flags and counts only, Silver untouched")
    # ~220 MB of execution memory per task
    p.add_argument("--rows-per-task", type=int, default=500_000)
    args = p.parse_args()
    silver = Path(args.silver).expanduser().absolute()
    rules = load_rules(args.rules)
    hours = int(rules["soft_outliers"]["context_hours"])
    report = LAKE / "reports" / args.run_id
    parts_dir = report / "_parts"
    staging = LAKE / "_staging" / "temporal_flags" / args.run_id
    parts_dir.mkdir(parents=True, exist_ok=True)
    months = sorted((int(y.name[5:]), int(m.name[6:])) for y in silver.glob("year=*") for m in y.glob("month=*"))
    keys = [f"{y:04d}-{m:02d}" for y, m in months]
    if args.months:
        keys = [k for k in keys if k in args.months]
    meta = {"status": "running", "args": vars(args), "rules_version": rules["version"],
            "soft_outliers": {k: rules["soft_outliers"][k] for k in ["context_hours", "flatline", "spike"]},
            "started_utc": datetime.now(timezone.utc).isoformat(), "months": {}}
    if (report / "run.json").exists():
        meta["months"] = json.loads((report / "run.json").read_text())["months"]
    (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    spark = (SparkSession.builder.appName("OpenAQ-Silver-TemporalFlags")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.codegen.hugeMethodLimit", "8000")
             .config("spark.sql.files.maxPartitionBytes", str(256 << 20))
             .config("spark.sql.parquet.compression.codec", "zstd")
             # keep the row-count partitions (AQE would merge them)
             .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
             # both join sides are partitioned by sensors_id: no second shuffle
             .config("spark.sql.requireAllClusterKeysForCoPartition", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    vn = (spark.read.parquet(uri(VN_LOCATIONS)).select(F.col("station_id").cast("long").alias("location_id"))
          .distinct().withColumn("vn", F.lit(True)))
    try:
        for key in keys:
            if (report / "done" / key).exists():
                continue
            y, m = int(key[:4]), int(key[5:])
            mdir = month_dir(silver, y, m)
            stage = staging / key
            if (stage / "old").exists():
                raise RuntimeError(f"{key}: {stage}/old holds the month's previous files from an interrupted "
                                   "move; put them back in place before rerunning")
            t0 = datetime.now(timezone.utc)
            files = sorted(mdir.glob("*.parquet"))
            rows = rows_in(files)
            n = max(40, math.ceil(rows * (1 + 2 * hours / 720) / args.rows_per_task))
            spark.conf.set("spark.sql.shuffle.partitions", str(n))

            # 1. flags
            df = context_frame(spark, silver, y, m, hours, vn).repartition(n, "sensors_id")
            flagged = (spike_kind(flatline_kind(df, rules), rules)
                       .filter(F.col("in_month") & (F.col("flatline_kind").isNotNull() | F.col("spike_kind").isNotNull()))
                       .drop("in_month").withColumn("ym", F.lit(key)))
            flagged.write.mode("overwrite").parquet(uri(parts_dir / "flags" / f"ym={key}"))
            flags = spark.read.parquet(uri(parts_dir / "flags" / f"ym={key}"))
            checked = (context_frame(spark, silver, y, m, 0, vn).filter("in_month")
                       .groupBy("parameter", "vn").agg(F.count("*").alias("rows_checked")))
            by_kind = (flags.groupBy("parameter", "vn", "flatline_kind", "spike_kind", "was_valid")
                            .agg(F.count("*").alias("rows")))
            (checked.join(by_kind, ["parameter", "vn"], "left").withColumn("ym", F.lit(key))
                    .write.mode("overwrite").parquet(uri(parts_dir / "flag_counts" / f"ym={key}")))
            (flags.filter("vn").groupBy("ym", "location_id", "sensors_id", "parameter", "flatline_kind", "spike_kind")
                  .agg(F.count("*").alias("rows"), F.min("datetime_utc").alias("first"),
                       F.max("datetime_utc").alias("last"), F.min("value").alias("min_value"),
                       F.max("value").alias("max_value"))
                  .write.mode("overwrite").parquet(uri(parts_dir / "vn_sensors" / f"ym={key}")))
            n_flagged = flags.count()

            # 2. rewrite
            if not args.dry_run:
                if stage.exists():
                    shutil.rmtree(stage)
                main_files = [f for f in files if not f.name.startswith("nullts-")]
                null_files = [f for f in files if f.name.startswith("nullts-")]
                new = rewrite(spark, main_files, flags, len(main_files), stage / "m", rules["version"]) if main_files else []
                new_null = rewrite(spark, null_files, flags, 1, stage / "n", rules["version"]) if null_files else []
                if rows_in(new) + rows_in(new_null) != rows:
                    raise RuntimeError(f"{key}: rewrite changed the row count")
                (stage / "old").mkdir()
                for f in files:
                    f.rename(stage / "old" / f.name)
                for f in new:
                    f.rename(mdir / f.name)
                for f in new_null:
                    f.rename(mdir / ("nullts-" + f.name))
                shutil.rmtree(stage)

            (report / "done").mkdir(exist_ok=True)
            (report / "done" / key).touch()
            meta["months"][key] = {"rows": rows, "flagged": n_flagged, "partitions": n,
                                   "seconds": round((datetime.now(timezone.utc) - t0).total_seconds(), 1)}
            (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
            print(f"{key} rows={rows} flagged={n_flagged} partitions={n} {meta['months'][key]['seconds']}s", flush=True)

        counts = spark.read.parquet(uri(parts_dir / "flag_counts"))
        save_csv(counts.orderBy("ym", "parameter", "vn", "flatline_kind", "spike_kind", "was_valid"), report, "flag_counts")
        r = F.coalesce(F.col("rows"), F.lit(0))
        summary = (counts.groupBy("ym", "parameter", "vn")
                         .agg(F.first("rows_checked").alias("rows_checked"),
                              F.sum(F.when(F.col("flatline_kind").isNotNull(), r).otherwise(0)).alias("flatline"),
                              F.sum(F.when(F.col("spike_kind").isNotNull(), r).otherwise(0)).alias("spike"),
                              F.sum(r).alias("flagged"),
                              F.sum(F.when(F.col("was_valid"), r).otherwise(0)).alias("valid_to_suspect"))
                         .withColumn("pct_flagged", F.round(100 * F.col("flagged") / F.col("rows_checked"), 3)))
        save_csv(summary.orderBy("ym", "parameter", "vn"), report, "parameter_summary")
        save_csv(spark.read.parquet(uri(parts_dir / "vn_sensors")).orderBy("ym", F.desc("rows")), report, "vn_sensors")
        meta.update(status="complete", finished_utc=datetime.now(timezone.utc).isoformat())
    except Exception as exc:
        meta.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
