"""Chaos run, step 1: inject the fault catalogue (faults.py) into one month of Near-Silver.

    Input : openaq_lake/near_silver_compact/year=/month=
    Output: openaq_lake/chaos/<run>/near_silver_compact/year=/month=   same layout, faulty rows tagged
            openaq_lake/chaos/<run>/truth/sensor_windows/             sensor faults: sensor, first/last reading
            openaq_lake/reports/<run>_inject/run.json                 seed, scale, rows per fault
Next: build Silver on the output, run R14, evaluate (scripts/chaos_batch.sh runs all four).
--scale 0 writes an untouched copy: the control run every chaos run is compared with.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys

from pyspark.sql import SparkSession, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.chaos.faults import BY_NAME, FAULTS, fault_label, inject
from src.common.config import LAKE
from src.eda.common import read_compact


def uri(path):
    return "file://" + str(path)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-id", required=True)
    p.add_argument("--month", required=True, help="YYYY-MM")
    p.add_argument("--input", default=str(LAKE / "near_silver_compact"))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--scale", type=float, default=1.0, help="multiplies every fault rate; 0 = control run")
    p.add_argument("--faults", nargs="*", choices=sorted(BY_NAME), help="subset of the catalogue (default: all)")
    p.add_argument("--bbox", help="lat_min,lon_min,lat_max,lon_max: only rows inside (quick runs)")
    args = p.parse_args()

    year, month = map(int, args.month.split("-"))
    root = LAKE / "chaos" / args.run_id
    target, staging = root / "near_silver_compact", root / "_staging"
    report = LAKE / "reports" / f"{args.run_id}_inject"
    if target.exists():
        raise SystemExit(f"{target} exists: pick a new --run-id or delete the folder")
    shutil.rmtree(staging, ignore_errors=True)
    report.mkdir(parents=True, exist_ok=True)
    faults = [BY_NAME[n] for n in args.faults] if args.faults else FAULTS
    meta = {"status": "running", "run_id": args.run_id, "month": args.month, "seed": args.seed,
            "scale": args.scale, "bbox": args.bbox, "input": args.input, "faults": [f.name for f in faults],
            "started_utc": datetime.now(timezone.utc).isoformat()}
    (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    spark = (SparkSession.builder.appName("OpenAQ-Chaos-Inject")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.parquet.compression.codec", "zstd")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = (read_compact(spark, args.input, include_null_ts=False)
              .filter((F.col("year") == year) & (F.col("month") == month)))
        if args.bbox:
            lat_min, lon_min, lat_max, lon_max = map(float, args.bbox.split(","))
            df = df.filter(F.col("lat_num").between(lat_min, lat_max) & F.col("lon_num").between(lon_min, lon_max))
        rows_in = df.count()
        # a month is a few large files (2-4 read tasks): spread it before the per-row expressions
        df = df.repartition(64, "sensors_id")
        out = inject(df, args.seed, faults, args.scale) if args.scale > 0 else df
        (out.repartition(8, "sensors_id").sortWithinPartitions("location_id", "sensors_id", "datetime_ts")
            .write.partitionBy("year", "month").parquet(uri(staging)))

        written = spark.read.parquet(uri(staging)).withColumn("fault", fault_label())
        counts = {r["fault"]: r["count"] for r in written.groupBy("fault").count().collect()}
        sensor_faults = [f.name for f in faults if f.level == "sensor"]
        (written.filter(F.col("fault").isin(*sensor_faults))
            .groupBy("fault", "location_id", "sensors_id", "parameter")
            .agg(F.min("datetime_ts").alias("first_ts"), F.max("datetime_ts").alias("last_ts"),
                 F.count("*").alias("rows"))
            .coalesce(1).write.mode("overwrite").parquet(uri(root / "truth" / "sensor_windows")))
        staging.rename(target)
        meta.update(status="complete", rows_in=rows_in, rows_out=sum(counts.values()),
                    rows_per_fault=dict(sorted(counts.items())))
        print(json.dumps(meta["rows_per_fault"], indent=2), flush=True)
    except Exception as exc:
        meta.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        meta["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
