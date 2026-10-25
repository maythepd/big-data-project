"""Build Silver from near_silver_compact with the rules of src/silver/rules.py.

Per month (idempotent, staging + move): apply R01-R12 (R06b from
silver/dim_sensor_unit, build_sensor_units.py) and R14, one shuffle by
sensors_id, dedup R13, write silver/measurements/year=/month= (~128 MB files,
zstd, sorted by sensor/time) and silver/quarantine/. The `_null_ts` rows get
their time from the offset repair, so they land in many months: they are
written last as `nullts-*.parquet` files inside those month folders.

Reconciliation per job: rows_in = silver + quarantine + dedup_removed.
--dry-run computes the same counts (rule finalization impact) without writing data.
"""

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import shutil
import sys

import pyarrow.parquet as pq
from pyspark.sql import SparkSession, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.common import read_compact
from src.silver.rules import DEFAULT_RULES, apply_rules, dedup, finalize, load_rules

LAKE = Path.home() / "openaq_lake"
MB = 1 << 20
TARGET_FILE = 128 * MB
# Dedup/rules run on at least this many tasks; output is then reshuffled to the
# ~128 MB file count. With 1 partition for a 53 MB month the dedup window ran on
# a single core (2017-03: 228 s for 12.6M rows).
MIN_COMPUTE_PARTITIONS = 40
# Rows per task cap, for the spike months (2023-10: 305M rows in 11 output tasks
# ran out of heap merging sort spills).
MAX_ROWS_COMPUTE_TASK = 2_000_000
MAX_ROWS_OUTPUT_FILE = 10_000_000
COLUMNS = [
    "location_id", "sensors_id", "location_name", "datetime_utc", "datetime_local_raw",
    "utc_offset_min", "lat", "lon", "coord_source", "parameter", "unit", "value",
    "parameter_raw", "units_raw", "value_raw", "dq_flags", "dq_status", "dup_count",
    "source_block", "rules_version", "build_run_id",
]


def uri(path):
    return "file://" + str(path)


def offset_minutes():
    m = F.regexp_extract("datetime", r"([+-])(\d{2}):-?(\d{2})$", 0)
    sign = F.when(F.substring(m, 1, 1) == "-", -1).otherwise(1)
    hh = F.regexp_extract("datetime", r"([+-])(\d{2}):-?(\d{2})$", 2).cast("int")
    mm = F.regexp_extract("datetime", r"([+-])(\d{2}):-?(\d{2})$", 3).cast("int")
    zulu = F.col("datetime").endswith("Z")
    return F.when(zulu, 0).when(m != "", sign * (hh * 60 + mm))


def silver_frame(df, rules, dim, build_utc, run_id, parts, compute, sensor_units, soft):
    measured, quarantine = apply_rules(df, rules, dim, build_utc, sensor_units, soft)
    measured = measured.repartition(compute, "sensors_id")
    deduped, keep = dedup(measured)
    out = finalize(deduped.filter(keep))
    if compute != parts:
        out = out.repartition(parts, "sensors_id")
    out = out.select(
        F.col("location_id").cast("long").alias("location_id"),
        F.col("sensors_id").cast("long").alias("sensors_id"),
        F.col("location").alias("location_name"), "datetime_utc",
        F.col("datetime").alias("datetime_local_raw"), offset_minutes().alias("utc_offset_min"),
        "lat", "lon", "coord_source",
        F.col("parameter_std").alias("parameter"), F.col("unit_std").alias("unit"), "value",
        F.col("parameter").alias("parameter_raw"), F.col("units").alias("units_raw"),
        "value_raw",
        "dq_flags", "dq_status", "dup_count", "source_block",
        F.lit(rules["version"]).alias("rules_version"), F.lit(run_id).alias("build_run_id"),
        F.year("datetime_utc").alias("year"), F.month("datetime_utc").alias("month"),
    ).sortWithinPartitions("sensors_id", "datetime_utc")
    return out, quarantine


def rows_in_files(folder, prefix=None):
    files = [p for p in Path(folder).rglob("*.parquet") if prefix is None or p.name.startswith(prefix)]
    return sum(pq.ParquetFile(p).metadata.num_rows for p in files), len(files)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", default=str(LAKE / "near_silver_compact"))
    p.add_argument("--output", default=str(LAKE / "silver"))
    p.add_argument("--rules", default=str(DEFAULT_RULES))
    p.add_argument("--run-id", default="silver_v1_01")
    p.add_argument("--months", nargs="*", help="YYYY-MM and/or null_ts (pilot)")
    p.add_argument("--sensor-units", default=str(LAKE / "silver" / "dim_sensor_unit"),
                   help="R06b per-sensor unit decisions (build_sensor_units.py); 'none' to skip")
    p.add_argument("--soft-outliers", action="store_true", help="compute R14 spike/flatline")
    p.add_argument("--dry-run", action="store_true", help="counts only (rule finalization impact)")
    args = p.parse_args()

    source = Path(args.input).expanduser().absolute()
    out_root = Path(args.output).expanduser().absolute()
    measurements, quarantine_root = out_root / "measurements", out_root / "quarantine"
    staging = LAKE / "_staging" / "silver" / args.run_id
    report = LAKE / "reports" / args.run_id
    for d in [report, staging]:
        d.mkdir(parents=True, exist_ok=True)
    rules = load_rules(args.rules)
    build_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = {"status": "running", "run_id": args.run_id, "rules_version": rules["version"],
            "dry_run": args.dry_run, "sensor_units": args.sensor_units, "soft_outliers": args.soft_outliers, "build_utc": build_utc,
            "input": str(source), "output": str(out_root), "months": args.months}
    (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    months = sorted((int(y.name[5:]), int(m.name[6:])) for y in source.glob("year=*") for m in y.glob("month=*"))
    keys = [f"{y:04d}-{m:02d}" for y, m in months] + ["null_ts"]
    if args.months:
        keys = [k for k in keys if k in args.months]

    spark = (SparkSession.builder.appName("OpenAQ-Silver-Build")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.codegen.hugeMethodLimit", "8000")
             .config("spark.sql.files.maxPartitionBytes", str(128 * MB))
             .config("spark.sql.files.openCostInBytes", str(1 * MB))
             .config("spark.sql.parquet.compression.codec", "zstd")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        dim = spark.read.parquet(uri(LAKE / "silver" / "dim_location"))
        sensor_units = None
        if args.sensor_units != "none":
            sensor_units = spark.read.parquet(uri(Path(args.sensor_units).expanduser().absolute()))
        compact = read_compact(spark, source)
        log = (report / "job_log.csv").open("a", encoding="utf-8")
        for key in keys:
            done = report / "done" / key
            if done.exists():
                continue
            if key == "null_ts":
                part, src_dir = compact.filter(F.col("year").isNull()), source / "_null_ts"
            else:
                y, m = int(key[:4]), int(key[5:])
                part = compact.filter((F.col("year") == y) & (F.col("month") == m))
                src_dir = source / f"year={y}" / f"month={m}"
            rows_in, _ = rows_in_files(src_dir)
            src_bytes = sum(f.stat().st_size for f in src_dir.glob("*.parquet"))
            parts = max(1, math.ceil(src_bytes * 1.3 / TARGET_FILE), math.ceil(rows_in / MAX_ROWS_OUTPUT_FILE))
            compute = max(parts, MIN_COMPUTE_PARTITIONS, math.ceil(rows_in / MAX_ROWS_COMPUTE_TASK))
            start = datetime.now(timezone.utc)
            out, quarantine = silver_frame(part, rules, dim, build_utc, args.run_id, parts, compute,
                                           sensor_units, args.soft_outliers)
            target = staging / key.replace("-", "_")
            if target.exists():
                shutil.rmtree(target)
            if args.dry_run:
                stats = out.groupBy("dq_status").count().collect()
                silver_rows = sum(r["count"] for r in stats)
                q_rows = quarantine.count()
                status = {r["dq_status"]: r["count"] for r in stats}
            else:
                out.write.partitionBy("year", "month").parquet(uri(target / "m"))
                quarantine.withColumn("job", F.lit(key)).write.parquet(uri(target / "q"))
                silver_rows, _ = rows_in_files(target / "m")
                q_rows, _ = rows_in_files(target / "q")
                status = None
                # Move into place.
                prefix = "nullts-" if key == "null_ts" else None
                for ydir in sorted((target / "m").glob("year=*")):
                    for mdir in sorted(ydir.glob("month=*")):
                        dst = measurements / ydir.name / mdir.name
                        dst.mkdir(parents=True, exist_ok=True)
                        if prefix:
                            for old in dst.glob(prefix + "*"):
                                old.unlink()
                        else:
                            for old in dst.glob("*.parquet"):
                                if not old.name.startswith("nullts-"):
                                    old.unlink()
                        for f in mdir.glob("*.parquet"):
                            f.rename(dst / ((prefix or "") + f.name))
                qdst = quarantine_root / f"job={key}"
                if qdst.exists():
                    shutil.rmtree(qdst)
                qdst.parent.mkdir(parents=True, exist_ok=True)
                (target / "q").rename(qdst)
                shutil.rmtree(target)
            removed = rows_in - silver_rows - q_rows
            seconds = (datetime.now(timezone.utc) - start).total_seconds()
            log.write(f"{key},{rows_in},{silver_rows},{q_rows},{removed},{seconds:.1f},{json.dumps(status) if status else ''}\n")
            log.flush()
            print(f"{key} in={rows_in} silver={silver_rows} quarantine={q_rows} dedup_removed={removed} {seconds:.0f}s", flush=True)
            if removed < 0:
                raise RuntimeError(f"{key}: more output rows than input rows")
            done.parent.mkdir(parents=True, exist_ok=True)
            done.write_text(json.dumps({"rows_in": rows_in, "silver": silver_rows, "quarantine": q_rows,
                                        "dedup_removed": removed, "status": status}), encoding="utf-8")
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
