"""block recovery.2: compact used DataNode blocks into near_silver_compact/year=/month=.

Technical layout change only: values are not modified. Adds `source_block`
provenance, sorts by location/sensor/time and writes zstd Parquet.
Each job (one large month, or a batch of small months) is written to staging
and moved into place when complete, so a rerun skips finished months.
A final reconciliation compares per-block row counts with the manifest.
"""

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import shutil

import pyarrow.parquet as pq
from pyspark.sql import SparkSession, functions as F

DEFAULT_LAKE = Path.home() / "openaq_lake"
SORT_KEYS = ["location_id", "sensors_id", "datetime_ts"]
MB = 1 << 20
SMALL_SPLIT = 160 * MB


def uri(path):
    # Path.as_uri() escapes "=" in year=/month= dirs, which Spark does not decode.
    return "file://" + str(path)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--lake", default=str(DEFAULT_LAKE))
    p.add_argument("--run-id", default="compact_blocks_01")
    p.add_argument("--large-month-mb", type=int, default=512,
                   help="months above this size get their own job")
    p.add_argument("--batch-mb", type=int, default=3072,
                   help="input size of one small-month batch")
    p.add_argument("--only", nargs="*", default=None,
                   help="limit to YYYY-MM months (pilot); null_ts allowed")
    p.add_argument("--skip-reconcile", action="store_true")
    return p.parse_args()


def month_key(year, month):
    return "null_ts" if year is None or year != year else f"{int(year):04d}-{int(month):02d}"


def month_dir(root, key):
    year, month = key.split("-")
    return root / f"year={int(year)}" / f"month={int(month)}"


def plan_jobs(manifest, large_bytes, batch_bytes, only):
    months = {}
    for row in manifest:
        if row["exclude_reason"] is not None:
            continue
        key = month_key(row["year"], row["month"])
        entry = months.setdefault(key, {"files": [], "bytes": 0, "rows": 0})
        entry["files"].append(row["path"])
        entry["bytes"] += row["size"]
        entry["rows"] += row["num_rows"]
    if only:
        months = {k: v for k, v in months.items() if k in only}

    jobs, batch = [], []
    for key in sorted(k for k in months if k != "null_ts"):
        if months[key]["bytes"] > large_bytes:
            jobs.append({"kind": "large", "months": [key]})
            continue
        if batch and sum(months[k]["bytes"] for k in batch) + months[key]["bytes"] > batch_bytes:
            jobs.append({"kind": "small", "months": batch})
            batch = []
        batch.append(key)
    if batch:
        jobs.append({"kind": "small", "months": batch})
    if "null_ts" in months:
        jobs.append({"kind": "null_ts", "months": ["null_ts"]})
    for i, job in enumerate(jobs):
        job["id"] = f"job{i:03d}"
    return months, jobs


def read_blocks(spark, files):
    return (
        spark.read.parquet(*[uri(f) for f in files])
        .withColumn("source_block", F.regexp_extract(F.input_file_name(), r"(blk_\d+)", 1))
        .withColumn("year", F.year("datetime_ts"))
        .withColumn("month", F.month("datetime_ts"))
    )


def run_job(spark, job, months, compact, staging):
    files = [f for key in job["months"] for f in months[key]["files"]]
    target = staging / job["id"]
    if target.exists():
        shutil.rmtree(target)
    job_bytes = sum(months[key]["bytes"] for key in job["months"])

    if job["kind"] == "large":
        # Small blocks are packed into ~bytes/12 read splits: no shuffle,
        # one output file per read task.
        split = min(256 * MB, max(128 * MB, job_bytes // 12))
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(split))
        df = read_blocks(spark, files)
    else:
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(256 * MB))
        df = read_blocks(spark, files)
        if job["kind"] == "small":
            # Hash by (month, sensor salt): a month of ~bytes/SMALL_SPLIT parts,
            # so one task never sorts a whole 500 MB month (OOM in job005 of
            # compact_blocks_01). Sensors stay together inside one part.
            parts = {key: max(1, math.ceil(months[key]["bytes"] / SMALL_SPLIT)) for key in job["months"]}
            salt_mod = F.lit(1)
            for key, n in parts.items():
                salt_mod = F.when(
                    (F.col("year") == int(key[:4])) & (F.col("month") == int(key[5:])), F.lit(n)
                ).otherwise(salt_mod)
            df = (df.withColumn("_salt", F.pmod(F.xxhash64("sensors_id"), salt_mod))
                  .repartition(2 * sum(parts.values()), "year", "month", "_salt")
                  .drop("_salt"))
        else:
            df = df.coalesce(1)

    if job["kind"] == "null_ts":
        df.drop("year", "month").sortWithinPartitions(*SORT_KEYS).write.parquet(uri(target))
        final = compact / "_null_ts"
        if final.exists():
            shutil.rmtree(final)
        target.rename(final)
        return

    (df.sortWithinPartitions("year", "month", *SORT_KEYS)
       .write.partitionBy("year", "month").parquet(uri(target)))
    # Move each written month into place. A month planned for this job holds
    # all its blocks, so it replaces any earlier copy (e.g. from a pilot run);
    # only rows of an unplanned month (block spanning months) are merged.
    owned = {month_dir(compact, key) for key in job["months"]}
    for year_dir in sorted(target.glob("year=*")):
        for src in sorted(year_dir.glob("month=*")):
            dst = compact / year_dir.name / src.name
            dst.parent.mkdir(parents=True, exist_ok=True)
            if dst in owned and dst.exists():
                shutil.rmtree(dst)
            if not dst.exists():
                src.rename(dst)
            else:
                for part in src.glob("*.parquet"):
                    part.rename(dst / f"{job['id']}-{part.name}")
    shutil.rmtree(target)


def job_done(job, compact):
    if job["kind"] == "null_ts":
        return (compact / "_null_ts" / "_SUCCESS").exists()
    return all(month_dir(compact, key).exists() for key in job["months"])


def reconcile(spark, manifest, compact, report, keys):
    expected = {
        r["block_id"]: (month_key(r["year"], r["month"]), r["num_rows"])
        for r in manifest
        if r["exclude_reason"] is None and month_key(r["year"], r["month"]) in keys
    }
    # Reading the root discovers year/month; "_null_ts" is hidden from discovery.
    wanted = [(int(k[:4]), int(k[5:])) for k in keys if k != "null_ts"]
    in_keys = F.lit(False)
    for year, month in wanted:
        in_keys = in_keys | ((F.col("year") == year) & (F.col("month") == month))
    counts = (spark.read.parquet(uri(compact)).filter(in_keys)
              .groupBy("source_block", "year", "month").count())
    if "null_ts" in keys:
        counts = counts.unionByName(
            spark.read.parquet(uri(compact / "_null_ts"))
            .groupBy("source_block").count()
            .select("source_block", F.lit(None).cast("int").alias("year"),
                    F.lit(None).cast("int").alias("month"), "count"))
    actual = {}
    for row in counts.collect():
        actual.setdefault(row["source_block"], []).append(
            (month_key(row["year"], row["month"]), row["count"]))

    problems = []
    for block, (key, rows) in expected.items():
        got = actual.get(block, [])
        if got != [(key, rows)]:
            problems.append({"block_id": block, "expected": [key, rows], "actual": got})
    for block in set(actual) - set(expected):
        problems.append({"block_id": block, "expected": None, "actual": actual[block]})

    per_month = {}
    for block, pairs in actual.items():
        for key, rows in pairs:
            per_month[key] = per_month.get(key, 0) + rows
    with (report / "reconciliation_by_month.csv").open("w", encoding="utf-8") as out:
        out.write("year_month,rows,files,bytes\n")
        for key in sorted(per_month):
            folder = compact / "_null_ts" if key == "null_ts" else month_dir(compact, key)
            parts = list(folder.glob("*.parquet"))
            out.write(f"{key},{per_month[key]},{len(parts)},{sum(p.stat().st_size for p in parts)}\n")
    (report / "reconciliation_problems.json").write_text(json.dumps(problems, indent=2), encoding="utf-8")
    return {
        "expected_rows": sum(v[1] for v in expected.values()),
        "actual_rows": sum(per_month.values()),
        "expected_blocks": len(expected), "actual_blocks": len(actual),
        "problem_blocks": len(problems),
    }


def main():
    args = parse_args()
    lake = Path(args.lake).expanduser().absolute()
    compact = lake / "near_silver_compact"
    staging = lake / "_staging" / "near_silver_compact"
    report = lake / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    staging.mkdir(parents=True, exist_ok=True)
    compact.mkdir(parents=True, exist_ok=True)

    manifest = pq.read_table(lake / "manifest" / "block_manifest.parquet").to_pylist()
    months, jobs = plan_jobs(manifest, args.large_month_mb * MB, args.batch_mb * MB, args.only)
    (report / "jobs.json").write_text(json.dumps(jobs, indent=2), encoding="utf-8")

    spark = (
        SparkSession.builder
        .appName("OpenAQ-BlockRecovery-Compact")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.files.openCostInBytes", str(1 * MB))
        .config("spark.sql.shuffle.partitions", "64")
        # Keep the planned (month, salt) parts; AQE would merge them into bigger sorts.
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run = {"phase": "0.2", "status": "running", "only": args.only,
           "started_utc": datetime.now(timezone.utc).isoformat(), "jobs": len(jobs)}
    (report / "run.json").write_text(json.dumps(run, indent=2), encoding="utf-8")
    try:
        log = (report / "job_log.csv").open("a", encoding="utf-8")
        for job in jobs:
            if job_done(job, compact):
                print(f"{job['id']} skip (done) {job['months'][0]}..{job['months'][-1]}", flush=True)
                continue
            start = datetime.now(timezone.utc)
            run_job(spark, job, months, compact, staging)
            seconds = (datetime.now(timezone.utc) - start).total_seconds()
            rows = sum(months[k]["rows"] for k in job["months"])
            log.write(f"{job['id']},{job['kind']},{job['months'][0]},{job['months'][-1]},{rows},{seconds:.1f}\n")
            log.flush()
            print(f"{job['id']} {job['kind']} {job['months'][0]}..{job['months'][-1]} "
                  f"rows={rows} {seconds:.0f}s", flush=True)
        log.close()
        if not args.skip_reconcile:
            run["reconciliation"] = reconcile(spark, manifest, compact, report, set(months))
            run["status"] = "complete" if run["reconciliation"]["problem_blocks"] == 0 else "reconcile_failed"
        else:
            run["status"] = "complete_unreconciled"
    except BaseException as error:
        run["status"] = "failed"
        run["error"] = repr(error)
        raise
    finally:
        run["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (report / "run.json").write_text(json.dumps(run, indent=2), encoding="utf-8")
        print(json.dumps(run, indent=2))
        spark.stop()


if __name__ == "__main__":
    main()
