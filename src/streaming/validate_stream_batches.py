"""Validate committed streaming Silver batches without reading historical data.

Run with spark-submit; --output is a local JSON report path.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F
from src.silver.rules import HARD_FLAGS, SOFT_FLAGS, reduce_or
from src.streaming.silver_batch_writer import KEY, SilverStreamWriter


def validate(writer):
    batches = writer.batch_paths()
    if not batches:
        return {"ok": False, "problems": ["No committed batches"], "batches": 0}
    problems, totals = [], {k: 0 for k in ["rows_in", "silver", "quarantine", "dedup_removed"]}
    for batch in batches:
        r = writer.read_json(batch + "/report.json")
        disk = {"rows_in": writer.spark.read.parquet(batch + "/raw").count(),
                "silver": writer.spark.read.parquet(batch + "/measurements").count(),
                "quarantine": writer.spark.read.parquet(batch + "/quarantine").count()}
        if any(r[k] != n for k, n in disk.items()):
            problems.append(f"{batch}: disk counts differ from report")
        if r["rows_in"] != r["silver"] + r["quarantine"] + r["dedup_removed"] or r["dedup_removed"] < 0:
            problems.append(f"{batch}: reconciliation failed")
        for k in totals:
            totals[k] += r[k]
    df = writer.read_measurements()
    duplicate_keys = df.groupBy(*KEY).count().filter(F.col("count") > 1).count()
    hard = F.arrays_overlap("dq_flags", F.array(*[F.lit(x) for x in
        HARD_FLAGS + ["value_missing", "value_not_numeric", "value_nonfinite"]]))
    soft = F.arrays_overlap("dq_flags", F.array(*[F.lit(x) for x in SOFT_FLAGS]))
    expected_status = F.when(hard, "invalid").when(soft, "suspect").otherwise("valid")
    bad_value = (F.isnan("value") | (F.abs("value") == float("inf"))
                 | ((F.col("dq_status") == "invalid") & F.col("value").isNotNull())
                 | ((F.col("dq_status") != "invalid") & F.col("value").isNull()))
    bad_key = reduce_or([F.col(k).isNull() for k in KEY + ["location_id"]])
    bad_unit = F.lit(False)
    for parameter, unit in writer.rules["canonical_units"].items():
        bad_unit = bad_unit | ((F.col("parameter") == parameter)
            & (F.col("dq_status") == "valid") & ~F.col("unit").eqNullSafe(F.lit(unit)))
    bad_bound = F.lit(False)
    for parameter, b in writer.rules["bounds"].items():
        unit = writer.rules["canonical_units"][parameter]
        bad_bound = bad_bound | ((F.col("parameter") == parameter) & (F.col("unit") == unit)
            & F.col("value").isNotNull() & ~F.col("value").between(float(b["min"]), float(b["max"])))
    failures = df.filter(F.coalesce(bad_value, F.lit(False)) | bad_key | bad_unit
                         | bad_bound | ~F.col("dq_status").eqNullSafe(expected_status)).count()
    if duplicate_keys:
        problems.append(f"{duplicate_keys} duplicate sensor/time keys")
    if failures:
        problems.append(f"{failures} rows fail key/value/unit/bounds/status checks")
    return {"ok": not problems, "root": writer.root, "batches": len(batches), **totals,
            "duplicate_keys": duplicate_keys, "invalid_rows": failures,
            "status_totals": {r["dq_status"]: r["count"] for r in df.groupBy("dq_status").count().collect()},
            "problems": problems, "finished_utc": datetime.now(timezone.utc).isoformat()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    hdfs = os.getenv("HDFS_URI", "hdfs://namenode:8020").rstrip("/")
    parser.add_argument("--root", default=os.getenv("STREAM_SILVER_ROOT", f"{hdfs}/openaq/silver/stream/v1"))
    parser.add_argument("--output", default="data/reports/stream_silver_validation.json",
                        help="Local JSON path, or '-' for stdout (use host redirection in Docker)")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("Validate-Streaming-Silver")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        writer = SilverStreamWriter(spark, args.root,
            dim_path=os.getenv("STREAM_DIM_LOCATION") or None,
            sensor_units_path=os.getenv("STREAM_SENSOR_UNITS") or None)
        result = validate(writer)
        print(json.dumps(result, indent=2))
        if args.output != "-":
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        return 0 if result["ok"] else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
