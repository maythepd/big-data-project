"""Validate gold/station_hourly per month against an independent recount of Silver.

Checks keys, units, time range, min <= mean <= max and sum(n_readings) = usable Silver rows.
Output: reports/<run-id>/checks.json.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from pyspark.sql import SparkSession, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.gold.silver_union import DEFAULT_GOLD_RULES, LAKE, load_gold_rules, month_range, read_batch, uri, use_reason

TOLERANCE = 1e-6     # value_mean averages twin-rounded sensor means


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--gold", default=str(LAKE / "gold"))
    p.add_argument("--silver", default=str(LAKE / "silver" / "measurements"))
    p.add_argument("--rules", default=str(DEFAULT_GOLD_RULES))
    p.add_argument("--months", nargs="+", required=True)
    p.add_argument("--run-id", default="gold_v1_01_validation")
    p.add_argument("--with-stream", action="store_true",
                   help="the build included stream rows: skip the batch-only recount")
    args = p.parse_args()
    rules = load_gold_rules(args.rules)
    gold = Path(args.gold).expanduser().absolute() / "station_hourly"
    silver = Path(args.silver).expanduser().absolute()
    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName("OpenAQ-Gold-Validate")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    checks, ok = {}, True
    try:
        for key in args.months:
            start, end = month_range(key)
            df = spark.read.parquet(uri(gold / f"year={start.year}" / f"month={start.month}"))
            s = df.agg(
                F.count("*").alias("rows"),
                F.countDistinct("location_id", "parameter", "hour_utc").alias("keys"),
                F.count(F.when((F.col("hour_utc") < F.lit(start)) | (F.col("hour_utc") >= F.lit(end)), 1)).alias("outside_month"),
                F.count(F.when((F.col("value_mean") < F.col("value_min") - TOLERANCE)
                               | (F.col("value_mean") > F.col("value_max") + TOLERANCE), 1)).alias("mean_outside_min_max"),
                F.count(F.when(F.col("value_mean").isNull(), 1)).alias("null_mean"),
                F.sum("n_readings").alias("n_readings"),
                F.count(F.when(F.col("lat").isNull(), 1)).alias("rows_without_coords"),
                F.countDistinct(F.when(F.col("lat").isNull(), F.col("location_id"))).alias("locations_without_coords"),
                F.count(F.when(F.col("is_vn"), 1)).alias("vn_rows"),
                F.countDistinct(F.when(F.col("is_vn"), F.col("location_id"))).alias("vn_locations"),
                F.count(F.when(F.col("value_count") < F.col("sensor_count"), 1)).alias("rows_with_twins"),
            ).first().asDict()
            units = df.groupBy("parameter").agg(F.countDistinct("unit").alias("u")).filter("u > 1").count()
            m = {k: int(v or 0) for k, v in s.items()}
            m["parameters_with_several_units"] = units
            fails = [n for n in ["outside_month", "mean_outside_min_max", "null_mean",
                                 "parameters_with_several_units"] if m[n]]
            if m["rows"] != m["keys"]:
                fails.append("duplicate_keys")
            if not args.with_stream:
                usable = (read_batch(spark, silver, start, end).withColumn("r", use_reason(rules))
                          .filter(F.col("r").isin("valid", "suspect_allowed")).count())
                m["silver_usable_rows"] = usable
                if usable != m["n_readings"]:
                    fails.append("n_readings_vs_silver")
            m["fails"] = fails
            ok = ok and not fails
            checks[key] = m
            print(key, json.dumps(m), flush=True)
    finally:
        out = {"ok": ok, "checked_utc": datetime.now(timezone.utc).isoformat(), "months": checks}
        (report / "checks.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
