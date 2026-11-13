"""Reprocessing with Delta Lake: atomic month overwrites, bad-batch detection and rollback.

Silver as a plain Parquet folder has no transaction: a rebuild that dies while moving files
leaves readers a half month (scripts/experiments/reprocess.sh shows it). A Delta table commits
a whole overwrite as one log entry, or nothing. Steps (one per call, so a step can be killed):

  load      v0: the control month (openaq_lake/chaos/control/silver)
  overwrite rewrite the month from --source (replaceWhere year/month): one new version
  gate      quality gate on the latest version vs the previous one: row count, share of invalid
            rows, and the count of each dq flag (a jump in one flag, e.g. sentinel, is a broken
            feed even when the totals barely move); exit 1 when it fails
  restore   RESTORE the table to the version before the latest
  status    versions (DESCRIBE HISTORY) and the rows a reader sees now

    docker compose -f docker-compose.spark.yml run --rm spark-lake /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.1 src/chaos/delta_reprocess.py load
"""

import argparse
import json
from pathlib import Path
import sys

from pyspark.sql import SparkSession, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import LAKE

TABLE = LAKE / "scratch" / "delta" / "silver"     # --table overrides
MONTH = (2025, 1)


def uri(path):
    return "file://" + str(path)


def spark_session():
    return (SparkSession.builder.appName("OpenAQ-Delta-Reprocess")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def month_frame(spark, silver_root):
    y, m = MONTH
    return (spark.read.parquet(uri(Path(silver_root).absolute() / "measurements"))
            .filter((F.col("year") == y) & (F.col("month") == m)))


def stats(df):
    r = df.agg(F.count("*").alias("rows"),
               F.avg((F.col("dq_status") == "invalid").cast("double")).alias("invalid_share")).first()
    flags = {f["flag"]: f["count"] for f in df.select(F.explode("dq_flags").alias("flag")).groupBy("flag").count()
             .collect()}
    return {"rows": r["rows"], "invalid_share": round(r["invalid_share"] or 0, 6), "flags": flags}


def version_stats(spark, version):
    y, m = MONTH
    df = spark.read.format("delta").option("versionAsOf", version).load(uri(TABLE))
    return stats(df.filter((F.col("year") == y) & (F.col("month") == m)))


def latest_version(spark):
    return spark.sql(f"DESCRIBE HISTORY delta.`{uri(TABLE)}`").agg(F.max("version")).first()[0]


def main():
    global TABLE
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("step", choices=["load", "overwrite", "gate", "restore", "status"])
    p.add_argument("--source", default=str(LAKE / "chaos" / "control" / "silver"),
                   help="Silver root (with measurements/) for load and overwrite")
    p.add_argument("--max-invalid-increase", type=float, default=0.005,
                   help="gate: allowed rise of the invalid share, absolute")
    p.add_argument("--max-row-change", type=float, default=0.01, help="gate: allowed relative change in rows")
    p.add_argument("--max-flag-rise", type=float, default=0.2,
                   help="gate: allowed relative rise of any one dq flag's count ...")
    p.add_argument("--min-flag-rows", type=int, default=100, help="... when it rises by at least this many rows")
    p.add_argument("--table", default=str(TABLE), help="Delta table folder")
    args = p.parse_args()
    TABLE = Path(args.table).expanduser().absolute()
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    y, m = MONTH
    result = {"step": args.step, "table": str(TABLE)}
    try:
        if args.step == "load":
            (month_frame(spark, args.source).write.format("delta").mode("overwrite")
             .partitionBy("year", "month").save(uri(TABLE)))
        elif args.step == "overwrite":
            (month_frame(spark, args.source).write.format("delta").mode("overwrite")
             .option("replaceWhere", f"year = {y} AND month = {m}").save(uri(TABLE)))
        elif args.step in ("gate", "restore"):
            v = latest_version(spark)
            if v < 1:
                raise SystemExit("no previous version to compare with or restore")
            if args.step == "gate":
                new, old = version_stats(spark, v), version_stats(spark, v - 1)
                rise = new["invalid_share"] - old["invalid_share"]
                change = abs(new["rows"] - old["rows"]) / max(old["rows"], 1)
                jumps = {f: {"before": old["flags"].get(f, 0), "after": n} for f, n in new["flags"].items()
                         if n - old["flags"].get(f, 0) >= args.min_flag_rows
                         and n > (1 + args.max_flag_rise) * old["flags"].get(f, 0)}
                failed = [name for name, bad in [("invalid share", rise > args.max_invalid_increase),
                                                 ("row count", change > args.max_row_change),
                                                 ("flag counts", bool(jumps))] if bad]
                result.update(version=v, invalid_share_rise=round(rise, 6), row_change=round(change, 6),
                              flag_jumps=jumps, failed_checks=failed, passed=not failed)
            else:
                spark.sql(f"RESTORE TABLE delta.`{uri(TABLE)}` TO VERSION AS OF {v - 1}")
                result.update(restored_to=v - 1)
        history = spark.sql(f"DESCRIBE HISTORY delta.`{uri(TABLE)}`").select(
            "version", "operation", "timestamp").orderBy("version").collect()
        result["versions"] = [f"v{r['version']} {r['operation']}" for r in history]
        seen = version_stats(spark, history[-1]["version"])
        result["reader_sees"] = {"rows": seen["rows"], "invalid_share": seen["invalid_share"]}
        print("delta_reprocess: " + json.dumps(result, default=str), flush=True)
        if args.step == "gate" and not result["passed"]:
            sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
