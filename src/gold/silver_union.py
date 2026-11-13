"""Gold input: historical Silver + stream Silver, one row per (sensors_id, datetime_utc).

Batch rows win over stream rows; use_reason (valid, suspect_allowed, suspect_excluded,
invalid) keeps Gold counts reconcilable with Silver.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import yaml
from pyspark.sql import SparkSession, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.common import save_csv
from src.silver.rules import SOFT_FLAGS
from src.common.config import LAKE

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_GOLD_RULES = ROOT / "config" / "gold_rules_v1.yaml"
COLUMNS = ["location_id", "sensors_id", "datetime_utc", "utc_offset_min", "parameter", "unit", "value",
           "dq_flags", "dq_status"]


def uri(path):
    return "file://" + str(path)


def load_gold_rules(path=DEFAULT_GOLD_RULES):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def month_range(key):
    """'YYYY-MM' -> [start, end) as tz-aware UTC datetimes."""
    y, m = int(key[:4]), int(key[5:])
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    end = datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc)
    return start, end


def months_between(start, end):
    """(year, month) of every month touching [start, end)."""
    out, y, m = [], start.year, start.month
    while datetime(y, m, 1, tzinfo=timezone.utc) < end:
        out.append((y, m))
        y, m = y + (m == 12), m % 12 + 1
    return out


def use_reason(rules):
    """valid / suspect_allowed / suspect_excluded / invalid."""
    excluded = [f for f in SOFT_FLAGS if f not in rules["usable"]["allowed_suspect_flags"]]
    blocked = F.exists("dq_flags", lambda x: x.isin(*excluded))
    status = F.col("dq_status")
    return (F.when(status == "valid", "valid")
             .when((status == "suspect") & ~blocked, "suspect_allowed")
             .when(status == "suspect", "suspect_excluded")
             .otherwise("invalid"))


def read_batch(spark, silver, start, end):
    """Historical Silver rows with start <= datetime_utc < end (partition pruning on year/month)."""
    dirs = [silver / f"year={y}" / f"month={m}" for y, m in months_between(start, end)]
    dirs = [d for d in dirs if d.is_dir()]
    if not dirs:
        return None
    df = spark.read.option("basePath", uri(silver)).parquet(*[uri(d) for d in dirs])
    return (df.filter((F.col("datetime_utc") >= F.lit(start)) & (F.col("datetime_utc") < F.lit(end)))
              .select(*COLUMNS).withColumn("origin", F.lit("batch")))


def read_stream(spark, stream_root, start, end):
    """Stream Silver (local copy), plus its R14 flags when present."""
    if stream_root is None or not (stream_root / "batches").is_dir():
        return None
    paths = sorted(str(p) for p in (stream_root / "batches").glob("batch=*/measurements") if p.is_dir())
    if not paths:
        return None
    df = (spark.read.parquet(*[uri(p) for p in paths])
          .filter(F.col("source") == "openaq")
          .filter((F.col("datetime_utc") >= F.lit(start)) & (F.col("datetime_utc") < F.lit(end)))
          .select(*COLUMNS))
    flags_dir = stream_root / "temporal_flags"
    if flags_dir.is_dir():
        flags = (spark.read.parquet(uri(flags_dir))
                 .groupBy("sensors_id", "datetime_utc")
                 .agg(F.array_sort(F.collect_set("flag")).alias("_r14")))
        df = (df.join(flags, ["sensors_id", "datetime_utc"], "left")
                .withColumn("dq_flags", F.array_union("dq_flags", F.coalesce("_r14", F.array().cast("array<string>"))))
                .withColumn("dq_status", F.when((F.col("dq_status") == "valid") & (F.size("_r14") > 0), "suspect")
                                          .otherwise(F.col("dq_status")))
                .drop("_r14"))
    return df.withColumn("origin", F.lit("stream"))


def union_silver(spark, silver, stream_root, start, end, rules):
    """Union with batch priority; adds use_reason. Returns None if there is no input row source."""
    batch = read_batch(spark, silver, start, end)
    stream = read_stream(spark, stream_root, start, end)
    if stream is not None and batch is not None:
        # semi-join batch to the few stream sensors first, so all of Silver is not shuffled
        sensors = stream.select("sensors_id").distinct()
        batch_keys = (batch.join(F.broadcast(sensors), "sensors_id", "left_semi")
                      .select("sensors_id", "datetime_utc"))
        stream = stream.join(batch_keys, ["sensors_id", "datetime_utc"], "left_anti")
    parts = [d for d in (batch, stream) if d is not None]
    if not parts:
        return None
    df = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    return df.withColumn("use_reason", use_reason(rules))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver", default=str(LAKE / "silver" / "measurements"))
    p.add_argument("--stream", default=str(LAKE / "stream_copy"), help="'none' to skip")
    p.add_argument("--rules", default=str(DEFAULT_GOLD_RULES))
    p.add_argument("--months", nargs="+", required=True, help="YYYY-MM")
    p.add_argument("--run-id", default="silver_union_counts_01")
    args = p.parse_args()
    rules = load_gold_rules(args.rules)
    silver = Path(args.silver).expanduser().absolute()
    stream = None if args.stream == "none" else Path(args.stream).expanduser().absolute()
    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=False)
    spark = (SparkSession.builder.appName("OpenAQ-Gold-SilverUnion")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        frames = []
        for key in args.months:
            start, end = month_range(key)
            df = union_silver(spark, silver, stream, start, end, rules)
            if df is not None:
                frames.append(df.groupBy("origin", "use_reason").count().withColumn("month", F.lit(key)))
        out = frames[0]
        for f in frames[1:]:
            out = out.unionByName(f)
        save_csv(out.orderBy("month", "origin", "use_reason"), report, "use_reason_counts")
        (report / "run.json").write_text(json.dumps({"status": "complete", "months": args.months,
                                                     "rules_version": rules["version"]}, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
