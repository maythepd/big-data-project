"""Reconcile a replay run: every message Kafka acknowledged vs what the consumer kept.

The replay manifest (src/chaos/replay.py) is the known input. Two outputs are checked:
  archive  hdfs:///replay/<run>/stream/readings, one row per (station_id, parameter, event_time);
           messages are matched by that key (it carries no Kafka offset)
  silver   hdfs:///replay/<run>/silver/stream/v1/batches/*, matched exactly by Kafka partition
           and offset: each message is in measurements, in quarantine (with a reason), or lost
  deadletter hdfs:///replay/<run>/stream/deadletter: records the archive rejected, with the reason
Plus the consumer's progress log (rates, batch durations, lag, watermark drops).
    Output: openaq_lake/reports/replay_<run>/{outcomes.csv, summary.json}

    docker compose -f docker-compose.spark.yml run --rm spark-job \
        /opt/spark/bin/spark-submit src/chaos/reconcile_stream.py --run-id r1
"""

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import sys

from pyspark.sql import SparkSession, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import LAKE

HDFS = "hdfs://namenode:8020"


def uri(path):
    return "file://" + str(path)


def exists(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).globStatus(p) not in (None, [])


def read_parquet(spark, path):
    """The Parquet data at path, or None when there is none yet (a sink that wrote no rows
    leaves only its _spark_metadata folder)."""
    from pyspark.errors import AnalysisException
    if not exists(spark, path):
        return None
    try:
        return spark.read.parquet(path)
    except AnalysisException as exc:
        if "Unable to infer schema" in str(exc):
            return None
        raise


def progress_summary(path):
    """Per query: batches, rows, throughput, batch duration percentiles, lag, watermark drops."""
    if not path.exists():
        return {}
    by_query = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        p = json.loads(line)
        by_query.setdefault(p["query"], []).append(p)
    out = {}
    for name, rows in by_query.items():
        busy = [r for r in rows if r["rows"] and not r.get("idle")]
        dur = sorted((r["duration_ms"] or {}).get("triggerExecution", 0) / 1000 for r in busy)
        pct = lambda q: dur[min(len(dur) - 1, int(q * len(dur)))] if dur else None
        out[name] = {
            "batches": len(busy), "rows": sum(r["rows"] for r in busy),
            "processed_rps_mean": round(statistics.mean(r["processed_rps"] or 0 for r in busy), 1) if busy else None,
            "processed_rps_max": round(max((r["processed_rps"] or 0) for r in busy), 1) if busy else None,
            "batch_s_p50": pct(0.5), "batch_s_p95": pct(0.95), "batch_s_max": dur[-1] if dur else None,
            "offsets_behind_max": max(float(r["offsets_behind_max"] or 0) for r in rows),
            "dropped_by_watermark": max((r.get("dropped_by_watermark") or 0) for r in rows),
            "first_batch": min(r["timestamp"] for r in busy) if busy else None,
            "last_batch": max(r["timestamp"] for r in busy) if busy else None,
        }
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", required=True)
    args = p.parse_args()
    run = LAKE / "replay" / args.run_id
    report = LAKE / "reports" / f"replay_{args.run_id}"
    report.mkdir(parents=True, exist_ok=True)
    root = f"{HDFS}/replay/{args.run_id}"

    spark = (SparkSession.builder.appName("OpenAQ-Replay-Reconcile")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        m = spark.read.parquet(uri(run / "manifest.parquet"))
        sent = m.filter("acked")
        flags = ["variant", "late", "copy"]

        # archive: matched by key; duplicates are keys stored more than once
        archive_key = ["station_id", "parameter", "event_time"]
        archive = read_parquet(spark, f"{root}/stream/readings")
        if archive is not None:
            stored = archive.groupBy(archive_key).count()
            archive_dups = stored.filter("count > 1").count()
            archive_rows = archive.count()
            found = sent.join(stored.select(*archive_key, F.lit(True).alias("in_archive")), archive_key, "left")
        else:
            archive_dups = archive_rows = None
            found = sent.withColumn("in_archive", F.lit(None).cast("boolean"))

        # silver: matched exactly by Kafka partition and offset
        batches = f"{root}/silver/stream/v1/batches/batch=*"
        if exists(spark, batches):
            meas = spark.read.parquet(f"{batches}/measurements")
            quar = spark.read.parquet(f"{batches}/quarantine")
            silver_dups = (meas.groupBy("sensors_id", "datetime_utc").count().filter("count > 1").count())
            silver_rows = meas.count()
            outcome = (meas.select(F.col("kafka_partition").alias("partition"), F.col("kafka_offset").alias("offset"),
                                   F.concat(F.lit("silver:"), F.col("dq_status")).alias("silver"))
                       .unionByName(quar.select(F.col("kafka_partition").alias("partition"),
                                                F.col("kafka_offset").alias("offset"),
                                                F.concat(F.lit("quarantine:"), F.col("quarantine_reason"))
                                                .alias("silver"))))
            found = found.join(outcome, ["partition", "offset"], "left")
            # a message whose own row is gone is lost only if no copy of its key was kept
            keys = meas.select("sensors_id", F.col("datetime_utc").alias("event_time"),
                               F.lit(True).alias("_key_kept")).distinct()
            found = (found.withColumn("event_time", F.col("event_time").cast("timestamp"))
                     .join(keys, ["sensors_id", "event_time"], "left")
                     .withColumn("silver", F.when(F.col("silver").isNull() & F.col("_key_kept"),
                                                  F.lit("deduplicated (a copy was kept)"))
                                 .otherwise(F.col("silver"))).drop("_key_kept"))
        else:
            silver_dups = silver_rows = None
            found = found.withColumn("silver", F.lit(None).cast("string"))
        found = found.withColumn("silver", F.coalesce("silver", F.lit("LOST: key not in silver")))

        dead = read_parquet(spark, f"{root}/stream/deadletter")
        if dead is not None:
            dead_rows = dead.count()
            found = found.join(dead.select(F.col("kafka_partition").alias("partition"),
                                           F.col("kafka_offset").alias("offset"),
                                           F.col("reject_reason").alias("deadletter")), ["partition", "offset"], "left")
        else:
            dead_rows = 0
            found = found.withColumn("deadletter", F.lit(None).cast("string"))
        found = found.withColumn("deadletter", F.coalesce("deadletter", F.lit("-")))

        rows = (found.groupBy(*flags, "in_archive", "deadletter", "silver").count()
                .orderBy(*flags, "in_archive", "deadletter", "silver").collect())
        with (report / "outcomes.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow([*flags, "in_archive", "deadletter", "silver", "messages"])
            for r in rows:
                w.writerow([r["variant"], r["late"], r["copy"], r["in_archive"], r["deadletter"], r["silver"],
                            r["count"]])
        print(f"{'variant':16} {'late':5} {'copy':5} {'archive':8} {'deadletter':18} {'silver':34} messages")
        for r in rows:
            print(f"{r['variant']:16} {str(r['late']):5} {str(r['copy']):5} {str(r['in_archive']):8} "
                  f"{r['deadletter']:18} {r['silver']:34} {r['count']}")

        failed = m.filter(~F.col("acked")).groupBy("variant", "error").count().collect()
        summary = {
            "run_id": args.run_id, "checked_utc": datetime.now(timezone.utc).isoformat(),
            "messages_sent": m.count(), "acked": sent.count(),
            "not_acked": {f"{r['variant']}:{r['error']}": r["count"] for r in failed},
            "archive_rows": archive_rows, "archive_duplicate_keys": archive_dups,
            "silver_rows": silver_rows, "silver_duplicate_keys": silver_dups, "deadletter_rows": dead_rows,
            "progress": progress_summary(run / "progress.jsonl"),
        }
        (report / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        print(json.dumps({k: v for k, v in summary.items() if k != "progress"}, indent=2, default=str))
        print(json.dumps(summary["progress"], indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
