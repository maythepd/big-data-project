"""Profile Silver to set the Gold parameters.

Outputs: sensor_agreement, hour_coverage, parameter_coverage, status_by_parameter,
bucketing_comparison (plain vs bucketed join). Steps with _SUCCESS are skipped on rerun.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import time

from pyspark.sql import SparkSession, Window, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.common import count_if, save_csv
from src.common.config import LAKE

VN_LOCATIONS = Path(__file__).resolve().parents[2] / "data" / "dim" / "openaq_locations.parquet"
QUANTILES = [0.5, 0.9, 0.99]
JOIN_KEYS = ["location_id", "parameter", "unit", "hour"]


def uri(path):
    return "file://" + str(path)


def plan(df, mode="formatted"):
    return df.sparkSession._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)


def build_sensor_hour(spark, silver, target):
    """One row per sensor and UTC hour; invalid rows count but give no value."""
    months = sorted((int(y.name[5:]), int(m.name[6:])) for y in silver.glob("year=*") for m in y.glob("month=*"))
    measurements = spark.read.parquet(uri(silver))
    valid, usable = F.col("dq_status") == "valid", F.col("dq_status") != "invalid"
    for y, m in months:
        out = target / f"year={y}" / f"month={m}"
        if (out / "_SUCCESS").exists():
            continue
        (measurements.filter((F.col("year") == y) & (F.col("month") == m))
         .groupBy("location_id", "sensors_id", "parameter", "unit",
                  F.date_trunc("hour", "datetime_utc").alias("hour"))
         .agg(F.count("*").alias("n"), count_if(valid, "n_valid"), count_if(usable, "n_usable"),
              F.avg(F.when(valid, F.col("value"))).alias("mean_valid"),
              F.avg(F.when(usable, F.col("value"))).alias("mean_usable"))
         .write.parquet(uri(out)))
        print(f"sensor_hour {y}-{m:02d}", flush=True)


def multi_sensor_pairs(hours):
    """Sensor pairs measuring one parameter at one location in the same hour."""
    a = hours.select(*JOIN_KEYS, F.col("sensors_id").alias("sensor_a"), F.col("mean_valid").alias("a"))
    b = hours.select(*JOIN_KEYS, F.col("sensors_id").alias("sensor_b"), F.col("mean_valid").alias("b"))
    pairs = a.join(b, JOIN_KEYS).filter(F.col("sensor_a") < F.col("sensor_b"))
    diff = F.abs(F.col("a") - F.col("b"))
    scale = (F.abs(F.col("a")) + F.abs(F.col("b"))) / 2
    return pairs.withColumn("abs_diff", diff).withColumn(
        "rel_diff", F.when(scale > 0, diff / scale))


def agreement_summary(pairs):
    return pairs.groupBy("parameter", "unit").agg(
        F.count("*").alias("pair_hours"),
        F.countDistinct("location_id").alias("locations"),
        *[F.expr(f"percentile_approx(abs_diff, {q}, 1000)").alias(f"abs_diff_p{int(q * 100)}") for q in QUANTILES],
        *[F.expr(f"percentile_approx(rel_diff, {q}, 1000)").alias(f"rel_diff_p{int(q * 100)}") for q in QUANTILES],
        count_if(F.col("rel_diff") > 0.5, "pair_hours_rel_gt_50pct"),
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver", default=str(LAKE / "silver" / "measurements"))
    p.add_argument("--report", default=str(LAKE / "reports" / "silver_for_gold_01"))
    p.add_argument("--bucket-year", type=int, default=2024)
    p.add_argument("--buckets", type=int, default=64)
    args = p.parse_args()
    silver = Path(args.silver).expanduser().absolute()
    report = Path(args.report).expanduser().absolute()
    (report / "plans").mkdir(parents=True, exist_ok=True)
    done = lambda name: (report / name / "_SUCCESS").exists()
    meta_path = report / "run.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    meta.update(status="running", silver=str(silver), started_utc=datetime.now(timezone.utc).isoformat())
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    spark = (SparkSession.builder.appName("OpenAQ-EDA-SilverForGold")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "400")
             .config("spark.sql.codegen.hugeMethodLimit", "8000")
             .config("spark.sql.files.maxPartitionBytes", str(256 << 20))
             .config("spark.sql.files.openCostInBytes", str(1 << 20))
             .config("spark.sql.warehouse.dir", uri(report / "_warehouse"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        sensor_hour_path = report / "_sensor_hour"
        build_sensor_hour(spark, silver, sensor_hour_path)
        hours = spark.read.parquet(uri(sensor_hour_path))

        # 4: status and flags per parameter, straight from Silver (one scan).
        if not done("status_by_parameter"):
            m = spark.read.parquet(uri(silver))
            save_csv(m.groupBy("parameter", "unit", "dq_status").count()
                     .orderBy("parameter", "unit", "dq_status"), report, "status_by_parameter")
            save_csv(m.filter(F.col("dq_status") != "valid")
                     .select("parameter", "dq_status", F.explode("dq_flags").alias("flag"))
                     .groupBy("parameter", "dq_status", "flag").count()
                     .orderBy("parameter", F.desc("count")), report, "flags_by_parameter")

        # 2: readings per sensor-hour and hours per sensor-day.
        if not done("hour_coverage"):
            per_day = (hours.groupBy("sensors_id", "parameter", F.to_date("hour").alias("day"))
                       .agg(F.count("*").alias("hours")))
            readings = hours.groupBy("parameter").agg(
                F.count("*").alias("sensor_hours"),
                *[F.expr(f"percentile_approx(n, {q}, 1000)").alias(f"readings_p{int(q * 100)}")
                  for q in [0.1, 0.5, 0.9]],
                count_if(F.col("n_usable") == 0, "hours_without_usable_value"))
            days = per_day.groupBy("parameter").agg(
                F.count("*").alias("sensor_days"),
                *[F.expr(f"percentile_approx(hours, {q}, 100)").alias(f"hours_per_day_p{int(q * 100)}")
                  for q in [0.1, 0.5, 0.9]],
                *[count_if(F.col("hours") >= h, f"days_ge_{h}h") for h in [6, 12, 18, 24]])
            save_csv(readings.join(days, "parameter").orderBy(F.desc("sensor_hours")), report, "hour_coverage")

        # 3: hours per location x parameter -> wide matrix (pivot) -> long again (unpivot).
        if not done("parameter_coverage"):
            params = [r[0] for r in hours.select("parameter").distinct().orderBy("parameter").collect()]
            # Distinct first: several sensors of a location share an hour.
            matrix = (hours.select("location_id", "parameter", "hour").distinct()
                      .groupBy("location_id").pivot("parameter", params).count().na.fill(0))
            matrix.write.parquet(uri(report / "_location_parameter_matrix"))
            matrix = spark.read.parquet(uri(report / "_location_parameter_matrix"))
            long = matrix.unpivot("location_id", params, "parameter", "hours").filter(F.col("hours") > 0)
            vn = spark.read.parquet(uri(VN_LOCATIONS)).select(F.col("station_id").alias("location_id")) \
                .withColumn("vn", F.lit(True))
            long = long.join(F.broadcast(vn), "location_id", "left").fillna(False, ["vn"])
            save_csv(long.groupBy("parameter").agg(
                F.count("*").alias("locations"), F.sum("hours").alias("location_hours"),
                count_if(F.col("hours") >= 24 * 30, "locations_ge_30_days"),
                count_if(F.col("vn"), "vn_locations"),
                F.sum(F.when(F.col("vn"), F.col("hours")).otherwise(0)).alias("vn_location_hours"),
            ).orderBy(F.desc("location_hours")), report, "parameter_coverage")

        # 1: co-located sensor agreement on all years (large x large: sort-merge join).
        if not done("sensor_agreement"):
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
            # Only (location, parameter) groups that ever have two sensors take part.
            multi = (hours.groupBy("location_id", "parameter")
                     .agg(F.countDistinct("sensors_id").alias("sensors")).filter("sensors > 1")
                     .select("location_id", "parameter"))
            usable = hours.filter(F.col("mean_valid").isNotNull()).join(
                F.broadcast(multi), ["location_id", "parameter"], "left_semi")
            pairs = multi_sensor_pairs(usable)
            pairs.write.parquet(uri(report / "_sensor_pairs"))
            (report / "plans" / "sensor_agreement_sortmerge.txt").write_text(plan(pairs), encoding="utf-8")
            pairs = spark.read.parquet(uri(report / "_sensor_pairs"))
            save_csv(agreement_summary(pairs).orderBy(F.desc("pair_hours")), report, "sensor_agreement")
            per_location = pairs.groupBy("location_id", "parameter").agg(
                F.count("*").alias("pair_hours"),
                F.expr("percentile_approx(rel_diff, 0.5, 1000)").alias("rel_diff_p50"))
            top = Window.partitionBy("parameter").orderBy(F.desc("rel_diff_p50"))
            save_csv(per_location.filter("pair_hours >= 168").withColumn("rank", F.row_number().over(top))
                     .filter("rank <= 20"), report, "sensor_agreement_worst_locations")
            spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

        # Bucketing: the same join on one year, plain Parquet vs bucketed table.
        if not done("bucketing_comparison"):
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
            year = hours.filter((F.col("year") == args.bucket_year) & F.col("mean_valid").isNotNull())
            # bucket on all join keys, or Spark 3.5 still shuffles
            (year.write.mode("overwrite").bucketBy(args.buckets, *JOIN_KEYS)
             .sortBy(*JOIN_KEYS).saveAsTable("sensor_hour_bucketed"))
            rows = []
            for name, frame in [("plain", year), ("bucketed", spark.table("sensor_hour_bucketed"))]:
                agg = multi_sensor_pairs(frame).groupBy("parameter").agg(F.count("*").alias("pair_hours"))
                start = time.time()
                result = agg.collect()
                seconds = time.time() - start
                text = plan(agg)
                (report / "plans" / f"bucketing_{name}.txt").write_text(text, encoding="utf-8")
                # Formatted plans list each operator once as "(id) Name".
                ops = re.findall(r"^\((\d+)\) (\w+)", text, flags=re.M)
                rows.append((name, args.bucket_year, round(seconds, 1), sum(o == "Exchange" for _, o in ops),
                             sum(o == "SortMergeJoin" for _, o in ops), sum(r["pair_hours"] for r in result)))
            save_csv(spark.createDataFrame(rows, "variant string, year int, seconds double, "
                                                 "exchanges int, sort_merge_joins int, pair_hours long"),
                     report, "bucketing_comparison")
            spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
        meta.update(status="complete")
    except Exception as exc:
        meta.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        meta["finished_utc"] = datetime.now(timezone.utc).isoformat()
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
