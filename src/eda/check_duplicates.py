"""duplicate checks: duplicates, key validation, measurement frequency and coverage.

Candidate key: (sensors_id, datetime_utc). For `_null_ts` rows the time is the
diagnostic offset repair of structure checks (`-02:-30` -> `-02:30`); rows without a
usable key (split records) are only counted.

Runs one Spark action per month (duplicates can only share a month, since the
key contains the time) and writes a small sensor x month table; all reports
are built from it. Reruns skip months already written. Read-only on input.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys

from pyspark.sql import SparkSession, Window, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.common import count_if, read_compact, save_csv
from src.eda.quality_flags import repaired_datetime

DEFAULT_LAKE = Path.home() / "openaq_lake"
SPIKE_MONTHS = [(2023, 9), (2023, 10), (2023, 11), (2024, 2), (2024, 3), (2024, 4)]


def uri(path):
    return "file://" + str(path)


def months_of(source):
    keys = sorted(
        (int(y.name[5:]), int(m.name[6:]))
        for y in source.glob("year=*") for m in y.glob("month=*")
    )
    if (source / "_null_ts").is_dir():
        keys.append((None, None))
    return keys


def sensor_month(df):
    """Per sensor and month: rows, key multiplicity, interval and value ranges."""
    ts = F.coalesce(F.col("datetime_ts"), repaired_datetime())
    keyed = df.withColumn("ts", ts).filter(F.col("sensors_id").isNotNull() & F.col("ts").isNotNull())
    # Labels per sensor: partial (map-side) aggregation, no wide shuffle.
    labels = keyed.groupBy("sensors_id").agg(
        F.min("location_id").alias("location_id_min"), F.max("location_id").alias("location_id_max"),
        F.min("parameter").alias("parameter_min"), F.max("parameter").alias("parameter_max"),
        F.min("units").alias("units_min"), F.max("units").alias("units_max"),
    )
    # One shuffle of narrow columns: hash by sensor satisfies the distribution of
    # groupBy(sensor, ts), the window by sensor and groupBy(sensor) below, so Spark
    # adds no further exchange (first version shuffled twice with string columns:
    # 26.6 min for 2024-03).
    narrow = keyed.select(
        "sensors_id", "ts", "value_num",
        F.substring("source_block", 5, 20).cast("long").alias("block"),
    ).repartition(200, "sensors_id")
    groups = narrow.groupBy("sensors_id", "ts").agg(
        F.count("*").alias("n"),
        F.min("value_num").alias("v_min"), F.max("value_num").alias("v_max"),
        F.count("value_num").alias("v_n"),
        F.min("block").alias("b_min"), F.max("block").alias("b_max"),
    )
    lag = Window.partitionBy("sensors_id").orderBy("ts")
    groups = groups.withColumn(
        "gap_s", F.col("ts").cast("long") - F.lag(F.col("ts").cast("long")).over(lag))
    # A conflict: same key, different values (or value present in only some copies).
    conflict = (F.col("n") > 1) & ((F.col("v_min") != F.col("v_max")) | (F.col("v_n") != F.col("n")))
    return groups.groupBy("sensors_id").agg(
        F.sum("n").alias("rows"), F.count("*").alias("distinct_ts"),
        count_if(F.col("n") > 1, "dup_groups"),
        F.sum(F.col("n") - 1).alias("dup_excess_rows"),
        count_if(conflict, "conflict_groups"),
        F.sum(F.when(conflict, F.col("n") - 1).otherwise(0)).alias("conflict_excess_rows"),
        count_if((F.col("n") > 1) & (F.col("b_min") != F.col("b_max")), "multi_block_dup_groups"),
        F.max("n").alias("max_multiplicity"),
        F.expr("percentile_approx(gap_s, 0.5, 1000)").alias("median_interval_s"),
        F.min("gap_s").alias("min_interval_s"),
        F.min("ts").alias("first_utc"), F.max("ts").alias("last_utc"),
    ).join(labels, "sensors_id")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", default=str(DEFAULT_LAKE / "near_silver_compact"))
    p.add_argument("--report", default=str(DEFAULT_LAKE / "reports" / "duplicates_full_01"))
    p.add_argument("--months", nargs="*", help="YYYY-MM and/or null_ts (pilot)")
    args = p.parse_args()
    source = Path(args.input).expanduser().absolute()
    report = Path(args.report).expanduser().absolute()
    table = report / "_sensor_month"
    report.mkdir(parents=True, exist_ok=True)
    meta_path = report / "run.json"
    meta = {"phase": 3, "status": "running", "input": str(source), "months": args.months,
            "key": "(sensors_id, coalesce(datetime_ts, repaired offset))",
            "started_utc": datetime.now(timezone.utc).isoformat()}
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    spark = (SparkSession.builder.appName("OpenAQ-EDA-Duplicates")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.sql.codegen.hugeMethodLimit", "8000")
             .config("spark.sql.files.maxPartitionBytes", str(128 << 20))
             .config("spark.sql.files.openCostInBytes", str(1 << 20))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        wanted = months_of(source)
        if args.months:
            pick = {(None, None) if m == "null_ts" else (int(m[:4]), int(m[5:])) for m in args.months}
            wanted = [k for k in wanted if k in pick]
        df = read_compact(spark, source)
        log = (report / "month_log.csv").open("a", encoding="utf-8")
        for year, month in wanted:
            name = "null_ts" if year is None else f"year={year}/month={month}"
            target = table / name
            if (target / "_SUCCESS").exists():
                continue
            if target.exists():
                shutil.rmtree(target)
            part = (df.filter(F.col("year").isNull()) if year is None
                    else df.filter((F.col("year") == year) & (F.col("month") == month)))
            no_key = part.filter(F.col("sensors_id").isNull()
                                 | F.coalesce(F.col("datetime_ts"), repaired_datetime()).isNull())
            start = datetime.now(timezone.utc)
            (sensor_month(part)
                .withColumn("year", F.lit(year).cast("int")).withColumn("month", F.lit(month).cast("int"))
                .coalesce(1).write.parquet(uri(target)))
            missing = no_key.count()
            seconds = (datetime.now(timezone.utc) - start).total_seconds()
            log.write(f"{name},{missing},{seconds:.1f}\n")
            log.flush()
            print(f"{name} no_key={missing} {seconds:.0f}s", flush=True)
        log.close()

        sm = spark.read.parquet(*[uri(p.parent) for p in table.rglob("_SUCCESS")])
        sm = sm.withColumn("ym", F.when(F.col("year").isNull(), F.lit("null_ts"))
                           .otherwise(F.format_string("%04d-%02d", "year", "month")))
        sums = [F.sum(c).alias(c) for c in [
            "rows", "distinct_ts", "dup_groups", "dup_excess_rows", "conflict_groups",
            "conflict_excess_rows", "multi_block_dup_groups"]]
        save_csv(sm.groupBy("ym").agg(F.count("*").alias("active_sensors"), *sums,
                                      F.max("max_multiplicity").alias("max_multiplicity"))
                 .orderBy("ym"), report, "duplicates_by_month")
        save_csv(sm.groupBy("parameter_max", "units_max").agg(
            F.countDistinct("sensors_id").alias("sensors"), *sums).orderBy(F.desc("rows")),
            report, "duplicates_by_parameter")
        save_csv(sm.orderBy(F.desc("dup_excess_rows")).limit(200), report, "top_duplicate_sensor_months")

        # Key validation: one parameter / units / location per sensor over all time.
        sensor = sm.groupBy("sensors_id").agg(
            F.sum("rows").alias("rows"), F.sum("distinct_ts").alias("distinct_ts"),
            F.sum("dup_excess_rows").alias("dup_excess_rows"),
            F.min("first_utc").alias("first_utc"), F.max("last_utc").alias("last_utc"),
            F.countDistinct("ym").alias("active_months"),
            F.expr("percentile_approx(median_interval_s, 0.5, 1000)").alias("median_interval_s"),
            F.min("location_id_min").alias("location_id_min"), F.max("location_id_max").alias("location_id_max"),
            F.min("parameter_min").alias("parameter_min"), F.max("parameter_max").alias("parameter_max"),
            F.min("units_min").alias("units_min"), F.max("units_max").alias("units_max"),
        )
        sensor.write.mode("overwrite").parquet(uri(DEFAULT_LAKE / "silver" / "dim_sensor"))
        sensor = spark.read.parquet(uri(DEFAULT_LAKE / "silver" / "dim_sensor"))
        save_csv(sensor.agg(
            F.count("*").alias("sensors"),
            count_if(F.col("parameter_min") != F.col("parameter_max"), "sensors_multi_parameter"),
            count_if(F.col("units_min") != F.col("units_max"), "sensors_multi_units"),
            count_if(F.col("location_id_min") != F.col("location_id_max"), "sensors_multi_location"),
        ), report, "key_validation")
        save_csv(sensor.filter(
            (F.col("parameter_min") != F.col("parameter_max")) | (F.col("units_min") != F.col("units_max"))
            | (F.col("location_id_min") != F.col("location_id_max"))
        ).orderBy(F.desc("rows")).limit(500), report, "key_violations")
        interval = F.col("median_interval_s")
        save_csv(sensor.groupBy(
            F.when(interval.isNull(), "single_point").when(interval < 60, "<1min")
            .when(interval < 600, "1-10min").when(interval < 3600, "10-60min")
            .when(interval < 3 * 3600, "1-3h").when(interval < 86400, "3-24h").otherwise(">=1d")
            .alias("interval_bucket")
        ).agg(F.count("*").alias("sensors"), F.sum("rows").alias("rows")), report, "interval_distribution")

        # Spike months vs neighbours: who contributed the extra rows.
        in_spike = F.lit(False)
        for y, m in SPIKE_MONTHS:
            in_spike = in_spike | ((F.col("year") == y) & (F.col("month") == m))
        spike = sm.filter(in_spike)
        save_csv(spike.groupBy("ym").agg(
            F.count("*").alias("active_sensors"), F.sum("rows").alias("rows"),
            F.sum("dup_excess_rows").alias("dup_excess_rows"),
            (F.sum("rows") / F.count("*")).alias("rows_per_sensor"),
            F.expr("percentile_approx(median_interval_s, 0.5, 1000)").alias("median_interval_s"),
        ).orderBy("ym"), report, "spike_months")
        rank = Window.partitionBy("ym").orderBy(F.desc("rows"))
        save_csv(spike.withColumn("rank", F.row_number().over(rank)).filter("rank <= 30")
                 .select("ym", "rank", "sensors_id", "location_id_max", "parameter_max", "rows",
                         "distinct_ts", "dup_excess_rows", "median_interval_s"),
                 report, "spike_top_sensors")
        totals = sm.agg(*sums).first().asDict()
        meta.update(status="complete", totals=totals)
    except Exception as exc:
        meta.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        meta["finished_utc"] = datetime.now(timezone.utc).isoformat()
        meta_path.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
