"""values/units checks: units, sentinels, distributions and outliers (impact of the draft rules).

Uses src/silver/rules.py + config/silver_rules_v1.yaml so the measured counts
are those the Silver build will produce. Read-only on near_silver_compact.

Outputs (CSV unless noted):
  value_flags_by_group_month  rows and value flags per (parameter, units, month)
  quantiles                   quantiles of usable values per canonical group (from _histogram)
  _sensor_values (Parquet), unit_suspects, unit_suspect_summary
                              per-sensor medians vs group median -> unit labels to check
  flagged_top_values          most frequent raw values among flagged rows (sentinel discovery)
  soft_outliers               flatline / spike / pm25 > pm10 rates on sample months
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from pyspark.sql import SparkSession, Window, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.common import count_if, read_compact, save_csv
from src.silver.rules import DEFAULT_RULES, load_rules, normalize_units, value_flags

DEFAULT_LAKE = Path.home() / "openaq_lake"
FLAGS = ["v_sentinel", "v_extreme", "v_invalid_pair", "v_below_min", "v_above_max",
         "v_negative_small", "v_unknown_parameter", "unit_unconverted"]
QUANTILES = [0.001, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 0.999]
QUANTILE_NAMES = ["p0_1", "p1", "p5", "p25", "p50", "p75", "p95", "p99", "p99_9"]


def usable():
    return ~(F.col("v_sentinel") | F.col("v_extreme") | F.col("v_invalid_pair")
             | F.col("v_below_min") | F.col("v_above_max")) & F.col("value_std").isNotNull() \
        & ~F.isnan("value_std")


def soft_outliers(df, rules, months):
    so = rules["soft_outliers"]
    keep = F.lit(False)
    for ym in months:
        keep = keep | ((F.col("year") == int(ym[:4])) & (F.col("month") == int(ym[5:])))
    base = (df.filter(keep & usable())
           .select("sensors_id", "location_id", "datetime_ts", "parameter_std", "unit_std", "value_std")
           .repartition(400, "sensors_id"))
    w = Window.partitionBy("sensors_id").orderBy("datetime_ts")
    prev, nxt = F.lag("value_std").over(w), F.lead("value_std").over(w)
    min_value = F.lit(None).cast("double")
    for parameter, threshold in so["spike_min_value"].items():
        min_value = F.when(F.col("parameter_std") == parameter, F.lit(float(threshold))).otherwise(min_value)
    ratio = float(so["spike_ratio"])
    spike = (F.col("value_std") >= min_value) & (F.col("value_std") > ratio * F.greatest(prev, nxt))
    # Run id grows each time the value changes (streaming row frame). Run lengths
    # come from an aggregation, not an unordered window: that window buffered a
    # whole sensor-run in memory and failed in the pilot (UNABLE_TO_ACQUIRE_MEMORY).
    change = F.when(F.coalesce(F.col("value_std") == prev, F.lit(False)), 0).otherwise(1)
    x = (base.withColumn("spike", F.coalesce(spike, F.lit(False)))
          .withColumn("run_id", F.sum(change).over(w.rowsBetween(Window.unboundedPreceding, 0))))
    runs = x.groupBy("parameter_std", "unit_std", "sensors_id", "run_id").agg(
        F.count("*").alias("n"), F.first("value_std").alias("v"), count_if(F.col("spike"), "spikes"))
    flat = (F.col("n") >= int(so["flatline_min_run"])) & (F.col("v") != 0)
    per_param = runs.groupBy("parameter_std", "unit_std").agg(
        F.sum("n").alias("rows"), F.sum(F.when(flat, F.col("n")).otherwise(0)).alias("flatline_rows"),
        F.sum("spikes").alias("spike_rows"))
    # pm25 > pm10 at the same location and hour.
    hourly = (base.filter(F.col("parameter_std").isin("pm25", "pm10"))
              .groupBy("location_id", F.date_trunc("hour", "datetime_ts").alias("hour"))
              .pivot("parameter_std", ["pm25", "pm10"]).agg(F.avg("value_std")))
    tol = so["pm25_gt_pm10_tolerance"]
    both = hourly.filter(F.col("pm25").isNotNull() & F.col("pm10").isNotNull())
    cross = both.agg(F.count("*").alias("location_hours_with_both"),
                     count_if(F.col("pm25") > F.col("pm10") * float(tol["ratio"]) + float(tol["abs"]),
                              "pm25_gt_pm10_location_hours")).first().asDict()
    return per_param, cross


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", default=str(DEFAULT_LAKE / "near_silver_compact"))
    p.add_argument("--report", default=str(DEFAULT_LAKE / "reports" / "values_units_full_01"))
    p.add_argument("--rules", default=str(DEFAULT_RULES))
    p.add_argument("--months", nargs="*", help="YYYY-MM subset for a pilot (default: all)")
    p.add_argument("--resume", action="store_true", help="reuse outputs of an earlier run (skip steps with _SUCCESS)")
    args = p.parse_args()
    report = Path(args.report).expanduser().absolute()
    report.mkdir(parents=True, exist_ok=args.resume)
    done = lambda name: (report / name / "_SUCCESS").exists()
    rules = load_rules(args.rules)
    meta = {"phase": 4, "status": "running", "input": args.input, "rules": args.rules,
            "rules_version": rules["version"], "months": args.months,
            "quantiles": "histogram (config histogram_decimals); sensor medians percentile_approx accuracy 100",
            "started_utc": datetime.now(timezone.utc).isoformat()}
    (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    spark = (SparkSession.builder.appName("OpenAQ-EDA-ValuesUnits")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.sql.codegen.hugeMethodLimit", "8000")
             .config("spark.sql.files.maxPartitionBytes", str(256 << 20))
             .config("spark.sql.files.openCostInBytes", str(1 << 20))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = read_compact(spark, args.input)
        if args.months:
            keep = F.lit(False)
            for ym in args.months:
                keep = keep | ((F.col("year") == int(ym[:4])) & (F.col("month") == int(ym[5:])))
            df = df.filter(keep)
        df = value_flags(normalize_units(df.select(
            "location_id", "sensors_id", "datetime_ts", "parameter", "units", "value_num",
            "year", "month"), rules), rules)
        group = ["parameter_std", "unit_std", "parameter", "units"]

        # A: one scan, every flag per group and month.
        if not done("value_flags_by_group_month"):
            save_csv(df.groupBy(*group, "year", "month").agg(
                F.count("*").alias("rows"), count_if(F.col("value_num").isNull(), "value_null"),
                *[count_if(F.col(f), f) for f in FLAGS],
                count_if(usable(), "usable_rows"),
                F.min(F.when(usable(), F.col("value_std"))).alias("usable_min"),
                F.max(F.when(usable(), F.col("value_std"))).alias("usable_max"),
            ).orderBy(*group, "year", "month"), report, "value_flags_by_group_month")

        # B: quantiles of usable values per canonical group from a histogram of
        # values rounded to the unit's resolution: deterministic (percentile_approx
        # drifted up to 14% at p99 between runs) and small in memory
        # (percentile_approx accuracy 100000 ran out of heap in the pilot).
        decimals = rules["histogram_decimals"]
        binned = F.round("value_std", int(decimals["default"]))
        for unit, digits in decimals.items():
            if unit != "default":
                binned = F.when(F.col("unit_std") == unit, F.round("value_std", int(digits))).otherwise(binned)
        hist_path = report / "_histogram"
        if not done("_histogram"):
            (df.filter(usable()).groupBy("parameter_std", "unit_std", binned.alias("bin")).count()
             .write.parquet("file://" + str(hist_path)))
        hist = spark.read.parquet("file://" + str(hist_path))
        g = Window.partitionBy("parameter_std", "unit_std")
        cum = hist.withColumn("cum", F.sum("count").over(g.orderBy("bin").rowsBetween(Window.unboundedPreceding, 0))) \
                  .withColumn("total", F.sum("count").over(g))
        quant = cum.groupBy("parameter_std", "unit_std").agg(
            F.first("total").alias("usable_rows"), F.min("bin").alias("min"), F.max("bin").alias("max"),
            (F.sum(F.col("bin") * F.col("count")) / F.first("total")).alias("mean"),
            F.count("*").alias("bins"),
            *[F.min(F.when(F.col("cum") >= q * F.col("total"), F.col("bin"))).alias(name)
              for q, name in zip(QUANTILES, QUANTILE_NAMES)])
        if not done("quantiles"):
            save_csv(quant, report, "quantiles")
        quant = spark.read.option("header", True).option("inferSchema", True).csv(
            "file://" + str(report / "quantiles"))

        # C: per-sensor medians vs group median -> suspected unit labels.
        # Repartition by sensor first: the aggregate then runs once per sensor
        # (no partial sketches for every sensor of a 256 MB split), which ran out
        # of execution memory in the first full run.
        sensor_path = report / "_sensor_values"
        if not done("_sensor_values"):
            df.filter(usable()).select("sensors_id", "location_id", *group, "value_std") \
                .repartition(400, "sensors_id").groupBy("sensors_id", "location_id", *group).agg(
                F.count("*").alias("rows"),
                F.expr("percentile_approx(value_std, 0.5, 100)").alias("median"),
                F.expr("percentile_approx(value_std, 0.99, 100)").alias("p99"),
                count_if(F.col("value_std") < 0, "negative_rows"),
            ).write.parquet("file://" + str(sensor_path))
        sensors = spark.read.parquet("file://" + str(sensor_path)).join(
            quant.select("parameter_std", "unit_std", F.col("p50").alias("group_median")),
            ["parameter_std", "unit_std"], "left")
        uc = rules["unit_check"]
        ratio = F.col("median") / F.col("group_median")
        sensors = sensors.withColumn("median_ratio", ratio).withColumn(
            "unit_suspect",
            ((F.col("group_median") > 0) & (F.col("median") > 0)
             & ((ratio >= float(uc["ratio_suspect"])) | (ratio <= 1.0 / float(uc["ratio_suspect"]))))
            | ((F.col("parameter_std") == "temperature") & (F.lower(F.trim("units")) == "c")
               & (F.col("median") > float(uc["temperature_c_median_max"]))))
        save_csv(sensors.groupBy(*group).agg(
            F.count("*").alias("sensors"), count_if(F.col("unit_suspect"), "suspect_sensors"),
            F.sum("rows").alias("rows"),
            F.sum(F.when(F.col("unit_suspect"), F.col("rows")).otherwise(0)).alias("suspect_rows"),
        ).orderBy(F.desc("rows")), report, "unit_suspect_summary")
        save_csv(sensors.filter("unit_suspect").orderBy(F.desc("rows")).limit(500),
                 report, "unit_suspects")

        # D: most frequent raw values among flagged rows (sentinel discovery).
        flagged = F.col("v_sentinel") | F.col("v_extreme") | F.col("v_below_min") | F.col("v_above_max")
        top = Window.partitionBy("parameter", "units").orderBy(F.desc("count"), "value_num")
        save_csv(df.filter(flagged).groupBy("parameter", "units", "value_num").count()
                 .withColumn("rank", F.row_number().over(top)).filter("rank <= 30"),
                 report, "flagged_top_values")

        # E: soft-outlier rates on sample months.
        months = rules["soft_outliers"]["sample_months"]
        if args.months:
            months = [m for m in months if m in args.months] or args.months[:1]
        per_param, cross = soft_outliers(df, rules, months)
        save_csv(per_param.orderBy(F.desc("rows")), report, "soft_outliers")
        meta.update(status="complete", soft_outlier_months=months, pm25_pm10=cross)
    except Exception as exc:
        meta.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        meta["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (report / "run.json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
