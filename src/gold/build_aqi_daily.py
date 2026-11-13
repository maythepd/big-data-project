"""Gold pass 2: station_hourly_aqi, station_daily and station_daily_aqi, per month.

Reads 2 days before and 1 after each month for rolling windows and local days.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import shutil
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.gold import aqi
from src.gold.build_gold import move_month, plan
from src.gold.silver_union import DEFAULT_GOLD_RULES, LAKE, load_gold_rules, month_range, months_between, uri
from src.silver.rules import DEFAULT_RULES, load_rules

HOURLY_INPUTS = {"pm25": ["1h", "24h", "nowcast"], "pm10": ["1h", "24h", "nowcast"], "o3": ["1h", "8h"],
                 "co": ["1h", "8h"], "no2": ["1h"], "so2": ["1h", "24h"]}
META = ["location_name", "lat", "lon", "is_vn"]
HW_COLUMNS = ["location_id", "parameter", "unit", "hour_utc", "value_mean", "value_min", "value_max",
              "n_readings", "utc_offset_min"]
MAX_ROWS_OUTPUT_FILE = 4_000_000


def read_hourly(spark, root, start, end):
    dirs = [root / f"year={y}" / f"month={m}" for y, m in months_between(start, end)]
    dirs = [d for d in dirs if d.is_dir()]
    df = spark.read.option("basePath", uri(root)).parquet(*[uri(d) for d in dirs])
    return df.filter((F.col("hour_utc") >= F.lit(start)) & (F.col("hour_utc") < F.lit(end))).drop("year", "month")


# rolling windows only where needed (windows on every parameter were slow)
ROLLING = {"pm25": ["24h", "nowcast"], "pm10": ["24h", "nowcast"], "so2": ["24h"], "o3": ["8h"], "co": ["8h"]}


def with_rolling(h, rules):
    """Trailing means over present hours (range window, so gaps count as missing)."""
    w = Window.partitionBy("location_id", "parameter").orderBy("t")
    out = h.withColumn("t", F.unix_timestamp("hour_utc"))
    nc = rules["nowcast"]
    groups = {}
    for p, needs in ROLLING.items():
        groups.setdefault(tuple(needs), []).append(p)
    parts = []
    for needs, params in groups.items():
        g = out.filter(F.col("parameter").isin(*params))
        for hours, min_hours in rules["rolling_min_hours"].items():
            col = f"avg_{hours}h"
            if f"{hours}h" in needs:
                win = w.rangeBetween(-(int(hours) - 1) * 3600, 0)
                g = g.withColumn(col, F.when(F.count("value_mean").over(win) >= min_hours,
                                             F.avg("value_mean").over(win)))
            else:
                g = g.withColumn(col, F.lit(None).cast("double"))
        if "nowcast" in needs:
            # materialize the window once, or nowcast() plans it six times
            recent = F.collect_list(F.struct("t", F.col("value_mean").alias("c"))).over(
                w.rangeBetween(-(nc["hours"] - 1) * 3600, 0))
            values = F.transform("_recent", lambda x: F.struct(
                ((F.col("t") - x["t"]) / 3600).cast("int").alias("ago"), x["c"].alias("c")))
            g = (g.withColumn("_recent", recent).withColumn("_values", values)
                 .withColumn("nowcast", aqi.nowcast(F.col("_values"), rules)).drop("_recent", "_values"))
        else:
            g = g.withColumn("nowcast", F.lit(None).cast("double"))
        parts.append(g)
    rest = out.filter(~F.col("parameter").isin(*ROLLING))
    for col in [f"avg_{hours}h" for hours in rules["rolling_min_hours"]] + ["nowcast"]:
        rest = rest.withColumn(col, F.lit(None).cast("double"))
    for g in parts:
        rest = rest.unionByName(g)
    return rest


def local_date(rules):
    shift = 60 * rules["day_label_shift_hours"]
    return F.to_date(F.timestamp_seconds(F.col("t") + (F.coalesce("utc_offset_min", F.lit(0)) - shift) * 60))


def hourly_wide(hw, rules, start, end):
    params = rules["aqi_parameters"]
    cols = {"1h": "value_mean", "8h": "avg_8h", "24h": "avg_24h", "nowcast": "nowcast"}
    month = hw.filter(F.col("parameter").isin(*params)
                      & (F.col("hour_utc") >= F.lit(start)) & (F.col("hour_utc") < F.lit(end)))
    wide = (month.groupBy("location_id", "hour_utc").pivot("parameter", params)
            .agg(*[F.first(c).alias(k) for k, c in cols.items()]))
    keep = [f"{p}_{k}" for p, ks in HOURLY_INPUTS.items() for k in ks]
    return wide.select("location_id", "hour_utc", *keep)


def station_daily(hw, rules, start, end):
    d = (hw.withColumn("local_date", local_date(rules))
         .filter((F.col("local_date") >= F.lit(start.date())) & (F.col("local_date") < F.lit(end.date()))))
    return (d.groupBy("location_id", "parameter", "unit", "local_date")
            .agg(F.avg("value_mean").alias("value_mean"), F.min("value_min").alias("value_min"),
                 F.max("value_max").alias("value_max"), F.max("value_mean").alias("max_1h"),
                 F.max("avg_8h").alias("max_8h"), F.count("*").alias("hours"),
                 F.sum("n_readings").alias("n_readings"))
            .withColumn("max_8h", F.when(F.col("parameter").isin("o3", "co"), F.col("max_8h")))
            .withColumn("day_valid", F.col("hours") >= rules["day"]["min_hours"]))


def daily_wide(daily, rules):
    params = rules["aqi_parameters"]
    return (daily.filter(F.col("parameter").isin(*params))
            .groupBy("location_id", "local_date").pivot("parameter", params)
            .agg(F.first("value_mean").alias("mean"), F.first("max_1h").alias("max_1h"),
                 F.first("max_8h").alias("max_8h"), F.first("hours").alias("hours"),
                 F.first("day_valid").alias("valid")))


def aqi_checksum(df):
    cols = [c for c in df.columns if c.startswith("aqi_") and not c.endswith("_main")]
    return df.agg(*[F.sum(F.coalesce(F.col(c), F.lit(-1))).alias(c) for c in cols]).first().asDict()


def write_month(df, staging, target, parts, sort):
    if staging.exists():
        shutil.rmtree(staging)
    df.repartition(parts, "location_id").sortWithinPartitions(*sort).write.parquet(uri(staging))
    rows = df.sparkSession.read.parquet(uri(staging)).count()
    move_month(staging, target)
    shutil.rmtree(staging)
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--gold", default=str(LAKE / "gold"))
    p.add_argument("--rules", default=str(DEFAULT_GOLD_RULES))
    p.add_argument("--silver-rules", default=str(DEFAULT_RULES))
    p.add_argument("--run-id", default="gold_aqi_daily_01")
    p.add_argument("--months", nargs="+", required=True, help="YYYY-MM")
    p.add_argument("--aqi-impl", choices=["builtin", "pandas"], default="builtin")
    p.add_argument("--compare-impl", action="store_true", help="time both AQI implementations (pilot)")
    p.add_argument("--max-rows-per-file", type=int, default=MAX_ROWS_OUTPUT_FILE,
                   help="rows per output file; fewer rows per write task need less heap")
    args = p.parse_args()

    rules, silver_rules = load_gold_rules(args.rules), load_rules(args.silver_rules)
    gold = Path(args.gold).expanduser().absolute()
    report = LAKE / "reports" / args.run_id
    staging = LAKE / "_staging" / "gold" / args.run_id
    for d in [report / "done", report / "plans", staging]:
        d.mkdir(parents=True, exist_ok=True)
    meta = {"status": "running", "run_id": args.run_id, "gold_version": rules["version"], "gold": str(gold),
            "aqi_impl": args.aqi_impl, "months": args.months, "started_utc": datetime.now(timezone.utc).isoformat()}
    (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    spark = (SparkSession.builder.appName("OpenAQ-Gold-AqiDaily")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.parquet.compression.codec", "zstd")
             # 64 MB row groups: 8 writers x 128 MB ran a 3 GB driver out of heap
             .config("spark.hadoop.parquet.block.size", str(64 * 1024 * 1024))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        log = (report / "job_log.csv").open("a", encoding="utf-8")
        for key in args.months:
            done = report / "done" / key
            if done.exists():
                continue
            t0 = time.time()
            start, end = month_range(key)
            if not (gold / "station_hourly" / f"year={start.year}" / f"month={start.month}").is_dir():
                print(f"{key}: no station_hourly, skipped", flush=True)
                continue
            h = read_hourly(spark, gold / "station_hourly", start - timedelta(days=2), end + timedelta(days=1))
            # persist only numeric columns; location metadata is joined back later
            locations = (h.select("location_id", *META).dropDuplicates(["location_id"])
                         .persist(StorageLevel.MEMORY_AND_DISK))
            hw = with_rolling(h.select(*HW_COLUMNS), rules).persist(StorageLevel.MEMORY_AND_DISK)
            rows_context = hw.count()
            t_rolling = time.time() - t0
            stamp = [F.lit(rules["version"]).alias("gold_version"), F.lit(args.run_id).alias("build_run_id")]
            result = {"hourly_rows_with_context": rows_context, "seconds_rolling_persist": round(t_rolling, 1)}

            # Hourly AQI
            wide = hourly_wide(hw, rules, start, end)
            if args.compare_impl:
                wide_c = wide.persist(StorageLevel.MEMORY_AND_DISK)
                wide_c.count()
                sums = {}
                for impl in ["builtin", "pandas"]:
                    t = time.time()
                    sums[impl] = aqi_checksum(aqi.hourly_aqi(wide_c, rules, silver_rules, impl))
                    result[f"seconds_hourly_aqi_{impl}"] = round(time.time() - t, 1)
                    (report / "plans" / f"hourly_aqi_{impl}.txt").write_text(
                        plan(aqi.hourly_aqi(wide_c, rules, silver_rules, impl)), encoding="utf-8")
                result["impl_checksums_equal"] = sums["builtin"] == sums["pandas"]
                pair = (aqi.hourly_aqi(wide_c, rules, silver_rules, "builtin").alias("b")
                        .join(aqi.hourly_aqi(wide_c, rules, silver_rules, "pandas").alias("p"),
                              ["location_id", "hour_utc"]))
                cols = [c for c in sums["builtin"]]
                result["impl_rows_differing"] = pair.filter(F.greatest(*[
                    (~F.col(f"b.{c}").eqNullSafe(F.col(f"p.{c}"))).cast("int") for c in cols]) > 0).count()
                wide_c.unpersist()
            hourly = (aqi.hourly_aqi(wide, rules, silver_rules, args.aqi_impl)
                      .join(F.broadcast(locations), "location_id", "left").select("*", *stamp))
            if not (report / "plans" / "station_hourly_aqi.txt").exists():
                (report / "plans" / "station_hourly_aqi.txt").write_text(plan(hourly), encoding="utf-8")
            n_hours = hw.filter(F.col("parameter").isin(*rules["aqi_parameters"])
                                & (F.col("hour_utc") >= F.lit(start)) & (F.col("hour_utc") < F.lit(end))) \
                .select("location_id", "hour_utc").distinct().count()
            parts = max(1, math.ceil(n_hours / args.max_rows_per_file))
            t1 = time.time()
            result["station_hourly_aqi"] = write_month(
                hourly, staging / f"{key}_hourly_aqi",
                gold / "station_hourly_aqi" / f"year={start.year}" / f"month={start.month}", parts,
                ["location_id", "hour_utc"])
            if result["station_hourly_aqi"] != n_hours:
                raise RuntimeError(f"{key}: station_hourly_aqi {result['station_hourly_aqi']} != location-hours {n_hours}")

            # Daily tables
            daily = (station_daily(hw, rules, start, end)
                     .join(F.broadcast(locations), "location_id", "left").select("*", *stamp))
            result["seconds_hourly_aqi"] = round(time.time() - t1, 1)
            t1 = time.time()
            daily = daily.persist(StorageLevel.MEMORY_AND_DISK)
            stats = daily.agg(F.count("*").alias("rows"), F.sum("hours").alias("hours"),
                              F.sum(F.col("day_valid").cast("int")).alias("valid")).first()
            expected_hours = (hw.withColumn("local_date", local_date(rules))
                              .filter((F.col("local_date") >= F.lit(start.date()))
                                      & (F.col("local_date") < F.lit(end.date()))).count())
            if stats["hours"] != expected_hours:
                raise RuntimeError(f"{key}: station_daily hours {stats['hours']} != hourly rows {expected_hours}")
            parts = max(1, math.ceil(stats["rows"] / args.max_rows_per_file))
            result["station_daily"] = write_month(
                daily, staging / f"{key}_daily", gold / "station_daily" / f"year={start.year}" / f"month={start.month}",
                parts, ["location_id", "parameter", "local_date"])
            result["station_daily_valid"] = int(stats["valid"])
            daily_aqi = (aqi.daily_aqi(daily_wide(daily, rules), rules, silver_rules, args.aqi_impl)
                         .join(F.broadcast(locations), "location_id", "left").select("*", *stamp))
            result["station_daily_aqi"] = write_month(
                daily_aqi, staging / f"{key}_daily_aqi",
                gold / "station_daily_aqi" / f"year={start.year}" / f"month={start.month}", 1,
                ["location_id", "local_date"])
            result["seconds_daily"] = round(time.time() - t1, 1)
            daily.unpersist()
            hw.unpersist()
            locations.unpersist()
            result["seconds"] = round(time.time() - t0, 1)
            log.write(f"{key},{json.dumps(result)}\n")
            log.flush()
            print(f"{key} {json.dumps(result)}", flush=True)
            done.write_text(json.dumps(result, indent=2), encoding="utf-8")
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
