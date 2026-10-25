"""structure checks.3: reference coordinates per location (draft dim_location for Silver).

Valid coordinates exclude every structure checks coordinate flag (non-finite, out of
range, 200/200 sentinel, 0/0, lat == lon). The reference point is the most
frequent valid pair (rounded to 5 decimals, ~1 m): for fixed stations this is
the reported station position. Spread is measured from that point, weighted by
row counts, so mobile locations can be told apart.

One full scan (5 columns) builds a (location, lat, lon) pair table on disk;
everything else runs on that table. Read-only on near_silver_compact.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from pyspark.sql import SparkSession, Window, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.common import finite, read_compact, save_csv

DEFAULT_LAKE = Path.home() / "openaq_lake"
EARTH_KM = 6371.0088


def valid_coordinate():
    lat, lon = F.col("lat_num"), F.col("lon_num")
    return (
        finite("lat_num") & finite("lon_num")
        & lat.between(-90, 90) & lon.between(-180, 180)
        & (lat != lon) & ~((lat == 0) & (lon == 0))
    )


def haversine_km(lat1, lon1, lat2, lon2):
    rad = F.radians
    a = (F.sin((rad(lat2) - rad(lat1)) / 2) ** 2
         + F.cos(rad(lat1)) * F.cos(rad(lat2)) * F.sin((rad(lon2) - rad(lon1)) / 2) ** 2)
    return 2 * EARTH_KM * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", default=str(DEFAULT_LAKE / "near_silver_compact"))
    p.add_argument("--output", default=str(DEFAULT_LAKE / "silver" / "dim_location"))
    p.add_argument("--report", default=str(DEFAULT_LAKE / "reports" / "location_reference_01"))
    p.add_argument("--mobile-km", type=float, default=1.0,
                   help="p95 distance from the reference point above which a location is mobile")
    args = p.parse_args()
    output = Path(args.output).expanduser().absolute()
    report = Path(args.report).expanduser().absolute()
    report.mkdir(parents=True, exist_ok=False)
    meta = {"phase": "2.3", "status": "running", "input": args.input, "output": str(output),
            "mobile_km": args.mobile_km, "started_utc": datetime.now(timezone.utc).isoformat()}
    (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    spark = (SparkSession.builder.appName("OpenAQ-EDA-dim-location")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.sql.files.maxPartitionBytes", str(256 << 20))
             .config("spark.sql.files.openCostInBytes", str(1 << 20))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = read_compact(spark, args.input)
        valid = valid_coordinate()
        pairs_path = report / "_coordinate_pairs"
        (df.select(
            "location_id", "location", "datetime_ts",
            F.when(valid, F.round("lat_num", 5)).alias("lat"),
            F.when(valid, F.round("lon_num", 5)).alias("lon"),
        ).groupBy("location_id", "lat", "lon").agg(
            F.count("*").alias("n"), F.max("location").alias("location"),
            F.min("datetime_ts").alias("first_utc"), F.max("datetime_ts").alias("last_utc"),
        ).write.parquet("file://" + str(pairs_path)))
        pairs = spark.read.parquet("file://" + str(pairs_path))

        totals = pairs.groupBy("location_id").agg(
            F.sum("n").alias("rows_total"),
            F.sum(F.when(F.col("lat").isNotNull(), F.col("n")).otherwise(0)).alias("rows_valid_coord"),
            F.count(F.col("lat")).alias("n_coord_pairs"),
            F.max("location").alias("location_name"),
            F.min("first_utc").alias("first_utc"), F.max("last_utc").alias("last_utc"),
        )
        good = pairs.filter(F.col("lat").isNotNull())
        best = Window.partitionBy("location_id").orderBy(F.desc("n"), "lat", "lon")
        ref = (good.withColumn("r", F.row_number().over(best)).filter("r = 1")
               .select("location_id", F.col("lat").alias("lat_ref"),
                       F.col("lon").alias("lon_ref"), F.col("n").alias("ref_rows")))
        dist = good.join(ref, "location_id").withColumn(
            "km", haversine_km(F.col("lat_ref"), F.col("lon_ref"), F.col("lat"), F.col("lon")))
        # Weighted p95: smallest distance whose cumulative row share reaches 95%.
        cum = Window.partitionBy("location_id").orderBy("km").rowsBetween(Window.unboundedPreceding, 0)
        whole = Window.partitionBy("location_id")
        dist = (dist.withColumn("cum", F.sum("n").over(cum))
                .withColumn("valid", F.sum("n").over(whole)))
        spread = dist.groupBy("location_id").agg(
            F.min(F.when(F.col("cum") >= 0.95 * F.col("valid"), F.col("km"))).alias("p95_km"),
            F.max("km").alias("max_km"),
            (F.sum(F.when(F.col("km") <= 1.0, F.col("n")).otherwise(0)) / F.first("valid"))
            .alias("share_within_1km"),
        )
        dim = (totals.join(ref, "location_id", "left").join(spread, "location_id", "left")
               .withColumn("ref_share", F.col("ref_rows") / F.col("rows_valid_coord"))
               .withColumn("location_kind", F.when(F.col("lat_ref").isNull(), "no_valid_coord")
                           .when(F.col("p95_km") > args.mobile_km, "mobile").otherwise("fixed"))
               .withColumn("location_id_num", F.col("location_id").cast("long")))
        dim.write.mode("overwrite").parquet("file://" + str(output))
        dim = spark.read.parquet("file://" + str(output))

        save_csv(dim.groupBy("location_kind").agg(
            F.count("*").alias("locations"), F.sum("rows_total").alias("rows_total"),
            F.sum("rows_valid_coord").alias("rows_valid_coord"),
        ), report, "kind_summary")
        km = F.col("p95_km")
        bucket = (F.when(km.isNull(), "none").when(km == 0, "0").when(km <= 0.1, "<=0.1")
                  .when(km <= 1, "<=1").when(km <= 10, "<=10").when(km <= 100, "<=100")
                  .otherwise(">100"))
        save_csv(dim.groupBy(bucket.alias("p95_km_bucket")).agg(
            F.count("*").alias("locations"), F.sum("rows_total").alias("rows_total"),
        ), report, "spread_distribution")
        save_csv(dim.orderBy(F.desc("rows_total")).limit(200), report, "top_locations")
        overview = dim.agg(
            F.count("*").alias("locations"), F.sum("rows_total").alias("rows_total"),
            F.sum("rows_valid_coord").alias("rows_valid_coord"),
            F.sum(F.when(F.col("location_id_num").isNull(), 1).otherwise(0)).alias("non_numeric_location_ids"),
        ).first().asDict()
        meta.update(status="complete", overview=overview)
    except Exception as exc:
        meta.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        meta["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (report / "run.json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
