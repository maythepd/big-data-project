"""Spark integration checks for overlapping flags, bounds, and report totals."""

from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F

from src.eda.common import RAW_COLUMNS, read_compact, read_near_silver
from src.eda.structure_checks import run
from src.eda.quality_flags import with_quality_flags


spark = (SparkSession.builder.appName("Check-EDA-Structure")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
try:
    # All expressions execute in JVM; no Python worker dependency for fixtures.
    base = spark.range(5).select(
        F.col("id").cast("string").alias("location_id"),
        F.lit("sensor").alias("sensors_id"), F.lit("station").alias("location"),
        F.when(F.col("id") == 1, "not-a-date").otherwise("2024-01-01T00:00:00Z").alias("datetime"),
        F.when(F.col("id") == 1, "120").when(F.col("id") == 2, "bad")
        .otherwise("90").alias("lat"),
        F.when(F.col("id") == 1, "20").otherwise("180").alias("lon"),
        F.when(F.col("id") == 2, "5.0").otherwise("pm25").alias("parameter"),
        F.when(F.col("id") == 2, "pm25").otherwise("µg/m³").alias("units"),
        F.when(F.col("id") == 2, F.lit(None).cast("string"))
        .when(F.col("id") == 3, "NaN").when(F.col("id") == 4, "Infinity")
        .otherwise("1").alias("value"),
    )
    for raw in ["value", "lat", "lon"]:
        base = base.withColumn(f"{raw}_num", F.col(raw).cast("double"))
    # structure checks findings: "-03:-30" offset, split-record head, 200/200, lat == lon.
    extra = spark.createDataFrame([
        ("5", "s5", "St. John's", "2024-03-10T00:00:01-03:-30", "47.5", "-52.7", "pm25", "µg/m³", "3"),
        ("6", "s6", "group D", None, None, None, None, None, None),
        ("7", "s7", "x", "2024-01-01T00:00:00Z", "200", "200", "pm25", "µg/m³", "1"),
        ("8", "s8", "x", "2024-01-01T00:00:00Z", "127.5", "127.5", "pm25", "µg/m³", "1"),
    ], ", ".join(f"{c} string" for c in RAW_COLUMNS))
    base = base.select(*RAW_COLUMNS).unionByName(extra)
    for raw in ["value", "lat", "lon"]:
        base = base.withColumn(f"{raw}_num", F.col(raw).cast("double"))
    base = base.withColumn("datetime_ts", F.to_timestamp("datetime"))
    with tempfile.TemporaryDirectory(prefix="structure-check-") as folder:
        root = Path(folder)
        base.write.parquet((root / "input").as_uri())
        df = read_near_silver(spark, sorted((root / "input").glob("*.parquet")))
        flags = {r.location_id: r.asDict() for r in with_quality_flags(df).collect()}
        assert flags["0"]["issues"] == []  # inclusive boundaries
        assert flags["1"]["swap_candidate"] and flags["1"]["datetime_cast_failed"]
        assert flags["2"]["suspected_column_shift"] and flags["2"]["lat_cast_failed"]
        assert flags["2"]["value_missing"] and not flags["2"]["value_cast_failed"]
        assert flags["3"]["value_num_nonfinite"] and flags["4"]["value_num_nonfinite"]
        assert flags["5"]["datetime_bad_offset"] and flags["5"]["datetime_offset_repairable"]
        assert flags["5"]["datetime_cast_failed"] and not flags["0"]["datetime_bad_offset"]
        assert flags["6"]["split_record_head"] and not flags["5"]["split_record_head"]
        assert flags["7"]["coord_sentinel_200"] and not flags["7"]["coord_lat_eq_lon"]
        assert flags["8"]["coord_lat_eq_lon"] and not flags["8"]["swap_candidate"]
        report = root / "reports"
        report.mkdir()
        overview = run(df, report, sample_rows=1, top_patterns=1)
        assert overview["rows"] == 9 and overview["rows_with_any_issue"] == 8
        assert overview["value_num_nonfinite"] == 2 and overview["datetime_bad_offset"] == 1
        overlap = spark.read.option("header", True).csv((report / "issue_overlap").as_uri())
        assert overlap.agg(F.sum(F.col("count").cast("long"))).first()[0] == 8
        samples = spark.read.option("header", True).csv((report / "diagnostic_samples").as_uri())
        assert samples.groupBy("issue").count().filter("count > 1").count() == 0
        assert samples.filter("source_block IS NULL").count() == 0
        offsets = spark.read.option("header", True).csv((report / "datetime_offset_repair").as_uri()).first()
        assert offsets["offset_raw"] == "-03:-30" and offsets["repairable_rows"] == "1"
        # block recovery layout: year=/month= partitions + _null_ts, provenance kept.
        layout = root / "compact"
        typed = df.withColumn("source_block", F.concat(F.lit("blk_"), F.col("location_id")))
        (typed.filter("datetime_ts IS NOT NULL").withColumn("year", F.year("datetime_ts"))
         .withColumn("month", F.month("datetime_ts")).write.partitionBy("year", "month")
         .parquet("file://" + str(layout)))
        typed.filter("datetime_ts IS NULL").write.parquet("file://" + str(layout / "_null_ts"))
        compact = read_compact(spark, layout)
        assert compact.count() == 9 and compact.filter("year IS NULL").count() == 3
        assert compact.filter("source_block IS NULL").count() == 0
        # Empty diagnostics must still emit valid, empty report schemas.
        clean = root / "clean_reports"
        clean.mkdir()
        clean_overview = run(df.filter("location_id = '0'"), clean, 1, 1)
        assert clean_overview["rows_with_any_issue"] == 0
    print("PASS: structure checks flags, overlap totals, sample bounds, provenance, empty diagnostics")
finally:
    spark.stop()
