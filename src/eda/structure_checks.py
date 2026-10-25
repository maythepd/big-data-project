"""structure checks reports from one full scan.

The only full pass writes rows that carry at least one flag to Parquet
(`_diagnostic_rows`, on disk). Per-flag totals and every report are then
computed from that small table; total rows come from a cheap count().
No cache/persist: /tmp is RAM-backed tmpfs on this machine.
"""

from functools import reduce
from operator import or_

from pyspark.sql import Window, functions as F

from .common import RAW_COLUMNS, TYPED_COLUMNS, count_if, save_csv
from .quality_flags import datetime_pattern, quality_flags, repaired_datetime

PROVENANCE = ["source_block", "year", "month"]
COORDINATE_ISSUES = [
    "lat_missing", "lon_missing", "lat_cast_failed", "lon_cast_failed",
    "lat_num_nonfinite", "lon_num_nonfinite", "latitude_out_of_range",
    "longitude_out_of_range", "swap_candidate", "coord_sentinel_200",
    "coord_zero", "coord_lat_eq_lon",
]


def write_diagnostic_rows(df, path):
    definitions = quality_flags()
    names = list(definitions)
    flagged = df.select("*", *[expr.alias(name) for name, expr in definitions.items()])
    any_issue = reduce(or_, [F.col(name) for name in names])
    extra = [c for c in PROVENANCE if c in df.columns]
    issues = F.filter(
        F.array(*[F.when(F.col(name), F.lit(name)) for name in names]),
        lambda item: item.isNotNull(),
    )
    (flagged.filter(any_issue)
        .select(*RAW_COLUMNS, *TYPED_COLUMNS, *extra, *names, issues.alias("issues"))
        .write.mode("errorifexists").parquet("file://" + str(path)))
    return names


def run(df, output, sample_rows=20, top_patterns=50):
    spark = df.sparkSession
    total = df.count()
    names = write_diagnostic_rows(df, output / "_diagnostic_rows")
    diagnostic = spark.read.parquet("file://" + str(output / "_diagnostic_rows"))
    if "source_block" not in diagnostic.columns:
        diagnostic = diagnostic.withColumn("source_block", F.lit(None).cast("string"))
    for name in ["year", "month"]:
        if name not in diagnostic.columns:
            diagnostic = diagnostic.withColumn(name, F.lit(None).cast("int"))

    counts = diagnostic.agg(
        F.count("*").alias("rows_with_any_issue"),
        *[count_if(F.col(name), name) for name in names],
    ).first().asDict()
    overview = {"rows": total, **counts}

    summary = [("all_rows", total, 1.0 if total else 0.0)] + [
        (name, overview[name], overview[name] / total if total else 0.0)
        for name in ["rows_with_any_issue", *names]
    ]
    save_csv(spark.createDataFrame(
        summary, "issue string, rows long, fraction_of_all_rows double"
    ), output, "quality_summary")
    save_csv(diagnostic.groupBy("year", "month").agg(
        F.count("*").alias("rows_with_any_issue"),
        *[count_if(F.col(name), name) for name in names],
    ).orderBy("year", "month"), output, "quality_by_month")
    save_csv(
        diagnostic.withColumn("issue_combination", F.concat_ws("|", "issues"))
        .groupBy("issue_combination").count().orderBy(F.desc("count")),
        output, "issue_overlap",
    )

    # Long form: one measurement contributes once per matching issue.
    long = diagnostic.withColumn("issue", F.explode("issues")).drop("issues")
    save_csv(long.groupBy("issue", "source_block").count(), output, "issues_by_file")
    save_csv(long.groupBy("issue", "location_id", "sensors_id").count(),
             output, "issues_by_sensor")

    # Bounded diagnostic examples, not a random/statistical sample.
    rank = Window.partitionBy("issue").orderBy(
        "source_block", "location_id", "sensors_id", "datetime", "value"
    )
    samples = long.withColumn("sample_rank", F.row_number().over(rank)).filter(
        F.col("sample_rank") <= sample_rows
    )
    save_csv(samples.select("issue", "sample_rank", *RAW_COLUMNS,
                            *TYPED_COLUMNS, "source_block"),
             output, "diagnostic_samples")

    # Exact-string counts can be numerous; save only top K per issue.
    raw_value = F.lit(None).cast("string")
    for raw in ["datetime", "value", "lat", "lon", "parameter", "units",
                "location_id", "sensors_id"]:
        raw_value = F.when(
            F.col("issue").isin(f"{raw}_missing", f"{raw}_cast_failed"), F.col(raw)
        ).otherwise(raw_value)
    parse = long.filter(F.col("issue").rlike("_(missing|cast_failed)$")).withColumn(
        "raw_value", raw_value
    ).withColumn("pattern", F.when(
        F.col("issue").startswith("datetime_"), datetime_pattern()
    ).otherwise(F.lit("not_datetime")))
    save_csv(parse.groupBy("issue", "pattern").count(), output, "parse_error_patterns")
    values = parse.groupBy("issue", "pattern", "raw_value").count()
    top = Window.partitionBy("issue").orderBy(F.desc("count"), "raw_value")
    save_csv(values.withColumn("rank", F.row_number().over(top)).filter(
        F.col("rank") <= top_patterns
    ), output, "parse_error_values")

    save_csv(diagnostic.filter("suspected_column_shift").groupBy(
        "parameter", "units", "datetime_cast_failed", "value_cast_failed",
        "lat_cast_failed", "lon_cast_failed", "split_record_head",
    ).count().orderBy(F.desc("count")), output, "suspected_column_shift")

    # Offset repair check: the offset after repair should follow DST
    # (-02:30 summer, -03:30 winter in Newfoundland).
    bad = diagnostic.filter("datetime_bad_offset")
    repaired = repaired_datetime()
    save_csv(bad.groupBy(
        F.regexp_extract("datetime", r"([+-]\d{2}:-?\d{2})$", 1).alias("offset_raw"),
        F.month(repaired).alias("month_of_year"),
    ).agg(
        F.count("*").alias("rows"),
        count_if(F.col("datetime_offset_repairable"), "repairable_rows"),
        F.countDistinct("location_id").alias("locations"),
        F.min(repaired).alias("first_utc"), F.max(repaired).alias("last_utc"),
    ).orderBy("offset_raw", "month_of_year"), output, "datetime_offset_repair")

    coord = diagnostic.filter(F.exists("issues", lambda x: x.isin(*COORDINATE_ISSUES)))
    save_csv(coord.groupBy("location_id", "sensors_id").agg(
        F.count("*").alias("coordinate_issue_rows"),
        *[count_if(F.col(name), name) for name in COORDINATE_ISSUES],
        F.min("datetime_ts").alias("first_issue_utc"),
        F.max("datetime_ts").alias("last_issue_utc"),
        F.min("lat_num").alias("min_lat"), F.max("lat_num").alias("max_lat"),
        F.min("lon_num").alias("min_lon"), F.max("lon_num").alias("max_lon"),
    ), output, "coordinate_errors_by_sensor")
    save_csv(coord.groupBy("location_id").agg(
        F.count("*").alias("coordinate_issue_rows"),
        F.countDistinct("sensors_id").alias("sensors"),
        *[count_if(F.col(name), name) for name in COORDINATE_ISSUES],
        F.min("datetime_ts").alias("first_issue_utc"),
        F.max("datetime_ts").alias("last_issue_utc"),
    ).orderBy(F.desc("coordinate_issue_rows")), output, "coordinate_errors_by_location")
    return overview
