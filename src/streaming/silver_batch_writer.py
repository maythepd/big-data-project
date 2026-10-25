"""Incremental OpenAQ Silver, committed atomically per Kafka micro-batch.

Each committed batch has raw/, measurements/, quarantine/, and report.json.
Only batches/ is visible to readers; _staging/ is disposable. A single writer
owns an output root. Cross-batch keys are checked against committed STREAM
Silver, never against the historical dataset. Late events are retained.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T

from src.silver.rules import DEFAULT_RULES, apply_rules, dedup, finalize, load_rules

KEY = ["source", "sensors_id", "datetime_utc"]
EVENT_SCHEMA = T.StructType([
    T.StructField(name, T.StringType()) for name in [
        "schema_version", "source", "station_id", "location_id", "sensors_id",
        "location_name", "lat", "lon", "parameter", "parameter_raw", "unit",
        "units_raw", "value", "value_raw", "datetime", "datetime_local_raw",
        "ingested_at", "_corrupt_record",
    ]
])
PROVENANCE = ["raw_json", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp"]


def decode(messages):
    """Keep every Kafka payload and its offsets, including malformed/legacy JSON."""
    raw = messages.select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"), F.col("timestamp").alias("kafka_timestamp"),
    )
    parsed = raw.withColumn("r", F.from_json("raw_json", EVENT_SCHEMA))
    df = parsed.select(*PROVENANCE, "r.*")
    df = df.withColumn("envelope_reason",
        F.when(F.col("_corrupt_record").isNotNull() | F.col("source").isNull(), "malformed_message")
         .when(F.col("source") != "openaq", "unsupported_source")
         .when(F.col("schema_version").isNull() | (F.col("schema_version") != "2"), "legacy_message")
         .when(F.col("sensors_id").cast("long").isNull(), "no_sensor_id"))
    return (df.withColumn("location_id", F.col("location_id").cast("long").cast("string"))
              .withColumn("sensors_id", F.col("sensors_id").cast("long").cast("string"))
              .withColumn("value", F.col("value_raw"))
              .withColumn("units", F.col("units_raw"))
              .withColumn("parameter", F.col("parameter_raw"))
              .drop("value_raw")
              .withColumn("datetime", F.coalesce("datetime_local_raw", "datetime"))
              .withColumn("datetime_ts", F.try_to_timestamp("datetime"))
              .withColumn("value_num", F.col("value").cast("double"))
              .withColumn("lat_num", F.col("lat").cast("double"))
              .withColumn("lon_num", F.col("lon").cast("double"))
              .withColumn("location", F.col("location_name"))
              .withColumn("source_block", F.concat_ws(":", "kafka_topic", "kafka_partition", "kafka_offset"))
              .withColumn("ingested_at", F.try_to_timestamp("ingested_at")))


def quarantine_frame(df, reason):
    return df.select("source", "station_id", *PROVENANCE, reason.alias("quarantine_reason"))


def clean_batch(messages, rules, dim, build_utc, run_id, sensor_units=None):
    decoded = decode(messages)
    envelope_bad = decoded.filter(F.col("envelope_reason").isNotNull())
    eligible = decoded.filter(F.col("envelope_reason").isNull()).drop("envelope_reason")
    measured, bad_keys = apply_rules(eligible, rules, dim, build_utc, sensor_units)
    # Historical data had no nonfinite numeric values. Streaming explicitly
    # handles missing, nonnumeric, NaN and infinity without changing v1 history.
    number = F.col("value_num")
    extra_flag = (F.when(F.col("value_raw").isNull(), "value_missing")
                  .when(number.isNull(), "value_not_numeric")
                  .when(F.isnan(number) | (F.abs(number) == float("inf")), "value_nonfinite"))
    measured = measured.withColumn("_numeric_issue", extra_flag)
    # Include stream-only hard failures in the shared tie-break's hard priority.
    measured = measured.withColumn("extreme", F.col("extreme") | F.col("_numeric_issue").isNotNull())
    measured = measured.withColumn("value", F.when(F.col("_numeric_issue").isNull(), F.col("value")))
    ranked, keep = dedup(measured.repartition("sensors_id"))
    out = finalize(ranked.filter(keep))
    # 'extreme' is only appropriate for actual extreme raw values.
    out = out.withColumn("dq_flags", F.when(F.col("_numeric_issue").isNotNull(),
        F.array_union(F.array_remove("dq_flags", "extreme"), F.array("_numeric_issue")))
        .otherwise(F.col("dq_flags")))
    local = F.col("datetime")
    offset = F.regexp_extract(local, r"([+-])(\d{2}):-?(\d{2})$", 0)
    minutes = (F.regexp_extract(local, r"([+-])(\d{2}):-?(\d{2})$", 2).cast("int") * 60
               + F.regexp_extract(local, r"([+-])(\d{2}):-?(\d{2})$", 3).cast("int"))
    minutes = minutes * F.when(offset.startswith("-"), -1).otherwise(1)
    out = out.select(
        F.col("location_id").cast("long").alias("location_id"),
        F.col("sensors_id").cast("long").alias("sensors_id"),
        F.col("location").alias("location_name"), "datetime_utc",
        local.alias("datetime_local_raw"),
        F.when(local.endswith("Z"), 0).when(offset != "", minutes).alias("utc_offset_min"),
        "lat", "lon", "coord_source", F.col("parameter_std").alias("parameter"),
        F.col("unit_std").alias("unit"), "value", F.col("parameter").alias("parameter_raw"),
        F.col("units").alias("units_raw"), "value_raw", "dq_flags", "dq_status", "dup_count",
        "source_block", F.lit(rules["version"]).alias("rules_version"),
        F.lit(run_id).alias("build_run_id"), "source", "station_id", "ingested_at",
        F.lit("stream").alias("origin"), *PROVENANCE,
        F.year("datetime_utc").alias("year"), F.month("datetime_utc").alias("month"),
    )
    quarantine = quarantine_frame(envelope_bad, F.col("envelope_reason")).unionByName(
        quarantine_frame(bad_keys, F.col("quarantine_reason")))
    return out, quarantine


class SilverStreamWriter:
    def __init__(self, spark, root, rules_path=DEFAULT_RULES, dim_path=None, sensor_units_path=None):
        self.spark, self.root = spark, root.rstrip("/")
        self.rules = load_rules(rules_path)
        self.rules_hash = hashlib.sha256(Path(rules_path).read_bytes()).hexdigest()
        self.path = spark._jvm.org.apache.hadoop.fs.Path
        self.fs = self.path(self.root).getFileSystem(spark._jsc.hadoopConfiguration())
        if self.fs.getUri().getScheme() not in ("file", "hdfs"):
            raise ValueError("Atomic Silver publication requires file:// or hdfs://")
        self.fs.mkdirs(self.path(self.root))
        contract = {"pipeline": "openaq-stream-v1", "rules_sha256": self.rules_hash,
                    "dim_location": dim_path, "sensor_units": sensor_units_path}
        contract_path = f"{self.root}/_contract.json"
        if self.exists(contract_path):
            if self.read_json(contract_path) != contract:
                raise ValueError("Silver contract changed; select a new output root and checkpoint")
        else:
            self.write_json(contract_path, contract)
        self.dim = (spark.read.parquet(dim_path) if dim_path else spark.createDataFrame([],
            "location_id string, location_kind string, lat_ref double, lon_ref double"))
        self.sensor_units = spark.read.parquet(sensor_units_path) if sensor_units_path else None

    def exists(self, name):
        return self.fs.exists(self.path(name))

    def read_json(self, name):
        stream = self.fs.open(self.path(name))
        try:
            return json.loads(self.spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
        finally:
            stream.close()

    def write_json(self, name, data):
        stream = self.fs.create(self.path(name), False)
        try:
            stream.write(bytearray(json.dumps(data, sort_keys=True, default=str).encode("utf-8")))
        finally:
            stream.close()

    def batch_paths(self):
        base = f"{self.root}/batches"
        if not self.exists(base):
            return []
        return sorted(str(s.getPath()) for s in self.fs.listStatus(self.path(base))
                      if s.isDirectory() and s.getPath().getName().startswith("batch="))

    def read_measurements(self):
        paths = [f"{p}/measurements" for p in self.batch_paths()]
        if not paths:
            raise ValueError("No Silver batches committed yet")
        return self.spark.read.parquet(*paths)

    def publish(self, stage, target):
        self.fs.mkdirs(self.path(f"{self.root}/batches"))
        if self.exists(target) or not self.fs.rename(self.path(stage), self.path(target)):
            raise RuntimeError(f"Cannot atomically publish {target}; use one writer per root")

    def write_batch(self, batch, batch_id):
        ranges = [r.asDict() for r in batch.groupBy("topic", "partition").agg(
            F.count("*").alias("rows"), F.min("offset").alias("first_offset"),
            F.max("offset").alias("last_offset"),
            F.sum(F.xxhash64("value").cast("decimal(38,0)")).cast("string").alias("payload_hash_sum"),
        ).orderBy("topic", "partition").collect()]
        if not ranges:
            return None
        rows_in = sum(r["rows"] for r in ranges)
        target = f"{self.root}/batches/batch={batch_id:020d}"
        stage = f"{self.root}/_staging/batch={batch_id:020d}"
        if self.exists(target):
            report = self.read_json(f"{target}/report.json")
            if report["offset_ranges"] != ranges or report["rules_sha256"] != self.rules_hash:
                raise RuntimeError("Committed batch differs: checkpoint/output/rules must be migrated together")
            return report
        if self.exists(stage):
            self.fs.delete(self.path(stage), True)
        build_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        run_id = f"stream_v1_batch_{batch_id}"
        out, q = clean_batch(batch, self.rules, self.dim, build_utc, run_id, self.sensor_units)
        out = out.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            before = out.count()
            duplicates = conflicts = 0
            if self.batch_paths():
                previous = self.read_measurements()
                # Restrict the cross-batch lookup to months touched by this batch.
                months = [r[0] for r in out.select((F.col("year") * 100 + F.col("month")).alias("ym")).distinct().collect()]
                previous = previous.filter((F.col("year") * 100 + F.col("month")).isin(months))
                old = previous.select(*KEY, F.col("parameter").alias("_old_parameter"),
                    F.col("unit").alias("_old_unit"), F.col("value").alias("_old_value"),
                    F.lit(True).alias("_exists"))
                joined = out.join(old, KEY, "left")
                duplicate = F.col("_exists").isNotNull()
                conflict = duplicate & (~F.col("parameter").eqNullSafe(F.col("_old_parameter"))
                    | ~F.col("unit").eqNullSafe(F.col("_old_unit"))
                    | ~F.col("value").eqNullSafe(F.col("_old_value")))
                duplicates = joined.filter(duplicate).count()
                conflict_rows = joined.filter(conflict)
                conflicts = conflict_rows.count()
                q = q.unionByName(quarantine_frame(conflict_rows, F.lit("duplicate_conflict")))
                fresh = joined.filter(~duplicate).select(*out.columns)
            else:
                fresh = out
            # Raw Kafka bytes + metadata are written even when parsing fails.
            batch.select(F.col("value").cast("string").alias("raw_json"),
                "topic", "partition", "offset", "timestamp").coalesce(1).write.parquet(f"{stage}/raw")
            fresh.coalesce(1).sortWithinPartitions("sensors_id", "datetime_utc").write.option(
                "compression", "zstd").parquet(f"{stage}/measurements")
            q.coalesce(1).write.option("compression", "zstd").parquet(f"{stage}/quarantine")
            written = self.spark.read.parquet(f"{stage}/measurements")
            statuses = {r["dq_status"]: r["count"] for r in written.groupBy("dq_status").count().collect()}
            silver_rows = sum(statuses.values())
            q_rows = self.spark.read.parquet(f"{stage}/quarantine").count()
            # Conflicts count as quarantine, identical repeats as duplicates.
            removed = rows_in - silver_rows - q_rows
            report = {"batch_id": batch_id, "build_utc": build_utc, "rules_version": self.rules["version"],
                "rules_sha256": self.rules_hash, "offset_ranges": ranges, "rows_in": rows_in,
                "silver": silver_rows, "quarantine": q_rows, "dedup_removed": removed,
                "cross_batch_duplicates": duplicates, "conflicts": conflicts,
                "status_totals": statuses, "within_batch_candidates": before}
            if removed < 0:
                raise RuntimeError("Silver batch reconciliation failed")
            self.write_json(f"{stage}/report.json", report)
            self.publish(stage, target)
            print("silver_stream: " + json.dumps(report, default=str), flush=True)
            return report
        finally:
            out.unpersist()
