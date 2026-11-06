"""Kafka -> Spark Structured Streaming: HDFS archive, MongoDB latest_readings, Silver.

Each query has its own checkpoint and restarts on failure without stopping the others.
A new checkpoint starts from STREAM_STARTING_OFFSETS[_ARCHIVE|_LATEST|_SILVER]
(earliest, latest or JSON offsets from src.streaming.checkpoint_offsets).

    docker compose -f docker-compose.spark.yml up -d --build
"""
from datetime import datetime, timezone
import os
import time

from pymongo import ASCENDING, MongoClient, UpdateOne
from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.common.config import KAFKA_BOOTSTRAP_SERVERS, MONGO_DB, TOPIC_NAME, mongo_uri

HDFS = os.getenv("HDFS_URI", "hdfs://namenode:8020").rstrip("/")
ARCHIVE_PATH = f"{HDFS}/openaq/stream/readings"
ARCHIVE_CHECKPOINT = f"{HDFS}/openaq/checkpoints/stream_readings"
LATEST_CHECKPOINT = f"{HDFS}/openaq/checkpoints/latest_readings"
SILVER_ROOT = os.getenv("STREAM_SILVER_ROOT", f"{HDFS}/openaq/silver/stream/v1")
SILVER_CHECKPOINT = os.getenv("STREAM_SILVER_CHECKPOINT", f"{HDFS}/openaq/checkpoints/silver_stream_v1")

# the fields schema.make_message writes
MESSAGE_SCHEMA = StructType([
    StructField("source", StringType()),
    StructField("station_id", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("parameter", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("datetime", StringType()),
    StructField("ingested_at", StringType()),
])
READING_KEY = ["station_id", "parameter", "event_time"]

# longer than the producers' 1-day max age; older readings are dropped as late
DEDUPE_WATERMARK = "25 hours"

LATEST_FIELDS = ["source", "lat", "lon", "value", "unit", "event_time", "ingested_at"]
_mongo = None

# restart delay doubles from 1 min up to 30; reset after 30 min without failure
RESTART_DELAY_S = int(os.getenv("STREAM_RESTART_DELAY_S", "60"))
RESTART_DELAY_MAX_S = 30 * 60
HEALTHY_AFTER_S = 30 * 60


def log(msg):
    print(f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} [consumer] {msg}", flush=True)


def starting_offsets(query):
    """Kafka start position, used only when the query's checkpoint is new."""
    return (os.getenv(f"STREAM_STARTING_OFFSETS_{query.upper()}")
            or os.getenv("STREAM_STARTING_OFFSETS") or "earliest")


def kafka_messages(spark, query):
    """One Kafka source per query, so each keeps its own offsets."""
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", TOPIC_NAME)
            .option("startingOffsets", starting_offsets(query))
            .load())


def mongo():
    """Shared client for the driver process; creates the unique index once."""
    global _mongo
    if _mongo is None:
        _mongo = MongoClient(mongo_uri(), tz_aware=True)[MONGO_DB]
        _mongo.latest_readings.create_index([("station_id", ASCENDING), ("parameter", ASCENDING)],
                                            unique=True)
    return _mongo


def newest_update(row):
    """Upsert that only overwrites the stored reading with a newer event_time."""
    newer = {"$gt": [row["event_time"], "$event_time"]}
    fields = {k: {"$cond": [newer, {"$literal": row[k]}, f"${k}"]} for k in LATEST_FIELDS}
    fields["updated_at"] = {"$cond": [newer, "$$NOW", "$updated_at"]}
    return UpdateOne({"station_id": row["station_id"], "parameter": row["parameter"]},
                     [{"$set": fields}], upsert=True)


def parse(messages):
    """Kafka records -> one typed row per reading; malformed messages are dropped."""
    r = F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA)
    return (messages.select(r.alias("r")).select("r.*")
            .withColumn("event_time", F.try_to_timestamp("datetime"))
            .withColumn("ingested_at", F.try_to_timestamp("ingested_at"))
            .drop("datetime")
            .where(F.col("station_id").isNotNull() & F.col("parameter").isNotNull()
                   & F.col("value").isNotNull() & F.col("event_time").isNotNull())
            # a future timestamp would push the watermark ahead
            .where(F.col("event_time") <= F.current_timestamp() + F.expr("INTERVAL 1 HOUR")))


def latest(readings):
    """Newest reading per station and parameter."""
    reading = F.struct("source", "lat", "lon", "value", "unit", "event_time", "ingested_at")
    return (readings.groupBy("station_id", "parameter")
            .agg(F.max_by(reading, "event_time").alias("r"))
            .select("station_id", "parameter", "r.*"))


def start_archive(readings):
    return (readings
            .withWatermark("event_time", DEDUPE_WATERMARK)
            .dropDuplicatesWithinWatermark(READING_KEY)
            .withColumn("date", F.to_date("event_time"))
            .writeStream.format("parquet")
            .queryName("archive")
            .option("path", ARCHIVE_PATH)
            .option("checkpointLocation", ARCHIVE_CHECKPOINT)
            .partitionBy("date")
            .trigger(processingTime="5 minutes")    # fewer, larger files than a short trigger
            .start())


def upsert_latest(batch, batch_id):
    """Write the newest reading per station and parameter in one micro-batch to MongoDB."""
    # collect() returns local-time naive datetimes, so go through epoch millis
    rows = [r.asDict() for r in latest(batch)
            .withColumn("event_time", F.unix_millis("event_time"))
            .withColumn("ingested_at", F.unix_millis("ingested_at"))
            .collect()]
    if not rows:
        return
    for r in rows:
        for k in ("event_time", "ingested_at"):
            if r[k] is not None:
                r[k] = datetime.fromtimestamp(r[k] / 1000, tz=timezone.utc)
    mongo().latest_readings.bulk_write([newest_update(r) for r in rows], ordered=False)
    print(f"latest_readings: batch {batch_id} sent {len(rows)} station/parameter rows", flush=True)


def start_latest(readings):
    return (readings.writeStream
            .queryName("latest")
            .foreachBatch(upsert_latest)
            # the upsert is idempotent, so retried batches are harmless
            .option("checkpointLocation", LATEST_CHECKPOINT)
            .trigger(processingTime="1 minute")
            .start())


def start_silver(messages):
    """Clean OpenAQ messages into Silver batches (atomic, idempotent on retry)."""
    from src.streaming.silver_batch_writer import SilverStreamWriter
    writer = SilverStreamWriter(messages.sparkSession, SILVER_ROOT,
        dim_path=os.getenv("STREAM_DIM_LOCATION") or None,
        sensor_units_path=os.getenv("STREAM_SENSOR_UNITS") or None)
    return (messages.writeStream.foreachBatch(writer.write_batch)
            .queryName("silver")
            .option("checkpointLocation", SILVER_CHECKPOINT)
            .trigger(processingTime=os.getenv("STREAM_SILVER_TRIGGER", "1 minute"))
            .start())


def main():
    spark = (SparkSession.builder.appName("air-quality-consumer")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.codegen.hugeMethodLimit", "8000")
             # small stream: one partition; fixed by the archive checkpoint
             .config("spark.sql.shuffle.partitions", "1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    supervise(spark, {
        "archive": lambda: start_archive(parse(kafka_messages(spark, "archive"))),
        "latest": lambda: start_latest(parse(kafka_messages(spark, "latest"))),
        "silver": lambda: start_silver(kafka_messages(spark, "silver")),
    })


def supervise(spark, starters):
    """Restart failed queries after a delay; other errors end the process."""
    queries, started, failures, retry_at = {}, {}, {}, {}

    def failed(name, exc):
        failures[name] = failures.get(name, 0) + 1
        delay = min(RESTART_DELAY_S * 2 ** (failures[name] - 1), RESTART_DELAY_MAX_S)
        retry_at[name] = time.time() + delay
        queries.pop(name, None)
        others = ", ".join(sorted(queries)) or "none"
        log(f"{name}: FAILED (failure {failures[name]}), restart in {delay}s, still running: {others}; "
            f"{type(exc).__name__}: {str(exc).strip().splitlines()[0][:400] if str(exc).strip() else ''}")

    def start(name):
        try:
            queries[name] = starters[name]()
            started[name] = time.time()
            log(f"{name}: started (Kafka start if the checkpoint is new: {starting_offsets(name)[:80]})")
        except Exception as exc:          # e.g. Silver contract changed
            failed(name, exc)

    for name in starters:
        start(name)
    while True:
        try:
            spark.streams.awaitAnyTermination(timeout=30)
        except StreamingQueryException:
            pass                          # handled below, query by query
        for name, q in list(queries.items()):
            if not q.isActive:
                failed(name, q.exception() or RuntimeError("query stopped"))
        spark.streams.resetTerminated()
        now = time.time()
        for name in list(queries):
            if failures.get(name) and now - started[name] > HEALTHY_AFTER_S:
                log(f"{name}: healthy for {HEALTHY_AFTER_S // 60} min, failure count reset")
                failures[name] = 0
        for name, at in list(retry_at.items()):
            if now >= at:
                del retry_at[name]
                start(name)


if __name__ == "__main__":
    main()
