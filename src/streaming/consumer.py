"""Kafka -> Spark Structured Streaming: HDFS archive, MongoDB latest_readings, Silver.

Each query has its own checkpoint and restarts on failure without stopping the others.
A new checkpoint starts from STREAM_STARTING_OFFSETS[_ARCHIVE|_LATEST|_SILVER]
(earliest, latest or JSON offsets from src.streaming.checkpoint_offsets).

    docker compose -f docker-compose.spark.yml up -d --build

The big-data experiments run a second copy (service spark-replay) on the replay topic with
its own root; the STREAM_* variables below default to the production consumer.
"""
from datetime import datetime, timezone
import json
import os
import time

from pymongo import ASCENDING, MongoClient, UpdateOne
from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.common.config import KAFKA_BOOTSTRAP_SERVERS, MONGO_DB, TOPIC_NAME, mongo_uri

HDFS = os.getenv("HDFS_URI", "hdfs://namenode:8020").rstrip("/")
TOPIC = os.getenv("STREAM_TOPIC") or TOPIC_NAME
ROOT = (os.getenv("STREAM_HDFS_ROOT") or f"{HDFS}/openaq").rstrip("/")
ARCHIVE_PATH = f"{ROOT}/stream/readings"
ARCHIVE_CHECKPOINT = f"{ROOT}/checkpoints/stream_readings"
LATEST_CHECKPOINT = f"{ROOT}/checkpoints/latest_readings"
DEADLETTER_PATH = f"{ROOT}/stream/deadletter"
DEADLETTER_CHECKPOINT = f"{ROOT}/checkpoints/deadletter"
SILVER_ROOT = os.getenv("STREAM_SILVER_ROOT", f"{ROOT}/silver/stream/v1")
SILVER_CHECKPOINT = os.getenv("STREAM_SILVER_CHECKPOINT", f"{ROOT}/checkpoints/silver_stream_v1")
QUERIES = [q.strip() for q in (os.getenv("STREAM_QUERIES") or "archive,latest,silver,deadletter").split(",")
           if q.strip()]
# backpressure: at most this many Kafka records per micro-batch (unset = everything available)
MAX_OFFSETS_PER_TRIGGER = os.getenv("STREAM_MAX_OFFSETS_PER_TRIGGER") or None
SHUFFLE_PARTITIONS = os.getenv("STREAM_SHUFFLE_PARTITIONS") or "1"
ARCHIVE_TRIGGER = os.getenv("STREAM_ARCHIVE_TRIGGER") or "5 minutes"
PROGRESS_LOG = os.getenv("STREAM_PROGRESS_LOG") or None   # one JSON line per micro-batch

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
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
              .option("subscribe", TOPIC)
              .option("startingOffsets", starting_offsets(query)))
    if MAX_OFFSETS_PER_TRIGGER:
        reader = reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    return reader.load()


class ProgressLog(StreamingQueryListener):
    """Append each micro-batch's progress (rates, durations, lag, watermark drops) as a JSON line."""

    def __init__(self, path):
        self.path = path
        self.names = {}
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def write(self, line):
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(line) + "\n")

    def onQueryStarted(self, event):
        self.names[str(event.id)] = event.name

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        source = (p.get("sources") or [{}])[0]
        metric = lambda k: float((source.get("metrics") or {}).get(k) or 0)   # Spark reports them as text
        state = p.get("stateOperators") or []
        line = {"query": p.get("name"), "batch": p.get("batchId"), "timestamp": p.get("timestamp"),
                "rows": p.get("numInputRows"), "input_rps": p.get("inputRowsPerSecond"),
                "processed_rps": p.get("processedRowsPerSecond"), "duration_ms": p.get("durationMs"),
                "offsets_behind_max": metric("maxOffsetsBehindLatest"),
                "offsets_behind_avg": metric("avgOffsetsBehindLatest"),
                "watermark": (p.get("eventTime") or {}).get("watermark"),
                "state_rows": sum(s.get("numRowsTotal", 0) for s in state),
                "state_memory_bytes": sum(s.get("memoryUsedBytes", 0) for s in state),
                "dropped_by_watermark": sum(s.get("numRowsDroppedByWatermark", 0) for s in state)}
        self.write(line)

    def onQueryIdle(self, event):
        # Spark 3.5 reports triggers without new data here, not in onQueryProgress
        self.write({"query": self.names.get(str(event.id)), "batch": None, "timestamp": event.timestamp,
                    "rows": 0, "offsets_behind_max": 0, "idle": True})

    def onQueryTerminated(self, event):
        pass


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


def checked(messages):
    """Kafka records -> typed readings plus reject_reason (null = usable) and Kafka provenance."""
    raw = F.col("value").cast("string")
    r = F.from_json(raw, MESSAGE_SCHEMA)
    df = (messages.select(raw.alias("raw_json"), F.col("partition").alias("kafka_partition"),
                          F.col("offset").alias("kafka_offset"), F.col("timestamp").alias("kafka_timestamp"),
                          r.alias("r"))
          .select("raw_json", "kafka_partition", "kafka_offset", "kafka_timestamp", "r.*")
          .withColumn("event_time", F.try_to_timestamp("datetime"))
          .withColumn("ingested_at", F.try_to_timestamp("ingested_at")))
    reason = (F.when(F.get_json_object("raw_json", "$").isNull(), "malformed_json")
               .when(F.col("station_id").isNull(), "no_station_id")
               .when(F.col("parameter").isNull(), "no_parameter")
               .when(F.col("value").isNull(), "no_numeric_value")
               .when(F.col("event_time").isNull(), "bad_datetime")
               # a future timestamp would push the watermark ahead
               .when(F.col("event_time") > F.current_timestamp() + F.expr("INTERVAL 1 HOUR"), "future_event_time"))
    return df.withColumn("reject_reason", reason)


def parse(messages):
    """Kafka records -> one typed row per usable reading; the rest go to the dead-letter store."""
    return (checked(messages).where(F.col("reject_reason").isNull())
            .select(*[f.name for f in MESSAGE_SCHEMA if f.name != "datetime"], "event_time"))


def start_deadletter(messages):
    """Records parse() rejects, raw and with the reason, so nothing disappears silently."""
    return (checked(messages).where(F.col("reject_reason").isNotNull())
            .select("raw_json", "reject_reason", "kafka_partition", "kafka_offset", "kafka_timestamp",
                    F.to_date("kafka_timestamp").alias("date"))
            .writeStream.format("parquet")
            .queryName("deadletter")
            .option("path", DEADLETTER_PATH)
            .option("checkpointLocation", DEADLETTER_CHECKPOINT)
            .partitionBy("date")
            .trigger(processingTime=ARCHIVE_TRIGGER)
            .start())


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
            .trigger(processingTime=ARCHIVE_TRIGGER)    # long by default: fewer, larger files
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
             .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    if PROGRESS_LOG:
        spark.streams.addListener(ProgressLog(PROGRESS_LOG))

    starters = {
        "archive": lambda: start_archive(parse(kafka_messages(spark, "archive"))),
        "latest": lambda: start_latest(parse(kafka_messages(spark, "latest"))),
        "silver": lambda: start_silver(kafka_messages(spark, "silver")),
        "deadletter": lambda: start_deadletter(kafka_messages(spark, "deadletter")),
    }
    log(f"topic {TOPIC}, root {ROOT}, queries {QUERIES}, maxOffsetsPerTrigger {MAX_OFFSETS_PER_TRIGGER}, "
        f"shuffle partitions {SHUFFLE_PARTITIONS}")
    supervise(spark, {name: starters[name] for name in QUERIES})


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
