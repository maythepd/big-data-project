"""Kafka -> Spark Structured Streaming, with three outputs:

- archive: every reading, deduplicated, appended to HDFS as Parquet partitioned by
  date. This is the permanent copy; Kafka deletes messages after 7 days.
- latest_readings: the newest reading per station and parameter, upserted every
  minute into the dashboard's PostgreSQL table of the same name
  (docker-compose.serving.yml). Postgres keeps whichever reading is newest, so a
  replayed or late micro-batch never replaces it with an older one.
- Silver: OpenAQ sensor measurements cleaned with historical v1 rules; raw,
  quarantine and reconciliation report are published with each atomic batch.

Runs in the spark-consumer container, which reaches Kafka, HDFS and Postgres:
    docker compose -f docker-compose.spark.yml up -d --build
"""
import os

import psycopg
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.common.config import (KAFKA_BOOTSTRAP_SERVERS, POSTGRES_DB, POSTGRES_HOST,
                               POSTGRES_USER, TOPIC_NAME, require_env)

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

# Producers send readings up to 1 day old (BaseProducer.max_age), so the archive
# remembers readings for longer than that; anything older is dropped as late.
DEDUPE_WATERMARK = "25 hours"

# ISO 8601 with offset, so Postgres reads the UTC time whatever the Python time zone
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"
UPSERT_LATEST = """
    INSERT INTO latest_readings AS cur
        (station_id, parameter, source, lat, lon, value, unit, event_time, ingested_at)
    VALUES (%(station_id)s, %(parameter)s, %(source)s, %(lat)s, %(lon)s, %(value)s, %(unit)s,
            %(event_time)s::timestamptz, %(ingested_at)s::timestamptz)
    ON CONFLICT (station_id, parameter) DO UPDATE SET
        source = excluded.source, lat = excluded.lat, lon = excluded.lon,
        value = excluded.value, unit = excluded.unit, event_time = excluded.event_time,
        ingested_at = excluded.ingested_at, updated_at = now()
    WHERE excluded.event_time > cur.event_time
"""


def parse(messages):
    """Kafka records -> one typed row per reading; malformed messages are dropped."""
    r = F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA)
    return (messages.select(r.alias("r")).select("r.*")
            .withColumn("event_time", F.try_to_timestamp("datetime"))
            .withColumn("ingested_at", F.try_to_timestamp("ingested_at"))
            .drop("datetime")
            .where(F.col("station_id").isNotNull() & F.col("parameter").isNotNull()
                   & F.col("value").isNotNull() & F.col("event_time").isNotNull())
            # one reading stamped in the future would push the watermark ahead
            # and make every later reading look late
            .where(F.col("event_time") <= F.current_timestamp() + F.expr("INTERVAL 1 HOUR")))


def latest(readings):
    """Newest reading per station and parameter. Duplicates carry the same reading,
    so they never change which row is newest."""
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
            .option("path", ARCHIVE_PATH)
            .option("checkpointLocation", ARCHIVE_CHECKPOINT)
            .partitionBy("date")
            .trigger(processingTime="5 minutes")    # fewer, larger files than a short trigger
            .start())


def upsert_latest(batch, batch_id):
    """Write the newest reading per station and parameter in one micro-batch to Postgres."""
    rows = (latest(batch)
            .withColumn("event_time", F.date_format("event_time", TIMESTAMP_FORMAT))
            .withColumn("ingested_at", F.date_format("ingested_at", TIMESTAMP_FORMAT))
            .collect())
    if not rows:
        return
    with psycopg.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER,
                         password=require_env("POSTGRES_PASSWORD")) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(UPSERT_LATEST, [row.asDict() for row in rows])
    print(f"latest_readings: batch {batch_id} sent {len(rows)} station/parameter rows", flush=True)


def start_latest(readings):
    return (readings.writeStream
            .foreachBatch(upsert_latest)
            # the upsert is idempotent, so a batch retried after a failure is harmless
            .option("checkpointLocation", LATEST_CHECKPOINT)
            .trigger(processingTime="1 minute")
            .start())


def start_silver(messages):
    """Replay legacy messages into quarantine; never guess their missing IDs.

    Atomic batch publication makes foreachBatch retries idempotent. Durable
    key lookup also handles duplicates older than the archive's watermark.
    """
    from src.streaming.silver_batch_writer import SilverStreamWriter
    writer = SilverStreamWriter(messages.sparkSession, SILVER_ROOT,
        dim_path=os.getenv("STREAM_DIM_LOCATION") or None,
        sensor_units_path=os.getenv("STREAM_SENSOR_UNITS") or None)
    return (messages.writeStream.foreachBatch(writer.write_batch)
            .queryName("openaq-silver-stream")
            .option("checkpointLocation", SILVER_CHECKPOINT)
            .trigger(processingTime=os.getenv("STREAM_SILVER_TRIGGER", "1 minute"))
            .start())


def main():
    spark = (SparkSession.builder.appName("air-quality-consumer")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.codegen.hugeMethodLimit", "8000")
             # A few thousand readings an hour fit in one state partition, which also
             # keeps it to one Parquet file per date per micro-batch. The archive
             # checkpoint fixes this number at the first run.
             .config("spark.sql.shuffle.partitions", "1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    messages = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", TOPIC_NAME)
                .option("startingOffsets", "earliest")
                .load())
    readings = parse(messages)
    start_archive(readings)
    start_latest(readings)
    start_silver(messages)
    spark.streams.awaitAnyTermination()     # a failed query raises and exits


if __name__ == "__main__":
    main()
