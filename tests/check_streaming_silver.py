"""Integration fixtures: real Spark/Parquet commits, retry, late and duplicate events.

spark-submit --master local[2] tests/check_streaming_silver.py
Uses a temporary local lake, never the real historical/streaming outputs.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F
from src.common.schema import make_openaq_message
from src.streaming.silver_batch_writer import SilverStreamWriter
from src.streaming.validate_stream_batches import validate


def event(sensor, value=12, parameter="pm25", unit="µg/m³", **changes):
    msg = make_openaq_message(1, sensor, 10, 20, parameter, value, unit,
                             "2024-01-01T00:00:00Z", "Fixture")
    msg.update(changes)
    return msg


spark = (SparkSession.builder.appName("Check-Streaming-Silver")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.codegen.hugeMethodLimit", "8000")
         .config("spark.sql.ansi.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def kafka(events, start=0):
    rows = [(json.dumps(e) if isinstance(e, dict) else e, "fixture", 0, start + i,
             datetime(2026, 10, 25, tzinfo=timezone.utc)) for i, e in enumerate(events)]
    return spark.createDataFrame(rows, "value string, topic string, partition int, offset long, timestamp timestamp")


try:
    with TemporaryDirectory(prefix="openaq-stream-test-") as tmp:
        root = Path(tmp).as_uri()
        writer = SilverStreamWriter(spark, root)
        fixtures = [event(11), event(11), event(12, 40, "o3", "ppb"),
                    event(13, -9999, "co", "ppb"), event(14, "NaN"),
                    event(15, None), event(16, "Infinity"), event(17, "oops"),
                    event(18, -3), event(19, 2500), event(20, lat=200, lon=200),
                    event(21, datetime_local_raw="bad timestamp"),
                    event(22, source="waqi"), {"source": "openaq", "station_id": "openaq-1"},
                    "{broken-json", event(23, 5, datetime_local_raw="2024-03-10T00:00:01-03:-30")]
        first = kafka(fixtures)
        report = writer.write_batch(first, 0)
        assert report["rows_in"] == len(fixtures)
        assert report["quarantine"] == 4, report
        assert report["silver"] == 11 and report["dedup_removed"] == 1, report
        data = {r.sensors_id: r.asDict() for r in writer.read_measurements().collect()}
        assert data[11]["dup_count"] == 2
        assert data[12]["unit"] == "ppm" and abs(data[12]["value"] - .04) < 1e-12
        assert data[13]["value"] is None and "sentinel" in data[13]["dq_flags"]
        for sensor, flag in [(14, "value_nonfinite"), (15, "value_missing"),
                             (16, "value_nonfinite"), (17, "value_not_numeric")]:
            assert data[sensor]["dq_status"] == "invalid" and data[sensor]["value"] is None
            assert flag in data[sensor]["dq_flags"], data[sensor]
        assert data[18]["dq_status"] == "suspect"
        assert data[19]["value"] is None and "above_max" in data[19]["dq_flags"]
        assert data[20]["lat"] is None and data[20]["coord_source"] == "none"
        assert "datetime_offset_repaired" in data[23]["dq_flags"]
        # A committed batch replay is a no-op, including after writer reconstruction.
        writer = SilverStreamWriter(spark, root)
        assert writer.write_batch(first, 0) == report
        try:
            writer.write_batch(kafka([event(99)]), 0)
            raise AssertionError("changed payload accepted for committed batch")
        except RuntimeError:
            pass
        # Previously committed repeat is removed; a changed value is quarantined.
        # A much older event is still accepted (no watermark data loss).
        second = kafka([event(11), event(12, 41, "o3", "ppb"),
                        event(24, 3, datetime_local_raw="2001-01-01T00:00:00Z")], 100)
        report2 = writer.write_batch(second, 1)
        assert (report2["silver"], report2["quarantine"], report2["dedup_removed"]) == (1, 1, 1), report2
        assert report2["conflicts"] == 1 and report2["cross_batch_duplicates"] == 2
        # Failure after staging must expose no partial batch. Retrying publishes once.
        third = kafka([event(25)], 200)
        with patch.object(writer, "publish", side_effect=RuntimeError("simulated crash")):
            try:
                writer.write_batch(third, 2)
                raise AssertionError("failure not injected")
            except RuntimeError as exc:
                assert "simulated crash" in str(exc)
        assert len(writer.batch_paths()) == 2
        assert writer.read_measurements().count() == 12
        assert writer.write_batch(third, 2)["silver"] == 1
        final = writer.read_measurements()
        assert final.count() == 13 and final.select("source", "sensors_id", "datetime_utc").distinct().count() == 13
        assert final.filter(F.col("sensors_id") == 24).first().year == 2001
        for batch_dir in writer.batch_paths():
            audit = writer.read_json(batch_dir + "/report.json")
            assert audit["rows_in"] == audit["silver"] + audit["quarantine"] + audit["dedup_removed"]
        assert spark.read.parquet(*[p + "/raw" for p in writer.batch_paths()]).count() == len(fixtures) + 4
        validation = validate(writer)
        assert validation["ok"] and validation["duplicate_keys"] == 0, validation
        print("PASS: stream raw preservation, historical rules, malformed/legacy quarantine, numeric failures,")
        print("      dedup across batches, conflicts, late data, writer restart and atomic failure/retry")
finally:
    spark.stop()
