"""OpenAQ v2 message fidelity and producer identity/ack regression tests."""
from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common.schema import make_openaq_message
from src.ingestion.base_producer import BaseProducer


class SensorMessages(unittest.TestCase):
    def events(self):
        now = datetime.now(timezone.utc).isoformat()
        return [make_openaq_message(1, sensor, 10, 20, "co", -.99991, "ppb", now)
                for sensor in (11, 12)]

    def test_raw_values_and_sensor_ids(self):
        a, b = self.events()
        self.assertEqual((a["value"], a["value_raw"], a["unit"]), (-.99991, "-0.99991", "ppb"))
        self.assertEqual((a["sensors_id"], b["sensors_id"]), (11, 12))

    def test_colocated_sensors_and_failed_send_retry(self):
        p = BaseProducer(dry_run=True)
        p.dry_run = False
        events = self.events()
        p.fetch = lambda: iter(events)
        producer = Mock()
        failed, ok = Mock(), Mock()
        failed.succeeded.return_value = False
        failed.exception = RuntimeError("fixture failure")
        ok.succeeded.return_value = True
        producer.send.side_effect = [failed, ok, ok]
        p.producer = producer
        p.run_once()
        self.assertEqual(producer.send.call_count, 2)
        self.assertEqual(len(p.last_seen), 1)
        p.run_once()
        self.assertEqual(producer.send.call_count, 3)
        self.assertEqual(len(p.last_seen), 2)


if __name__ == "__main__":
    unittest.main()
