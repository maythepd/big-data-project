"""Locust load for the fault-tolerance demo (docs/fault-tolerance-report.md, "Live demo").

Two kinds of simulated users; Locust charts requests/s, latency and failures for both, live:
  SensorFleet      a fleet of sensors sending readings to Kafka (LOADTEST_TOPIC) with acks=all;
                   one request = BATCH readings, its latency = until 2 brokers hold every one
  DashboardViewer  someone opening the dashboard: the live map (latest_readings) and one
                   station's month of daily AQI, the two heaviest queries of the dashboard

    docker compose -f docker-compose.loadtest.yml up -d --build      # UI: http://localhost:8089

The consumer under test reads the same topic (scripts/experiments/demo.sh starts it), so a broker,
DataNode or consumer killed during the load shows as failures or a latency spike here and as
lag in the consumer's progress, then recovery.
"""
import json
import os
import random
import time
from datetime import datetime, timezone

from locust import User, between, constant_throughput, task

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:29092,kafka2:29092,kafka3:29092")
TOPIC = os.getenv("LOADTEST_TOPIC", "replay-demo")
BATCH = int(os.getenv("LOADTEST_BATCH", "100"))
SENSORS = int(os.getenv("LOADTEST_SENSORS", "5000"))       # distinct simulated sensors
PARAMETERS = [("pm25", "µg/m³", 5, 150), ("pm10", "µg/m³", 10, 250), ("no2", "ppm", 0.001, 0.08)]


def reading(sensor):
    """One OpenAQ v2 message (src/common/schema.py make_openaq_message) for a simulated sensor."""
    parameter, unit, lo, hi = PARAMETERS[sensor % len(PARAMETERS)]
    value = round(random.uniform(lo, hi), 4)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    location = 900000 + sensor // len(PARAMETERS)
    return {"schema_version": 2, "source": "openaq", "station_id": f"openaq-{location}", "location_id": location,
            "sensors_id": 9000000 + sensor, "location_name": f"loadtest-{location}",
            "lat": 21.0 + (location % 100) / 100, "lon": 105.8 + (location % 37) / 100,
            "parameter": parameter, "parameter_raw": parameter, "value": value, "value_raw": str(value),
            "unit": unit, "units_raw": unit, "datetime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "datetime_local_raw": now.astimezone().isoformat(), "ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ")}


class SensorFleet(User):
    weight = 3
    # each simulated user sends one batch per second at most; add users in the UI to raise the load
    wait_time = constant_throughput(1)

    def on_start(self):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", enable_idempotence=True,
                                      linger_ms=5, request_timeout_ms=10_000, delivery_timeout_ms=30_000,
                                      value_serializer=lambda m: json.dumps(m).encode(),
                                      key_serializer=lambda k: k.encode())
        self.first = random.randrange(SENSORS)

    def on_stop(self):
        self.producer.close(timeout=5)

    @task
    def send_batch(self):
        start, error = time.perf_counter(), None
        try:
            futures = []
            for i in range(BATCH):
                msg = reading((self.first + i) % SENSORS)
                futures.append(self.producer.send(TOPIC, key=msg["station_id"], value=msg))
            for f in futures:
                f.get(timeout=30)          # acknowledged by the leader and one more in-sync replica
        except Exception as exc:          # NotEnoughReplicas, timeouts while a broker is down
            error = exc
        self.environment.events.request.fire(
            request_type="kafka", name=f"send {BATCH} readings (acks=all)",
            response_time=(time.perf_counter() - start) * 1000, response_length=BATCH, exception=error, context={})


# bad data a real feed sends, and where the pipeline should put it (the demo's watcher prints
# the Silver side per micro-batch; the archive side is the dead-letter store)
#   kind              archive (raw readings)            streaming Silver
BAD_KINDS = {
    "malformed_json":  "dead-letter: malformed_json     quarantine: malformed_message",
    "missing_key":     "dead-letter: no_station_id      quarantine: no_sensor_id",
    "renamed_field":   "dead-letter: no_numeric_value   kept, invalid: value_missing",
    "decimal_comma":   "dead-letter: no_numeric_value   kept, invalid: value_not_numeric",
    "legacy_format":   "kept                            quarantine: legacy_message",
    "sentinel_-999":   "kept (raw)                      kept, invalid: sentinel",
    "impossible_value": "kept (raw)                     kept, invalid: above_max",
    "future_time":     "dead-letter: future_event_time  kept, invalid: ts_out_of_range",
    "unknown_param":   "kept (raw)                      kept, invalid: unknown_parameter",
    "duplicate":       "deduplicated                    deduplicated",
}


def bad(kind, sensor):
    """Raw bytes of one bad message of the given kind."""
    m = reading(sensor)
    if kind == "malformed_json":
        data = json.dumps(m).encode()
        return data[: len(data) // 2]
    if kind == "missing_key":
        del m["station_id"], m["sensors_id"]
    elif kind == "renamed_field":
        m["measurement"], m["measurement_raw"] = m.pop("value"), m.pop("value_raw")
    elif kind == "decimal_comma":
        m["value"], m["value_raw"] = None, m["value_raw"].replace(".", ",")
    elif kind == "legacy_format":
        m = {k: m[k] for k in ["source", "station_id", "lat", "lon", "parameter", "value", "unit", "datetime",
                               "ingested_at"]}
    elif kind == "sentinel_-999":
        m["value"], m["value_raw"] = -999.0, "-999"
    elif kind == "impossible_value":
        m["value"], m["value_raw"] = 987654.0, "987654"
    elif kind == "future_time":
        later = datetime.now(timezone.utc).replace(microsecond=0, year=datetime.now(timezone.utc).year + 2)
        m["datetime"] = later.strftime("%Y-%m-%dT%H:%M:%SZ")
        m["datetime_local_raw"] = later.isoformat()
    elif kind == "unknown_param":
        m["parameter"] = m["parameter_raw"] = "pm2.5"
    return json.dumps(m).encode()


class BadDataSender(User):
    """A broken feed: one bad message of a random kind per second (Locust row per kind)."""
    weight = 1
    wait_time = constant_throughput(1)

    def on_start(self):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", enable_idempotence=True)

    def on_stop(self):
        self.producer.close(timeout=5)

    @task
    def send_bad(self):
        kind = random.choice(list(BAD_KINDS))
        sensor = random.randrange(SENSORS)
        data = bad(kind, sensor)
        start, error = time.perf_counter(), None
        try:
            copies = 2 if kind == "duplicate" else 1
            for _ in range(copies):
                self.producer.send(TOPIC, key=b"bad-data", value=data).get(timeout=30)
        except Exception as exc:
            error = exc
        self.environment.events.request.fire(request_type="bad data", name=kind,
                                             response_time=(time.perf_counter() - start) * 1000,
                                             response_length=len(data), exception=error, context={})


class DashboardViewer(User):
    weight = 1
    wait_time = between(1, 3)

    def on_start(self):
        import sys
        sys.path.insert(0, "/app")
        from pymongo import MongoClient
        from src.common.config import MONGO_DB, mongo_uri
        self.db = MongoClient(os.getenv("LOADTEST_MONGO_URI") or mongo_uri(), serverSelectionTimeoutMS=5000)[MONGO_DB]
        self.stations = self.db.station_daily_aqi.distinct("meta.location_id")   # time series: station in meta

    def timed(self, name, query):
        start, error, n = time.perf_counter(), None, 0
        try:
            n = len(query())
        except Exception as exc:
            error = exc
        self.environment.events.request.fire(request_type="mongodb", name=name,
                                             response_time=(time.perf_counter() - start) * 1000,
                                             response_length=n, exception=error, context={})

    @task(2)
    def live_map(self):
        self.timed("live map: latest_readings", lambda: list(
            self.db.latest_readings.find({}, {"_id": 0}).sort("event_time", -1).limit(500)))

    @task(1)
    def station_month(self):
        if not self.stations:
            return
        sid = random.choice(self.stations)
        self.timed("station page: daily AQI", lambda: list(
            self.db.station_daily_aqi.find({"meta.location_id": sid}, {"_id": 0}).sort("date", -1).limit(365)))
