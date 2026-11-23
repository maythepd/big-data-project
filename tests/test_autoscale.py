"""Autoscaler rule (src/streaming/autoscaler.py) and the validator's message check (validator.py)."""
import json

from src.streaming.autoscaler import desired_replicas, next_replicas
from src.streaming.validator import classify


def test_desired_is_lag_over_threshold_within_bounds():
    assert desired_replicas(0, 20_000, 1, 6, 12) == 1            # idle: the floor
    assert desired_replicas(1, 20_000, 1, 6, 12) == 1
    assert desired_replicas(45_000, 20_000, 1, 6, 12) == 3       # ceil(45k / 20k)
    assert desired_replicas(10_000_000, 20_000, 1, 6, 12) == 6   # the ceiling
    assert desired_replicas(10_000_000, 20_000, 1, 20, 4) == 4   # never more than partitions


def test_rate_term_keeps_replicas_while_the_load_lasts():
    # run3: 3 replicas drained the lag mid-burst (10.7k msg/s in); lag alone asks for 1
    assert desired_replicas(1_332, 20_000, 1, 6, 12) == 1
    assert desired_replicas(1_332, 20_000, 1, 6, 12, rate_in=10_718, rate_per_replica=4_000) == 3
    assert desired_replicas(0, 20_000, 1, 6, 12, rate_in=0, rate_per_replica=4_000) == 1   # idle


def test_scale_out_at_once_scale_in_after_the_window():
    # wishes over time (seconds): high at 0 and 30, low from 40
    history = [(0, 5), (30, 5), (40, 1)]
    assert next_replicas(2, [(0, 5)], 0, 60) == 5                # out: at once
    assert next_replicas(5, history, 40, 60) == 5                # in: 5 still wanted within 60 s
    later = [(30, 5), (40, 1), (100, 1)]
    assert next_replicas(5, [h for h in later if 100 - h[0] <= 60], 100, 60) == 1   # 5 left the window


def message(**over):
    m = {"schema_version": 2, "source": "openaq", "station_id": "openaq-1", "location_id": 1, "sensors_id": 2,
         "parameter": "pm25", "value_raw": "12.5", "value": 12.5, "unit": "µg/m³",
         "datetime": "2025-01-01T00:00:00Z", "ingested_at": "2025-01-01T00:05:00Z"}
    m.update(over)
    return json.dumps(m).encode()


def test_validator_routes_by_contract():
    assert classify(message()) is None
    assert classify(message(schema_version=3, sensor_grade="reference")) is None   # additive v3
    assert classify(message(sensor_grade="reference")).startswith("invalid:")     # v2 is closed
    assert classify(message(schema_version=1)) == "unknown_schema_version:1"
    assert classify(b'{"schema_version": 2, "sour') == "not_json"
    assert classify(message(sensors_id="2")).startswith("invalid:sensors_id")
