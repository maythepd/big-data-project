"""WAQI (aqicn.org) -> Kafka. One request per station returns all of its readings.

Note: WAQI publishes pollutants as US AQI sub-indices (pm25: 57 means AQI 57,
not 57 µg/m³), so they are sent as aqi_pm25, aqi_no2, ... with unit "us_aqi".

Run from the project root:  python -m ingestion.waqi.producer [--once] [--dry-run]
"""
import pandas as pd
from ingestion.common.base_producer import BaseProducer
from ingestion.common.config import DIM_DIR, require_env
from ingestion.common.schema import make_message

POLLUTANTS = {"pm25", "pm10", "no2", "o3", "so2", "co"}
WEATHER_UNITS = {"t": "c", "h": "%"}       # temperature, relative humidity


def parse_feed(data, station):
    """Turn one /feed response's `data` into shared-format messages."""
    dt = data["time"]["iso"]
    for key, reading in data.get("iaqi", {}).items():
        if key in POLLUTANTS:
            parameter, unit = f"aqi_{key}", "us_aqi"
        elif key in WEATHER_UNITS:
            parameter, unit = key, WEATHER_UNITS[key]
        else:
            continue                     # wind, pressure, dew point, ...: units not documented
        yield make_message(
            source="waqi", station_id=station["station_id"],
            lat=station["lat"], lon=station["lon"],
            parameter=parameter, value=reading["v"], unit=unit, dt=dt,
        )


class WaqiProducer(BaseProducer):
    source = "waqi"
    poll_seconds = 900               # stations update about hourly
    min_interval = 1.0

    def __init__(self, dry_run=False):
        super().__init__(dry_run)
        self.token = require_env("WAQI_TOKEN")
        self.stations = pd.read_parquet(f"{DIM_DIR}/waqi_stations.parquet").to_dict("records")

    def fetch(self):
        for st in self.stations:
            r = self.safe_get(f"https://api.waqi.info/feed/@{st['station_id']}/",
                              params={"token": self.token})
            if r is None:
                continue
            body = r.json()
            if body["status"] != "ok":   # WAQI reports errors with HTTP 200
                self.log.warning("station %s: %s", st["station_id"], body["data"])
                continue
            yield from parse_feed(body["data"], st)


if __name__ == "__main__":
    WaqiProducer.main()
