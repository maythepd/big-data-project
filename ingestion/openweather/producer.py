"""OpenWeather Air Pollution -> Kafka. One request per grid point returns all pollutants.

Run from the project root:  python -m ingestion.openweather.producer [--once] [--dry-run]
"""
import pandas as pd
from ingestion.common.base_producer import BaseProducer
from ingestion.common.config import DIM_DIR, require_env
from ingestion.common.schema import make_message


def parse_air_pollution(body, station):
    """Turn one /air_pollution response into shared-format messages.
    components are in µg/m³: co, no, no2, o3, so2, pm2_5, pm10, nh3."""
    for entry in body["list"]:
        for parameter, value in entry["components"].items():
            yield make_message(
                source="openweather", station_id=station["station_id"],
                lat=station["lat"], lon=station["lon"],
                parameter=parameter, value=value, unit="µg/m³", dt=entry["dt"],
            )


class OpenWeatherProducer(BaseProducer):
    source = "openweather"
    poll_seconds = 3600              # data updates about hourly; also keeps us inside the free monthly quota
    min_interval = 1.0               # free tier: 60 calls/minute

    def __init__(self, dry_run=False):
        super().__init__(dry_run)
        self.api_key = require_env("OPENWEATHER_API_KEY")
        self.stations = pd.read_parquet(f"{DIM_DIR}/openweather_stations.parquet").to_dict("records")

    def fetch(self):
        for st in self.stations:
            r = self.safe_get("https://api.openweathermap.org/data/2.5/air_pollution",
                              params={"lat": st["lat"], "lon": st["lon"], "appid": self.api_key})
            if r is not None:
                yield from parse_air_pollution(r.json(), st)


if __name__ == "__main__":
    OpenWeatherProducer.main()
