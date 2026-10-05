"""OpenAQ -> Kafka. One request per station returns all of its sensors.

Run from the project root:  python -m ingestion.openaq.producer [--once] [--dry-run]
"""
import pandas as pd
from ingestion.common.base_producer import BaseProducer
from ingestion.common.config import DIM_DIR, require_env
from ingestion.common.schema import make_message


class OpenAQProducer(BaseProducer):
    source = "openaq"
    poll_seconds = 600               # stations report hourly; 10 min catches them soon after
    min_interval = 1.0               # OpenAQ allows 60 requests/minute

    def __init__(self, dry_run=False):
        super().__init__(dry_run)
        self.headers = {"X-API-Key": require_env("OPENAQ_API_KEY")}
        self.stations = pd.read_parquet(f"{DIM_DIR}/openaq_stations.parquet").to_dict("records")
        # readings only carry a sensor id; this tells us what each sensor measures
        sensors = pd.read_parquet(f"{DIM_DIR}/openaq_sensors.parquet")
        self.sensors = sensors.set_index("sensor_id")[["parameter", "units"]].to_dict("index")

    def fetch(self):
        for st in self.stations:
            r = self.safe_get(f"https://api.openaq.org/v3/locations/{st['station_id']}/latest",
                              headers=self.headers)
            if r is None:
                continue
            for m in r.json()["results"]:
                sensor = self.sensors.get(m["sensorsId"])
                if sensor is None:
                    continue             # sensor added since the last catalog refresh
                yield make_message(
                    source=self.source, station_id=st["station_id"],
                    lat=st["lat"], lon=st["lon"],
                    parameter=sensor["parameter"], value=m["value"], unit=sensor["units"],
                    dt=m["datetime"]["utc"],
                )


if __name__ == "__main__":
    OpenAQProducer.main()
