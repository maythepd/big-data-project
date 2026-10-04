"""Builds the OpenAQ station catalog for Vietnam. Run once, then daily.

Writes:
    data/dim/openaq_stations.parquet  which stations to poll (+ name, coordinates)
    data/dim/openaq_sensors.parquet   sensor_id -> parameter + units (OpenAQ readings only carry a sensor id)
"""
import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from ingestion.common.base_producer import RateLimiter
from ingestion.common.config import DIM_DIR, require_env

HEADERS = {"X-API-Key": require_env("OPENAQ_API_KEY")}
BASE = "https://api.openaq.org/v3/locations"
PARAMS = {"countries_id": 56, "limit": 1000}      # 56 = Vietnam (see /v3/countries)
ACTIVE_WITHIN = timedelta(days=30)                 # VN has few stations, many report irregularly


def parse_location(loc: dict) -> tuple[dict, list[dict]]:
    station = {
        "station_id":  loc["id"],
        "name":        loc["name"],
        "locality":    loc.get("locality"),
        "provider":    loc["provider"]["name"],
        "lat":         loc["coordinates"]["latitude"],
        "lon":         loc["coordinates"]["longitude"],
        "timezone":    loc["timezone"],
        "is_mobile":   loc["isMobile"],
        "last_seen":   loc["datetimeLast"]["utc"] if loc.get("datetimeLast") else None,
    }
    sensors = [
        {"sensor_id": s["id"], "station_id": loc["id"],
         "parameter": s["parameter"]["name"], "units": s["parameter"]["units"]}
        for s in loc["sensors"]
    ]
    return station, sensors


def fetch_all_locations() -> list[dict]:
    api = RateLimiter(min_interval=1.0)
    results, page = [], 1
    while True:
        body = api.get(BASE, headers=HEADERS, params={**PARAMS, "page": page}).json()
        results.extend(body["results"])
        # OpenAQ returns 404 (not an empty list) past the last page, so stop using meta.found
        if page * PARAMS["limit"] >= body["meta"]["found"]:
            return results
        page += 1


def main():
    stations, sensors = [], []
    for loc in fetch_all_locations():
        st, se = parse_location(loc)
        stations.append(st)
        sensors.extend(se)

    stations = pd.DataFrame(stations)
    sensors = pd.DataFrame(sensors)

    # keep only fixed, recently active stations
    cutoff = datetime.now(timezone.utc) - ACTIVE_WITHIN
    stations["last_seen"] = pd.to_datetime(stations["last_seen"], utc=True)
    stations = stations[(~stations["is_mobile"]) & (stations["last_seen"] > cutoff)]
    sensors = sensors[sensors["station_id"].isin(stations["station_id"])]

    os.makedirs(DIM_DIR, exist_ok=True)
    stations.to_parquet(f"{DIM_DIR}/openaq_stations.parquet", index=False)
    sensors.to_parquet(f"{DIM_DIR}/openaq_sensors.parquet", index=False)
    print(f"openaq: {len(stations)} active stations, {len(sensors)} sensors")


if __name__ == "__main__":
    main()
