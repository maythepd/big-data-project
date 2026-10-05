"""Builds the WAQI (aqicn.org) station catalog for Vietnam. Run once, then daily.

/map/bounds caps how many stations it returns for a large area, so the Vietnam
bounding box is split into 1°x1° tiles. The box also covers parts of China, Laos,
Thailand and Cambodia, so each station's feed is checked and kept only if its
url, name or address mentions Vietnam (community stations have urls like
aqicn.org/station/@476182, so the url alone is not enough).

Writes: data/dim/waqi_stations.parquet

    python -m src.ingestion.waqi.fetch_dimensions
"""
import os
import pandas as pd
from src.ingestion.base_producer import RateLimiter
from src.common.config import DIM_DIR, VIETNAM_BBOX, require_env

TOKEN = require_env("WAQI_TOKEN")
TILE = 1.0                                         # degrees


def tiles():
    lat_min, lon_min, lat_max, lon_max = VIETNAM_BBOX
    lat = lat_min
    while lat < lat_max:
        lon = lon_min
        while lon < lon_max:
            yield lat, lon, min(lat + TILE, lat_max), min(lon + TILE, lon_max)
            lon += TILE
        lat += TILE


def is_vietnam(city):
    text = " ".join([city.get("url", ""), city.get("name", ""), city.get("location", "")])
    return "vietnam" in text.lower()


def main():
    api = RateLimiter(min_interval=1.0)

    candidates = {}                                # uid -> bounds entry (tiles overlap at edges)
    for lat1, lon1, lat2, lon2 in tiles():
        body = api.get("https://api.waqi.info/map/bounds",
                       params={"latlng": f"{lat1},{lon1},{lat2},{lon2}",
                               "networks": "all", "token": TOKEN}).json()
        if body["status"] != "ok":
            raise SystemExit(f"WAQI error: {body['data']}")
        for c in body["data"]:
            candidates[c["uid"]] = c
    print(f"{len(candidates)} stations in the bounding box, checking which are in Vietnam...")

    stations = []
    for i, c in enumerate(candidates.values(), 1):
        feed = api.get(f"https://api.waqi.info/feed/@{c['uid']}/", params={"token": TOKEN}).json()
        if feed["status"] == "ok" and is_vietnam(feed["data"]["city"]):
            stations.append({
                "station_id": c["uid"],
                "name": feed["data"]["city"]["name"],
                "lat": c["lat"],
                "lon": c["lon"],
            })
        if i % 50 == 0:
            print(f"checked {i}/{len(candidates)} stations, {len(stations)} in Vietnam")

    os.makedirs(DIM_DIR, exist_ok=True)
    pd.DataFrame(stations).to_parquet(f"{DIM_DIR}/waqi_stations.parquet", index=False)
    print(f"waqi: {len(stations)} stations in Vietnam (of {len(candidates)} in the bounding box)")


if __name__ == "__main__":
    main()
