"""Builds the OpenWeather "station" catalog: a grid of points over Vietnam.

OpenWeather's air pollution data is model-based, so it works at any coordinate.
We lay a grid over the Vietnam bounding box and keep only points that the reverse
geocoder places in Vietnam (drops the sea and neighbouring countries).
Run once (takes a few minutes: one request per grid point); rerun only to change the grid.

Writes: data/dim/openweather_stations.parquet

    python -m ingestion.openweather.fetch_dimensions [--step 0.5]
"""
import argparse, os
import pandas as pd
from ingestion.common.base_producer import RateLimiter
from ingestion.common.config import DIM_DIR, VIETNAM_BBOX, require_env

API_KEY = require_env("OPENWEATHER_API_KEY")


def grid(step):
    lat_min, lon_min, lat_max, lon_max = VIETNAM_BBOX
    lat = lat_min
    while lat <= lat_max:
        lon = lon_min
        while lon <= lon_max:
            yield round(lat, 2), round(lon, 2)
            lon += step
        lat += step


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", type=float, default=0.5, help="grid spacing in degrees (0.5 ≈ 55 km)")
    args = parser.parse_args()

    api = RateLimiter(min_interval=1.0)          # free tier: 60 calls/minute
    points = list(grid(args.step))
    stations = []
    for i, (lat, lon) in enumerate(points, 1):
        places = api.get("https://api.openweathermap.org/geo/1.0/reverse",
                         params={"lat": lat, "lon": lon, "limit": 1, "appid": API_KEY}).json()
        if places and places[0].get("country") == "VN":
            stations.append({
                "station_id": f"{lat:.2f}_{lon:.2f}",
                "name": places[0]["name"],
                "province": places[0].get("state"),
                "lat": lat,
                "lon": lon,
            })
        if i % 50 == 0:
            print(f"checked {i}/{len(points)} grid points, {len(stations)} in Vietnam")

    os.makedirs(DIM_DIR, exist_ok=True)
    pd.DataFrame(stations).to_parquet(f"{DIM_DIR}/openweather_stations.parquet", index=False)
    print(f"openweather: {len(stations)} grid points in Vietnam (of {len(points)} checked)")


if __name__ == "__main__":
    main()
