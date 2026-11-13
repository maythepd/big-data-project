"""OpenWeather point catalog: the OpenAQ Vietnam locations plus a grid over Vietnam.

    python -m src.ingestion.openweather.fetch_dimensions [--step 0.5]
"""
import argparse
import os

import pandas as pd

from src.common.config import DIM_DIR, VIETNAM_BBOX, require_env
from src.ingestion.base_producer import RateLimiter

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


def openaq_sites():
    loc = pd.read_parquet(DIM_DIR / "openaq_locations.parquet").dropna(subset=["lat", "lon"])
    return [{"station_id": f"oaq-{int(r.station_id)}", "name": r.name, "province": r.locality,
             "lat": round(float(r.lat), 4), "lon": round(float(r.lon), 4),
             "kind": "openaq_site", "openaq_location_id": int(r.station_id)} for r in loc.itertuples()]


def grid_points(step):
    api = RateLimiter(min_interval=1.0)          # free tier: 60 calls/minute
    points = list(grid(step))
    out = []
    for i, (lat, lon) in enumerate(points, 1):
        places = api.get("https://api.openweathermap.org/geo/1.0/reverse",
                         params={"lat": lat, "lon": lon, "limit": 1, "appid": API_KEY}).json()
        if places and places[0].get("country") == "VN":
            out.append({"station_id": f"g{lat:.2f}_{lon:.2f}", "name": places[0]["name"],
                        "province": places[0].get("state"), "lat": lat, "lon": lon,
                        "kind": "grid", "openaq_location_id": None})
        if i % 50 == 0:
            print(f"checked {i}/{len(points)} grid points, {len(out)} in Vietnam", flush=True)
    return out, len(points)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", type=float, default=0.5, help="grid spacing in degrees (0.5 ≈ 55 km)")
    args = parser.parse_args()

    sites = openaq_sites()
    cells, checked = grid_points(args.step)
    df = pd.DataFrame(sites + cells).astype({"openaq_location_id": "Int64"})
    os.makedirs(DIM_DIR, exist_ok=True)
    df.to_parquet(DIM_DIR / "openweather_stations.parquet", index=False)
    print(f"openweather: {len(sites)} OpenAQ sites + {len(cells)} grid points in Vietnam (of {checked} checked)")


if __name__ == "__main__":
    main()
