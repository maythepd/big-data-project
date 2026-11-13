"""Map WAQI stations and OpenWeather points to Gold location ids (virtual id without an OpenAQ match).

    python3 src/gold/build_station_xref.py
"""
import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import yaml

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import DIM_DIR, ROOT

LAKE = Path.home() / "openaq_lake"
DEFAULT_GOLD_RULES = ROOT / "config" / "gold_rules_v1.yaml"


def load_gold_rules(path):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def virtual_id(source, key, lat, lon):
    """Stable negative ids: OpenWeather grid by its coordinates, WAQI by its uid."""
    if source == "openweather":
        return -(1_000_000_000 + round(lat * 100) * 100_000 + round(lon * 100))
    return -(5_000_000_000 + int(key) + 100_000_000)           # WAQI uids are within +-1e8


def nearest(points, sites):
    d = haversine_km(points["lat"].to_numpy()[:, None], points["lon"].to_numpy()[:, None],
                     sites["lat"].to_numpy()[None, :], sites["lon"].to_numpy()[None, :])
    i = d.argmin(axis=1)
    return sites.iloc[i].reset_index(drop=True), d[np.arange(len(points)), i]


def xref(source, points, sites, radius_km):
    site, dist = nearest(points, sites)
    rows = []
    for k, p in enumerate(points.itertuples()):
        fixed = getattr(p, "openaq_location_id", None)
        if fixed is not None and not pd.isna(fixed):
            match = sites[sites["location_id"] == int(fixed)].iloc[0]
            rows.append((source, str(p.station_id), int(fixed), False, 0.0, match["name"], match["lat"], match["lon"]))
        elif dist[k] <= radius_km:
            s = site.iloc[k]
            rows.append((source, str(p.station_id), int(s["location_id"]), False, round(float(dist[k]), 3),
                         s["name"], s["lat"], s["lon"]))
        else:
            rows.append((source, str(p.station_id), virtual_id(source, p.station_id, p.lat, p.lon), True,
                         round(float(dist[k]), 3), p.name, p.lat, p.lon))
    return pd.DataFrame(rows, columns=["source", "source_station_id", "location_id", "is_virtual",
                                       "distance_km", "location_name", "lat", "lon"])


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rules", default=str(DEFAULT_GOLD_RULES))
    p.add_argument("--run-id", default="station_xref_01")
    args = p.parse_args()
    src = load_gold_rules(args.rules)["sources"]

    sites = (pd.read_parquet(DIM_DIR / "openaq_locations.parquet").dropna(subset=["lat", "lon"])
             .rename(columns={"station_id": "location_id"})[["location_id", "name", "lat", "lon"]])
    sites["location_id"] = sites["location_id"].astype("int64")
    waqi = pd.read_parquet(DIM_DIR / "waqi_stations.parquet")
    owm = pd.read_parquet(DIM_DIR / "openweather_stations.parquet")
    out = pd.concat([xref("waqi", waqi, sites, src["waqi_match_km"]),
                     xref("openweather", owm, sites, src["openweather_match_km"])], ignore_index=True)
    if out.duplicated(["source", "source_station_id"]).any() or \
            out[out["is_virtual"]].duplicated("location_id").any():
        raise SystemExit("duplicate keys in the xref")

    path = LAKE / src["xref"]
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(path, index=False)
    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    out.to_csv(report / "xref.csv", index=False)
    summary = (out.groupby(["source", "is_virtual"])
               .agg(stations=("source_station_id", "size"), locations=("location_id", "nunique"),
                    median_km=("distance_km", "median")).reset_index())
    (report / "run.json").write_text(json.dumps({"rows": len(out), "summary": summary.to_dict("records")},
                                                indent=2, default=str), encoding="utf-8")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
