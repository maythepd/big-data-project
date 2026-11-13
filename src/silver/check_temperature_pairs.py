"""Which channel of a temperature c/f pair (same raw values) carries the wrong unit?

Each reading is compared with nearby long-running reference stations; pairs with no
neighbour fall back on the c channel's median. Output: reports/<run>/pair_decisions.parquet.
Thresholds: config/silver_rules_v1.yaml unit_relabel.temperature_pair_*. pandas + pyarrow.
"""

import argparse
from datetime import datetime, timezone
import gc
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.silver.rules import DEFAULT_RULES, load_rules
from src.common.config import LAKE

EARTH_KM = 6371.0088


def temperature_sensors(path):
    s = pd.read_parquet(path, columns=["sensors_id", "location_id", "parameter_std", "units", "rows", "median"])
    s = s[s["parameter_std"] == "temperature"].copy()
    s["units_l"] = s["units"].str.strip().str.lower()
    s["sensors_id"] = s["sensors_id"].astype("int64")
    s["location_id"] = pd.to_numeric(s["location_id"], errors="coerce")
    return s


def matched_pairs(path, sensors, rl):
    """Pair-hours of c/f pairs whose raw values match, as (sensor_c, sensor_f)."""
    unit = dict(zip(sensors["sensors_id"], sensors["units_l"]))
    p = pd.read_parquet(path, columns=["location_id", "parameter", "hour", "sensor_a", "a", "sensor_b", "b"])
    p = p[p["parameter"] == "temperature"]
    ua, ub = p["sensor_a"].map(unit), p["sensor_b"].map(unit)
    p = p[((ua == "c") & (ub == "f")) | ((ua == "f") & (ub == "c"))]
    swap = (p["sensor_a"].map(unit) == "f").values
    p = pd.DataFrame({"location_id": p["location_id"].values, "hour": p["hour"].values,
                      "sensor_c": np.where(swap, p["sensor_b"], p["sensor_a"]),
                      "sensor_f": np.where(swap, p["sensor_a"], p["sensor_b"]),
                      "c": np.where(swap, p["b"], p["a"]), "f": np.where(swap, p["a"], p["b"])})
    match = (p["f"] - (p["c"] - 32.0) * 5.0 / 9.0).abs() < float(rl["temperature_pair_tolerance_c"])
    share = match.groupby([p["sensor_c"], p["sensor_f"]]).agg(["size", "mean"])
    share.columns = ["pair_hours", "match_share"]
    share = share[share["match_share"] > float(rl["temperature_pair_min_share"])].reset_index()
    return p.merge(share[["sensor_c", "sensor_f"]], on=["sensor_c", "sensor_f"]), share


def unit_vectors(df):
    lat, lon = np.radians(df["lat_ref"].values), np.radians(df["lon_ref"].values)
    return np.c_[np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)]


def neighbours(pair_locs, ref_locs, radius_km, k):
    """The k nearest reference locations within radius_km of each pair location."""
    a, b = unit_vectors(pair_locs), unit_vectors(ref_locs)
    ids = ref_locs.index.values
    out = []
    for i, lid in enumerate(pair_locs.index):
        dist = EARTH_KM * np.arccos(np.clip(b @ a[i], -1.0, 1.0))
        cand = np.where((dist < radius_km) & (ids != lid))[0]
        cand = cand[np.argsort(dist[cand])[:k]]
        out.append(pd.DataFrame({"location_id": lid, "nb": ids[cand], "km": dist[cand]}))
    return pd.concat(out, ignore_index=True)


def reference(sensor_hour, pairs, near, exclude):
    """Median temperature per (neighbour location, hour) at the pair hours, month by month."""
    dataset = ds.dataset(sensor_hour, partitioning="hive")
    nb = pa.array(near["nb"].unique(), pa.int64())
    excl = pa.array(sorted(exclude), pa.int64())
    ym = pairs["hour"].dt.year * 100 + pairs["hour"].dt.month
    refs = []
    for key in sorted(ym.unique()):
        hours = pa.array(pairs.loc[ym == key, "hour"].unique())
        scanner = dataset.scanner(
            columns=["location_id", "sensors_id", "hour", "mean_valid"],
            filter=((pc.field("year") == int(key) // 100) & (pc.field("month") == int(key) % 100)
                    & (pc.field("parameter") == "temperature") & pc.is_in(pc.field("location_id"), value_set=nb)
                    & pc.is_in(pc.field("hour"), value_set=hours) & ~pc.is_in(pc.field("sensors_id"), value_set=excl)),
            use_threads=False, batch_readahead=0, fragment_readahead=0, batch_size=200_000)
        chunks = [b.to_pandas() for b in scanner.to_batches() if b.num_rows]
        if chunks:
            t = pd.concat(chunks).dropna(subset=["mean_valid"])
            refs.append(t.groupby(["location_id", "hour"])["mean_valid"].median().reset_index())
            del t, chunks
            gc.collect()
        print(f"reference {key}: {sum(len(r) for r in refs)} location-hours", flush=True)
    return pd.concat(refs).rename(columns={"location_id": "nb", "mean_valid": "r"})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver-eda-report", default=str(LAKE / "reports" / "silver_for_gold_01"))
    p.add_argument("--values-units-report", default=str(LAKE / "reports" / "values_units_full_01"))
    p.add_argument("--sensor-units-v1", default=str(LAKE / "reports" / "sensor_units_01" / "dim_sensor_unit_v1.parquet"))
    p.add_argument("--rules", default=str(DEFAULT_RULES))
    p.add_argument("--run-id", default="temperature_pairs_01")
    args = p.parse_args()
    rules = load_rules(args.rules)
    rl = rules["unit_relabel"]
    eda = Path(args.silver_eda_report)
    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)

    sensors = temperature_sensors(Path(args.values_units_report) / "_sensor_values")
    pairs, share = matched_pairs(eda / "_sensor_pairs", sensors, rl)
    print(f"matched pairs {len(share)}, pair-hours {len(pairs)}", flush=True)

    loc = pd.read_parquet(LAKE / "silver" / "dim_location", columns=["location_id_num", "lat_ref", "lon_ref"]).dropna()
    loc["location_id"] = loc["location_id_num"].astype("int64")
    loc = loc.drop_duplicates("location_id").set_index("location_id")[["lat_ref", "lon_ref"]]
    suspects = pd.read_parquet(args.sensor_units_v1)
    short = sensors.loc[sensors["rows"] < int(rl["temperature_pair_reference_min_rows"]), "sensors_id"]
    exclude = (set(pairs["sensor_c"]) | set(pairs["sensor_f"]) | set(short)
               | set(suspects.loc[suspects["action"] == "suspect", "sensors_id"].astype("int64")))
    ref_locs = loc.loc[loc.index.intersection(sensors["location_id"].dropna().astype("int64").unique())]
    pair_locs = loc.reindex(pairs["location_id"].unique()).dropna()
    near = neighbours(pair_locs, ref_locs, float(rl["temperature_pair_neighbour_km"]),
                      int(rl["temperature_pair_neighbours"]))
    print(f"pair locations {len(pair_locs)}, with neighbours {near['location_id'].nunique()}", flush=True)

    ref = reference(eda / "_sensor_hour", pairs, near, exclude)
    x = pairs.merge(near[["location_id", "nb"]], on="location_id").merge(ref, on=["nb", "hour"])
    x = (x.groupby(["sensor_c", "sensor_f", "hour", "c", "f"])
          .agg(ref=("r", "median"), n_nb=("nb", "nunique")).reset_index())
    x["err_c"], x["err_f"] = (x["c"] - x["ref"]).abs(), (x["f"] - x["ref"]).abs()
    ev = (x.groupby(["sensor_c", "sensor_f"])
           .agg(ref_hours=("hour", "size"), n_neighbours=("n_nb", "median"), err_c=("err_c", "median"),
                err_f=("err_f", "median"), ref_median=("ref", "median")).reset_index())

    stats = (pairs.groupby(["sensor_c", "sensor_f"])
             .agg(location_id=("location_id", "first"), first_hour=("hour", "min"), last_hour=("hour", "max"),
                  c_median=("c", "median"), f_median=("f", "median")).reset_index())
    out = share.merge(stats, on=["sensor_c", "sensor_f"]).merge(ev, on=["sensor_c", "sensor_f"], how="left")
    rows = sensors[sensors["units_l"] == "c"].set_index("sensors_id")["rows"]
    out["c_rows"] = out["sensor_c"].map(rows)
    out = out.join(loc, on="location_id")
    margin = float(rl["temperature_pair_margin_c"])
    out["verdict"] = np.select([out["err_c"].isna(), out["err_f"] + margin < out["err_c"],
                                out["err_c"] + margin < out["err_f"]],
                               ["no_neighbour", "c_holds_f", "f_holds_c"], default="unclear")
    long_c = out["c_rows"] >= int(rl["temperature_pair_reference_min_rows"])
    none = out["verdict"] == "no_neighbour"
    fb_f = none & ~long_c & (out["c_median"] >= float(rl["temperature_pair_fallback_c_median_min"]))
    fb_c = none & long_c & (out["c_median"] <= float(rl["temperature_pair_fallback_c_median_max"]))
    decided = out["verdict"].isin(["c_holds_f", "f_holds_c"])
    out["decision"] = np.select([decided, fb_f, fb_c], [out["verdict"], "c_holds_f", "f_holds_c"], default="suspect")
    out["basis"] = np.select([decided, fb_f | fb_c], ["neighbours", "c_median_fallback"], default="none")
    out.to_parquet(report / "pair_decisions.parquet", index=False)
    out.to_csv(report / "pair_decisions.csv", index=False)
    summary = (out.groupby(["verdict", "decision", "basis"]).agg(pairs=("sensor_c", "size"), c_rows=("c_rows", "median"),
                                          err_c=("err_c", "median"), err_f=("err_f", "median"),
                                          c_median=("c_median", "median"), f_median=("f_median", "median"))
               .reset_index())
    summary.to_csv(report / "summary.csv", index=False)
    meta = {"run_id": args.run_id, "rules_version": rules["version"], "finished_utc": datetime.now(timezone.utc).isoformat(),
            "inputs": [str(eda / "_sensor_pairs"), str(eda / "_sensor_hour"), args.values_units_report, args.sensor_units_v1],
            "decisions": out.groupby(["decision", "basis"]).size().reset_index(name="pairs").to_dict("records")}
    (report / "run.json").write_text(json.dumps(meta, indent=2, default=int), encoding="utf-8")
    print(summary.round(2).to_string(), flush=True)


if __name__ == "__main__":
    main()
