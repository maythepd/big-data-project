"""Does an unpaired temperature channel carry the other unit (°C vs °F)?

Compares each channel, as declared and converted, with nearby reference stations.
Output: reports/<run>/channel_decisions.parquet (as_declared, other_unit, unclear, no_neighbour).
pandas + pyarrow, month by month; no Spark.
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

from src.silver.check_temperature_pairs import LAKE, neighbours, reference, temperature_sensors
from src.silver.rules import DEFAULT_RULES, load_rules


def scan(dataset, months, sensors, columns):
    """Batches of temperature sensor-hours of the given sensors, one month at a time."""
    ids = pa.array(sorted(sensors), pa.int64())
    for y, m in months:
        scanner = dataset.scanner(
            columns=columns,
            filter=((pc.field("year") == y) & (pc.field("month") == m) & (pc.field("parameter") == "temperature")
                    & pc.is_in(pc.field("sensors_id"), value_set=ids) & pc.is_valid(pc.field("mean_valid"))),
            use_threads=False, batch_readahead=0, fragment_readahead=0, batch_size=200_000)
        yield (y, m), [b.to_pandas() for b in scanner.to_batches() if b.num_rows]


def sample_hours(dataset, months, candidates, per_sensor):
    """About per_sensor hours of each candidate, spread over its life."""
    counts = []
    for _, chunks in scan(dataset, months, candidates, ["sensors_id"]):
        counts += [c["sensors_id"].value_counts() for c in chunks]
    hours = pd.concat(counts).groupby(level=0).sum()
    keep = (per_sensor / hours).clip(upper=1.0)
    out = []
    for (y, m), chunks in scan(dataset, months, candidates, ["location_id", "sensors_id", "hour", "mean_valid"]):
        for c in chunks:
            h = (c["sensors_id"].values * 1_000_003 + c["hour"].values.astype("datetime64[h]").astype("int64"))
            u = ((h * 2_654_435_761) % (1 << 32)) / float(1 << 32)
            out.append(c[u < c["sensors_id"].map(keep).values])
        print(f"sample {y}-{m:02d}: {sum(len(o) for o in out)} sensor-hours", flush=True)
    return pd.concat(out, ignore_index=True), hours


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver-eda-report", default=str(LAKE / "reports" / "silver_for_gold_01"))
    p.add_argument("--values-units-report", default=str(LAKE / "reports" / "values_units_full_01"))
    p.add_argument("--pair-decisions", default=str(LAKE / "reports" / "temperature_pairs_01" / "pair_decisions.parquet"))
    p.add_argument("--sensor-units", default=str(LAKE / "silver" / "dim_sensor_unit"))
    p.add_argument("--rules", default=str(DEFAULT_RULES))
    p.add_argument("--run-id", default="temperature_channels_01")
    args = p.parse_args()
    rules = load_rules(args.rules)
    rl = rules["unit_relabel"]
    eda = Path(args.silver_eda_report)
    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)

    sensors = temperature_sensors(Path(args.values_units_report) / "_sensor_values")
    pairs = pd.read_parquet(args.pair_decisions, columns=["sensor_c", "sensor_f"])
    paired = set(pairs["sensor_c"]) | set(pairs["sensor_f"])
    dim = pd.read_parquet(args.sensor_units, columns=["sensors_id", "action"])
    dim["sensors_id"] = dim["sensors_id"].astype("int64")
    candidates = sensors[sensors["units_l"].isin(["c", "f"]) & ~sensors["sensors_id"].isin(paired)
                         & ~sensors["sensors_id"].isin(dim["sensors_id"])]
    unit = candidates.set_index("sensors_id")["units_l"]
    print(f"candidates: {unit.value_counts().to_dict()}", flush=True)

    dataset = ds.dataset(eda / "_sensor_hour", partitioning="hive")
    months = sorted((int(f.split("year=")[1].split("/")[0]), int(f.split("month=")[1].split("/")[0]))
                    for f in {str(Path(x).parent) for x in dataset.files})
    sample, hours = sample_hours(dataset, months, set(unit.index), int(rl["temperature_channel_sample_hours"]))

    loc = pd.read_parquet(LAKE / "silver" / "dim_location", columns=["location_id_num", "lat_ref", "lon_ref"]).dropna()
    loc["location_id"] = loc["location_id_num"].astype("int64")
    loc = loc.drop_duplicates("location_id").set_index("location_id")[["lat_ref", "lon_ref"]]
    short = sensors.loc[sensors["rows"] < int(rl["temperature_pair_reference_min_rows"]), "sensors_id"]
    # reference: long-running "c" channels only ("f" ones misled run 01)
    declared_f = sensors.loc[sensors["units_l"] != "c", "sensors_id"]
    exclude = paired | set(short) | set(declared_f) | set(dim["sensors_id"])
    ref_locs = loc.loc[loc.index.intersection(sensors["location_id"].dropna().astype("int64").unique())]
    cand_locs = loc.reindex(sample["location_id"].unique()).dropna()
    near = neighbours(cand_locs, ref_locs, float(rl["temperature_pair_neighbour_km"]),
                      int(rl["temperature_pair_neighbours"]))
    print(f"candidate locations {len(cand_locs)}, with neighbours {near['location_id'].nunique()}", flush=True)

    ref = reference(eda / "_sensor_hour", sample, near, exclude)
    # month by month: joining everything at once exhausts memory
    ym = sample["hour"].dt.to_period("M")
    ref_ym = ref["hour"].dt.to_period("M")
    parts = []
    for key in sorted(ym.unique()):
        x = (sample[ym == key].merge(near[["location_id", "nb"]], on="location_id")
             .merge(ref[ref_ym == key], on=["nb", "hour"]))
        parts.append(x.groupby(["sensors_id", "hour", "mean_valid"])
                      .agg(ref=("r", "median"), n_nb=("nb", "nunique")).reset_index())
    x = pd.concat(parts, ignore_index=True)
    del ref, parts
    gc.collect()
    is_c = x["sensors_id"].map(unit).eq("c")
    v = x["mean_valid"]
    x["other"] = np.where(is_c, (v - 32.0) * 5.0 / 9.0, v * 9.0 / 5.0 + 32.0)
    x["err_declared"], x["err_other"] = (v - x["ref"]).abs(), (x["other"] - x["ref"]).abs()
    ev = (x.groupby("sensors_id").agg(ref_hours=("hour", "size"), n_neighbours=("n_nb", "median"),
                                      err_declared=("err_declared", "median"), err_other=("err_other", "median"),
                                      ref_median=("ref", "median")).reset_index())

    st = (sample.groupby("sensors_id").agg(location_id=("location_id", "first"), sampled_hours=("hour", "size"),
                                           first_hour=("hour", "min"), last_hour=("hour", "max"),
                                           value_median=("mean_valid", "median")).reset_index())
    out = (candidates[["sensors_id", "units_l", "rows", "median"]].rename(columns={"median": "raw_median"})
           .merge(st, on="sensors_id", how="left").merge(ev, on="sensors_id", how="left"))
    out["hours"] = out["sensors_id"].map(hours)
    out = out.join(loc, on="location_id")
    margin = float(rl["temperature_pair_margin_c"])
    out["verdict"] = np.select([out["err_declared"].isna(), out["err_other"] + margin < out["err_declared"],
                                out["err_declared"] + margin < out["err_other"]],
                               ["no_neighbour", "other_unit", "as_declared"], default="unclear")
    out.to_parquet(report / "channel_decisions.parquet", index=False)
    out.to_csv(report / "channel_decisions.csv", index=False)
    summary = (out.assign(short=out["rows"] < int(rl["temperature_pair_reference_min_rows"]))
               .groupby(["units_l", "short", "verdict"])
               .agg(channels=("sensors_id", "size"), rows=("rows", "sum"), raw_median=("raw_median", "median"),
                    err_declared=("err_declared", "median"), err_other=("err_other", "median"),
                    ref_hours=("ref_hours", "median"))
               .reset_index())
    summary.to_csv(report / "summary.csv", index=False)
    meta = {"run_id": args.run_id, "rules_version": rules["version"], "started_utc": started.isoformat(),
            "finished_utc": datetime.now(timezone.utc).isoformat(),
            "inputs": [str(eda / "_sensor_hour"), args.values_units_report, args.pair_decisions, args.sensor_units],
            "verdicts": out.groupby(["units_l", "verdict"]).size().reset_index(name="channels").to_dict("records")}
    (report / "run.json").write_text(json.dumps(meta, indent=2, default=int), encoding="utf-8")
    print(summary.round(2).to_string(), flush=True)


if __name__ == "__main__":
    main()
