"""Do co-located gas sensors disagree by an exact unit-conversion factor?

A stable ratio of 1, MW/24.45, 1000 or 1000*MW/24.45 means one channel has the wrong unit label.
Output: reports/<run>/pair_ratios.parquet, summary.csv, disagreement.csv. pandas only.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.schema import MOLAR_VOLUME, MOLECULAR_WEIGHT

LAKE = Path.home() / "openaq_lake"
GASES = ["no2", "so2", "co", "o3", "no"]


def factors(parameter):
    f = MOLECULAR_WEIGHT[parameter] / MOLAR_VOLUME
    return {"same": 1.0, "ppb_ugm3": f, "ppm_ppb": 1000.0, "ppm_ugm3": 1000.0 * f}


def pair_ratios(pairs, tol, min_share):
    p = pairs[pairs["parameter"].isin(GASES) & (pairs["a"] > 0) & (pairs["b"] > 0)].copy()
    p["lr"] = np.log10(p["a"] / p["b"])
    keys = ["parameter", "location_id", "sensor_a", "sensor_b"]
    p["lr_med"] = p.groupby(keys)["lr"].transform("median")
    p["stable"] = (p["lr"] - p["lr_med"]).abs() < np.log10(1 + tol)
    g = p.groupby(keys).agg(pair_hours=("lr", "size"), lr=("lr", "median"), stable_share=("stable", "mean"),
                            first_hour=("hour", "min"), last_hour=("hour", "max")).reset_index()
    g["ratio"] = 10 ** g["lr"].abs()
    hi_a = g["lr"] >= 0
    g["sensor_hi"] = np.where(hi_a, g["sensor_a"], g["sensor_b"])
    g["sensor_lo"] = np.where(hi_a, g["sensor_b"], g["sensor_a"])
    g["factor"] = "none"
    for parameter in GASES:
        rows = g["parameter"] == parameter
        for name, value in factors(parameter).items():
            hit = rows & ((g["ratio"] / value - 1).abs() < tol) & (g["stable_share"] >= min_share)
            g.loc[hit, "factor"] = name
    return g.drop(columns=["sensor_a", "sensor_b", "lr"]), p


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--pairs", default=str(LAKE / "reports" / "silver_for_gold_01" / "_sensor_pairs"))
    p.add_argument("--sensor-values", default=str(LAKE / "reports" / "values_units_full_01" / "_sensor_values"))
    p.add_argument("--run-id", default="gas_sensor_agreement_01")
    p.add_argument("--tol", type=float, default=0.005, help="relative tolerance around a factor")
    p.add_argument("--min-share", type=float, default=0.9, help="share of pair-hours near the median ratio")
    args = p.parse_args()
    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)

    pairs = pd.read_parquet(args.pairs, columns=["location_id", "parameter", "hour", "sensor_a", "a", "sensor_b", "b"])
    out, hours = pair_ratios(pairs, args.tol, args.min_share)

    sv = pd.read_parquet(args.sensor_values, columns=["sensors_id", "units", "rows", "median"])
    sv["sensors_id"] = sv["sensors_id"].astype("int64")
    sv["units"] = sv["units"].str.strip().str.lower()
    for side in ["hi", "lo"]:
        out = out.merge(sv.rename(columns=lambda c: f"{side}_{c}"), how="left",
                        left_on=f"sensor_{side}", right_on=f"{side}_sensors_id").drop(columns=f"{side}_sensors_id")
    out = out.sort_values(["parameter", "factor", "pair_hours"], ascending=[True, True, False])
    out.to_parquet(report / "pair_ratios.parquet", index=False)
    out.to_csv(report / "pair_ratios.csv", index=False)

    summary = (out.groupby(["parameter", "factor", "hi_units", "lo_units"], dropna=False)
                  .agg(pairs=("pair_hours", "size"), pair_hours=("pair_hours", "sum"),
                       locations=("location_id", "nunique"), hi_rows_median=("hi_rows", "median"),
                       lo_rows_median=("lo_rows", "median"), first_hour=("first_hour", "min"),
                       last_hour=("last_hour", "max"))
                  .reset_index())
    summary.to_csv(report / "summary.csv", index=False)

    hours = hours.merge(out[["parameter", "location_id", "sensor_hi", "sensor_lo", "factor"]].assign(
        sensor_a=lambda d: d[["sensor_hi", "sensor_lo"]].min(axis=1),
        sensor_b=lambda d: d[["sensor_hi", "sensor_lo"]].max(axis=1))[
        ["parameter", "location_id", "sensor_a", "sensor_b", "factor"]],
        on=["parameter", "location_id", "sensor_a", "sensor_b"])
    far = hours[10 ** hours["lr"].abs() > 1.5]
    disagreement = pd.DataFrame({
        "pair_hours": hours.groupby("parameter").size(),
        "pair_hours_ratio_gt_1_5": far.groupby("parameter").size(),
        "explained_by_unit_factor": far[~far["factor"].isin(["none", "same"])].groupby("parameter").size(),
    }).fillna(0).astype("int64")
    disagreement["explained_share"] = (disagreement["explained_by_unit_factor"]
                                       / disagreement["pair_hours_ratio_gt_1_5"].replace(0, np.nan)).round(3)
    disagreement.reset_index(names="parameter").to_csv(report / "disagreement.csv", index=False)

    run = {"status": "complete", "args": vars(args), "started_utc": started.isoformat(),
           "finished_utc": datetime.now(timezone.utc).isoformat(), "pairs": len(out),
           "factor_pairs": out["factor"].value_counts().to_dict()}
    (report / "run.json").write_text(json.dumps(run, indent=2, default=str), encoding="utf-8")
    print(summary[summary["factor"] != "none"].to_string(index=False))
    print(disagreement.to_string())


if __name__ == "__main__":
    main()
