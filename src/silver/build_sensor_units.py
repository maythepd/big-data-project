"""rule finalization: per-sensor unit decisions (R06b) from the values/units checks sensor medians.

Input : reports/values_units_full_01/_sensor_values (median per sensor and raw parameter/units,
        value converted with the declared unit) and quantiles (group median p50).
Output: silver/dim_sensor_unit/dim_sensor_unit.parquet, one row per
        (sensors_id, parameter, units) that is not taken at face value:
          action = relabel_ppm  "ppb" gas whose values are already ppm (no /1000)
                   relabel_f    "c" temperature whose values are °F
                   suspect      other median off by >= ratio_suspect (flag unit_suspect)
        reports/<run>/sensor_units_summary.csv per (parameter, units, action).
Thresholds: config/silver_rules_v1.yaml `unit_check` and `unit_relabel`.
pandas only (the inputs are small): no Spark JVM.
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

from src.silver.rules import DEFAULT_RULES, load_rules

LAKE = Path.home() / "openaq_lake"


def decide(sensors, rules):
    uc, rl = rules["unit_check"], rules["unit_relabel"]
    ratio = sensors["median"] / sensors["group_median"]
    positive = (sensors["median"] > 0) & (sensors["group_median"] > 0)
    off = positive & ((ratio >= float(uc["ratio_suspect"])) | (ratio <= 1.0 / float(uc["ratio_suspect"])))
    units = sensors["units"].str.strip().str.lower()
    # Values of a "ppb" gas divided by 1000 sit ~1000x below the group: they were ppm.
    # The lower limit keeps sensors that would still be off after undoing the /1000.
    ppm = ((sensors["unit_std"] == "ppm") & (units == "ppb") & positive
           & (ratio <= float(rl["ppb_as_ppm_max_ratio"])) & (ratio * 1000 >= 1.0 / float(uc["ratio_suspect"])))
    f_median = (sensors["median"] - 32.0) * 5.0 / 9.0
    lo, hi = rl["c_as_f_converted_median"]
    hot = sensors["median"] > float(uc["temperature_c_median_max"])
    f = ((sensors["parameter_std"] == "temperature") & (units == "c") & hot
         & f_median.between(float(lo), float(hi)))
    action = np.select([ppm, f, off | ((sensors["parameter_std"] == "temperature") & (units == "c") & hot)],
                       ["relabel_ppm", "relabel_f", "suspect"], default="")
    return sensors.assign(median_ratio=ratio, action=action)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--values-units-report", default=str(LAKE / "reports" / "values_units_full_01"))
    p.add_argument("--rules", default=str(DEFAULT_RULES))
    p.add_argument("--output", default=str(LAKE / "silver" / "dim_sensor_unit"))
    p.add_argument("--run-id", default="sensor_units_01")
    args = p.parse_args()
    rules = load_rules(args.rules)
    source = Path(args.values_units_report)
    sensors = pd.read_parquet(source / "_sensor_values")
    quant = pd.concat(pd.read_csv(f) for f in (source / "quantiles").glob("*.csv"))
    sensors = sensors.merge(quant[["parameter_std", "unit_std", "p50"]].rename(columns={"p50": "group_median"}),
                            on=["parameter_std", "unit_std"], how="left")
    decided = decide(sensors, rules)
    out = decided[decided["action"] != ""][
        ["sensors_id", "parameter", "units", "parameter_std", "unit_std", "action",
         "rows", "median", "group_median", "median_ratio"]]
    target = Path(args.output)
    target.mkdir(parents=True, exist_ok=True)
    out.to_parquet(target / "dim_sensor_unit.parquet", index=False)

    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    summary = (decided.assign(action=decided["action"].replace("", "as_declared"))
               .groupby(["parameter_std", "parameter", "units", "action"])
               .agg(sensors=("sensors_id", "count"), rows=("rows", "sum")).reset_index()
               .sort_values(["parameter_std", "units", "action"]))
    summary.to_csv(report / "sensor_units_summary.csv", index=False)
    meta = {"run_id": args.run_id, "rules_version": rules["version"], "values_units_report": str(source),
            "output": str(target), "finished_utc": datetime.now(timezone.utc).isoformat(),
            "actions": out.groupby("action").agg(sensors=("sensors_id", "count"),
                                                 rows=("rows", "sum")).to_dict("index")}
    (report / "run.json").write_text(json.dumps(meta, indent=2, default=int), encoding="utf-8")
    print(json.dumps(meta["actions"], indent=2, default=int))


if __name__ == "__main__":
    main()
