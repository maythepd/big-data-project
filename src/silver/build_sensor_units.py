"""Per-sensor unit decisions (R06b) -> silver/dim_sensor_unit (pandas, no Spark).

Combines sensor medians, temperature pair/channel checks and gas twin checks into one
action per (sensors_id, parameter, units): relabel_ppm, relabel_f, relabel_c or suspect.
Thresholds: config/silver_rules_v1.yaml `unit_check`, `unit_relabel`.
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
    # ~1000x below the group: "ppb" values that were already ppm
    ppm = ((sensors["unit_std"] == "ppm") & (units == "ppb") & positive
           & (ratio <= float(rl["ppb_as_ppm_max_ratio"])) & (ratio * 1000 >= 1.0 / float(uc["ratio_suspect"])))
    f_median = (sensors["median"] - 32.0) * 5.0 / 9.0
    lo, hi = rl["c_as_f_converted_median"]
    hot = sensors["median"] > float(uc["temperature_c_median_max"])
    f = ((sensors["parameter_std"] == "temperature") & (units == "c") & hot
         & f_median.between(float(lo), float(hi)))
    hot_p99 = (sensors["parameter_std"] == "temperature") & (units == "c") & (
        sensors["p99"] > float(rl["temperature_c_p99_max"]))
    conditions = [ppm, f, off | ((sensors["parameter_std"] == "temperature") & (units == "c") & hot), hot_p99]
    action = np.select(conditions, ["relabel_ppm", "relabel_f", "suspect", "suspect"], default="")
    reason = np.select(conditions, ["ppb_median_1000x_low", "c_median_above_max", "median_off_group",
                                    "c_p99_above_max"], default="")
    return sensors.assign(median_ratio=ratio, action=action, reason=reason)


def decide_pairs(pairs):
    """Unit actions for temperature c/f pairs; conflicts and unresolved pairs -> suspect."""
    d = pairs[["sensor_c", "sensor_f"]].astype(str)
    d["holds"] = pairs["decision"].map({"c_holds_f": "f", "f_holds_c": "c"}).values
    d["linked"] = False

    def channels(d):
        return pd.concat([
            d[["sensor_c", "holds", "linked"]].rename(columns={"sensor_c": "sensors_id"}).assign(units_l="c"),
            d[["sensor_f", "holds", "linked"]].rename(columns={"sensor_f": "sensors_id"}).assign(units_l="f")],
            ignore_index=True)

    def agreed(d):
        """(sensors_id, units_l) -> the one unit its decided pairs agree on."""
        known = channels(d).dropna(subset=["holds"]).groupby(["sensors_id", "units_l"])["holds"].agg(set)
        return known[known.map(len) == 1].map(min)

    while True:
        unit = agreed(d)
        via_c = pd.Series(list(zip(d["sensor_c"], ["c"] * len(d))), index=d.index).map(unit)
        via_f = pd.Series(list(zip(d["sensor_f"], ["f"] * len(d))), index=d.index).map(unit)
        new = via_c.fillna(via_f)
        new = new[d["holds"].isna() & new.notna() & ~(via_c.notna() & via_f.notna() & (via_c != via_f))]
        if new.empty:
            break
        d.loc[new.index, "holds"] = new
        d.loc[new.index, "linked"] = True

    ch = channels(d)
    per = (ch.groupby(["sensors_id", "units_l"])
             .agg(units=("holds", lambda s: set(s.dropna())),
                  direct=("linked", lambda s: bool((~s & ch.loc[s.index, "holds"].notna()).any())))
             .reset_index())
    n = per["units"].map(len)
    holds = per["units"].map(lambda s: min(s) if len(s) == 1 else "")
    c, f = per["units_l"] == "c", per["units_l"] == "f"
    per["action"] = np.select([n != 1, c & (holds == "f"), f & (holds == "c")],
                              ["suspect", "relabel_f", "relabel_c"], default="")
    reason = pd.Series(np.select([c & (holds == "f"), f & (holds == "c"), c],
                                 ["pair_c_holds_f", "pair_f_holds_c", "pair_c_as_declared"],
                                 default="pair_f_as_declared"), index=per.index)
    reason = reason.where(per["direct"], reason + "_linked")
    per["reason"] = np.select([n > 1, n == 0], ["pair_conflict", "pair_unresolved"], default=reason)
    return per[["sensors_id", "units_l", "action", "reason"]].assign(parameter_std="temperature")


def decide_channels(channels, rl):
    """Unit actions for unpaired temperature channels, judged against neighbouring stations."""
    c = channels.assign(sensors_id=channels["sensors_id"].astype(str))
    other = c["verdict"] == "other_unit"
    fits = other & (c["err_other"] <= float(rl["temperature_channel_max_err_c"]))
    far = ((c["verdict"] == "as_declared")
           & (c[["err_declared", "err_other"]].min(axis=1) > float(rl["temperature_channel_far_c"]))
           & (c["ref_hours"] >= int(rl["temperature_channel_far_min_hours"]))
           & (c["n_neighbours"] >= int(rl["temperature_channel_far_min_neighbours"])))
    unsure = (other | (c["verdict"] == "unclear")) & ~fits
    is_c = c["units_l"] == "c"
    c["action"] = np.select([fits & is_c, fits, unsure | far], ["relabel_f", "relabel_c", "suspect"], default="")
    c["reason"] = np.select([fits & is_c, fits, unsure, far],
                            ["channel_c_holds_f", "channel_f_holds_c", "channel_unresolved", "far_from_neighbours"],
                            default="")
    return c.loc[c["action"] != "", ["sensors_id", "units_l", "action", "reason"]].assign(parameter_std="temperature")


def decide_twins(ratios, rl):
    """Suspect the newer channel of a gas pair off by an exact unit factor."""
    t = ratios[ratios["factor"].isin(rl["gas_twin_factors"])]
    newer = np.where(t["hi_rows"] < t["lo_rows"], t["sensor_hi"], t["sensor_lo"])
    units = np.where(t["hi_rows"] < t["lo_rows"], t["hi_units"], t["lo_units"])
    return (pd.DataFrame({"sensors_id": newer.astype(str), "units_l": units, "parameter_std": t["parameter"].values})
            .drop_duplicates().assign(action="suspect", reason="twin_unit_mismatch"))


def overlay(decided, actions):
    """Replace the median-based decision of the sensors listed in actions."""
    keys = decided.assign(units_l=decided["units"].str.strip().str.lower(), _i=range(len(decided)))
    hit = keys.merge(actions, on=["sensors_id", "units_l", "parameter_std"], suffixes=("", "_new"))
    decided.loc[decided.index[hit["_i"]], "action"] = hit["action_new"].values
    decided.loc[decided.index[hit["_i"]], "reason"] = hit["reason_new"].values
    return len(hit)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--values-units-report", default=str(LAKE / "reports" / "values_units_full_01"))
    p.add_argument("--pair-decisions",
                   default=str(LAKE / "reports" / "temperature_pairs_01" / "pair_decisions.parquet"),
                   help="temperature c/f pair decisions (check_temperature_pairs.py); 'none' to skip")
    p.add_argument("--channel-decisions",
                   default=str(LAKE / "reports" / "temperature_channels_02" / "channel_decisions.parquet"),
                   help="unpaired temperature channels (check_temperature_channels.py); 'none' to skip")
    p.add_argument("--gas-pair-ratios",
                   default=str(LAKE / "reports" / "gas_sensor_agreement_01" / "pair_ratios.parquet"),
                   help="co-located gas pairs (check_gas_sensor_agreement.py); 'none' to skip")
    p.add_argument("--rules", default=str(DEFAULT_RULES))
    p.add_argument("--output", default=str(LAKE / "silver" / "dim_sensor_unit"))
    p.add_argument("--run-id", default="sensor_units_03")
    args = p.parse_args()
    rules = load_rules(args.rules)
    rl = rules["unit_relabel"]
    source = Path(args.values_units_report)
    sensors = pd.read_parquet(source / "_sensor_values")
    quant = pd.concat(pd.read_csv(f) for f in (source / "quantiles").glob("*.csv"))
    sensors = sensors.merge(quant[["parameter_std", "unit_std", "p50"]].rename(columns={"p50": "group_median"}),
                            on=["parameter_std", "unit_std"], how="left")
    decided = decide(sensors, rules)
    # later sources win (they never share a sensor)
    sources = [(args.pair_decisions, lambda f: decide_pairs(pd.read_parquet(f))),
               (args.channel_decisions, lambda f: decide_channels(pd.read_parquet(f), rl)),
               (args.gas_pair_ratios, lambda f: decide_twins(pd.read_parquet(f), rl))]
    overlaid = {}
    for path, build in sources:
        if path != "none":
            overlaid[path] = overlay(decided, build(path))
    out = decided[decided["action"] != ""][
        ["sensors_id", "parameter", "units", "parameter_std", "unit_std", "action",
         "reason", "rows", "median", "group_median", "median_ratio"]]
    target = Path(args.output)
    target.mkdir(parents=True, exist_ok=True)
    out.to_parquet(target / "dim_sensor_unit.parquet", index=False)

    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    summary = (decided.assign(action=decided["action"].replace("", "as_declared"))
               .groupby(["parameter_std", "parameter", "units", "action", "reason"])
               .agg(sensors=("sensors_id", "count"), rows=("rows", "sum")).reset_index()
               .sort_values(["parameter_std", "units", "action"]))
    summary.to_csv(report / "sensor_units_summary.csv", index=False)
    meta = {"run_id": args.run_id, "rules_version": rules["version"], "values_units_report": str(source),
            "overlays": overlaid,
            "output": str(target), "finished_utc": datetime.now(timezone.utc).isoformat(),
            "actions": out.groupby(["action", "reason"]).agg(sensors=("sensors_id", "count"),
                                                 rows=("rows", "sum")).reset_index().to_dict("records")}
    (report / "run.json").write_text(json.dumps(meta, indent=2, default=int), encoding="utf-8")
    print(json.dumps(meta["actions"], indent=2, default=int))


if __name__ == "__main__":
    main()
