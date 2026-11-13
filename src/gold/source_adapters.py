"""WAQI and OpenWeather -> source x station x hour in Silver units, per month.

    python3 src/gold/source_adapters.py [--months YYYY-MM ...]
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import yaml

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import ROOT

LAKE = Path.home() / "openaq_lake"
DEFAULT_GOLD_RULES = ROOT / "config" / "gold_rules_v1.yaml"


def load_gold_rules(path):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

HISTORY = LAKE / "bronze" / "openweather_history"
ARCHIVE = LAKE / "stream_copy" / "readings"        # scripts/fetch_stream_copy.sh
OWM_NAMES = {"pm2_5": "pm25"}
SCHEMA = pa.schema([("source", pa.string()), ("source_station_id", pa.string()), ("parameter", pa.string()),
                    ("unit", pa.string()), ("hour_utc", pa.timestamp("us", tz="UTC")),
                    ("value_mean", pa.float64()), ("value_min", pa.float64()), ("value_max", pa.float64()),
                    ("n_readings", pa.int64())])


def month_bounds(key):
    y, m = int(key[:4]), int(key[5:])
    return (pd.Timestamp(datetime(y, m, 1, tzinfo=timezone.utc)),
            pd.Timestamp(datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc)))


def to_silver_units(df, keep, mw, molar_volume):
    """µg/m³ -> ppm for gases with a molecular weight; PM stays µg/m³."""
    df = df[df["parameter"].isin(keep)].copy()
    gas = df["parameter"].isin(list(mw))
    df.loc[gas, "value"] = df.loc[gas, "value"] * molar_volume / (df.loc[gas, "parameter"].map(mw) * 1000.0)
    df["unit"] = "µg/m³"
    df.loc[gas, "unit"] = "ppm"
    return df


def owm_history(start, end):
    paths = [p for y in range(start.year, end.year + 1) for p in (HISTORY / f"year={y}").glob("*.parquet")]
    if not paths:
        return pd.DataFrame()
    t = ds.dataset(paths, format="parquet").to_table(
        filter=(ds.field("dt") >= start.to_pydatetime()) & (ds.field("dt") < end.to_pydatetime()))
    df = t.drop(["owm_aqi"]).to_pandas()
    df = df.melt(id_vars=["station_id", "dt"], var_name="parameter", value_name="value").dropna(subset=["value"])
    df["parameter"] = df["parameter"].replace(OWM_NAMES)
    return df.rename(columns={"dt": "time"}).assign(origin_rank=0)


def archive(source, start, end):
    if not ARCHIVE.is_dir():
        return pd.DataFrame()
    dirs = [d for d in ARCHIVE.glob("date=*") if start.date() <= pd.Timestamp(d.name[5:]).date() < end.date()]
    if not dirs:
        return pd.DataFrame()
    files = [str(f) for d in dirs for f in d.glob("*.parquet")]
    if not files:
        return pd.DataFrame()
    t = ds.dataset(files, format="parquet").to_table(
        columns=["source", "station_id", "parameter", "value", "unit", "event_time"],
        filter=ds.field("source") == source)
    df = t.to_pandas().rename(columns={"event_time": "time"})
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df[(df["time"] >= start) & (df["time"] < end)]
    df["station_id"] = df["station_id"].str.replace(f"{source}-", "", n=1, regex=False)
    return df.assign(origin_rank=1)


def hourly(df, source):
    df = df.assign(hour_utc=df["time"].dt.floor("h"))
    df = df.sort_values("origin_rank")
    # one origin per hour: the history beats the stream copy for OpenWeather
    first = df.groupby(["station_id", "parameter", "hour_utc"])["origin_rank"].transform("min")
    df = df[df["origin_rank"] == first]
    g = (df.groupby(["station_id", "parameter", "unit", "hour_utc"])["value"]
         .agg(value_mean="mean", value_min="min", value_max="max", n_readings="size").reset_index())
    return g.rename(columns={"station_id": "source_station_id"}).assign(source=source)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rules", default=str(DEFAULT_GOLD_RULES))
    p.add_argument("--months", nargs="*", help="YYYY-MM (default: every month with source data)")
    p.add_argument("--run-id", default="source_adapters_01")
    args = p.parse_args()
    src = load_gold_rules(args.rules)["sources"]
    silver_rules = yaml.safe_load((ROOT / "config" / "silver_rules_v1.yaml").read_text(encoding="utf-8"))
    mw = {str(k): float(v) for k, v in silver_rules["molecular_weight"].items()}
    molar_volume = float(silver_rules["molar_volume_l"])

    months = args.months
    if not months:
        years = sorted(int(d.name[5:]) for d in HISTORY.glob("year=*"))
        dates = sorted(d.name[5:12] for d in ARCHIVE.glob("date=*")) if ARCHIVE.is_dir() else []
        first = f"{years[0]}-11" if years else dates[0]
        last = max([datetime.now(timezone.utc).strftime("%Y-%m")] + dates[-1:])
        months = [str(m) for m in pd.period_range(first, last, freq="M")]

    out_root = LAKE / src["root"]
    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    counts = []
    for key in months:
        start, end = month_bounds(key)
        frames = [f for f in (owm_history(start, end), archive("openweather", start, end)) if len(f)]
        parts = []
        if frames:
            owm = pd.concat(frames, ignore_index=True)
            owm["time"] = pd.to_datetime(owm["time"], utc=True)
            owm = to_silver_units(owm, src["openweather_parameters"], mw, molar_volume)
            parts.append(hourly(owm, "openweather"))
        waqi = archive("waqi", start, end)
        if len(waqi):
            parts.append(hourly(waqi, "waqi"))
        if not parts:
            continue
        df = pd.concat(parts, ignore_index=True)[SCHEMA.names]
        target = out_root / f"year={start.year}" / f"month={start.month}"
        target.mkdir(parents=True, exist_ok=True)
        tmp = target / "part-0.parquet.tmp"
        pq.write_table(pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False), tmp, compression="zstd")
        tmp.rename(target / "part-0.parquet")
        c = df.groupby(["source", "parameter"]).agg(rows=("n_readings", "size"),
                                                    stations=("source_station_id", "nunique")).reset_index()
        counts.append(c.assign(month=key))
        print(key, {s: int(n) for s, n in df.groupby("source").size().items()}, flush=True)
    if counts:
        pd.concat(counts).to_csv(report / "counts.csv", index=False)
    (report / "run.json").write_text(json.dumps({"months": months, "finished_utc": datetime.now(timezone.utc).isoformat()},
                                                indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
