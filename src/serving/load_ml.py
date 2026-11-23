"""Load one run of src/ml/aqi_forecast.py into MongoDB for the dashboard's forecast page.

Collections (small, regular, replaced whole on each load): ml_forecast, ml_warning, ml_backtest,
ml_warning_backtest, ml_recent, and ml_metrics (one document per standard, a "run" document and,
after src/ml/compare_models.py, a "comparison" document).

    python -m src.serving.load_ml               # the newest run in openaq_lake/gold/ml
    python -m src.serving.load_ml --run ml_20261123_1010
"""
import argparse
import json
from pathlib import Path
import sys

import pyarrow.dataset as ds
from pymongo import ASCENDING, MongoClient

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import LAKE, MONGO_DB, mongo_uri  # noqa: E402

ML = LAKE / "gold" / "ml"
TABLES = {"forecast": "ml_forecast", "warning": "ml_warning", "backtest": "ml_backtest",
          "warning_backtest": "ml_warning_backtest", "recent": "ml_recent"}


def records(path):
    table = ds.dataset(path, format="parquet", partitioning="hive").to_table()
    return table.to_pylist()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run", help="run id (default: the newest with a metrics.json)")
    args = p.parse_args()
    runs = sorted(d.name for d in ML.glob("*") if (d / "metrics.json").exists())
    if not runs:
        raise SystemExit(f"no finished run in {ML}: run src/ml/aqi_forecast.py first")
    run = args.run or runs[-1]
    root = ML / run
    db = MongoClient(mongo_uri(), serverSelectionTimeoutMS=10_000)[MONGO_DB]
    for folder, name in TABLES.items():
        docs = records(root / folder)
        for d in docs:
            d["run_id"] = run
        db[name].drop()
        if docs:
            db[name].insert_many(docs, ordered=False)
        db[name].create_index([("std", ASCENDING), ("location_id", ASCENDING)])
        print(f"{name}: {len(docs):,} documents")
    metrics = json.loads((root / "metrics.json").read_text(encoding="utf-8"))
    per_std = metrics.pop("std")
    db.ml_metrics.drop()
    docs = [{"_id": "run", **metrics}] + [{"_id": s, "run_id": run, **m} for s, m in per_std.items()]
    if (root / "comparison.json").exists():            # src/ml/compare_models.py: XGBoost and LSTM
        docs.append({"_id": "comparison", **json.loads((root / "comparison.json").read_text(encoding="utf-8"))})
    db.ml_metrics.insert_many(docs)
    print(f"ml_metrics: run {run}")


if __name__ == "__main__":
    main()
