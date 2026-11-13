"""Small summary collections for the dashboard, so pages never scan the large ones.

locations, pm25_monthly, pipeline_monthly, parameter_status, pipeline_runs, dashboard_meta;
each replaced as a whole. Run after a Gold or MongoDB load.
"""

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

import numpy as np
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pymongo import GEOSPHERE, MongoClient

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import DIM_DIR, LAKE, MONGO_DB, mongo_uri

REPORTS = LAKE / "reports"
RUNS = {"silver_validation": "silver_v1_2_01_validation", "gold_hourly": "gold_v1_01",
        "gold_aqi_daily": "gold_aqi_daily_v1_01", "gold_validation": "gold_v1_01_validation",
        "mongo_load": "mongo_load_v1_01"}


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def replace(db, name, docs, indexes=()):
    db.drop_collection(name)
    if docs:
        db[name].insert_many(docs, ordered=False)
    for keys in indexes:
        db[name].create_index(keys)
    return len(docs)


def locations():
    vn = set(pq.read_table(DIM_DIR / "openaq_locations.parquet", columns=["station_id"])
             .column("station_id").to_pylist())
    t = ds.dataset(str(LAKE / "silver" / "dim_location"), format="parquet").to_table(
        columns=["location_id_num", "location_name", "lat_ref", "lon_ref", "location_kind",
                 "first_utc", "last_utc", "rows_total"])
    docs = []
    for r in t.to_pylist():
        lat, lon = r["lat_ref"], r["lon_ref"]
        if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        docs.append({"_id": int(r["location_id_num"]), "name": r["location_name"],
                     "kind": r["location_kind"], "first_utc": r["first_utc"], "last_utc": r["last_utc"],
                     "rows": r["rows_total"], "is_vn": int(r["location_id_num"]) in vn,
                     "location": {"type": "Point", "coordinates": [lon, lat]}})
    return docs


def pm25_monthly():
    """Valid PM2.5 station-days per local month (Gold partition = local month), VN vs rest."""
    root = LAKE / "gold" / "station_daily"
    docs = []
    for y in sorted(root.glob("year=*"), key=lambda p: int(p.name[5:])):
        for m in sorted(y.glob("month=*"), key=lambda p: int(p.name[6:])):
            t = ds.dataset(str(m), format="parquet").to_table(
                columns=["location_id", "value_mean", "is_vn"],
                filter=(ds.field("parameter") == "pm25") & (ds.field("day_valid") == True))  # noqa: E712
            if t.num_rows == 0:
                continue
            month = datetime(int(y.name[5:]), int(m.name[6:]), 1)
            for is_vn in (True, False):
                part = t.filter(pc.equal(t.column("is_vn"), is_vn))
                if part.num_rows == 0:
                    continue
                v = part.column("value_mean").to_numpy()
                v = v[np.isfinite(v)]
                docs.append({"month": month, "is_vn": is_vn, "station_days": int(len(v)),
                             "stations": len(pc.unique(part.column("location_id"))),
                             "mean": float(v.mean()), "median": float(np.median(v)),
                             "p90": float(np.percentile(v, 90))})
    return docs


def pipeline_monthly():
    docs = {}
    for f in (REPORTS / RUNS["gold_hourly"] / "done").iterdir():
        d = read_json(f)
        docs[f.name] = {"month": datetime(int(f.name[:4]), int(f.name[5:]), 1), "silver_rows": d["rows"],
                        **{k: d[k] for k in ("valid", "suspect_allowed", "suspect_excluded", "invalid",
                                             "station_hours", "vn_station_hours")},
                        "seconds_gold_hourly": d.get("seconds")}
    for f in (REPORTS / RUNS["gold_aqi_daily"] / "done").iterdir():
        d = read_json(f)
        docs.setdefault(f.name, {"month": datetime(int(f.name[:4]), int(f.name[5:]), 1)}).update(
            {k: d.get(k) for k in ("station_hourly_aqi", "station_daily", "station_daily_valid",
                                   "station_daily_aqi")}, seconds_gold_aqi_daily=d.get("seconds"))
    return sorted(docs.values(), key=lambda d: d["month"])


def parameter_status():
    path = REPORTS / RUNS["silver_validation"] / "status_by_parameter.csv"
    with path.open(encoding="utf-8") as f:
        return [{"parameter": r["parameter"], "unit": r["unit"], "dq_status": r["dq_status"],
                 "rows": int(r["rows"])} for r in csv.DictReader(f)]


def pipeline_runs(db):
    out = []
    sv = read_json(REPORTS / RUNS["silver_validation"] / "validation.json")
    if sv:
        rec = sv["checks"]["reconciliation"]
        out.append({"stage": "Silver v1.2", "run_id": sv["run_id"], "ok": sv["ok"],
                    "finished_utc": sv.get("finished_utc"),
                    "numbers": {"compact_rows": rec["compact_rows"], "silver_rows": rec["silver"],
                                "dedup_removed": rec["dedup_removed"], "quarantine": rec["quarantine"],
                                **{f"status_{k}": v for k, v in sv["status_totals"].items()}}})
    for stage, key in [("Gold lượt 1 (station_hourly)", "gold_hourly"),
                       ("Gold lượt 2 (AQI, bảng ngày)", "gold_aqi_daily")]:
        run = read_json(REPORTS / RUNS[key] / "run.json") or {}
        done = [read_json(f) for f in (REPORTS / RUNS[key] / "done").iterdir()]
        sums = {}
        for d in done:
            for k, v in d.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    sums[k] = sums.get(k, 0) + v
        out.append({"stage": stage, "run_id": RUNS[key], "ok": run.get("status") == "complete",
                    "finished_utc": run.get("finished_utc"), "months": len(done),
                    "numbers": {k: (round(v / 3600, 2) if k.startswith("seconds") else int(v))
                                for k, v in sums.items()}})
    gv = read_json(REPORTS / RUNS["gold_validation"] / "checks.json")
    if gv:
        months = list(gv["months"].values()) if isinstance(gv["months"], dict) else gv["months"]
        out.append({"stage": "Kiểm định Gold", "run_id": RUNS["gold_validation"], "ok": gv["ok"],
                    "finished_utc": gv.get("checked_utc"), "months": len(months),
                    "numbers": {k: sum(m[k] for m in months)
                                for k in ("rows", "n_readings", "silver_usable_rows", "outside_month",
                                          "mean_outside_min_max", "null_mean")}})
    mr = read_json(REPORTS / RUNS["mongo_load"] / "run.json")
    if mr:
        out.append({"stage": "Nạp MongoDB", "run_id": mr["run_id"], "ok": mr.get("ok"),
                    "finished_utc": mr.get("finished_utc"),
                    "numbers": {"seconds_load": mr.get("seconds_load"),
                                **{f"docs_{n}": c.get("documents") for n, c in mr.get("collections", {}).items()},
                                **{f"disk_bytes_{n}": c["stats"].get("storageSize")
                                   for n, c in mr.get("collections", {}).items()}}})
    bench = read_json(REPORTS / RUNS["mongo_load"] / "bench.json")
    if bench:
        g = bench["global_month_timeseries"]
        out.append({"stage": "Đo nạp 1 tháng toàn cầu theo giờ", "run_id": RUNS["mongo_load"], "ok": True,
                    "numbers": {"rows": g["rows"], "seconds": g["seconds"], "docs_per_second": g["docs_per_second"],
                                "parquet_bytes": g["parquet_bytes"], "mongo_disk_bytes": g["stats"]["storageSize"],
                                "regular_vn_disk_index_bytes": bench["vn_regular"]["stats"]["storageSize"]
                                + bench["vn_regular"]["stats"]["totalIndexSize"],
                                "timeseries_vn_disk_index_bytes": bench["vn_timeseries"]["stats"]["storageSize"]
                                + bench["vn_timeseries"]["stats"]["totalIndexSize"]}})
    for i, d in enumerate(out):
        d["order"] = i
    return out


def collection_stats(db):
    """Size and document count per collection, computed once here (counting is slow)."""
    load = (read_json(REPORTS / RUNS["mongo_load"] / "run.json") or {}).get("collections", {})
    out = []
    for name in ["station_daily", "station_hourly", "station_aqi_hourly", "station_daily_aqi",
                 "stations", "locations", "latest_readings"]:
        if name not in db.list_collection_names():
            continue
        s = next(db[name].aggregate([{"$collStats": {"storageStats": {}}}]))["storageStats"]
        docs = load.get(name, {}).get("documents")
        out.append({"collection": name, "documents": docs if docs is not None else db[name].count_documents({}),
                    "bson_mb": (s.get("size") or 0) / 1e6, "disk_mb": (s.get("storageSize") or 0) / 1e6,
                    "index_mb": (s.get("totalIndexSize") or 0) / 1e6, "time_series": "timeseries" in s})
    return out


def time_range():
    """First and last day of the Gold daily tables (partition months)."""
    months = sorted(f.name for f in (REPORTS / RUNS["gold_aqi_daily"] / "done").iterdir())
    y, m = int(months[-1][:4]), int(months[-1][5:])
    last = datetime(y + (m == 12), m % 12 + 1, 1)
    return datetime(int(months[0][:4]), int(months[0][5:]), 1), last


def main():
    db = MongoClient(mongo_uri())[MONGO_DB]
    t0 = time.time()
    counts = {
        "locations": replace(db, "locations", locations(), [[("location", GEOSPHERE)]]),
        "pm25_monthly": replace(db, "pm25_monthly", pm25_monthly()),
        "pipeline_monthly": replace(db, "pipeline_monthly", pipeline_monthly()),
        "parameter_status": replace(db, "parameter_status", parameter_status()),
        "pipeline_runs": replace(db, "pipeline_runs", pipeline_runs(db)),
    }
    first, end = time_range()
    db.dashboard_meta.replace_one({"_id": "summaries"}, {
        "_id": "summaries", "built_utc": datetime.now(timezone.utc), "counts": counts,
        "first_month": first, "end_exclusive": end, "collections": collection_stats(db)}, upsert=True)
    print(json.dumps({"seconds": round(time.time() - t0, 1), **counts}))


if __name__ == "__main__":
    main()
