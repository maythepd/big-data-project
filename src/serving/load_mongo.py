"""Load Gold into MongoDB time series collections, one (collection, month) per task; reruns resume."""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pymongo import ASCENDING, GEOSPHERE, MongoClient

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import DIM_DIR, MONGO_DB, mongo_uri

LAKE = Path.home() / "openaq_lake"
GOLD = LAKE / "gold"
AQI_PARAMETERS = ["pm25", "pm10", "o3", "no2", "so2", "co"]
BATCH_ROWS = 20_000
YEAR_SECONDS = 365 * 24 * 3600          # max bucket span MongoDB allows

AQI_COLS = [f"aqi_{s}_{p}" for s in ("us", "vn") for p in AQI_PARAMETERS] + \
           ["aqi_us", "aqi_us_main", "aqi_vn", "aqi_vn_main"]

SPECS = {
    "station_hourly": dict(
        table="station_hourly", time=("hour_utc", "hour_utc"),
        meta=["location_id", "parameter", "unit"],
        fields=["value_mean", "value_min", "value_max", "sensor_count", "value_count",
                "n_readings", "origin", "utc_offset_min"],
        where=lambda: ds.field("is_vn") == True,                      # noqa: E712
        ts=dict(granularity="hours"),
        indexes=[[("meta.location_id", ASCENDING), ("meta.parameter", ASCENDING), ("hour_utc", ASCENDING)]]),
    "station_aqi_hourly": dict(
        table="station_hourly_aqi", time=("hour_utc", "hour_utc"),
        meta=["location_id"],
        fields=["pm25_1h", "pm25_24h", "pm25_nowcast", "pm10_1h", "pm10_24h", "pm10_nowcast",
                "o3_1h", "o3_8h", "co_1h", "co_8h", "no2_1h", "so2_1h", "so2_24h"] + AQI_COLS,
        where=lambda: ds.field("is_vn") == True,                      # noqa: E712
        ts=dict(granularity="hours"),
        indexes=[[("meta.location_id", ASCENDING), ("hour_utc", ASCENDING)]]),
    "station_daily": dict(
        table="station_daily", time=("local_date", "date"),
        meta=["location_id", "parameter", "unit", "is_vn"],
        fields=["value_mean", "value_min", "value_max", "max_1h", "max_8h", "hours",
                "n_readings", "day_valid"],
        where=lambda: (ds.field("is_vn") == True) | ds.field("parameter").isin(AQI_PARAMETERS),  # noqa: E712
        ts=dict(bucketMaxSpanSeconds=YEAR_SECONDS, bucketRoundingSeconds=YEAR_SECONDS),
        # one station's timeline; map of one parameter on one day (every station)
        indexes=[[("meta.location_id", ASCENDING), ("meta.parameter", ASCENDING), ("date", ASCENDING)],
                 [("meta.parameter", ASCENDING), ("date", ASCENDING)]]),
    "station_daily_aqi": dict(
        table="station_daily_aqi", time=("local_date", "date"),
        meta=["location_id"],
        fields=[f"{p}_{s}" for p in AQI_PARAMETERS for s in ("mean", "max_1h", "max_8h", "hours", "valid")]
               + AQI_COLS,
        where=lambda: ds.field("is_vn") == True,                      # noqa: E712
        ts=dict(bucketMaxSpanSeconds=YEAR_SECONDS, bucketRoundingSeconds=YEAR_SECONDS),
        indexes=[[("meta.location_id", ASCENDING), ("date", ASCENDING)]]),
    # global hourly, only with --collections station_hourly_global
    "station_hourly_global": dict(
        table="station_hourly", time=("hour_utc", "hour_utc"),
        meta=["location_id", "parameter", "unit", "is_vn"],
        fields=["value_mean", "value_min", "value_max", "sensor_count", "value_count",
                "n_readings", "origin", "utc_offset_min"],
        where=None,
        ts=dict(granularity="hours"),
        # one station's timeline; one parameter at one hour (every station)
        indexes=[[("meta.location_id", ASCENDING), ("meta.parameter", ASCENDING), ("hour_utc", ASCENDING)],
                 [("meta.parameter", ASCENDING), ("hour_utc", ASCENDING)]]),
}
DEFAULT_COLLECTIONS = ["station_hourly", "station_aqi_hourly", "station_daily", "station_daily_aqi"]
# counting documents unpacks every bucket: use the done markers for the largest one
COUNT_FROM_MARKERS = {"station_hourly_global"}

# the automatic {meta, time} index cannot serve filters on meta sub-fields, hence the indexes above


def connect():
    return MongoClient(mongo_uri(), serverSelectionTimeoutMS=20_000)


def month_dirs(table):
    """[(YYYY-MM, path)] of a Gold table, in time order."""
    out = []
    for y in sorted((GOLD / table).glob("year=*"), key=lambda p: int(p.name[5:])):
        for m in sorted(y.glob("month=*"), key=lambda p: int(p.name[6:])):
            out.append((f"{int(y.name[5:]):04d}-{int(m.name[6:]):02d}", m))
    return out


def month_range(key):
    y, m = int(key[:4]), int(key[5:])
    return (datetime(y, m, 1, tzinfo=timezone.utc),
            datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc))


def create_ts(db, name, spec):
    opts = {"timeField": spec["time"][1], "metaField": "meta", **spec["ts"]}
    db.create_collection(name, timeseries=opts)
    for keys in spec["indexes"]:
        db[name].create_index(keys)


def ensure_indexes(db, names):
    for name in names:
        for keys in SPECS[name]["indexes"]:
            db[name].create_index(keys)


def setup(db, drop, names):
    existing = set(db.list_collection_names())
    for name in names:
        if drop and name in existing:
            db.drop_collection(name)
            existing.discard(name)
        if name not in existing:
            create_ts(db, name, SPECS[name])
    db.latest_readings.create_index([("station_id", ASCENDING), ("parameter", ASCENDING)], unique=True)


def to_documents(batch, spec):
    """Arrow record batch -> list of BSON-ready dicts (meta sub-document, no null fields)."""
    src_time, dst_time = spec["time"]
    cols = {}
    for name in spec["meta"] + spec["fields"]:
        cols[name] = batch.column(name).to_pylist()
    t = batch.column(src_time)
    if pa.types.is_date(t.type):
        t = t.cast(pa.timestamp("ms"))
    elif t.type != pa.timestamp("ms"):
        t = t.cast(pa.timestamp("ms"))
    times = t.to_pylist()                        # naive datetimes, read by pymongo as UTC
    meta_names, field_names = spec["meta"], spec["fields"]
    docs = []
    for i, ts in enumerate(times):
        d = {dst_time: ts, "meta": {k: cols[k][i] for k in meta_names}}
        for k in field_names:
            v = cols[k][i]
            if v is not None:
                d[k] = v
        docs.append(d)
    return docs


def load_month(name, key, path, run_dir, collection=None, where=True, vn_only=False):
    """Replace one month of a collection (only its Vietnam documents with vn_only)."""
    spec = SPECS[name]
    target = collection or name
    t0 = time.time()
    db = connect()[MONGO_DB]
    coll = db[target]
    start, end = month_range(key)
    vn_only = vn_only and "is_vn" in spec["meta"]
    query = {spec["time"][1]: {"$gte": start, "$lt": end}}
    row_filter = spec["where"]() if where and spec["where"] else None
    if vn_only:
        query["meta.is_vn"] = True
        vn = ds.field("is_vn") == True                                 # noqa: E712
        row_filter = vn if row_filter is None else row_filter & vn
    deleted = coll.delete_many(query).deleted_count
    dataset = ds.dataset(str(path), format="parquet")
    columns = list(dict.fromkeys(spec["meta"] + spec["fields"] + [spec["time"][0]]))
    # no read-ahead: the defaults buffer whole files per worker and exhaust memory
    scanner = dataset.scanner(columns=columns, filter=row_filter,
                              batch_size=BATCH_ROWS, use_threads=False,
                              batch_readahead=0, fragment_readahead=0)
    rows = 0
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        docs = to_documents(batch, spec)
        coll.insert_many(docs, ordered=False)
        rows += len(docs)
    result = {"collection": target, "month": key, "rows": rows, "deleted_before": deleted,
              "vn_only": vn_only, "seconds": round(time.time() - t0, 1)}
    if run_dir is not None:
        (run_dir / "done" / f"{target}__{key}").write_text(json.dumps(result), encoding="utf-8")
    return result


def expected_rows(name):
    spec = SPECS[name]
    return ds.dataset(str(GOLD / spec["table"]), format="parquet", partitioning="hive") \
             .count_rows(filter=spec["where"]() if spec["where"] else None)


def coll_stats(db, name):
    s = next(db[name].aggregate([{"$collStats": {"storageStats": {}}}]))["storageStats"]
    keys = ["count", "size", "storageSize", "totalIndexSize", "avgObjSize", "nindexes"]
    out = {k: s.get(k) for k in keys}
    if "timeseries" in s:
        ts = s["timeseries"]
        out["buckets"] = ts.get("bucketCount")
        out["avg_docs_per_bucket"] = ts.get("avgNumMeasurementsPerCommit")
    return out


def load_stations(db):
    """VN station catalogue (data/dim/openaq_locations.parquet) with GeoJSON points."""
    t = pq.read_table(DIM_DIR / "openaq_locations.parquet").to_pylist()
    docs = [{"_id": int(r["station_id"]), "name": r["name"], "locality": r["locality"],
             "provider": r["provider"], "timezone": r["timezone"], "is_mobile": r["is_mobile"],
             "location": {"type": "Point", "coordinates": [r["lon"], r["lat"]]}}
            for r in t if r["lat"] is not None and r["lon"] is not None]
    db.stations.drop()
    db.stations.insert_many(docs)
    db.stations.create_index([("location", GEOSPHERE)])
    return len(docs)


def run_parallel(tasks, workers, log):
    results = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(load_month, *t): t for t in tasks}
        for f in as_completed(futures):
            r = f.result()
            results.append(r)
            line = json.dumps(r)
            log.write(line + "\n")
            log.flush()
            print(line, flush=True)
    return results


def bench(db, key, workers, keep, report):
    """One global month of station_hourly as time series; VN station_hourly as a regular collection."""
    out = {}
    ts_name, reg_name = f"bench_station_hourly_global_{key.replace('-', '_')}", "bench_station_hourly_vn_regular"
    for n in (ts_name, reg_name):
        db.drop_collection(n)
    create_ts(db, ts_name, SPECS["station_hourly"])
    path = GOLD / "station_hourly" / f"year={int(key[:4])}" / f"month={int(key[5:])}"
    # split the month's files across workers
    files = sorted(path.glob("*.parquet"))
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        parts = list(pool.map(_bench_file, [ts_name] * len(files), [str(f) for f in files], [key] * len(files)))
    secs = time.time() - t0
    rows = sum(parts)
    out["global_month_timeseries"] = dict(month=key, rows=rows, seconds=round(secs, 1),
                                          docs_per_second=round(rows / secs), parquet_bytes=sum(f.stat().st_size for f in files),
                                          stats=coll_stats(db, ts_name))
    # the same VN documents as a regular collection, for the size comparison
    t0 = time.time()
    db.station_hourly.aggregate([{"$project": {"_id": 0}}, {"$out": reg_name}])
    db[reg_name].create_index([("meta.location_id", ASCENDING), ("meta.parameter", ASCENDING),
                               ("meta.unit", ASCENDING), ("hour_utc", ASCENDING)])
    out["vn_regular"] = dict(rows=db[reg_name].count_documents({}), seconds=round(time.time() - t0, 1),
                             stats=coll_stats(db, reg_name))
    out["vn_timeseries"] = dict(stats=coll_stats(db, "station_hourly")) \
        if "station_hourly" in db.list_collection_names() else None
    if not keep:
        db.drop_collection(ts_name)
        db.drop_collection(reg_name)
    (report / "bench.json").write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(json.dumps(out, indent=2, default=str), flush=True)


def _bench_file(target, file, key):
    spec = SPECS["station_hourly"]
    coll = connect()[MONGO_DB][target]
    columns = list(dict.fromkeys(spec["meta"] + spec["fields"] + [spec["time"][0]]))
    rows = 0
    for batch in pq.ParquetFile(file).iter_batches(batch_size=BATCH_ROWS, columns=columns):
        docs = to_documents(batch, spec)
        coll.insert_many(docs, ordered=False)
        rows += len(docs)
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", default="mongo_load_v1_01")
    p.add_argument("--collections", nargs="+", default=DEFAULT_COLLECTIONS, choices=list(SPECS))
    p.add_argument("--months", nargs="*", help="YYYY-MM (default: every Gold month)")
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--vn-only-months", nargs="*", default=[],
                   help="YYYY-MM whose non-VN documents are unchanged: replace only the VN ones")
    p.add_argument("--drop", action="store_true", help="drop and recreate the collections first")
    p.add_argument("--bench", metavar="YYYY-MM", help="measured global hourly load of one month")
    p.add_argument("--keep-bench", action="store_true")
    args = p.parse_args()

    report = LAKE / "reports" / args.run_id
    (report / "done").mkdir(parents=True, exist_ok=True)
    db = connect()[MONGO_DB]
    meta = {"status": "running", "run_id": args.run_id, "mongo": db.client.server_info()["version"],
            "started_utc": datetime.now(timezone.utc).isoformat()}
    try:
        if args.bench:
            meta["bench"] = args.bench
            bench(db, args.bench, args.workers, args.keep_bench, report)
            meta["status"] = "complete"
            return
        if args.drop:
            for f in (report / "done").glob("*"):
                if f.name.split("__")[0] in args.collections:
                    f.unlink()
        setup(db, args.drop, args.collections)
        ensure_indexes(db, args.collections)    # also on collections created by older runs
        meta["stations"] = load_stations(db)
        tasks = []
        for name in args.collections:
            for key, path in month_dirs(SPECS[name]["table"]):
                if args.months and key not in args.months:
                    continue
                if not (report / "done" / f"{name}__{key}").exists():
                    tasks.append((name, key, path, report, None, True, key in args.vn_only_months))
        meta["tasks"] = len(tasks)
        t0 = time.time()
        with (report / "load_log.jsonl").open("a", encoding="utf-8") as log:
            run_parallel(tasks, args.workers, log)
        meta["seconds_load"] = round(time.time() - t0, 1)
        checks = {}
        for name in args.collections:
            stats = coll_stats(db, name)
            checks[name] = dict(stats=stats)
            if not args.months:
                exp = expected_rows(name)
                if name in COUNT_FROM_MARKERS and not args.vn_only_months:
                    got = sum(json.loads(f.read_text())["rows"] for f in (report / "done").glob(f"{name}__*"))
                    checks[name]["count_method"] = "sum of done markers"
                else:
                    got = db[name].count_documents({})
                checks[name].update(expected_rows=exp, documents=got, ok=exp == got)
        meta["collections"] = checks
        meta["ok"] = all(c.get("ok", True) for c in checks.values())
        meta["status"] = "complete"
    except Exception as exc:
        meta.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        meta["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (report / ("bench_run.json" if args.bench else "run.json")).write_text(
            json.dumps(meta, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
