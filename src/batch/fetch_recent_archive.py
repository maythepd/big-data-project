"""One-time backfill: OpenAQ S3 archive after the recovered HDFS blocks -> Bronze -> near_silver_compact.

    python3 src/batch/fetch_recent_archive.py all --start 2026-09-29
"""
import argparse
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import csv
from datetime import date, datetime, timezone
import gzip
import json
from pathlib import Path
import re
import shutil
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.batch.utils import BUCKET, PREFIX, s3_client

LAKE = Path.home() / "openaq_lake"
BRONZE = LAKE / "bronze" / "openaq_archive"
COMPACT = LAKE / "near_silver_compact"
RAW = ["location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter", "units", "value"]
SCHEMA = pa.schema([(c, pa.string()) for c in RAW] +
                   [("value_num", pa.float64()), ("lat_num", pa.float64()), ("lon_num", pa.float64()),
                    ("datetime_ts", pa.timestamp("ns")), pa.field("source_block", pa.string(), nullable=False)])
FILE_DATE = re.compile(r"-(\d{8})\.csv\.gz$")
# tasks by compressed size: 17.6 MB of CSV (3.5M rows) peaked at 1.9 GB in one worker
TASK_BYTES = 4 * 1024 * 1024


def months(start, end):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def list_location(client, prefix, start, end):
    out = []
    for y, m in months(start, end):
        token = None
        while True:
            req = {"Bucket": BUCKET, "Prefix": f"{prefix}year={y}/month={m:02d}/"}
            if token:
                req["ContinuationToken"] = token
            page = client.list_objects_v2(**req)
            for o in page.get("Contents", []):
                d = FILE_DATE.search(o["Key"])
                if d and o["Size"] > 0 and start <= datetime.strptime(d.group(1), "%Y%m%d").date() <= end:
                    out.append((o["Key"], o["Size"]))
            if not page.get("IsTruncated"):
                break
            token = page["NextContinuationToken"]
    return out


def step_list(report, start, end, workers):
    client = s3_client()
    prefixes = [p["Prefix"] for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=PREFIX, Delimiter="/") for p in page.get("CommonPrefixes", [])
        if p["Prefix"].startswith(PREFIX + "locationid=")]
    rows, t0 = [], time.time()
    with ThreadPoolExecutor(workers) as pool:
        futures = [pool.submit(list_location, client, p, start, end) for p in prefixes]
        for i, f in enumerate(as_completed(futures), 1):
            rows += f.result()
            if i % 5000 == 0:
                print(f"listed {i}/{len(prefixes)} locations, {len(rows)} files, {time.time() - t0:.0f} s", flush=True)
    tmp = report / "manifest.csv.part"
    with tmp.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["key", "size"])
        w.writerows(sorted(rows))
    tmp.rename(report / "manifest.csv")
    return {"locations": len(prefixes), "files": len(rows), "bytes": sum(s for _, s in rows),
            "seconds": round(time.time() - t0, 1)}


def manifest(report):
    with (report / "manifest.csv").open(newline="") as fh:
        return [(r["key"], int(r["size"])) for r in csv.DictReader(fh)]


def bronze_path(key):
    return BRONZE / key[len(PREFIX):]


def download_one(client, key, size):
    path = bronze_path(key)
    if path.is_file() and path.stat().st_size == size:
        return "skipped"
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    for attempt in range(5):
        try:
            client.download_file(BUCKET, key, str(part))
            if part.stat().st_size != size:
                raise OSError("size differs from the listing")
            part.rename(path)
            return "downloaded"
        except Exception:
            if attempt == 4:
                return "failed"
            time.sleep(2 * (attempt + 1))


def step_download(report, workers):
    client, rows, counts, failed, t0 = s3_client(), manifest(report), {}, [], time.time()
    with ThreadPoolExecutor(workers) as pool:
        futures = {pool.submit(download_one, client, k, s): k for k, s in rows}
        for i, f in enumerate(as_completed(futures), 1):
            r = f.result()
            counts[r] = counts.get(r, 0) + 1
            if r == "failed":
                failed.append(futures[f])
            if i % 50000 == 0:
                print(f"{i}/{len(rows)} files {counts} {time.time() - t0:.0f} s", flush=True)
    (report / "download_failed.txt").write_text("\n".join(failed), encoding="utf-8")
    return dict(counts, seconds=round(time.time() - t0, 1))


NUMBER = r"^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$"
SCHEMA_2 = ["location_id", "sensors_id", "location", "datetime", "lat", "lat_2", "lon", "lon_2",
            "parameter", "units", "value"]


def read_one(path):
    """One archive CSV as the 9 raw string columns (duplicated lat/lon schema folded)."""
    with gzip.open(path, "rt", encoding="utf-8-sig") as fh:
        header = next(csv.reader([fh.readline()]))
    if header == RAW:
        names = RAW
    elif header == [c.replace("_2", "") for c in SCHEMA_2]:
        names = SCHEMA_2
    else:
        raise ValueError(f"unknown header {header}")
    t = pcsv.read_csv(path, read_options=pcsv.ReadOptions(skip_rows=1, column_names=names),
                      convert_options=pcsv.ConvertOptions(column_types={c: pa.string() for c in names},
                                                          strings_can_be_null=True))
    if names is SCHEMA_2:
        t = t.set_column(4, "lat", pc.coalesce(t["lat"], t["lat_2"])) \
             .set_column(6, "lon", pc.coalesce(t["lon"], t["lon_2"]))
    return t.select(RAW)


def to_double(col):
    return pc.cast(pc.if_else(pc.match_substring_regex(col, NUMBER), col, pa.scalar(None, pa.string())),
                   pa.float64())


def convert_task(task_id, keys, run_id, staging):
    tables, bad = [], []
    for key in keys:
        try:
            t = read_one(bronze_path(key))
            tables.append(t.append_column("source_block", pa.array(["s3:" + key.rsplit("/", 1)[1]] * t.num_rows)))
        except Exception as exc:
            bad.append((key, f"{type(exc).__name__}: {exc}"))
    if not tables:
        return {"task": task_id, "rows": 0, "bad": bad, "parts": {}}
    t = pa.concat_tables(tables)
    del tables
    ts = pc.strptime(t["datetime"], format="%Y-%m-%dT%H:%M:%S%z", unit="ns", error_is_null=True)
    t = pa.table({**{c: t[c] for c in RAW},
                  "value_num": to_double(t["value"]), "lat_num": to_double(t["lat"]),
                  "lon_num": to_double(t["lon"]),
                  "datetime_ts": pc.cast(ts, pa.timestamp("ns")),       # naive UTC, as the blocks
                  "source_block": t["source_block"]}, schema=SCHEMA)
    ym = pc.add(pc.multiply(pc.year(ts), 100), pc.month(ts))          # null when the time is unparsed
    parts = {}
    for key in pc.unique(ym).to_pylist():
        mask = pc.is_null(ym) if key is None else pc.equal(ym, key)
        g = t.filter(mask)
        name = "_null_ts" if key is None else f"year={key // 100}/month={key % 100}"
        out = staging / name / f"s3recent-{run_id}-{task_id:05d}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(g, out, compression="zstd", use_deprecated_int96_timestamps=True)
        parts[name] = g.num_rows
    return {"task": task_id, "rows": t.num_rows, "bad": bad, "parts": parts}


def step_convert(report, run_id, workers):
    files = [(k, s) for k, s in manifest(report) if bronze_path(k).is_file()]
    keys = [k for k, _ in files]
    staging = LAKE / "_staging" / "archive_recent" / run_id
    shutil.rmtree(staging, ignore_errors=True)
    tasks, cur, size = [], [], 0
    for k, s in files:
        if cur and size + s > TASK_BYTES:
            tasks.append(cur)
            cur, size = [], 0
        cur.append(k)
        size += s
    tasks += [cur] if cur else []
    totals, bad, t0 = {}, [], time.time()
    with ProcessPoolExecutor(workers) as pool:
        futures = [pool.submit(convert_task, i, t, run_id, staging) for i, t in enumerate(tasks)]
        for i, f in enumerate(as_completed(futures), 1):
            r = f.result()
            bad += r["bad"]
            for k, n in r["parts"].items():
                totals[k] = totals.get(k, 0) + n
            if i % 20 == 0:
                print(f"converted {i}/{len(tasks)} tasks, {sum(totals.values())} rows, {time.time() - t0:.0f} s", flush=True)
    # publish: replace this run's files in near_silver_compact
    for d in sorted(p for p in staging.rglob("*") if p.is_dir() and any(p.glob("*.parquet"))):
        target = COMPACT / d.relative_to(staging)
        target.mkdir(parents=True, exist_ok=True)
        for old in target.glob(f"s3recent-{run_id}-*.parquet"):
            old.unlink()
        for f in d.glob("*.parquet"):
            f.rename(target / f.name)
    shutil.rmtree(staging, ignore_errors=True)
    with (report / "convert_bad_files.csv").open("w", newline="") as fh:
        csv.writer(fh).writerows([("key", "error")] + bad)
    return {"files": len(keys), "rows_by_partition": dict(sorted(totals.items())),
            "rows": sum(totals.values()), "bad_files": len(bad), "seconds": round(time.time() - t0, 1)}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("step", choices=["list", "download", "convert", "all"])
    p.add_argument("--start", default="2026-09-29", help="first file date (YYYY-MM-DD)")
    p.add_argument("--end", help="last file date (default: today, UTC)")
    p.add_argument("--run-id", default="archive_recent_01")
    p.add_argument("--workers", type=int, default=32, help="threads for S3")
    p.add_argument("--convert-workers", type=int, default=2)
    args = p.parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else datetime.now(timezone.utc).date()
    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    meta_path = report / "run.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {"run_id": args.run_id}
    meta.update(start=str(start), end=str(end))
    steps = ["list", "download", "convert"] if args.step == "all" else [args.step]
    for s in steps:
        if s == "list" and args.step == "all" and (report / "manifest.csv").exists():
            continue
        print(f"{datetime.now(timezone.utc):%H:%M:%S} {s} ...", flush=True)
        if s == "list":
            meta["list"] = step_list(report, start, end, args.workers)
        elif s == "download":
            meta["download"] = step_download(report, args.workers)
        else:
            meta["convert"] = step_convert(report, args.run_id, args.convert_workers)
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        print(json.dumps(meta[s]), flush=True)


if __name__ == "__main__":
    main()
