"""Fetch a slice of the team's Near-Silver history from Hugging Face into a local lake.

The dataset (maythepd/big-data-near-silver, year=/month= folders, about 7 GB per recent year) is
global. Each file is read in memory and only these rows are kept:
  - Vietnam: the OpenAQ locations in data/dim/openaq_locations.parquet, plus any location inside
    VIETNAM_BBOX (Gold decides is_vn from the list; the box only keeps neighbours for context)
  - a fixed share of every other location (--world-share), chosen by a hash of location_id, so
    the same stations are kept in every month and their timelines stay whole
Output: <lake>/near_silver_compact/year=Y/month=M/part-hf-*.zstd.parquet, the layout and columns
src/silver/build_silver.py reads (`source_block` = hf:<file>, the dedup tie-break). A month is
skipped when its _DONE marker exists, so an interrupted fetch resumes.

    LAKE_DIR=openaq_lake_hist python -m src.ingestion.hf_near_silver --from 2024-01 --to 2025-12
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import io
import json
from pathlib import Path
import sys
import time
import zlib

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import DIM_DIR, LAKE, VIETNAM_BBOX  # noqa: E402

REPO = "maythepd/big-data-near-silver"
API = "https://huggingface.co/api/datasets/{repo}/tree/main/year={y}/month={m}"
FILE = "https://huggingface.co/datasets/{repo}/resolve/main/{path}"


def months(first, last):
    y, m = map(int, first.split("-"))
    ly, lm = map(int, last.split("-"))
    while (y, m) <= (ly, lm):
        yield y, m
        y, m = y + (m == 12), m % 12 + 1


def sampled(location_id, share):
    """Stable across months and runs: the same location is always in or always out."""
    return zlib.crc32(location_id.encode()) % 10_000 < share * 10_000


def get(session, url, tries=5):
    for i in range(tries):
        try:
            r = session.get(url, timeout=120)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r
        except (requests.RequestException, requests.HTTPError):
            if i == tries - 1:
                raise
            time.sleep(2 ** i * 3)


def keep(table, vn_ids, share):
    ids = table["location_id"]
    uniq = pc.unique(ids).to_pylist()
    chosen = [i for i in uniq if i is not None and (i in vn_ids or sampled(i, share))]
    lat0, lon0, lat1, lon1 = VIETNAM_BBOX
    in_box = pc.and_(pc.and_(pc.greater_equal(table["lat_num"], lat0), pc.less_equal(table["lat_num"], lat1)),
                     pc.and_(pc.greater_equal(table["lon_num"], lon0), pc.less_equal(table["lon_num"], lon1)))
    mask = pc.or_kleene(pc.is_in(ids, value_set=pa.array(chosen, pa.string())), pc.fill_null(in_box, False))
    return table.filter(mask)


def write(table, path):
    """Spark-readable: no inherited Spark schema metadata (Spark trusts it over the file's columns,
    and the source files' copy lacks source_block), and microsecond timestamps (Spark 3.5 cannot
    read nanosecond ones)."""
    table = table.replace_schema_metadata(None)
    table = table.set_column(table.schema.get_field_index("datetime_ts"), "datetime_ts",
                             table["datetime_ts"].cast(pa.timestamp("us")))
    pq.write_table(table, path, compression="zstd", row_group_size=2_000_000)


def fetch_month(session, repo, y, m, out_root, vn_ids, share, workers):
    out = out_root / f"year={y}" / f"month={m}"
    if (out / "_DONE").exists():
        return None
    files = [f["path"] for f in get(session, API.format(repo=repo, y=y, m=m)).json() if f["type"] == "file"]

    def one(path):
        t = pq.read_table(io.BytesIO(get(session, FILE.format(repo=repo, path=path)).content))
        rows = t.num_rows
        t = keep(t, vn_ids, share)
        return rows, t.append_column("source_block", pa.array(["hf:" + path] * t.num_rows, pa.string()))

    t0 = time.time()
    with ThreadPoolExecutor(workers) as pool:
        parts = list(pool.map(one, files))
    rows_in = sum(r for r, _ in parts)
    table = pa.concat_tables([t for _, t in parts if t.num_rows], promote_options="default") if parts else None
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.parquet"):
        old.unlink()
    kept = 0
    if table is not None and table.num_rows:
        kept = table.num_rows
        write(table, out / "part-hf-00000.zstd.parquet")
    stats = dict(files=len(files), rows_in=rows_in, rows_kept=kept,
                 locations=len(pc.unique(table["location_id"])) if kept else 0, seconds=round(time.time() - t0))
    (out / "_DONE").write_text(json.dumps(stats), encoding="utf-8")
    return stats


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repo", default=REPO)
    p.add_argument("--from", dest="first", required=True, help="YYYY-MM")
    p.add_argument("--to", dest="last", required=True, help="YYYY-MM")
    p.add_argument("--world-share", type=float, default=0.05)
    p.add_argument("--workers", type=int, default=6, help="parallel downloads")
    args = p.parse_args()
    vn_ids = {str(i) for i in pq.read_table(DIM_DIR / "openaq_locations.parquet", columns=["station_id"])
              ["station_id"].to_pylist()}
    out_root = LAKE / "near_silver_compact"
    print(f"{args.repo} {args.first}..{args.last} -> {out_root}: {len(vn_ids)} VN locations + "
          f"{args.world_share:.0%} of the others", flush=True)
    with requests.Session() as session:
        for y, m in months(args.first, args.last):
            stats = fetch_month(session, args.repo, y, m, out_root, vn_ids, args.world_share, args.workers)
            print(f"{y}-{m:02d}: " + ("done before" if stats is None else
                  f"{stats['rows_kept']:,} of {stats['rows_in']:,} rows, {stats['locations']} locations, "
                  f"{stats['files']} files, {stats['seconds']} s"), flush=True)


if __name__ == "__main__":
    main()
