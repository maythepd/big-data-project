"""block recovery.1: inventory DataNode blocks and decide which ones feed Near-Silver compaction.

Every block file gets one manifest row: Parquet or not, duplicate group,
UTC year-month (from the first non-null datetime_ts) and an exclusion reason.
Block files are only read, never modified. No Spark required.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_BLOCKS = (
    Path.home() / "hadoop_data" / "datanode" / "current"
    / "BP-431263432-127.0.1.1-1792161140699" / "current" / "finalized"
)
DEFAULT_LAKE = Path.home() / "openaq_lake"
EXPECTED_COLUMNS = [
    "location_id", "sensors_id", "location", "datetime", "lat", "lon",
    "parameter", "units", "value", "value_num", "lat_num", "lon_num", "datetime_ts",
]
CHUNK = 8 << 20


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--blocks", default=str(DEFAULT_BLOCKS))
    p.add_argument("--lake", default=str(DEFAULT_LAKE))
    p.add_argument("--run-id", default="inventory_blocks_01")
    return p.parse_args()


def is_zero_filled(path):
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK):
            if chunk.count(0) != len(chunk):
                return False
    return True


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def first_month(parquet):
    # Spark wrote with partitionBy(year, month), so a block holds one UTC month;
    # check_block_months.py verifies this on every row.
    for batch in parquet.iter_batches(batch_size=4096, columns=["datetime_ts"]):
        values = batch.column(0).drop_null()
        if len(values):
            ts = values[0].as_py()
            return ts.year, ts.month
    return None, None


def inspect_block(path):
    size = path.stat().st_size
    row = {
        "block_id": path.name, "block_num": int(path.name[4:].split("_")[0]),
        "path": str(path), "size": size, "mtime": int(path.stat().st_mtime),
        "is_parquet": False, "num_rows": 0, "schema_ok": False,
        "year": None, "month": None, "sha256": None, "exclude_reason": None,
    }
    if not path.name[4:].isdigit():
        row["exclude_reason"] = "not_block_data_file"
        return row
    with path.open("rb") as stream:
        head = stream.read(4)
        if size >= 8:
            stream.seek(-4, os.SEEK_END)
        tail = stream.read(4)
    if head != b"PAR1" or tail != b"PAR1":
        row["exclude_reason"] = "zero_filled" if is_zero_filled(path) else "not_parquet"
        return row
    parquet = pq.ParquetFile(path)
    row["is_parquet"] = True
    row["num_rows"] = parquet.metadata.num_rows
    row["schema_ok"] = parquet.schema_arrow.names == EXPECTED_COLUMNS
    if not row["schema_ok"]:
        row["exclude_reason"] = "unexpected_schema"
        return row
    row["year"], row["month"] = first_month(parquet)
    return row


def mark_duplicates(rows):
    # Only blocks sharing an exact size can be byte-identical.
    by_size = defaultdict(list)
    for row in rows:
        if row["exclude_reason"] is None:
            by_size[row["size"]].append(row)
    by_hash = defaultdict(list)
    for same_size in by_size.values():
        if len(same_size) > 1:
            for row in same_size:
                row["sha256"] = sha256(Path(row["path"]))
                by_hash[row["sha256"]].append(row)
    groups = 0
    for same in by_hash.values():
        if len(same) > 1:
            groups += 1
            keep = min(same, key=lambda r: r["block_num"])
            for row in same:
                if row is not keep:
                    row["exclude_reason"] = f"duplicate_of:{keep['block_id']}"
    return groups


def main():
    args = parse_args()
    blocks = Path(args.blocks).expanduser().absolute()
    lake = Path(args.lake).expanduser().absolute()
    report = lake / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=False)
    (lake / "manifest").mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc).isoformat()

    paths = sorted(
        p for p in blocks.rglob("blk_*") if p.is_file() and not p.name.endswith(".meta")
    )
    rows = []
    for i, path in enumerate(paths, 1):
        rows.append(inspect_block(path))
        if i % 2000 == 0:
            print(f"inspected {i}/{len(paths)}", flush=True)
    duplicate_groups = mark_duplicates(rows)

    table = pa.Table.from_pylist(rows)
    pq.write_table(table, lake / "manifest" / "block_manifest.parquet")

    used = [r for r in rows if r["exclude_reason"] is None]
    reasons = defaultdict(lambda: [0, 0])
    for r in rows:
        key = (r["exclude_reason"] or "used").split(":")[0]
        reasons[key][0] += 1
        reasons[key][1] += r["num_rows"]
    months = defaultdict(lambda: [0, 0, 0])
    for r in used:
        key = f"{r['year']:04d}-{r['month']:02d}" if r["year"] else "null_ts"
        months[key][0] += 1
        months[key][1] += r["num_rows"]
        months[key][2] += r["size"]

    with (report / "exclusion_summary.csv").open("w", encoding="utf-8") as out:
        out.write("reason,blocks,rows\n")
        for key, (n, nrows) in sorted(reasons.items()):
            out.write(f"{key},{n},{nrows}\n")
    with (report / "month_summary.csv").open("w", encoding="utf-8") as out:
        out.write("year_month,blocks,rows,bytes\n")
        for key, (n, nrows, size) in sorted(months.items()):
            out.write(f"{key},{n},{nrows},{size}\n")

    summary = {
        "phase": "0.1", "status": "complete", "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "blocks_dir": str(blocks), "block_files": len(rows),
        "parquet_blocks": sum(r["is_parquet"] for r in rows),
        "parquet_rows": sum(r["num_rows"] for r in rows if r["is_parquet"]),
        "used_blocks": len(used), "used_rows": sum(r["num_rows"] for r in used),
        "duplicate_groups": duplicate_groups,
        "excluded": {k: {"blocks": v[0], "rows": v[1]} for k, v in reasons.items() if k != "used"},
        "months": len(months),
        "manifest": str(lake / "manifest" / "block_manifest.parquet"),
    }
    (report / "run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
