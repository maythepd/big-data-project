"""What a reader sees in one Silver month: files, rows, distinct (sensors_id, datetime_utc) keys.

    python src/chaos/check_month.py openaq_lake/scratch/reproc/silver/measurements 2025-01 [--expect N]

Prints one JSON line; exits 1 when --expect is given and rows or distinct keys differ from it
(lost rows, or rows stored twice after a crash and rerun).
"""
import argparse
import json
from pathlib import Path
import sys

import pyarrow.dataset as ds


def check(root, month):
    year, mon = map(int, month.split("-"))
    folder = Path(root) / f"year={year}" / f"month={mon}"
    files = sorted(folder.glob("*.parquet")) if folder.exists() else []
    if not files:
        return {"folder": str(folder), "files": 0, "rows": 0, "distinct_keys": 0}
    table = ds.dataset([str(f) for f in files], format="parquet").to_table(columns=["sensors_id", "datetime_utc"])
    keys = table.group_by(["sensors_id", "datetime_utc"]).aggregate([]).num_rows
    return {"folder": str(folder), "files": len(files), "rows": table.num_rows, "distinct_keys": keys}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("root")
    p.add_argument("month")
    p.add_argument("--expect", type=int)
    args = p.parse_args()
    out = check(args.root, args.month)
    if args.expect is not None:
        out["expected"] = args.expect
        out["ok"] = out["rows"] == out["distinct_keys"] == args.expect
    print(json.dumps(out))
    sys.exit(0 if out.get("ok", True) else 1)


if __name__ == "__main__":
    main()
