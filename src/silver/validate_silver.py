"""Validate Silver (silver/measurements + quarantine) after build_silver.py.

Checks (report: reports/<run>_validation/):
  1. reconciliation: Σ rows_in = near_silver_compact rows; per job in = silver + quarantine + removed
  2. key uniqueness of (sensors_id, datetime_utc): per file with pyarrow (each sensor is in one
     file per month by construction; nullts-* files must not share sensors with the others)
  3. valid rows: value inside the hard bounds, canonical unit, datetime_utc not null
  4. distribution of dq_status per parameter and month
"""

import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import numpy as np
import pyarrow.parquet as pq

LAKE = Path.home() / "openaq_lake"


def file_keys(path):
    t = pq.read_table(path, columns=["sensors_id", "datetime_utc"])
    s = t.column("sensors_id").to_numpy()
    ts = t.column("datetime_utc").cast("int64").to_numpy()
    order = np.lexsort((ts, s))
    s, ts = s[order], ts[order]
    dup = int(np.count_nonzero((s[1:] == s[:-1]) & (ts[1:] == ts[:-1]))) if len(s) > 1 else 0
    return str(path), len(s), dup, np.unique(s)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--silver", default=str(LAKE / "silver"))
    p.add_argument("--run-id", default="silver_v1_01")
    p.add_argument("--compact", default=str(LAKE / "near_silver_compact"))
    p.add_argument("--workers", type=int, default=4)
    args = p.parse_args()
    silver = Path(args.silver).expanduser().absolute()
    report = LAKE / "reports" / f"{args.run_id}_validation"
    report.mkdir(parents=True, exist_ok=True)
    result = {"run_id": args.run_id, "started_utc": datetime.now(timezone.utc).isoformat(), "checks": {}}

    # 1. Reconciliation from the build's per-job records.
    jobs = [json.loads(f.read_text()) | {"job": f.name}
            for f in (LAKE / "reports" / args.run_id / "done").glob("*")]
    compact_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in Path(args.compact).rglob("*.parquet"))
    silver_files = sorted((silver / "measurements").rglob("*.parquet"))
    silver_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in silver_files)
    q_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in (silver / "quarantine").rglob("*.parquet"))
    totals = {k: sum(j[k] for j in jobs) for k in ["rows_in", "silver", "quarantine", "dedup_removed"]}
    result["checks"]["reconciliation"] = {
        "jobs": len(jobs), "compact_rows": compact_rows, **totals,
        "silver_rows_on_disk": silver_rows, "quarantine_rows_on_disk": q_rows,
        "ok": totals["rows_in"] == compact_rows and silver_rows == totals["silver"]
              and q_rows == totals["quarantine"]
              and all(j["rows_in"] == j["silver"] + j["quarantine"] + j["dedup_removed"] for j in jobs),
    }

    # 2. Key uniqueness per file + nullts sensors disjoint from other files of the month.
    dups, by_month = 0, {}
    with ProcessPoolExecutor(args.workers) as pool:
        for path, n, dup, sensors in pool.map(file_keys, silver_files):
            dups += dup
            month = str(Path(path).parent)
            kind = "nullts" if Path(path).name.startswith("nullts-") else "main"
            by_month.setdefault(month, {}).setdefault(kind, []).append(sensors)
    overlap = 0
    for parts in by_month.values():
        if "nullts" in parts and "main" in parts:
            overlap += len(np.intersect1d(np.concatenate(parts["nullts"]), np.concatenate(parts["main"])))
    result["checks"]["key_unique"] = {"files": len(silver_files), "duplicate_keys": dups,
                                      "nullts_sensor_overlap": int(overlap), "ok": dups == 0 and overlap == 0}

    # 3-4. One Spark aggregate scan.
    from pyspark.sql import SparkSession, functions as F
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from src.silver.rules import load_rules
    rules = load_rules()
    spark = (SparkSession.builder.appName("OpenAQ-Silver-Validate")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = spark.read.parquet("file://" + str(silver / "measurements"))
        agg = df.groupBy("parameter", "unit", "dq_status").agg(
            F.count("*").alias("rows"), F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count(F.when(F.col("datetime_utc").isNull(), 1)).alias("null_time"),
            F.count(F.when(F.col("coord_source") == "none", 1)).alias("no_coord"),
        ).toPandas()
        agg.to_csv(report / "status_by_parameter.csv", index=False)
        df.groupBy("year", "month", "dq_status").count().toPandas().sort_values(
            ["year", "month"]).to_csv(report / "status_by_month.csv", index=False)
        problems = []
        for r in agg.itertuples():
            if r.null_time:
                problems.append(f"{r.parameter}/{r.unit}/{r.dq_status}: {r.null_time} rows without datetime_utc")
            if r.dq_status != "valid":
                continue
            canon = rules["canonical_units"].get(r.parameter)
            if canon is not None and r.unit != canon:
                problems.append(f"valid {r.parameter} in non-canonical unit {r.unit}")
            b = rules["bounds"].get(r.parameter)
            if b and r.unit == canon and (r.min_value < b["min"] or r.max_value > b["max"]):
                problems.append(f"valid {r.parameter} outside bounds: {r.min_value}..{r.max_value}")
        result["checks"]["valid_rows"] = {"problems": problems, "ok": not problems}
        result["status_totals"] = agg.groupby("dq_status").rows.sum().to_dict()
    finally:
        spark.stop()
    result["ok"] = all(c["ok"] for c in result["checks"].values())
    result["finished_utc"] = datetime.now(timezone.utc).isoformat()
    (report / "validation.json").write_text(json.dumps(result, indent=2, default=int), encoding="utf-8")
    print(json.dumps(result, indent=2, default=int))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
