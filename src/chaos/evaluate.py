"""Chaos run, step 4: how Silver handled each injected fault.

Per fault: rows injected, quarantined (R01/R03), removed by dedup (R13), and the dq_status and
expected flag of the rows left in Silver. "caught" counts quarantined, invalid and suspect rows;
dedup removals count only for the `duplicate` fault, since a conflicting copy dropped by block
order was resolved silently, not detected. Clean rows are compared with a baseline Silver of the
same month (the control run, see inject_batch.py --scale 0) to count collateral status changes.
    Input : openaq_lake/chaos/<run>/{near_silver_compact, silver/measurements, silver/quarantine}
    Output: openaq_lake/reports/<run>_eval/{summary.csv, collateral.csv, run.json}
"""

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from pyspark.sql import SparkSession, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.chaos.faults import BY_NAME, fault_label
from src.common.config import LAKE
from src.eda.common import read_compact

COLUMNS = ["fault", "level", "expected", "injected", "quarantined", "removed", "invalid", "suspect", "valid",
           "expected_hits", "expected_share", "caught_share", "verdict"]


def uri(path):
    return "file://" + str(path)


def verdict(expected_share, caught_share):
    if expected_share is not None and expected_share >= 0.95:
        return "caught"
    if caught_share >= 0.95:
        return "caught (other rule)" if expected_share is not None else "caught (no dedicated rule)"
    return "partly" if caught_share >= 0.05 else "missed"


def summarize(spark, root):
    injected = (read_compact(spark, root / "near_silver_compact", include_null_ts=False)
                .groupBy(fault_label().alias("fault")).agg(F.count("*").alias("injected")))
    silver = spark.read.parquet(uri(root / "silver" / "measurements")).withColumn("fault", fault_label())
    quarantine = spark.read.parquet(uri(root / "silver" / "quarantine")).withColumn("fault", fault_label())

    flag_hit, reason_hit = F.lit(False), F.lit(False)
    for f in BY_NAME.values():
        if f.expected and f.expected.startswith("quarantine:"):
            reason_hit = F.when(F.col("fault") == f.name,
                                F.col("quarantine_reason") == f.expected.split(":", 1)[1]).otherwise(reason_hit)
        elif f.expected and f.expected != "dedup":
            flag_hit = F.when(F.col("fault") == f.name,
                              F.array_contains("dq_flags", f.expected)).otherwise(flag_hit)
    status = lambda s: F.sum((F.col("dq_status") == s).cast("long")).alias(s)
    s = silver.groupBy("fault").agg(F.count("*").alias("silver"), status("invalid"), status("suspect"),
                                    status("valid"), F.sum(flag_hit.cast("long")).alias("flag_hits"))
    q = quarantine.groupBy("fault").agg(F.count("*").alias("quarantined"),
                                        F.sum(reason_hit.cast("long")).alias("reason_hits"))
    rows = injected.join(s, "fault", "left").join(q, "fault", "left").fillna(0).collect()

    out = []
    for r in sorted(rows, key=lambda r: (r["fault"] == "clean", r["fault"])):
        f = BY_NAME.get(r["fault"])
        removed = r["injected"] - r["silver"] - r["quarantined"]
        caught = r["quarantined"] + r["invalid"] + r["suspect"]
        hits = None
        if f and f.expected == "dedup":
            caught, hits = caught + removed, removed
        elif f and f.expected:
            hits = r["reason_hits"] if f.expected.startswith("quarantine:") else r["flag_hits"]
        expected_share = None if hits is None else hits / r["injected"]
        caught_share = caught / r["injected"]
        out.append({"fault": r["fault"], "level": f.level if f else "", "expected": (f.expected or "-") if f else "",
                    "injected": r["injected"], "quarantined": r["quarantined"], "removed": removed,
                    "invalid": r["invalid"], "suspect": r["suspect"], "valid": r["valid"],
                    "expected_hits": "" if hits is None else hits,
                    "expected_share": "" if expected_share is None else round(expected_share, 4),
                    "caught_share": round(caught_share, 4),
                    "verdict": verdict(expected_share, caught_share) if f else "baseline"})
    return out, silver


def collateral(silver, baseline, year, month):
    """Status of clean rows in the chaos run vs the same (sensor, time) in the baseline."""
    clean = silver.filter(F.col("fault") == "clean").select("sensors_id", "datetime_utc",
                                                            F.col("dq_status").alias("chaos_status"))
    base = (baseline.filter((F.col("year") == year) & (F.col("month") == month))
            .select("sensors_id", "datetime_utc", F.col("dq_status").alias("baseline_status")))
    return (clean.join(base, ["sensors_id", "datetime_utc"], "left")
            .fillna("missing", ["baseline_status"])
            .groupBy("baseline_status", "chaos_status").count()
            .orderBy("baseline_status", "chaos_status").collect())


def write_csv(path, rows, columns):
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-id", required=True)
    p.add_argument("--month", required=True, help="YYYY-MM")
    p.add_argument("--baseline", help="Silver measurements root to compare clean rows with "
                                      "(e.g. openaq_lake/chaos/control/silver/measurements)")
    args = p.parse_args()
    year, month = map(int, args.month.split("-"))
    root = LAKE / "chaos" / args.run_id
    report = LAKE / "reports" / f"{args.run_id}_eval"
    report.mkdir(parents=True, exist_ok=True)
    meta = {"run_id": args.run_id, "month": args.month, "baseline": args.baseline,
            "started_utc": datetime.now(timezone.utc).isoformat()}

    spark = (SparkSession.builder.appName("OpenAQ-Chaos-Evaluate")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary, silver = summarize(spark, root)
        write_csv(report / "summary.csv", summary, COLUMNS)
        width = max(len(r["fault"]) for r in summary)
        print(f"{'fault':{width}} {'injected':>9} {'caught':>7} {'expected':>8}  verdict")
        for r in summary:
            print(f"{r['fault']:{width}} {r['injected']:>9} {r['caught_share']:>7.1%} "
                  f"{(format(r['expected_share'], '.1%') if r['expected_share'] != '' else '-'):>8}  {r['verdict']}")
        if args.baseline:
            baseline = spark.read.parquet(uri(Path(args.baseline).expanduser().absolute()))
            trans = [r.asDict() for r in collateral(silver, baseline, year, month)]
            write_csv(report / "collateral.csv", trans, ["baseline_status", "chaos_status", "count"])
            changed = sum(r["count"] for r in trans if r["baseline_status"] != r["chaos_status"])
            total = sum(r["count"] for r in trans)
            meta.update(clean_rows=total, clean_rows_changed=changed)
            print(f"clean rows whose status differs from the baseline: {changed} of {total}")
        meta.update(status="complete", summary=summary)
    finally:
        meta["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
