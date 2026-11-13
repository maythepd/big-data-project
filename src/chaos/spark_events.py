"""Summarize Spark event logs: duration, task failures, spill, GC and peak execution memory.

Jobs write event logs when started with
    --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///app/openaq_lake/spark-events
(scripts/experiments/memory.sh gives every run its own folder, named after the run). An
application that crashed leaves an `.inprogress` file; it is summarized too, as "incomplete".

    python src/chaos/spark_events.py openaq_lake/spark-events/mem_* [--csv out.csv]
"""

import argparse
import csv
import json
from pathlib import Path

FIELDS = ["run", "app", "status", "seconds", "tasks", "failed_tasks", "failure_reasons", "spill_memory_mb",
          "spill_disk_mb", "gc_share", "peak_exec_mem_mb_max", "jobs_failed"]


def summarize(path):
    app, start, end, status = path.name, None, None, "incomplete"
    tasks = failed = jobs_failed = 0
    spill_mem = spill_disk = gc = run = peak = last = 0
    reasons = {}
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            e = json.loads(line)
            kind = e.get("Event")
            if kind == "SparkListenerApplicationStart":
                app, start = e.get("App Name", app), e["Timestamp"]
            elif kind == "SparkListenerApplicationEnd":
                end, status = e["Timestamp"], "complete"
            elif kind == "SparkListenerJobEnd" and e.get("Job Result", {}).get("Result") != "JobSucceeded":
                jobs_failed += 1
            elif kind == "SparkListenerTaskEnd":
                tasks += 1
                reason = e.get("Task End Reason", {}).get("Reason", "Success")
                if reason != "Success":
                    failed += 1
                    detail = e["Task End Reason"].get("Class Name") or reason
                    reasons[detail] = reasons.get(detail, 0) + 1
                m = e.get("Task Metrics") or {}
                spill_mem += m.get("Memory Bytes Spilled", 0)
                spill_disk += m.get("Disk Bytes Spilled", 0)
                gc += m.get("JVM GC Time", 0)
                run += m.get("Executor Run Time", 0)
                peak = max(peak, m.get("Peak Execution Memory", 0))
                last = max(last, (e.get("Task Info") or {}).get("Finish Time") or 0)
    mb = lambda b: round(b / 2 ** 20, 1)
    return {"app": app, "status": status, "seconds": round(((end or last or start) - start) / 1000, 1) if start else None,
            "tasks": tasks, "failed_tasks": failed,
            "failure_reasons": "; ".join(f"{k} x{v}" for k, v in reasons.items()),
            "spill_memory_mb": mb(spill_mem), "spill_disk_mb": mb(spill_disk),
            "gc_share": round(gc / run, 3) if run else None, "peak_exec_mem_mb_max": mb(peak),
            "jobs_failed": jobs_failed}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("folders", nargs="+", help="one event-log folder per run")
    p.add_argument("--csv")
    args = p.parse_args()
    rows = [{"run": Path(d).name, **summarize(f)} for d in args.folders
            for f in sorted(Path(d).iterdir()) if f.is_file()]
    for r in rows:
        print(json.dumps(r))
    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)


if __name__ == "__main__":
    main()
