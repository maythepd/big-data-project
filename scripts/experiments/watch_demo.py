"""Terminal view for the live demo: python watch_demo.py RUN_DIR [CONTAINER]

Every few seconds prints, as it happens:
  lag     one line per micro-batch of each query: rows, rows/s, offsets still behind
  silver  one line per Silver batch: rows kept by status, rows quarantined by reason, and the
          dq flags on kept rows; this is where the bad data sent by Locust's BadDataSender lands
Survives the consumer being killed and restarted (it re-reads `docker logs` by time).
"""
import json
from pathlib import Path
import subprocess
import sys
import time

run, container = Path(sys.argv[1]), sys.argv[2] if len(sys.argv) > 2 else "spark-replay"
log = run / "progress.jsonl"
offset, since = (log.stat().st_size if log.exists() else 0), int(time.time())
INFO = {"dup_resolved", "coord_filled", "coord_invalid", "datetime_offset_repaired"}

while True:
    if log.exists():
        with log.open(encoding="utf-8") as fh:
            fh.seek(offset)
            for line in fh:
                p = json.loads(line)
                if not p.get("idle"):
                    print(f"  lag    [{p['query']:10}] batch {p['batch']}: {p['rows']:>7} rows, "
                          f"{p.get('processed_rps') or 0:>9,.0f} rows/s, {int(p['offsets_behind_max'] or 0)} behind",
                          flush=True)
            offset = fh.tell()
    now = int(time.time())
    out = subprocess.run(["docker", "logs", "--since", str(since), "--until", str(now), container],
                         capture_output=True, text=True, encoding="utf-8", errors="replace")
    since = now
    for line in (out.stdout + out.stderr).splitlines():
        if line.startswith("silver_stream: "):
            r = json.loads(line[len("silver_stream: "):])
            flags = {k: v for k, v in (r.get("flags") or {}).items() if k not in INFO}
            print(f"  silver batch {r['batch_id']}: {r['rows_in']} in -> kept {r.get('status_totals')}, "
                  f"quarantined {r.get('quarantine_reasons') or {}}, deduplicated {r['dedup_removed']}, "
                  f"flags {flags}", flush=True)
        elif "[consumer]" in line and ("FAILED" in line or "started" in line):
            print("  " + line.strip(), flush=True)
    time.sleep(5)
