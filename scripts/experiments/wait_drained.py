"""Wait until the consumer under test has caught up: python wait_drained.py RUN_DIR [TIMEOUT_S].

Caught up = the replay has finished (RUN_DIR/replay.json) and every query has since logged at
least 3 progress entries, the last 3 reading nothing with 0 offsets behind. Row totals are not
used: a batch committed just before a kill -9 never logs its progress.
Exit 0 when drained, 1 on timeout.
"""
from datetime import datetime
import json
from pathlib import Path
import sys
import time

run, timeout = Path(sys.argv[1]), float(sys.argv[2]) if len(sys.argv) > 2 else 1800
deadline = time.time() + timeout
stamp = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
while time.time() < deadline:
    log, replay = run / "progress.jsonl", run / "replay.json"
    if log.exists() and replay.exists():
        done = stamp(json.loads(replay.read_text(encoding="utf-8"))["finished_utc"])
        by_query = {}
        for line in log.read_text(encoding="utf-8").splitlines():
            p = json.loads(line)
            if p["timestamp"] and stamp(p["timestamp"]) > done:
                by_query.setdefault(p["query"], []).append(p)
        if by_query and all(len(v) >= 3 and all(not e["rows"] and not (e["offsets_behind_max"] or 0) for e in v[-3:])
                            for v in by_query.values()):
            print("drained: " + ", ".join(sorted(by_query)) + " idle since the replay finished")
            sys.exit(0)
    time.sleep(5)
print(f"not drained after {timeout:.0f}s", file=sys.stderr)
sys.exit(1)
