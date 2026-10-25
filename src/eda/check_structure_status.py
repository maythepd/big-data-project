"""Summarize a structure checks run; optionally wait for its terminal state."""

import argparse
import json
from pathlib import Path
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.structure_report import summarize, update_tracking


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", required=True)
    parser.add_argument("--tracking")
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=10800)
    args = parser.parse_args()
    report = Path(args.reports).expanduser().absolute()
    deadline = time.monotonic() + args.timeout_seconds
    while True:
        metadata = json.loads((report / "run.json").read_text())
        if metadata["status"] != "running" or not args.wait:
            break
        if time.monotonic() >= deadline:
            raise SystemExit("Timed out waiting for structure checks; tracking remains pending")
        time.sleep(10)
    if metadata["status"] == "complete":
        metadata = summarize(report)
        print(f"Summary and consistency checks complete: {report / 'SUMMARY.md'}", flush=True)
    if args.tracking and metadata["status"] != "running":
        update_tracking(Path(args.tracking).expanduser().absolute(), report, metadata)
    if metadata["status"] != "complete":
        raise SystemExit(f"Run status: {metadata['status']}")


if __name__ == "__main__":
    main()
