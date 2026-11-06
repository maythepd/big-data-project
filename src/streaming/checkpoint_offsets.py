"""Print the Kafka offsets of the last committed batch in a local checkpoint copy,
as JSON for STREAM_STARTING_OFFSETS, so a new checkpoint continues without gaps.

    python -m src.streaming.checkpoint_offsets <checkpoint dir>
"""
import argparse
import json
from pathlib import Path
import sys


def batch_ids(folder):
    return sorted(int(p.name) for p in folder.iterdir() if p.name.isdigit()) if folder.is_dir() else []


def committed_offsets(checkpoint, source=0):
    checkpoint = Path(checkpoint).expanduser()
    committed = batch_ids(checkpoint / "commits")
    if not committed:
        raise SystemExit(f"{checkpoint}: no committed batch, so nothing to continue from")
    batch = committed[-1]
    lines = (checkpoint / "offsets" / str(batch)).read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "v1":
        raise SystemExit(f"{checkpoint}/offsets/{batch}: unexpected format (first line {lines[:1]})")
    offsets = json.loads(lines[2 + source])
    return batch, offsets


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("checkpoint", help="local copy of a query's checkpoint directory")
    p.add_argument("--source", type=int, default=0, help="source index (the consumer's queries have one)")
    args = p.parse_args()
    batch, offsets = committed_offsets(args.checkpoint, args.source)
    planned = batch_ids(Path(args.checkpoint).expanduser() / "offsets")
    note = f" (batch {planned[-1]} was planned but not committed: it is replayed)" if planned[-1] > batch else ""
    # stderr, so $(...) captures only the JSON
    print(f"last committed batch {batch}{note}", file=sys.stderr, flush=True)
    print(json.dumps(offsets, separators=(",", ":")))


if __name__ == "__main__":
    main()
