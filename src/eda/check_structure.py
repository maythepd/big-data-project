"""CLI for structure checks; usable with spark-submit or python -m src.eda.check_structure.

Input is either block recovery output (near_silver_compact with year=/month= and
_null_ts) or a flat directory of Near-Silver Parquet files (old layout).
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from pyspark.sql import SparkSession, functions as F

# spark-submit executes the file as a script, with src/eda on sys.path.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.eda.common import input_files, read_compact, read_near_silver
from src.eda.structure_checks import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Local Parquet directory, recursive")
    parser.add_argument("--output", required=True, help="New local report directory")
    parser.add_argument("--limit-files", type=int, help="Flat layout only")
    parser.add_argument("--months", nargs="*", help="Compact layout only: YYYY-MM and/or null_ts")
    parser.add_argument("--sample-rows", type=int, default=20, help="Maximum examples per issue")
    parser.add_argument("--top-patterns", type=int, default=50)
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    args = parser.parse_args()
    for name in ["limit_files", "sample_rows", "top_patterns", "shuffle_partitions"]:
        value = getattr(args, name)
        if value is not None and value < 1:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    source = Path(args.input).expanduser().absolute()
    compact = any(source.glob("year=*")) or (source / "_null_ts").is_dir()
    if compact and args.limit_files:
        parser.error("--limit-files applies to the flat layout; use --months")
    if compact:
        files = sorted(source.rglob("*.parquet"))
    else:
        source, files = input_files(args.input, args.limit_files)
    output = Path(args.output).expanduser().absolute()
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "phase": 2, "status": "running", "started_utc": datetime.now(timezone.utc).isoformat(),
        "input": str(source), "layout": "compact" if compact else "flat",
        "files": len(files), "limit_files": args.limit_files, "months": args.months,
        "timezone": "UTC", "sample_rows_per_issue": args.sample_rows,
        "top_values_per_issue": args.top_patterns,
        "shuffle_partitions": args.shuffle_partitions,
        "source_trace": "source_block = DataNode block id (block recovery manifest); no CSV provenance",
    }
    def save_metadata():
        (output / "run.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    save_metadata()
    (output / "input_files.txt").write_text(
        "\n".join(str(p) for p in files) + "\n", encoding="utf-8"
    )
    spark = None
    try:
        spark = (SparkSession.builder.appName("OpenAQ-EDA-Structure")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.legacy.parquet.nanosAsLong", "true")
                 .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
                 # ~30 flags in one filter generate a method too big for HotSpot JIT;
                 # splitting at 8000 bytes made the scan 4x faster (86.6s -> 21.6s / 10M rows).
                 .config("spark.sql.codegen.hugeMethodLimit", "8000")
                 .config("spark.sql.files.maxPartitionBytes", str(256 << 20))
                 .config("spark.sql.files.openCostInBytes", str(1 << 20))
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        if compact:
            df = read_compact(spark, source, include_null_ts=not args.months or "null_ts" in args.months)
            wanted = [m for m in args.months or [] if m != "null_ts"]
            if args.months:
                keep = F.col("year").isNull()
                for month in wanted:
                    keep = keep | ((F.col("year") == int(month[:4])) & (F.col("month") == int(month[5:])))
                df = df.filter(keep)
        else:
            df = read_near_silver(spark, files)
        (output / "schema.json").write_text(df.schema.json(), encoding="utf-8")
        metrics = run(df, output, args.sample_rows, args.top_patterns)
        metadata.update(status="complete", overview=metrics)
    except Exception as exc:
        metadata.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
        save_metadata()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
