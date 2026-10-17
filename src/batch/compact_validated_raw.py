import argparse
import csv
import gzip
import io
import queue
import re
import shutil
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VALIDATION = ROOT / "data" / "reports" / "raw_validation.csv"
REPORTS = ROOT / "data" / "reports"

# For speed, default staging is on WSL's Linux filesystem, not /mnt/e.
DEFAULT_OUTPUT = Path.home() / "openaq_near_silver_staging"

SCHEMA_1 = [
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lon", "parameter", "units", "value",
]
SCHEMA_2 = [
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lat", "lon", "lon", "parameter", "units", "value",
]
SCHEMA_3 = [
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lon", "measurand", "units", "value",
]
CANONICAL = SCHEMA_1

DEFAULT_WORKERS = 16
DEFAULT_CHUNK_MB = 256
QUEUE_PER_WORKER = 64
PROGRESS_EVERY = 1_000_000


def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Ultra-fast OpenAQ compaction. Schema 1/3 gzip files are copied "
            "byte-for-byte as concatenated gzip members; only rare schema 2 "
            "files are parsed/re-written."
        )
    )
    p.add_argument("--output", default=str(DEFAULT_OUTPUT))
    p.add_argument("--limit-files", type=int, default=None)
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    p.add_argument(
        "--chunk-mb",
        type=int,
        default=DEFAULT_CHUNK_MB,
        help="Target COMPRESSED size per output chunk.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete an existing output directory before starting.",
    )
    return p.parse_args()


def to_wsl_path(value):
    value = value.strip()
    p = Path(value)

    if p.exists():
        return p

    m = re.match(r"^([A-Za-z]):[\\/](.*)$", value)
    if m:
        drive = m.group(1).lower()
        rest = m.group(2).replace("\\", "/")
        return Path(f"/mnt/{drive}/{rest}")

    return p


def valid_row(row):
    return (
        row.get("exists") == "True"
        and row.get("size_ok") == "True"
        and row.get("gzip_ok") == "True"
        and row.get("csv_ok") == "True"
        and not row.get("error", "").strip()
    )


def read_header_from_uncompressed(data):
    end = data.find(b"\n")
    first = data if end == -1 else data[:end + 1]
    text = first.decode("utf-8-sig").rstrip("\r\n")
    return next(csv.reader([text], strict=True), None)


def transform_schema_2(uncompressed):
    """
    Convert:
      ...,lat,lat,lon,lon,parameter,...
    to the 9-column canonical layout.

    Output remains its own gzip member and includes a canonical header.
    Repeated headers are removed later by Spark.
    """
    text = uncompressed.decode("utf-8-sig")
    source = io.StringIO(text, newline="")
    reader = csv.reader(source, strict=True)

    header = next(reader, None)
    if header != SCHEMA_2:
        raise ValueError("schema_2 header changed while processing")

    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(CANONICAL)

    for values in reader:
        if len(values) != 11:
            raise ValueError(
                f"schema_2 row has {len(values)} columns, expected 11"
            )

        lat = values[4] if values[4] != "" else values[5]
        lon = values[6] if values[6] != "" else values[7]

        writer.writerow([
            values[0],
            values[1],
            values[2],
            values[3],
            lat,
            lon,
            values[8],
            values[9],
            values[10],
        ])

    return gzip.compress(
        out.getvalue().encode("utf-8"),
        compresslevel=1,
        mtime=0,
    )


class Worker(threading.Thread):
    def __init__(
        self,
        worker_id,
        task_queue,
        output_dir,
        chunk_target_bytes,
        failure_writer,
        failure_lock,
    ):
        super().__init__(name=f"compact-worker-{worker_id}", daemon=False)
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.output_dir = output_dir
        self.chunk_target_bytes = chunk_target_bytes
        self.failure_writer = failure_writer
        self.failure_lock = failure_lock

        self.chunk_id = 0
        self.stream = None
        self.current_size = 0

        self.processed = 0
        self.schema1 = 0
        self.schema2 = 0
        self.schema3 = 0
        self.failed = 0
        self.chunks = 0
        self.bytes_written = 0

    def open_chunk(self):
        if self.stream is not None:
            self.stream.flush()
            self.stream.close()

        path = self.output_dir / (
            f"part-w{self.worker_id:02d}-{self.chunk_id:06d}.csv.gz"
        )
        self.chunk_id += 1
        self.stream = path.open("wb", buffering=8 * 1024 * 1024)
        self.current_size = 0
        self.chunks += 1

    def append_member(self, data):
        if (
            self.stream is None
            or (
                self.current_size > 0
                and self.current_size + len(data) > self.chunk_target_bytes
            )
        ):
            self.open_chunk()

        self.stream.write(data)
        self.current_size += len(data)
        self.bytes_written += len(data)

    def report_failure(self, key, path, error):
        self.failed += 1
        with self.failure_lock:
            self.failure_writer.writerow([key, str(path), error])

    def run(self):
        while True:
            item = self.task_queue.get()

            if item is None:
                self.task_queue.task_done()
                break

            key, local_path_text = item
            path = to_wsl_path(local_path_text)

            try:
                # One physical read of the compressed source file.
                compressed = path.read_bytes()

                # Decompress once only to identify its already-known schema.
                # No row parsing/re-encoding for the ~99.9% schema 1/3 fast path.
                uncompressed = gzip.decompress(compressed)
                header = read_header_from_uncompressed(uncompressed)

                if header == SCHEMA_1:
                    member = compressed
                    self.schema1 += 1

                elif header == SCHEMA_3:
                    # Same 9 field positions as canonical. Only the header label
                    # differs (measurand vs parameter), so rows are positionally
                    # compatible and the raw gzip member can be copied unchanged.
                    member = compressed
                    self.schema3 += 1

                elif header == SCHEMA_2:
                    # Rare slow path (~0.1% from schema inspection).
                    member = transform_schema_2(uncompressed)
                    self.schema2 += 1

                else:
                    raise ValueError(f"unknown schema: {header!r}")

                self.append_member(member)
                self.processed += 1

            except Exception as error:
                self.report_failure(
                    key,
                    path,
                    f"{type(error).__name__}: {error}",
                )

            finally:
                self.task_queue.task_done()

        if self.stream is not None:
            self.stream.flush()
            self.stream.close()
            self.stream = None


def main():
    args = parse_args()

    if args.workers < 1:
        raise ValueError("--workers must be >= 1")
    if args.chunk_mb < 1:
        raise ValueError("--chunk-mb must be >= 1")
    if not VALIDATION.is_file():
        raise FileNotFoundError(
            f"Missing {VALIDATION}. Run validate_raw.py first."
        )

    output = Path(args.output).expanduser().resolve()

    if output.exists():
        if args.overwrite:
            shutil.rmtree(output)
        elif any(output.iterdir()):
            raise RuntimeError(
                f"Output is not empty: {output}\n"
                "Use a new path or pass --overwrite."
            )

    output.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)

    failure_report = REPORTS / "near_silver_compact_failed.csv"
    chunk_target = args.chunk_mb * 1024 * 1024

    queues = [
        queue.Queue(maxsize=QUEUE_PER_WORKER)
        for _ in range(args.workers)
    ]

    failure_lock = threading.Lock()

    selected = 0
    validation_rows = 0
    invalid_skipped = 0

    with failure_report.open(
        "w",
        newline="",
        encoding="utf-8",
        buffering=1024 * 1024,
    ) as failure_stream:

        failure_writer = csv.writer(failure_stream)
        failure_writer.writerow(["key", "local_path", "error"])

        workers = [
            Worker(
                worker_id=i,
                task_queue=queues[i],
                output_dir=output,
                chunk_target_bytes=chunk_target,
                failure_writer=failure_writer,
                failure_lock=failure_lock,
            )
            for i in range(args.workers)
        ]

        for worker in workers:
            worker.start()

        with VALIDATION.open(
            newline="",
            encoding="utf-8",
            buffering=8 * 1024 * 1024,
        ) as source:
            reader = csv.DictReader(source)

            required = {
                "key",
                "local_path",
                "exists",
                "size_ok",
                "gzip_ok",
                "csv_ok",
                "error",
            }
            if not required.issubset(reader.fieldnames or []):
                raise ValueError(
                    "raw_validation.csv does not have the expected columns"
                )

            worker_index = 0

            for row in reader:
                validation_rows += 1

                if not valid_row(row):
                    invalid_skipped += 1
                    continue

                queues[worker_index].put(
                    (row["key"], row["local_path"])
                )

                selected += 1
                worker_index = (worker_index + 1) % args.workers

                if selected % PROGRESS_EVERY == 0:
                    print(
                        f"queued={selected:,} | "
                        f"validation_rows={validation_rows:,} | "
                        f"invalid_skipped={invalid_skipped:,}",
                        flush=True,
                    )

                if (
                    args.limit_files is not None
                    and selected >= args.limit_files
                ):
                    break

        # Wait until every queued file has actually been processed.
        for q in queues:
            q.join()

        # Stop workers.
        for q in queues:
            q.put(None)

        for q in queues:
            q.join()

        for worker in workers:
            worker.join()

        failure_stream.flush()

    processed = sum(w.processed for w in workers)
    failed = sum(w.failed for w in workers)
    schema1 = sum(w.schema1 for w in workers)
    schema2 = sum(w.schema2 for w in workers)
    schema3 = sum(w.schema3 for w in workers)
    chunks = sum(w.chunks for w in workers)
    bytes_written = sum(w.bytes_written for w in workers)

    print("\nDONE")
    print(f"validation_rows: {validation_rows:,}")
    print(f"valid_files_selected: {selected:,}")
    print(f"invalid_skipped: {invalid_skipped:,}")
    print(f"processed_files: {processed:,}")
    print(f"failed_files: {failed:,}")
    print(f"schema_1_fast_path: {schema1:,}")
    print(f"schema_2_transformed: {schema2:,}")
    print(f"schema_3_fast_path: {schema3:,}")
    print(f"chunks_created: {chunks:,}")
    print(f"compressed_bytes_written: {bytes_written:,}")
    print(f"output: {output}")
    print(f"failure_report: {failure_report}")
    print(
        "\nNOTE: output files are concatenated gzip streams. "
        "Repeated source headers are intentionally preserved and must be "
        "filtered in the Spark near-Silver step."
    )


if __name__ == "__main__":
    main()
