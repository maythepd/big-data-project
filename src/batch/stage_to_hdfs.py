import argparse
import csv
import posixpath
import re
import subprocess
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

RAW = ROOT / "data" / "raw"
VALIDATION = ROOT / "data" / "reports" / "raw_validation.csv"
FAILED_REPORT = ROOT / "data" / "reports" / "hdfs_stage_failed.csv"

DEFAULT_HDFS_ROOT = "/openaq/bronze"
PREFIX = "records/csv.gz/"

BATCH_SIZE = 200
MAX_RETRIES = 3

# Chỉ in progress sau mỗi 1,000,000 file upload thành công.
PROGRESS_EVERY_UPLOADS = 1_000_000


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Chỉ stage N file valid đầu tiên để test. Bỏ option này để chạy full.",
    )
    parser.add_argument(
        "--hdfs-root",
        default=DEFAULT_HDFS_ROOT,
        help="HDFS destination root. Ví dụ /openaq/bronze_test khi test.",
    )
    return parser.parse_args()


def run_hdfs(args, cwd=None, check=True):
    result = subprocess.run(
        ["hdfs", "dfs", *args],
        cwd=cwd,
        text=True,
        capture_output=True,
    )

    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed: hdfs dfs {' '.join(args)}\n"
            f"{result.stderr.strip()}"
        )

    return result


def to_wsl_path(value):
    value = value.strip()

    path = Path(value)
    if path.exists():
        return path

    match = re.match(r"^([A-Za-z]):[\\/](.*)$", value)
    if match:
        drive = match.group(1).lower()
        rest = match.group(2).replace("\\", "/")
        return Path(f"/mnt/{drive}/{rest}")

    return path


def is_valid(row):
    return (
        row.get("exists") == "True"
        and row.get("size_ok") == "True"
        and row.get("gzip_ok") == "True"
        and row.get("csv_ok") == "True"
        and not row.get("error", "").strip()
    )


def key_to_relative(key):
    if not key.startswith(PREFIX):
        raise ValueError(f"Unexpected key: {key}")

    relative = key[len(PREFIX):]

    if not relative or relative.startswith("/") or ".." in relative.split("/"):
        raise ValueError(f"Unsafe key: {key}")

    return relative


def list_hdfs_dir(hdfs_dir):
    result = run_hdfs(["-ls", hdfs_dir], check=False)

    if result.returncode != 0:
        return {}

    files = {}

    for line in result.stdout.splitlines():
        line = line.strip()

        if not line or line.startswith("Found "):
            continue

        parts = line.split(None, 7)

        if len(parts) < 8 or parts[0].startswith("d"):
            continue

        try:
            size = int(parts[4])
        except ValueError:
            continue

        files[posixpath.basename(parts[7])] = size

    return files


def put_batch(local_parent, filenames, hdfs_dir):
    args = ["-put"]
    args.extend(f"./{name}" for name in filenames)
    args.append(hdfs_dir.rstrip("/") + "/")

    last_error = ""

    for attempt in range(1, MAX_RETRIES + 1):
        result = run_hdfs(args, cwd=local_parent, check=False)

        if result.returncode == 0:
            return True, ""

        last_error = result.stderr.strip()

        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)

    return False, last_error


def print_progress_if_needed(counts, last_printed_uploads):
    uploaded = counts["uploaded"]

    if uploaded - last_printed_uploads >= PROGRESS_EVERY_UPLOADS:
        print(
            f"uploaded={uploaded:,} | "
            f"existing={counts['skipped_existing']:,} | "
            f"invalid_skipped={counts['invalid_skipped']:,} | "
            f"failed={counts['failed']:,}",
            flush=True,
        )
        return uploaded

    return last_printed_uploads


def flush_group(hdfs_dir, rows, failed_writer, counts):
    if not rows:
        return

    run_hdfs(["-mkdir", "-p", hdfs_dir])
    existing = list_hdfs_dir(hdfs_dir)

    uploads_by_parent = defaultdict(list)
    expected_after_upload = {}

    for row in rows:
        key = row["key"]
        local_path = to_wsl_path(row["local_path"])
        expected_size = int(row["expected_size"])
        filename = posixpath.basename(key_to_relative(key))

        if not local_path.is_file():
            # Path recorded on another machine, e.g. the Windows path seen from
            # the Docker HDFS client: use this checkout's raw archive instead.
            local_path = RAW / key_to_relative(key)

        if not local_path.is_file():
            failed_writer.writerow([key, str(local_path), "local file not found"])
            counts["failed"] += 1
            continue

        local_size = local_path.stat().st_size

        if local_size != expected_size:
            failed_writer.writerow([
                key,
                str(local_path),
                f"local size changed: expected={expected_size}, actual={local_size}",
            ])
            counts["failed"] += 1
            continue

        hdfs_size = existing.get(filename)

        if hdfs_size is not None:
            if hdfs_size == expected_size:
                counts["skipped_existing"] += 1
            else:
                failed_writer.writerow([
                    key,
                    str(local_path),
                    f"HDFS conflict: expected={expected_size}, actual={hdfs_size}",
                ])
                counts["failed"] += 1
            continue

        uploads_by_parent[local_path.parent].append(
            (filename, key, local_path, expected_size)
        )

    for local_parent, items in uploads_by_parent.items():
        for start in range(0, len(items), BATCH_SIZE):
            batch = items[start:start + BATCH_SIZE]
            filenames = [item[0] for item in batch]

            ok, error = put_batch(local_parent, filenames, hdfs_dir)

            if ok:
                counts["uploaded"] += len(batch)
                for filename, key, local_path, expected_size in batch:
                    expected_after_upload[filename] = (
                        expected_size,
                        key,
                        local_path,
                    )
            else:
                for _, key, local_path, _ in batch:
                    failed_writer.writerow([key, str(local_path), error])
                    counts["failed"] += 1

    # Verify only files uploaded in this run for this directory.
    if expected_after_upload:
        final_files = list_hdfs_dir(hdfs_dir)

        for filename, (expected_size, key, local_path) in expected_after_upload.items():
            actual = final_files.get(filename)

            if actual != expected_size:
                failed_writer.writerow([
                    key,
                    str(local_path),
                    f"post-upload verification failed: expected={expected_size}, hdfs={actual}",
                ])
                counts["failed"] += 1


def main():
    args = parse_args()
    hdfs_root = args.hdfs_root.rstrip("/")

    if not VALIDATION.is_file():
        raise FileNotFoundError(
            f"Validation report not found: {VALIDATION}\n"
            "Run validate_raw.py first."
        )

    result = subprocess.run(
        ["hdfs", "dfsadmin", "-report"],
        text=True,
        capture_output=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "HDFS is not reachable. Start HDFS first with start-dfs.sh.\n"
            + result.stderr.strip()
        )

    ROOT.joinpath("data", "reports").mkdir(parents=True, exist_ok=True)

    counts = {
        "validation_rows": 0,
        "valid_rows": 0,
        "invalid_skipped": 0,
        "uploaded": 0,
        "skipped_existing": 0,
        "failed": 0,
    }

    current_hdfs_dir = None
    current_rows = []
    last_printed_uploads = 0

    with VALIDATION.open(newline="", encoding="utf-8") as source, \
            FAILED_REPORT.open("w", newline="", encoding="utf-8") as failed_stream:

        reader = csv.DictReader(source)

        required = {
            "key",
            "local_path",
            "exists",
            "expected_size",
            "size_ok",
            "gzip_ok",
            "csv_ok",
            "error",
        }

        if not required.issubset(reader.fieldnames or []):
            raise ValueError(
                "raw_validation.csv does not have the expected columns."
            )

        failed_writer = csv.writer(failed_stream)
        failed_writer.writerow(["key", "local_path", "error"])

        stop = False

        for row in reader:
            counts["validation_rows"] += 1

            if not is_valid(row):
                counts["invalid_skipped"] += 1
                continue

            if args.limit is not None and counts["valid_rows"] >= args.limit:
                stop = True
                break

            counts["valid_rows"] += 1

            relative = key_to_relative(row["key"])
            parent = posixpath.dirname(relative)
            hdfs_dir = posixpath.join(hdfs_root, parent)

            if current_hdfs_dir is None:
                current_hdfs_dir = hdfs_dir

            if hdfs_dir != current_hdfs_dir:
                flush_group(
                    current_hdfs_dir,
                    current_rows,
                    failed_writer,
                    counts,
                )

                last_printed_uploads = print_progress_if_needed(
                    counts,
                    last_printed_uploads,
                )

                current_hdfs_dir = hdfs_dir
                current_rows = []

            current_rows.append(row)

        if current_rows:
            flush_group(
                current_hdfs_dir,
                current_rows,
                failed_writer,
                counts,
            )

    print("\nDONE")
    for name, value in counts.items():
        print(f"{name}: {value:,}")

    print(f"HDFS root: {hdfs_root}")
    print(f"Failure report: {FAILED_REPORT}")

    if args.limit is not None:
        print(f"TEST LIMIT: {args.limit:,} valid files")

    if counts["failed"]:
        print(
            "\nSome files failed. Fix the cause and rerun. "
            "Files already present in HDFS with the correct size are skipped."
        )


if __name__ == "__main__":
    main()
