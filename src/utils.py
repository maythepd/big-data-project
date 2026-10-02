import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw"
REPORTS = ROOT / "data/reports"
MANIFEST = ROOT / "data/metadata/archive_manifest.csv"
BUCKET = "openaq-data-archive"
PREFIX = "records/csv.gz/"
KEY_PATTERN = re.compile(
    r"records/csv\.gz/locationid=(\d+)/year=(\d{4})/"
    r"month=(0[1-9]|1[0-2])/([^/\\:<>\"|?*\x00-\x1f]+\.csv\.gz)"
)


def local_path(key):
    if not KEY_PATTERN.fullmatch(key):
        raise ValueError(f"Invalid archive key: {key!r}")
    path = RAW / key[len(PREFIX):]
    if not path.resolve().is_relative_to(RAW.resolve()):
        raise ValueError(f"Path escapes raw directory: {key!r}")
    return path


def manifest_rows():
    with MANIFEST.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if not {"key", "size"}.issubset(reader.fieldnames or []):
            raise ValueError("Manifest must have key and size columns")
        for row in reader:
            local_path(row["key"])
            row["size"] = int(row["size"])
            if row["size"] <= 0:
                raise ValueError(f"Invalid size: {row['key']}")
            yield row


def s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    return boto3.client("s3", region_name="us-east-1", config=Config(
        signature_version=UNSIGNED, connect_timeout=20, read_timeout=60,
        retries={"mode": "standard", "total_max_attempts": 3},
    ))
