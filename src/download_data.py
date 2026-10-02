import time

from boto3.s3.transfer import TransferConfig
from boto3.exceptions import S3TransferFailedError
from botocore.exceptions import BotoCoreError, ClientError

from utils import BUCKET, REPORTS, local_path, manifest_rows, s3_client


def download_one(client, row):
    path = local_path(row["key"])
    partial = path.with_name(path.name + ".part")
    for attempt in range(1, 6):
        try:
            if path.is_file() and path.stat().st_size == row["size"]:
                return "skipped"
            path.parent.mkdir(parents=True, exist_ok=True)
            partial.unlink(missing_ok=True)
            client.download_file(BUCKET, row["key"], str(partial),
                                 Config=TransferConfig(use_threads=False))
            if partial.stat().st_size != row["size"]:
                raise OSError("Downloaded size differs from manifest")
            partial.replace(path)
            return "downloaded"
        except (BotoCoreError, ClientError, S3TransferFailedError, OSError) as error:
            try:
                partial.unlink(missing_ok=True)
            except OSError as cleanup_error:
                print(f"Cannot remove {partial}: {cleanup_error}", flush=True)
            if attempt == 5:
                print(f"Failed {row['key']}: {error}", flush=True)
                return "failed"
            time.sleep(attempt * 2)


def main():
    total = sum(1 for _ in manifest_rows())
    client = s3_client()
    REPORTS.mkdir(parents=True, exist_ok=True)
    counts = {"downloaded": 0, "skipped": 0, "failed": 0}
    with (REPORTS / "download_failed.txt").open("w", encoding="utf-8") as failures:
        for index, row in enumerate(manifest_rows(), 1):
            result = download_one(client, row)
            counts[result] += 1
            if result == "failed":
                failures.write(row["key"] + "\n")
                failures.flush()
            if index % 1000 == 0 or index == total:
                print(f"{index} / {total} " + " ".join(f"{k}={v}" for k, v in counts.items()), flush=True)
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
