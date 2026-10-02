import csv
import time

from botocore.exceptions import BotoCoreError, ClientError

from utils import BUCKET, KEY_PATTERN, MANIFEST, PREFIX, s3_client


def main():
    client = s3_client()
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    temporary = MANIFEST.with_suffix(".csv.part")
    request = {"Bucket": BUCKET, "Prefix": PREFIX, "MaxKeys": 1000}
    pages = files = total_bytes = excluded = 0
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["key", "size", "last_modified", "location_id", "year", "month", "filename"])
        while True:
            for attempt in range(1, 9):
                try:
                    page = client.list_objects_v2(**request)
                    break
                except (BotoCoreError, ClientError, OSError) as error:
                    if attempt == 8:
                        raise
                    print(f"Page {pages + 1}, attempt {attempt} failed: {error}", flush=True)
                    time.sleep(min(2 * attempt, 30))
            for obj in page.get("Contents", []):
                match = KEY_PATTERN.fullmatch(obj["Key"])
                if match is None or obj["Size"] <= 0:
                    excluded += 1
                    continue
                writer.writerow([obj["Key"], obj["Size"], obj["LastModified"].isoformat(), *match.groups()])
                files += 1
                total_bytes += obj["Size"]
            pages += 1
            if pages % 100 == 0:
                stream.flush()
                print(f"pages={pages} files={files} size={total_bytes / 2**30:.2f} GiB", flush=True)
            if not page.get("IsTruncated", False):
                break
            token = page.get("NextContinuationToken")
            if not token or token == request.get("ContinuationToken"):
                raise RuntimeError("S3 returned a missing or repeated continuation token")
            request["ContinuationToken"] = token
    if not files:
        raise RuntimeError("No valid objects found; previous manifest was preserved")
    temporary.replace(MANIFEST)
    print(f"Total valid files: {files}\nCompressed bytes: {total_bytes}\n"
          f"Compressed GiB: {total_bytes / 2**30:.2f}\nExcluded objects: {excluded}\nManifest: {MANIFEST}")


if __name__ == "__main__":
    main()
