import csv
import time

from botocore.exceptions import BotoCoreError, ClientError

from utils import BUCKET, KEY_PATTERN, MANIFEST, PREFIX, REPORTS, s3_client


def main():
    client = s3_client()
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)
    temporary = MANIFEST.with_suffix(".csv.part")
    excluded_report = REPORTS / "manifest_excluded.csv"
    request = {"Bucket": BUCKET, "Prefix": PREFIX, "MaxKeys": 1000}
    pages = files = total_bytes = 0
    excluded = {"zero_size": 0, "not_csv_gz": 0, "unrecognized_path": 0}
    layouts = {"location": 0, "provider_country": 0}
    with temporary.open("w", newline="", encoding="utf-8") as stream, \
            excluded_report.open("w", newline="", encoding="utf-8") as excluded_stream:
        writer = csv.writer(stream)
        writer.writerow(["key", "size", "last_modified", "location_id", "year", "month", "filename"])
        excluded_writer = csv.writer(excluded_stream)
        excluded_writer.writerow(["key", "size", "reason"])
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
                reason = None
                if obj["Size"] <= 0:
                    reason = "zero_size"
                elif not obj["Key"].endswith(".csv.gz"):
                    reason = "not_csv_gz"
                elif match is None:
                    reason = "unrecognized_path"
                if reason:
                    excluded[reason] += 1
                    excluded_writer.writerow([obj["Key"], obj["Size"], reason])
                    continue
                writer.writerow([obj["Key"], obj["Size"], obj["LastModified"].isoformat(), *match.groups()])
                files += 1
                total_bytes += obj["Size"]
                layout = "provider_country" if obj["Key"].startswith(PREFIX + "provider=") else "location"
                layouts[layout] += 1
            pages += 1
            if pages % 100 == 0:
                stream.flush()
                excluded_stream.flush()
                print(f"pages={pages} files={files} size={total_bytes / 2**30:.2f} GiB "
                      f"excluded={sum(excluded.values())} unknown_paths={excluded['unrecognized_path']}", flush=True)
            if not page.get("IsTruncated", False):
                break
            token = page.get("NextContinuationToken")
            if not token or token == request.get("ContinuationToken"):
                raise RuntimeError("S3 returned a missing or repeated continuation token")
            request["ContinuationToken"] = token
    print(f"Total valid files: {files}\nCompressed bytes: {total_bytes}\n"
          f"Compressed GiB: {total_bytes / 2**30:.2f}\n"
          f"Location layout: {layouts['location']}\nProvider/country layout: {layouts['provider_country']}\n"
          f"Excluded objects: {sum(excluded.values())}")
    for reason, count in excluded.items():
        print(f"  {reason}: {count}")
    print(f"Excluded report: {excluded_report}")
    if excluded["unrecognized_path"]:
        raise RuntimeError(
            "Unrecognized nonempty CSV.GZ paths found; inspect manifest_excluded.csv. "
            "The incomplete scan output remains in .csv.part; no manifest was published."
        )
    if not files:
        raise RuntimeError("No valid objects found; no manifest was published")
    temporary.replace(MANIFEST)
    print(f"Manifest: {MANIFEST}")


if __name__ == "__main__":
    main()
