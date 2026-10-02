import csv
import gzip
import zlib

from utils import REPORTS, local_path, manifest_rows


def validate(row):
    path = local_path(row["key"])
    result = dict(key=row["key"], local_path=str(path), exists=False,
                  expected_size=row["size"], local_size="", size_ok=False,
                  gzip_ok="", csv_ok="", error="")
    errors = []
    try:
        result["exists"] = path.is_file()
        if not result["exists"]:
            result["error"] = "Missing file"
            return result
        result["local_size"] = path.stat().st_size
        result["size_ok"] = result["local_size"] == row["size"]
        if not result["size_ok"]:
            errors.append("Size mismatch")
        try:
            with gzip.open(path, "rb") as stream:
                while stream.read(1024 * 1024):
                    pass
            result["gzip_ok"] = True
        except (OSError, EOFError, zlib.error) as error:
            result["gzip_ok"] = False
            errors.append(f"gzip: {error}")
        if result["gzip_ok"]:
            try:
                with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as stream:
                    header = next(csv.reader(stream, strict=True), None)
                    if not header or any(not name.strip() for name in header):
                        raise ValueError("Missing or empty header column")
                result["csv_ok"] = True
            except (OSError, EOFError, zlib.error, UnicodeError, csv.Error, ValueError) as error:
                result["csv_ok"] = False
                errors.append(f"CSV header: {error}")
    except OSError as error:
        errors.append(str(error))
    result["error"] = "; ".join(errors)
    return result


def main():
    REPORTS.mkdir(parents=True, exist_ok=True)
    target = REPORTS / "raw_validation.csv"
    temporary = target.with_suffix(".csv.part")
    counts = dict(total=0, missing=0, size_mismatch=0, invalid_gzip=0, invalid_csv=0, valid=0)
    fields = ["key", "local_path", "exists", "expected_size", "local_size", "size_ok", "gzip_ok", "csv_ok", "error"]
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in manifest_rows():
            result = validate(row)
            writer.writerow(result)
            counts["total"] += 1
            counts["missing"] += not result["exists"]
            counts["size_mismatch"] += result["exists"] and not result["size_ok"]
            counts["invalid_gzip"] += result["gzip_ok"] is False
            counts["invalid_csv"] += result["csv_ok"] is False
            counts["valid"] += all(result[name] is True for name in ("exists", "size_ok", "gzip_ok", "csv_ok")) and not result["error"]
            if counts["total"] % 1000 == 0:
                print(f"Validated: {counts['total']} valid={counts['valid']}", flush=True)
    temporary.replace(target)
    for name, value in counts.items():
        print(f"{name.replace('_', ' ').capitalize()}: {value}")
    return int(counts["valid"] != counts["total"])


if __name__ == "__main__":
    raise SystemExit(main())
