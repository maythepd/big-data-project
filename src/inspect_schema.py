import csv
import gzip
import json
import zlib

from utils import REPORTS, local_path


def main():
    schemas = {}
    errors = excluded = processed = 0
    source = REPORTS / "raw_validation.csv"
    with source.open(newline="", encoding="utf-8") as validation, \
            (REPORTS / "schema_errors.csv").open("w", newline="", encoding="utf-8") as failure_stream:
        reader = csv.DictReader(validation)
        required = {"key", "exists", "size_ok", "gzip_ok", "csv_ok", "expected_size", "error"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError("Invalid validation report; run validate_raw.py first")
        failures = csv.writer(failure_stream)
        failures.writerow(["key", "error"])
        for row in reader:
            if any(row[name] != "True" for name in ("exists", "size_ok", "gzip_ok", "csv_ok")) or row["error"]:
                excluded += 1
                continue
            processed += 1
            try:
                path = local_path(row["key"])
                if path.stat().st_size != int(row["expected_size"]):
                    raise ValueError("File changed after validation")
                with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as stream:
                    columns = next(csv.reader(stream, strict=True), None)
                if not columns or any(not name.strip() for name in columns):
                    raise ValueError("Missing or empty header column")
                schema = tuple(columns)
                if schema not in schemas:
                    schemas[schema] = [len(schemas) + 1, 0, str(path)]
                schemas[schema][1] += 1
            except (OSError, EOFError, zlib.error, UnicodeError, csv.Error, ValueError) as error:
                failures.writerow([row["key"], str(error)])
                errors += 1
            if processed % 1000 == 0:
                print(f"Inspected: {processed} schemas={len(schemas)} errors={errors}", flush=True)
    with (REPORTS / "schema_report.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["schema_id", "file_count", "columns", "example_file"])
        print(f"Schema variants: {len(schemas)}")
        for columns, (schema_id, count, example) in schemas.items():
            writer.writerow([schema_id, count, json.dumps(columns, ensure_ascii=False), example])
            print(f"\nSchema {schema_id}\nFiles: {count}\nColumns: {list(columns)}")
    print(f"\nExcluded by validation: {excluded}\nSchema errors: {errors}")
    return int(errors > 0)


if __name__ == "__main__":
    raise SystemExit(main())
