# OpenAQ historical raw archive

Current progress:
1. Download data from OpenAQ: AWS S3 -> manifest -> full local raw archive -> file validation -> schema inspection
2. EDA, cleaning, Spark, Kafka, HDFS, analytics and visualization (ongoing)

## Setup and run

- Run in order: 
1. Install (if necessary) from requirements.txt
2. build_manifest.py: write `data/metadata/archive_manifest.csv`
3. download_data.py: download every entry to `data/raw/locationid=.../year=.../month=.../`, failures go to `data/reports/download_failed.txt`
4. validate_raw.py: write `data/reports/raw_validation.csv`
5. inspect_schema.py: write `data/reports/schema_report.csv` and `schema_errors.csv`

## Scripts and outputs

- `build_manifest.py`: scans every page under `records/csv.gz/`, filters nonempty objects matching the partition structure and writes `data/metadata/archive_manifest.csv`. It retries the same page up to eight times, retaining its continuation token. Only a complete scan replaces the manifest; `.part` is not a usable manifest. Restarting the process starts a new scan.
- `download_data.py`: streams the manifest without listing S3. Downloads every entry to `data/raw/locationid=.../year=.../month=.../`. Existing files of the expected size are skipped. Each file gets five attempts; a checked `.part` file replaces an absent or size-mismatched local file. Exhausted failures go to `data/reports/download_failed.txt`; remaining files continue. Rerun to resume. The failure report describes the latest run.
- `validate_raw.py`: checks every manifest entry, reads gzip to EOF (including CRC/trailer checks), then checks that a UTF-8 CSV header exists and has no empty column names. Writes `data/reports/raw_validation.csv` atomically. Blank integrity flags mean not checked. It does not validate data-row semantics or modify raw files.
- `inspect_schema.py`: reads headers only from entries that passed all validation checks. Groups exact column names and order, writing `data/reports/schema_report.csv` and `schema_errors.csv`. Columns are stored as a JSON array. IDs follow first occurrence. Invalid raw files are excluded and counted.
- `utils.py`: shares project paths, unsigned S3 configuration and safe key-to-path checks across the four scripts.

Downloads/validation/schema inspection return a nonzero exit code for detected failures. Fix reported problems and rerun validation before inspecting schemas. A corrupt file with the correct size will be skipped by download; manually move it aside before downloading again. Validation never deletes it.

Source: [OpenAQ archive documentation](https://docs.openaq.org/aws/about).
