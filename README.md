# OpenAQ historical raw archive

Current progress:
1. Download data from OpenAQ: AWS S3 -> manifest -> full local raw archive -> file validation -> schema inspection
2. EDA, cleaning, Spark, Kafka, HDFS, analytics and visualization (ongoing)

## Update

1. Crawl:
- `build_manifest.py` now accepts both month partitions `1`–`9` and `01`–`09`, as well as `10`–`12`. Downloads preserve the original month spelling instead of renaming directories.
- `download_data.py` now allows 32 threads
- To further increase the speed of crawling, we utilize Kaggle to run parallel sessions:
  + The Python files for crawling are uploaded as a dataset, and added as an input dataset to the 2 notebooks below
  + First notebook: `prepare_shard.ipynb` to divide the `data/metadata/archive_manifest.csv` into 30 parts (shards)
  + Second notebook: `kaggle_crawl.ipynb` to download data from corresponding shard (for each session, shard_id must be changed to a value in 1-30)

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
