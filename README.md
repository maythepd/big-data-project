# OpenAQ historical raw archive

The OpenAQ historical archive contains a very large number of small compressed CSV files and a few schema variants. Processing these files directly with Spark or uploading all of them to HDFS is inefficient because of the small-file problem.

Therefore, the historical pipeline is split into technical preprocessing and data cleaning layers:

Raw archive
-> validation & schema inspection
-> Bronze
-> local compaction & technical normalization
-> Near-Silver Parquet
-> HDFS
-> Spark EDA & cleaning
-> Silver
-> analytics / Gold

## Raw / Bronze

The original OpenAQ historical archive is downloaded locally and validated. The validated raw files form the logical Bronze layer because they preserve the original measurements with minimal modification.

The full Bronze archive is currently kept locally instead of being uploaded file-by-file to HDFS, since the archive contains tens of millions of very small compressed CSV files.

### Near-Silver

Near-Silver is an intermediate technical layer between Bronze and Silver.

Its purpose is to make the historical data efficient for distributed processing by:
- keeping only validated raw files,
- compacting millions of small `.csv.gz` files into larger chunks,
- normalizing known schema variants,
- adding typed numeric and datetime columns,
- storing the result as Snappy-compressed Parquet partitioned by `year/month`.

No domain-level cleaning is performed here.

### Silver

Silver is produced after Spark EDA and cleaning. This layer contains the cleaned analytical dataset after handling data-quality issues such as missing values, duplicates, invalid measurements, and outliers.

### Gold

Gold contains aggregated or analysis-ready outputs used for analytics and visualization.

## Current progress:

1. Historical ingestion: AWS S3 -> manifest -> full local raw archive -> file validation -> schema inspection
2. Historical technical preprocessing: validated raw -> compaction -> schema normalization -> Spark -> Near-Silver Parquet
3. Distributed storage: Near-Silver -> HDFS
4. Spark EDA, cleaning -> Silver; analytics and visualization (ongoing)
5. Streaming pipeline: OpenAQ live/API -> Kafka -> Spark Structured Streaming -> serving / visualization
## Update

1. Crawl:
- `build_manifest.py` now accepts both month partitions `1`–`9` and `01`–`09`, as well as `10`–`12`. Downloads preserve the original month spelling instead of renaming directories.
- `download_data.py` now allows 32 threads
- To further increase the speed of crawling, we utilize Kaggle to run parallel sessions:
  + The Python files for crawling are uploaded as a dataset, and added as an input dataset to the 2 notebooks below
  + First notebook: `prepare_shard.ipynb` to divide the `data/metadata/archive_manifest.csv` into 30 parts (shards)
  + Second notebook: `kaggle_crawl.ipynb` to download data from corresponding shard (for each session, shard_id must be changed to a value in 1-30)
2. Near-Silver preprocessing:
- `compact_validated_raw.py` compacts validated raw `.csv.gz` files into larger gzip chunks for faster Spark reads. It also normalizes the rare duplicated `lat/lon` schema to the canonical 9-column layout.
- `build_near_silver.py` reads the compacted chunks with Spark, removes repeated source headers, adds typed numeric/timestamp columns, and writes Snappy Parquet partitioned by `year/month`.
- Near-Silver is only technically normalized data; domain cleaning such as missing-value handling, outlier rules and business/domain deduplication is deferred to the Silver step.
3. Upload HDFS:
- Require Hadoop 3.4.3, WSL2, Ubuntu, Java 11

## Setup and run

- Run in order, from the project root (scripts live in `src/batch/`): 
1. Install (if necessary) from requirements.txt
2. `python -m src.batch.build_manifest`: write `data/metadata/archive_manifest.csv`
3. `python -m src.batch.download_data`: download every entry to `data/raw/locationid=.../year=.../month=.../`, failures go to `data/reports/download_failed.txt`
4. `python -m src.batch.validate_raw`: write `data/reports/raw_validation.csv`
5. `python -m src.batch.inspect_schema`: write `data/reports/schema_report.csv` and `schema_errors.csv`
6. Start HDFS in WSL:
   - `sudo service ssh start`
   - `start-dfs.sh`
   - verify with `jps` and `hdfs dfsadmin -report`
7. Build Near-Silver with Spark:
   - `mkdir -p ~/spark_tmp`
   - `spark-submit --master local[4] --driver-memory 5g --conf "spark.local.dir=$HOME/spark_tmp" src/batch/build_near_silver.py`
   - output: `data/near_silver/` as Snappy Parquet partitioned by `year/month`
8. Start HDFS in WSL:
   - `sudo service ssh start`
   - `start-dfs.sh`
   - verify with `jps` and `hdfs dfsadmin -report`
9. `python src/batch/stage_to_hdfs.py`: upload all validated raw files to `/openaq/bronze/` in HDFS while preserving the OpenAQ partition structure

## Scripts and outputs

- `build_manifest.py`: scans every page under `records/csv.gz/`, filters nonempty objects matching the partition structure and writes `data/metadata/archive_manifest.csv`. It retries the same page up to eight times, retaining its continuation token. Only a complete scan replaces the manifest; `.part` is not a usable manifest. Restarting the process starts a new scan.
- `download_data.py`: streams the manifest without listing S3. Downloads every entry to `data/raw/locationid=.../year=.../month=.../`. Existing files of the expected size are skipped. Each file gets five attempts; a checked `.part` file replaces an absent or size-mismatched local file. Exhausted failures go to `data/reports/download_failed.txt`; remaining files continue. Rerun to resume. The failure report describes the latest run.
- `validate_raw.py`: checks every manifest entry, reads gzip to EOF (including CRC/trailer checks), then checks that a UTF-8 CSV header exists and has no empty column names. Writes `data/reports/raw_validation.csv` atomically. Blank integrity flags mean not checked. It does not validate data-row semantics or modify raw files.
- `inspect_schema.py`: reads headers only from entries that passed all validation checks. Groups exact column names and order, writing `data/reports/schema_report.csv` and `schema_errors.csv`. Columns are stored as a JSON array. IDs follow first occurrence. Invalid raw files are excluded and counted.
- `utils.py`: shares project paths, unsigned S3 configuration and safe key-to-path checks across the four scripts.
- `stage_to_hdfs.py`: reads `data/reports/raw_validation.csv` and uploads only valid files to HDFS under `/openaq/bronze/`, preserving paths such as `locationid=.../year=.../month=.../` and provider/country partitions. Existing HDFS files with the expected size are skipped, so rerunning resumes safely. Files with conflicting sizes or failed uploads are written to `data/reports/hdfs_stage_failed.csv`. The script supports `--limit N` and `--hdfs-root PATH` for test uploads before a full run.
- `compact_validated_raw.py`: reads `data/reports/raw_validation.csv`, selects only fully valid local files, and compacts millions of tiny gzip CSVs into larger concatenated gzip chunks under `~/openaq_near_silver_staging/`. The common schemas are copied through quickly; the duplicated `lat/lat/lon/lon` schema is rewritten to the canonical 9-column schema. Failures are written to `data/reports/near_silver_compact_failed.csv`.
- `build_near_silver.py`: reads the compacted gzip chunks with Spark, filters repeated headers, preserves the original string columns, adds `value_num`, `lat_num`, `lon_num`, `datetime_ts`, `year`, and `month`, then writes Snappy Parquet to `data/near_silver/` partitioned by `year/month`. It intentionally performs no domain cleaning.

Downloads/validation/schema inspection return a nonzero exit code for detected failures. Fix reported problems and rerun validation before inspecting schemas. A corrupt file with the correct size will be skipped by download; manually move it aside before downloading again. Validation never deletes it.

Before running `stage_to_hdfs.py`, HDFS must be started and at least one DataNode must be live. Do not reformat the NameNode after HDFS contains data.

Source: [OpenAQ archive documentation](https://docs.openaq.org/aws/about).

# Vietnamese Air Quality Big data
## Phase 1: Data stream
- First, regulate which area to fetch data from, i.e giving locations and sensors that we will fetch data from:
```
python -m src.ingestion.openaq.fetch_dimensions      
python -m src.ingestion.waqi.fetch_dimensions         
python -m src.ingestion.openweather.fetch_dimensions   
```
this will return a table inside a parquet file signaling where to call API (most are api/{locationid})
- Second, forming the data stream:
```
python -m src.ingestion.openaq.producer
python -m src.ingestion.waqi.producer
python -m src.ingestion.openweather.producer
```
each make an API request using a key, then format them into 1 uniform format to process
