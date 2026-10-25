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

## Simulated HDFS cluster with replication (Docker)

`docker-compose.hdfs.yml` runs HDFS as several machines on one computer: a NameNode and 4 DataNodes, each in its own container with its own disk (named volume). Files are written with `dfs.replication=3`, so every block is stored on 3 different DataNodes and survives the loss of a machine. It replaces the single-node WSL HDFS from steps 6 and 8; only Docker Desktop is needed.

Run commands from the project root. The `hdfs-client` container has the project mounted at `/project`, so it can run `hdfs` commands and the project's scripts inside the cluster network. Clients outside Docker (Windows or WSL) cannot reach the DataNodes.

1. Start the cluster: `docker compose -f docker-compose.hdfs.yml up -d`
   - the web UI at http://localhost:9870 lists the live DataNodes
   - `docker compose -f docker-compose.hdfs.yml run --rm hdfs-client hdfs dfsadmin -report` should show `Live datanodes (4)`
2. Upload data:
   - raw files: `docker compose -f docker-compose.hdfs.yml run --rm hdfs-client python src/batch/stage_to_hdfs.py --limit 1000`
   - Near-Silver: `docker compose -f docker-compose.hdfs.yml run --rm hdfs-client hdfs dfs -put data/near_silver /openaq/near_silver`
   - reading millions of small files through the Windows folder mount is slow, so use `--limit` or Near-Silver for experiments
3. See where each block's 3 replicas are stored: `docker compose -f docker-compose.hdfs.yml run --rm hdfs-client hdfs fsck /openaq -files -blocks -locations`
4. Simulate a machine failure: `docker stop datanode2`
   - after about one minute the NameNode marks it dead and copies its blocks to the other DataNodes; run `fsck` again until `Under-replicated blocks` returns to 0
   - bring it back with `docker start datanode2`; HDFS removes the extra replicas
5. Stop the cluster with `docker compose -f docker-compose.hdfs.yml down`. Data is kept in the volumes; `down -v` deletes it.

To simulate more machines, copy a `datanodeN` service and its volume in `docker-compose.hdfs.yml`. Replication cannot exceed the number of live DataNodes.

## Scripts and outputs

- `build_manifest.py`: scans every page under `records/csv.gz/`, filters nonempty objects matching the partition structure and writes `data/metadata/archive_manifest.csv`. It retries the same page up to eight times, retaining its continuation token. Only a complete scan replaces the manifest; `.part` is not a usable manifest. Restarting the process starts a new scan.
- `download_data.py`: streams the manifest without listing S3. Downloads every entry to `data/raw/locationid=.../year=.../month=.../`. Existing files of the expected size are skipped. Each file gets five attempts; a checked `.part` file replaces an absent or size-mismatched local file. Exhausted failures go to `data/reports/download_failed.txt`; remaining files continue. Rerun to resume. The failure report describes the latest run.
- `validate_raw.py`: checks every manifest entry, reads gzip to EOF (including CRC/trailer checks), then checks that a UTF-8 CSV header exists and has no empty column names. Writes `data/reports/raw_validation.csv` atomically. Blank integrity flags mean not checked. It does not validate data-row semantics or modify raw files.
- `inspect_schema.py`: reads headers only from entries that passed all validation checks. Groups exact column names and order, writing `data/reports/schema_report.csv` and `schema_errors.csv`. Columns are stored as a JSON array. IDs follow first occurrence. Invalid raw files are excluded and counted.
- `utils.py`: shares project paths, unsigned S3 configuration and safe key-to-path checks across the four scripts.
- `stage_to_hdfs.py`: reads `data/reports/raw_validation.csv` and uploads only valid files to HDFS under `/openaq/bronze/`, preserving paths such as `locationid=.../year=.../month=.../` and provider/country partitions. Existing HDFS files with the expected size are skipped, so rerunning resumes safely. Files with conflicting sizes or failed uploads are written to `data/reports/hdfs_stage_failed.csv`. The script supports `--limit N` and `--hdfs-root PATH` for test uploads before a full run. When the recorded `local_path` does not exist on the current machine (for example inside the Docker HDFS client), the file is read from this checkout's `data/raw/`.
- `compact_validated_raw.py`: reads `data/reports/raw_validation.csv`, selects only fully valid local files, and compacts millions of tiny gzip CSVs into larger concatenated gzip chunks under `~/openaq_near_silver_staging/`. The common schemas are copied through quickly; the duplicated `lat/lat/lon/lon` schema is rewritten to the canonical 9-column schema. Failures are written to `data/reports/near_silver_compact_failed.csv`.
- `build_near_silver.py`: reads the compacted gzip chunks with Spark, filters repeated headers, preserves the original string columns, adds `value_num`, `lat_num`, `lon_num`, `datetime_ts`, `year`, and `month`, then writes Snappy Parquet to `data/near_silver/` partitioned by `year/month`. It intentionally performs no domain cleaning.

Downloads/validation/schema inspection return a nonzero exit code for detected failures. Fix reported problems and rerun validation before inspecting schemas. A corrupt file with the correct size will be skipped by download; manually move it aside before downloading again. Validation never deletes it.

Before running `stage_to_hdfs.py`, HDFS must be started and at least one DataNode must be live. Do not reformat the NameNode after HDFS contains data.

Source: [OpenAQ archive documentation](https://docs.openaq.org/aws/about).

# Vietnamese Air Quality Big data
## Phase 1: Data stream
- Start Kafka with `docker compose up -d`. It runs 3 brokers (`kafka1`, `kafka2`, `kafka3`, reachable from the laptop at `localhost:9092`, `localhost:9094` and `localhost:9096`) and creates the `air-quality` topic with 3 partitions, each copied to all 3 brokers. A write is acknowledged only after at least 2 brokers have it (`min.insync.replicas=2` with the producers' `acks="all"`), so the stream keeps working and loses nothing when one broker is down. Browse topics at http://localhost:8080.
  - Try it: `docker stop kafka2`, keep the producers running, then `docker start kafka2`. With 2 brokers down, writes fail until one returns.
  - Producers only remember a reading as sent after Kafka acknowledges it. Failed sends are logged and retried in the next cycle, so a reading can occasionally arrive twice; consumers should deduplicate on `(station_id, parameter, datetime)`.
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
- Third, the dashboard database: PostgreSQL in `docker-compose.serving.yml`. Add `POSTGRES_PASSWORD=<choose one>` to `.env`, then `docker compose -f docker-compose.serving.yml up -d`. Dashboard tools on this laptop connect to `localhost:5432`, database and user `airquality`. The tables are created by `docker/postgres/init.sql` when the data volume is first created.
- Fourth, consuming the stream with Spark Structured Streaming (`src/streaming/consumer.py`). Start Kafka, the HDFS cluster (`docker compose -f docker-compose.hdfs.yml up -d`) and Postgres first, then:
```
docker compose -f docker-compose.spark.yml up -d --build
docker logs -f spark-consumer
```
the consumer runs in a container that joins the Kafka, HDFS and Postgres networks, and writes two outputs:
  - archive: every reading, deduplicated on `(station_id, parameter, event_time)`, appended to HDFS at `/openaq/stream/readings/date=YYYY-MM-DD/` as Parquet. This is the permanent copy, since Kafka deletes messages after 7 days. The checkpoint at `/openaq/checkpoints/stream_readings` records how far the archive has read, so a restart continues without gaps or duplicates. Readings more than 25 hours older than the newest one seen, or stamped more than 1 hour in the future, are dropped.
  - `latest_readings`: the newest reading per station and parameter, upserted every minute into the Postgres table `latest_readings` (columns `station_id, parameter, source, lat, lon, value, unit, event_time, ingested_at, updated_at`, key `(station_id, parameter)`, times in UTC). A row is only replaced by a reading with a later `event_time`, so replays and late batches never overwrite newer data. Its checkpoint is `/openaq/checkpoints/latest_readings`. Check it with `docker exec postgres psql -U airquality -c "SELECT source, count(*), max(event_time) FROM latest_readings GROUP BY source"`.
  - the archive writes small files (one per date every 5 minutes at most); Silver rewrites them into one file per month. If the consumer is stopped for longer than Kafka's 7-day retention, the archive query fails on start instead of silently skipping the lost messages.
  - Spark UI: http://localhost:4040

## Phase 2: Silver (history + stream)
`src/batch/build_silver.py` combines the OpenAQ historical archive (Near-Silver) and the stream archive into one cleaned table at `/openaq/silver/readings/year=YYYY/month=M/` in HDFS, one Parquet file per month, sorted by station and time. A dashboard (for example a slider over the year) can load one month at a time.

What it does:
- keeps Vietnam only: archive rows from fixed OpenAQ locations in Vietnam, active or not, listed in `data/dim/openaq_locations.parquet` (written by `python -m src.ingestion.openaq.fetch_dimensions`)
- keeps the parameters in `schema.UNITS`, renames them with `NAME_MAP` and converts ppm/ppb to µg/m³, exactly like the producers
- drops rows with a missing value, an unconvertible unit, a future timestamp, or a value outside `VALID_RANGE` (first-pass physical limits such as PM2.5 0–2000 µg/m³, to tighten after EDA)
- keeps one row per station, parameter and timestamp: OpenAQ archive readings replace stream readings with the same timestamp, since the archive is OpenAQ's final record, and co-located sensors measuring the same parameter are averaged. The `origin` column says whether a row came from the `archive` or the `stream`
- prints how many rows were kept and dropped for each reason, and the parameter/unit pairs dropped most often, so a missing unit spelling is easy to spot

Columns: `source, station_id, lat, lon, parameter, value, unit, event_time, origin` (+ `year`, `month` partitions; times in UTC). OpenAQ `station_id`s are `openaq-<location_id>`, the same as in the stream.

Run, with Kafka and HDFS started:
1. Refresh the Vietnam location list: `python -m src.ingestion.openaq.fetch_dimensions`
2. Upload Near-Silver once: `docker compose -f docker-compose.hdfs.yml run --rm hdfs-client hdfs dfs -put data/near_silver /openaq/near_silver`. HDFS keeps 3 copies of every block on the same disk here, so for a large Near-Silver skip the upload and pass `--near-silver /app/data/near_silver` below instead
3. First run, all months: `docker compose -f docker-compose.spark.yml run --rm spark-job /opt/spark/bin/spark-submit --driver-memory 4g src/batch/build_silver.py`
4. Daily afterwards, only the recent months: add `--since YYYY-MM` (for example last month). Only the months written are replaced
