# Vietnamese Air Quality Big Data

A big-data pipeline for air quality in Vietnam. It combines two kinds of data:

- **History:** the full OpenAQ historical archive on AWS S3, tens of millions of small compressed CSV files, downloaded once and processed in batch.
- **Live:** current readings from three APIs (OpenAQ, WAQI and OpenWeather), streamed through Kafka every 10–60 minutes.

Both end up in a replicated HDFS cluster, are cleaned into one Silver table, and are served to a dashboard through PostgreSQL. The live map reads the newest readings and the timeline reads history.

```
HISTORY (batch)
OpenAQ S3 archive ─► data/raw (Bronze) ─► validate ─► compact ─► Near-Silver Parquet ─► HDFS
                                                                                         │
LIVE (streaming)                                                                         │
OpenAQ / WAQI / OpenWeather APIs                                                         │
  └─► producers ─► Kafka (3 brokers) ─► Spark consumer ─┬─► HDFS stream archive ─────────┤
                                                        └─► PostgreSQL latest_readings   │
                                                                   │                     ▼
                                                                   │      Silver (history + stream, cleaned)
                                                                   │                     │
                                                                   │      Gold (to be built) ─► PostgreSQL
                                                                   ▼                     ▼
                                                              dashboard: live map + timeline slider
```

## Current progress

| Part | Status |
|---|---|
| Historical ingestion: S3 manifest, download (with parallel Kaggle sessions), validation, schema inspection | Done |
| Historical preprocessing: compaction, Near-Silver Parquet | Done |
| Uploading raw files to HDFS (`stage_to_hdfs.py`) | Done |
| Station catalogs and producers for OpenAQ, WAQI and OpenWeather | Done |
| Producers count a reading as sent only after Kafka confirms it | Written; logic tested locally |
| Simulated HDFS cluster in Docker: 1 NameNode and 4 DataNodes, 3 copies of every block | Written; not yet run |
| Kafka cluster in Docker: 3 brokers, 3 copies of every message | Written; not yet run |
| Spark streaming consumer: stream archive in HDFS and `latest_readings` in PostgreSQL | Written; parsing and the PostgreSQL upsert tested locally |
| PostgreSQL serving database | Written; schema and upsert tested locally |
| Silver job combining history and stream | Written; cleaning and merge logic tested locally, not yet run on real data |
| Gold daily table for the timeline, dashboard | To be built ([see below](#to-be-built)) |

"Tested locally" means the logic ran against PySpark 3.5.9 and PostgreSQL 17.6 with sample data. The Docker stacks have not yet been started together end to end.

## Repository layout

```
docker-compose.yml           Kafka: 3 brokers, topic creation, Kafka UI
docker-compose.hdfs.yml      HDFS: NameNode, 4 DataNodes and an hdfs-client container
docker-compose.serving.yml   PostgreSQL for the dashboard
docker-compose.spark.yml     Spark: the streaming consumer and one-off batch jobs
docker/spark/Dockerfile      Spark image with python-dotenv and psycopg
docker/postgres/init.sql     dashboard tables
src/common/                  settings (config.py) and the shared message format (schema.py)
src/ingestion/<source>/      fetch_dimensions.py (station catalog) and producer.py (API to Kafka)
src/streaming/consumer.py    Kafka to the HDFS stream archive and PostgreSQL latest_readings
src/batch/                   historical archive pipeline, HDFS staging and the Silver job
prepare_shard.ipynb          splits the archive manifest into 30 shards for Kaggle
kaggle_crawl.ipynb           downloads one shard in a Kaggle session
```

## Setup

- Docker Desktop, with at least 8 GB of memory under Settings → Resources (all containers together use about 6 GB).
- Python packages: `pip install -r requirements.txt`.
- For the historical preprocessing: WSL2 (Ubuntu) with Spark, to run `spark-submit`.
- A `.env` file in the project root:

| Variable | Used by |
|---|---|
| `OPENAQ_API_KEY`, `WAQI_TOKEN`, `OPENWEATHER_API_KEY` | station catalogs and producers |
| `POSTGRES_PASSWORD` | the PostgreSQL container and the streaming consumer |
| `KAFKA_BOOTSTRAP_SERVERS` (optional) | defaults to all 3 brokers, `localhost:9092,localhost:9094,localhost:9096`; leave it unset unless that changes |
| `POSTGRES_HOST` (optional) | defaults to `localhost` |

All commands below run from the project root.

## Running

### 1. Historical load (once)

1. `python -m src.batch.build_manifest`: writes `data/metadata/archive_manifest.csv`.
2. `python -m src.batch.download_data`: downloads every entry to `data/raw/locationid=.../year=.../month=.../`. Failures go to `data/reports/download_failed.txt`.
   - To download faster, the crawl can run in parallel Kaggle sessions. Upload the Python files as a Kaggle dataset and add it as input to both notebooks. `prepare_shard.ipynb` splits `archive_manifest.csv` into 30 shards. `kaggle_crawl.ipynb` downloads one shard per session; set `shard_id` to a value from 1 to 30 in each session.
3. `python -m src.batch.validate_raw`: writes `data/reports/raw_validation.csv`.
4. `python -m src.batch.inspect_schema`: writes `data/reports/schema_report.csv` and `schema_errors.csv`.
5. `python src/batch/compact_validated_raw.py`: combines the small files into larger gzip chunks under `~/openaq_near_silver_staging/`.
6. Build Near-Silver with Spark in WSL:
   ```
   mkdir -p ~/spark_tmp
   spark-submit --master local[4] --driver-memory 5g --conf "spark.local.dir=$HOME/spark_tmp" src/batch/build_near_silver.py
   ```
   The output is `data/near_silver/`, Snappy Parquet partitioned by `year/month`.

### 2. Infrastructure (Docker), in this order

1. **Kafka:** `docker compose up -d`. The first time after switching from the old single broker, add `--remove-orphans`.
   - Check: `docker logs kafka-init` shows `ReplicationFactor: 3` for each partition of `air-quality`. Topics can be browsed at http://localhost:8080.
2. **HDFS:** `docker compose -f docker-compose.hdfs.yml up -d`.
   - Check: `docker compose -f docker-compose.hdfs.yml run --rm hdfs-client hdfs dfsadmin -report` shows `Live datanodes (4)`. The web UI is at http://localhost:9870.
3. **PostgreSQL:** `docker compose -f docker-compose.serving.yml up -d`. Dashboard tools on this computer connect to `localhost:5432`, with database and user `airquality`.

### 3. Live pipeline

1. Build the station catalogs, and refresh them daily:
   ```
   python -m src.ingestion.openaq.fetch_dimensions
   python -m src.ingestion.waqi.fetch_dimensions
   python -m src.ingestion.openweather.fetch_dimensions
   ```
   These write `data/dim/*.parquet`, the stations each producer polls. OpenAQ also writes `openaq_locations.parquet`, every fixed Vietnam location including inactive ones, which Silver uses to pick Vietnam out of the global archive.
2. Start the producers, one terminal each:
   ```
   python -m src.ingestion.openaq.producer
   python -m src.ingestion.waqi.producer
   python -m src.ingestion.openweather.producer
   ```
   Add `--dry-run` to print one cycle of messages without sending them. Each cycle logs `cycle done, sent N new readings`.
3. Start the streaming consumer:
   ```
   docker compose -f docker-compose.spark.yml up -d --build
   docker logs -f spark-consumer
   ```
   The Spark UI is at http://localhost:4040. To check the dashboard table:
   ```
   docker exec postgres psql -U airquality -c "SELECT source, count(*), max(event_time) FROM latest_readings GROUP BY source"
   ```

### 4. Silver

1. Upload Near-Silver to HDFS once:
   ```
   docker compose -f docker-compose.hdfs.yml run --rm hdfs-client hdfs dfs -put data/near_silver /openaq/near_silver
   ```
   The simulated HDFS keeps 3 copies of every block on the same disk, so a large Near-Silver takes three times its size. In that case skip the upload and add `--near-silver /app/data/near_silver` to the commands below, to read it from the project folder instead.
2. First run, all months:
   ```
   docker compose -f docker-compose.spark.yml run --rm spark-job /opt/spark/bin/spark-submit --driver-memory 4g src/batch/build_silver.py
   ```
   Check the printed report: rows kept and dropped for each reason, and the parameter/unit pairs dropped most often.
3. Daily afterwards: add `--since YYYY-MM` (for example last month) to rebuild only the recent months.

### Stopping

Stop in reverse order:

1. `docker compose -f docker-compose.spark.yml down`
2. Ctrl+C in each producer terminal
3. `docker compose -f docker-compose.serving.yml down`
4. `docker compose -f docker-compose.hdfs.yml down`
5. `docker compose down`

`down` keeps all data in Docker volumes. `down -v` deletes that stack's data permanently.

## Components

### Data layers

| Layer | Where | What |
|---|---|---|
| Bronze | `data/raw/` (and optionally HDFS `/openaq/bronze/`) | Validated raw archive files, unchanged. Kept locally because uploading tens of millions of tiny files to HDFS is inefficient. |
| Near-Silver | `data/near_silver/`, HDFS `/openaq/near_silver/` | Historical data compacted and technically normalized: schema variants unified, typed numeric and timestamp columns added, Parquet partitioned by `year/month`. No domain cleaning. |
| Stream archive | HDFS `/openaq/stream/readings/date=YYYY-MM-DD/` | Every live reading, duplicates removed. The permanent copy of the stream, since Kafka deletes messages after 7 days. |
| Silver | HDFS `/openaq/silver/readings/year=YYYY/month=M/` | History and stream combined and cleaned, Vietnam only. |
| Gold | to be built | Aggregates for analytics and the dashboard, such as daily averages per station. |
| Serving | PostgreSQL `latest_readings` | The newest reading per station and parameter, for the live map. |

### Historical pipeline scripts (`src/batch/`)

- `build_manifest.py`: scans every page under `records/csv.gz/`, keeps nonempty objects matching the partition structure and writes `data/metadata/archive_manifest.csv`. It accepts month partitions written as `1`–`9` or `01`–`09`, as well as `10`–`12`. It retries the same page up to eight times, keeping its continuation token. Only a complete scan replaces the manifest; `.part` is not a usable manifest. Restarting the process starts a new scan.
- `download_data.py`: streams the manifest without listing S3, using up to 32 threads. Downloads every entry to `data/raw/locationid=.../year=.../month=.../`, keeping the original month spelling. Existing files of the expected size are skipped. Each file gets five attempts; a checked `.part` file replaces an absent or size-mismatched local file. Exhausted failures go to `data/reports/download_failed.txt` and the remaining files continue. Rerun to resume. The failure report describes the latest run.
- `validate_raw.py`: checks every manifest entry, reads the gzip to the end (including CRC and trailer checks), then checks that a UTF-8 CSV header exists with no empty column names. Writes `data/reports/raw_validation.csv` atomically. Blank integrity flags mean not checked. It does not validate data rows or modify raw files.
- `inspect_schema.py`: reads headers only from entries that passed all validation checks. Groups exact column names and order, writing `data/reports/schema_report.csv` and `schema_errors.csv`. Columns are stored as a JSON array. IDs follow first occurrence. Invalid raw files are excluded and counted.
- `utils.py`: shared project paths, unsigned S3 configuration and safe key-to-path checks.
- `stage_to_hdfs.py`: reads `data/reports/raw_validation.csv` and uploads only valid files to HDFS under `/openaq/bronze/`, keeping paths such as `locationid=.../year=.../month=.../` and provider/country partitions. Files already in HDFS with the expected size are skipped, so rerunning resumes safely. Conflicting sizes and failed uploads are written to `data/reports/hdfs_stage_failed.csv`. Supports `--limit N` and `--hdfs-root PATH` for test uploads. When the recorded `local_path` does not exist on the current machine (for example inside the Docker HDFS client), the file is read from this checkout's `data/raw/`. With the Docker HDFS, run it through the client: `docker compose -f docker-compose.hdfs.yml run --rm hdfs-client python src/batch/stage_to_hdfs.py --limit 1000`. Reading millions of small files through the Windows folder mount is slow.
- `compact_validated_raw.py`: selects only fully valid local files from `raw_validation.csv` and compacts millions of tiny gzip CSVs into larger concatenated gzip chunks under `~/openaq_near_silver_staging/`. The common schemas are copied through quickly; the rare duplicated `lat/lat/lon/lon` schema is rewritten to the canonical 9-column schema. Failures are written to `data/reports/near_silver_compact_failed.csv`.
- `build_near_silver.py`: reads the compacted chunks with Spark, filters repeated headers, keeps the original string columns, adds `value_num`, `lat_num`, `lon_num`, `datetime_ts`, `year` and `month`, then writes Snappy Parquet to `data/near_silver/` partitioned by `year/month`. It performs no domain cleaning.
- `build_silver.py`: see [Silver](#silver-srcbatchbuild_silverpy).

Downloads, validation and schema inspection return a nonzero exit code when they detect failures. Fix the reported problems and rerun validation before inspecting schemas. A corrupt file with the correct size is skipped by the download; move it aside manually before downloading again. Validation never deletes it.

Source: [OpenAQ archive documentation](https://docs.openaq.org/aws/about).

### Producers (`src/ingestion/`)

Each source has a `fetch_dimensions.py`, which builds its station catalog in `data/dim/`, and a `producer.py`, which polls the API and sends readings to the Kafka topic `air-quality`.

- OpenAQ polls every 10 minutes, WAQI every 15 and OpenWeather every 60. Stations themselves report about hourly.
- Every reading is translated by `schema.make_message` into one shared format, one message per measurement:
  ```
  {"source": "openaq", "station_id": "openaq-4946812", "lat": 21.0, "lon": 105.8,
   "parameter": "pm25", "value": 23.4, "unit": "µg/m³",
   "datetime": "2026-10-04T10:00:00Z", "ingested_at": "2026-10-04T10:05:12Z"}
  ```
  Gas concentrations in ppm/ppb are converted to µg/m³. WAQI publishes pollutants as US AQI sub-indices rather than concentrations, so they are kept as separate parameters (`aqi_pm25`, `aqi_no2`, ...) with unit `us_aqi`.
- Messages are keyed by `station_id`, so each station's readings stay in order within one partition.
- Readings older than 1 day, and readings already sent, are skipped. A reading is only recorded as sent after Kafka confirms the write (`acks="all"`). Failed sends are logged as `N readings failed to send, retrying next cycle` and sent again in the next cycle, so a reading can occasionally arrive twice. Consumers deduplicate.

### Kafka (`docker-compose.yml`)

- 3 brokers in KRaft mode (`kafka1`, `kafka2`, `kafka3`), reachable from this computer at `localhost:9092`, `localhost:9094` and `localhost:9096`, and from other containers at `kafka1:29092`, `kafka2:29092` and `kafka3:29092`.
- The topic `air-quality` has 3 partitions, each copied to all 3 brokers. A write is confirmed only once at least 2 brokers have it (`min.insync.replicas=2`), so the stream keeps working and loses nothing when one broker is down. With 2 brokers down, writes fail until one returns, and the producers retry them.
- Messages are kept for 7 days (Kafka's default).

### Streaming consumer (`src/streaming/consumer.py`)

A Spark Structured Streaming job in the `spark-consumer` container, which joins the Kafka, HDFS and PostgreSQL networks. It writes two outputs:

- **Stream archive:** every 5 minutes, new readings are deduplicated on `(station_id, parameter, event_time)` and appended to `/openaq/stream/readings/` as Parquet. The checkpoint at `/openaq/checkpoints/stream_readings` records how far the archive has read, so a restart continues without gaps or duplicates. Readings more than 25 hours older than the newest one seen, or stamped more than 1 hour in the future, are dropped. If the consumer is stopped for longer than Kafka's 7-day retention, the archive query fails on start instead of silently skipping the lost messages. The archive writes small files (at most one per date every 5 minutes); Silver rewrites them into one file per month.
- **`latest_readings`:** every minute, the newest reading per station and parameter is upserted into PostgreSQL. A row is only replaced by a reading with a later `event_time`, so replays and late batches never overwrite newer data. Its checkpoint is `/openaq/checkpoints/latest_readings`.

Spark runs with 1 shuffle partition, which suits a few thousand readings an hour. The archive checkpoint fixes this number at the first run; changing it later means deleting `/openaq/checkpoints/stream_readings`.

### Silver (`src/batch/build_silver.py`)

Combines Near-Silver and the stream archive into one cleaned table at `/openaq/silver/readings/year=YYYY/month=M/`, one Parquet file per month, sorted by station and time.

- Keeps Vietnam only: archive rows from the fixed OpenAQ locations in `data/dim/openaq_locations.parquet`.
- Keeps the parameters in `schema.UNITS`, renames them with `NAME_MAP` and converts ppm/ppb to µg/m³, exactly like the producers.
- Drops rows with a missing value, a unit that cannot be converted, a future timestamp, or a value outside `VALID_RANGE`. These are first-pass physical limits, such as PM2.5 between 0 and 2000 µg/m³, to be tightened after EDA.
- Keeps one row per station, parameter and timestamp. OpenAQ archive readings replace stream readings with the same timestamp, since the archive is OpenAQ's final record. Co-located sensors measuring the same parameter are averaged.
- Prints how many rows were kept and dropped for each reason, and the parameter/unit pairs dropped most often, so a missing unit spelling is easy to spot.
- `--since YYYY-MM` rebuilds only the months from then on; other months are left unchanged.

Columns: `source, station_id, lat, lon, parameter, value, unit, event_time, origin`, plus the `year` and `month` partitions. Times are UTC. `origin` is `archive` or `stream`. OpenAQ `station_id`s are `openaq-<location_id>`, the same in history and stream.

### Serving database (`docker-compose.serving.yml`)

PostgreSQL 17.6, reachable from this computer only, at `localhost:5432` (database and user `airquality`, password from `.env`). `docker/postgres/init.sql` creates the tables when the data volume is first created; to change an existing database, run the statements by hand or recreate the volume.

`latest_readings`: one row per `(station_id, parameter)`, with `source, lat, lon, value, unit, event_time, ingested_at, updated_at`. Times are UTC.

### Simulated HDFS cluster (`docker-compose.hdfs.yml`)

HDFS runs as several machines on one computer: a NameNode and 4 DataNodes, each in its own container with its own disk (a named volume). Files are written with `dfs.replication=3`, so every block is stored on 3 different DataNodes and survives the loss of a machine. This replaces a single-node HDFS in WSL.

The `hdfs-client` container has the project mounted at `/project`, so it can run `hdfs` commands and the project's scripts inside the cluster network. Programs outside Docker (Windows or WSL) cannot reach the DataNodes, so anything that reads or writes HDFS data must run in a container on the cluster network, as the Spark jobs do.

To simulate more machines, copy a `datanodeN` service and its volume in `docker-compose.hdfs.yml`. Replication cannot exceed the number of live DataNodes.

HDFS paths:

| Path | Written by |
|---|---|
| `/openaq/bronze/` | `stage_to_hdfs.py` |
| `/openaq/near_silver/` | manual upload (see [Silver](#4-silver)) |
| `/openaq/stream/readings/` | streaming consumer |
| `/openaq/silver/readings/` | `build_silver.py` |
| `/openaq/checkpoints/` | streaming consumer |

### Fault-tolerance drills

- **Lose a Kafka broker:** `docker stop kafka2`. Producers keep sending without errors. Bring it back with `docker start kafka2`.
- **Lose an HDFS machine:** `docker stop datanode2`. After about a minute the NameNode marks it dead and copies its blocks to the other DataNodes. Watch with `docker compose -f docker-compose.hdfs.yml run --rm hdfs-client hdfs fsck /openaq -files -blocks -locations` until `Under-replicated blocks` returns to 0. Bring it back with `docker start datanode2`; HDFS then removes the extra copies.
- **Lose the consumer:** `docker restart spark-consumer`. The archive continues from its checkpoint without gaps or duplicates.

## To be built

1. **Gold daily table:** daily averages per station and parameter computed from Silver and loaded into PostgreSQL, for the timeline slider. Silver is hourly per station, which is too heavy to load for a whole year, and dashboard tools cannot read Parquet in HDFS directly.
2. **Dashboard:** live map from `latest_readings` and a timeline from the Gold table. The tool is not chosen yet; Grafana, Superset and Streamlit can all read PostgreSQL.
3. **Today's hourly curve:** Silver and Gold are rebuilt daily, so the timeline ends at the last run and `latest_readings` holds only one value per station. Either run them more often with `--since`, or have the consumer also write recent readings to PostgreSQL.
4. **Automatic restarts for the producers,** for example by running them in containers with a restart policy. WAQI has no history API, so hours missed while its producer is down are lost for good.
5. **Scheduling** for the daily Silver run and the station catalog refresh.
6. **OpenWeather backfill:** OpenWeather's `/air_pollution/history` endpoint (free, hourly, back to November 2020) can fill gaps when its producer was down. OpenAQ gaps are filled by the historical archive.
7. **EDA and tighter cleaning rules:** tighten `VALID_RANGE` and add outlier rules once Silver has run on real data.

## Notes

- **WAQI terms of use:** attribution to the World Air Quality Index Project and the originating EPA is mandatory, the data cannot be used in paid applications, and it cannot be redistributed as cached or archived data. Keep the WAQI archive internal and show the attribution on the dashboard.
- **WAQI values are US AQI sub-indices,** not concentrations, so they cannot be averaged with OpenAQ or OpenWeather values without conversion.
- **Disk use:** the simulated HDFS stores 3 copies of everything on one physical disk.
- **Do not reformat the NameNode** once HDFS contains data. The NameNode container formats itself only on first start.
