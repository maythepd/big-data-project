# Fault injection and new tools

The project was judged too simple. This plan makes it harder in a measurable way: we break the data
and the infrastructure on purpose, the way real feeds and clusters break, then measure how much of
it the pipeline catches, add rules and tools for what it misses, and measure again with the same
faults. Every new tool answers a problem the injection exposed.

All work runs on one machine. The data lake lives in `openaq_lake/` inside this repository (git-ignored),
and every Spark job runs in Docker, so the demo is the same commands on the same stacks.

## Setup

1. **Data.** The team's 2025 lake is on Hugging Face (`vdmhth123/openaq-lake-2025`). Download one
   month into `openaq_lake/` (about 0.6 GB; drop the `month=1` filters to get the whole year, 10.9 GB):
   ```
   python -c "from huggingface_hub import snapshot_download; snapshot_download('vdmhth123/openaq-lake-2025', repo_type='dataset', local_dir='openaq_lake', allow_patterns=['README.md','silver/dim_*/*','gold/dim_station_xref.parquet','near_silver_compact/year=2025/month=1/*','silver/measurements/year=2025/month=1/*','gold/station_hourly/year=2025/month=1/*'])"
   ```
   `LAKE_DIR` in `.env` points the code at another folder (for example `~/openaq_lake` in WSL,
   which is faster than `/mnt/c`).
2. **Spark.** No Java is needed on the host. Build the image once:
   `docker compose -f docker-compose.spark.yml build spark-lake`. The `spark-lake` service mounts
   this folder at `/app` and needs no other stack.
3. **Tests:** `docker compose -f docker-compose.spark.yml run --rm spark-lake python3 -m pytest tests`.

## How a batch chaos run works

`scripts/chaos_batch.sh RUN_ID YYYY-MM [options]` runs four steps in the container:

1. `src/chaos/inject_batch.py` copies one month of Near-Silver to `openaq_lake/chaos/<run>/near_silver_compact/`
   and injects the fault catalogue (`src/chaos/faults.py`) into the copy. The original lake is never
   changed.
2. `src/silver/build_silver.py` builds Silver on the copy with the normal rules (R01–R13).
3. `src/silver/flag_temporal_outliers.py` adds the R14 spike and flatline flags.
4. `src/chaos/evaluate.py` compares what was injected with what Silver did and writes
   `openaq_lake/reports/<run>_eval/summary.csv`.

Run a control first (`scripts/chaos_batch.sh control 2025-01 --scale 0`): the same month, the same
steps, no faults. Each chaos run is compared with it, so differences come from the faults, not from
the build. `--bbox lat_min,lon_min,lat_max,lon_max` limits a run to a region for quick iterations;
the control must then use the same box (`CONTROL=<run>` names it).

**Ground truth.** Selection is deterministic (a hash of the seed and the row or sensor), so the same
seed injects the same faults before and after a rule change. Each changed or added row carries its
label in `source_block` as `chaos-<fault>-<original block>`. Silver passes `source_block` through
unchanged and no rule reads it, except R13 dedup as its last tie-breaker, where `chaos-` sorts after
`blk_`, so an original row always wins over its injected copy.

**Metrics per fault.**
- `caught_share`: rows quarantined, `invalid` or `suspect`, over rows injected. Dedup removals count
  only for the `duplicate` fault: a conflicting copy dropped by block order was resolved silently.
- `expected_share`: rows caught by the rule meant for that fault.
- Collateral: clean rows whose status differs from the control run (false positives caused by the
  faults, for example a spike that shifts a neighbouring hour's median).

## Fault catalogue (batch)

Row faults hit single readings (rate 0.05% of rows each). Sensor faults hit every reading of a
sensor inside a window of a few days (rate 0.4% of sensors each). A sensor with a sensor fault gets
no row faults, so every changed row has one cause. Each fault copies a problem the real archive
showed (counts from `config/silver_rules_v1.yaml` and the EDA reports).

| Fault | Level | What | Real precedent | Meant to be caught by |
|---|---|---|---|---|
| sentinel | row | value becomes -999 | -999 in 7.9M rows, -9999 in 1.5M | `sentinel` |
| out_of_range | row | value 987654 | values like 42949472 | `above_max` |
| spike | row | 10x the level plus a jump, inside the hard bound | pm25 > pm10 in 0.59% of hours | R14 `spike` |
| bad_number | row | `12.5` sent as `12,5` | not seen in the archive; common in locale-formatted CSV exports | **none** |
| unknown_parameter | row | `pm2.5` instead of `pm25` | `pm25-old`, `humidity` aliases | `unknown_parameter` |
| unit_alias | row | unit `ug/m3` | `μg/m³` (Greek mu), `ugm3` | `unit_unconverted` |
| future_ts | row | timestamp two years ahead | clock errors | R04 `ts_out_of_range` |
| coord_swap | row | lat and lon swapped | invalid row coordinates (R11) | R11 `coord_invalid` |
| null_island | row | coordinates (0, 0) | (0, 0) rows in the archive | R11 `coord_invalid` |
| missing_key | row | blank `sensors_id` | split records | quarantine `no_key` (R01/R03) |
| shifted_row | row | columns shifted, value lands in parameter | multiline split records | quarantine `multiline_split_record` (R01/R03) |
| duplicate | row | same record twice | `dup_count` > 1 across blocks | R13 dedup |
| dup_conflict | row | second record, same sensor and time, 1.5x | duplicates exist across blocks; how many disagree is not yet measured | **none** |
| stuck | sensor, 3 days | repeats its first value | dead sensors | R14 `flatline` |
| unit_x1000 | sensor, 5 days | values 1000x | co/ppb: 630 of 661 sensors already ppm | `above_max` |
| unit_div1000 | sensor, 5 days | values /1000 | same, other direction | **none** |
| calibration_bias | sensor, 7 days | values 1.6x | low-cost sensor drift (not measured in the archive) | **none** |
| tz_shift | sensor, 7 days | local wall time sent as UTC | Newfoundland `+HH:-MM` offsets (R02) | **none** |

Faults marked **none** are known gaps; the first run measures them.

## Results

Run `chaos_01`: January 2025, global, seed 42, 33,117,282 Near-Silver rows after injection
(`openaq_lake/reports/chaos_01_eval/summary.csv`). The control run rebuilt exactly the published
Silver (33,085,632 rows), so the Docker build reproduces the team's v1.2 build.

| Fault | Injected | Caught | By the expected rule | Verdict |
|---|---:|---:|---:|---|
| duplicate | 16,230 | 100% | 100% | caught |
| future_ts | 16,472 | 100% | 100% | caught |
| missing_key | 16,445 | 100% | 100% | caught |
| out_of_range | 12,780 | 100% | 100% | caught |
| sentinel | 16,040 | 100% | 100% | caught |
| shifted_row | 16,012 | 100% | 100% | caught |
| unit_alias | 10,191 | 100% | 100% | caught |
| unknown_parameter | 3,906 | 100% | 100% | caught |
| spike | 6,258 | 98.2% | 98.2% | caught |
| null_island | 16,230 | (repaired) | 100% | caught: coordinates replaced by the reference |
| unit_x1000 | 14,210 | 87.8% | 81.0% | partly: small gas values x1000 stay inside the bounds |
| stuck | 7,866 | 82.6% | 81.7% | partly |
| coord_swap | 15,826 | (repaired) | 40.8% | partly: swaps in mid-latitudes are not impossible values |
| bad_number | 16,226 | 1.8% | - | missed |
| calibration_bias | 10,859 | 2.6% | - | missed |
| dup_conflict | 15,382 | 0% | - | missed: the copy is dropped by block order, silently |
| tz_shift | 32,838 | 2.0% | - | missed |
| unit_div1000 | 21,297 | 2.9% | - | missed |

Collateral: 1,909 of 32,852,170 clean rows (0.006%) changed status against the control, mostly
suspect to valid (injected rows split real flatline runs). A Southeast Asia run (930k rows) gave
the same picture.

What the run itself taught about Spark (see docs/bigdata-experiments.md): a month is 2 large
files, so the injection read with 2–4 tasks until it was repartitioned to 64; the fault
expressions overflowed the 64 KB generated-code limit until the hashes were computed once; and
the selected rows were recomputed three times until persisted.

## Gaps and the rules that close them (Silver v1.3)

| Gap | Proposed rule | How it decides |
|---|---|---|
| bad_number | R15 `unparseable_value` (hard) | raw value present but not a number; today the row stays `valid` with a null value (test `test_unparseable_value_is_flagged`) |
| tz_shift | R16 `offset_mismatch` (soft) | the row's UTC offset differs from the location's usual offset that month (DST changes of 60 minutes allowed) |
| coord_swap in mid-latitudes | R17 `coord_far` (info, coordinates replaced) | row coordinates more than 50 km from the location's reference in `dim_location`; R11 only catches impossible values |
| dup_conflict | R18 `dup_conflict` (soft) | a dedup group whose values differ by more than 1% |
| unit_div1000, unit_x1000 inside bounds | R19 `level_shift` (soft) | a sensor-day median 100x above or below the sensor's month median (the `unit_check.ratio_suspect` already in the config, run per day instead of once per sensor) |
| calibration_bias | R20 `neighbour_disagree` (soft) | a sensor-hour far from the median of at least 3 sensors of the same parameter within 25 km; measured for precision first, since real local pollution also differs |

Phase 2 adds these to `src/silver/rules.py` and `config/silver_rules_v1.yaml` as v1.3, reruns the
same seed, and reports the before and after tables. A strict `xfail` test marks each gap and starts
failing when its rule lands, so the test is then turned into a normal one.

## Phases and tools

Each tool is added for a problem the runs expose. Heavy services run as a separate Docker profile
next to the lite stack, never all at once (Docker has about 16 GB here; the full stack uses about 6).

| Phase | Work | Tool | Problem it answers |
|---|---|---|---|
| 1 (done) | batch faults, evaluation, tests | Spark, pytest | how much bad data reaches Silver today |
| 2 | rules R15–R20, rerun the same seed | Spark | closing the measured gaps |
| 3 | quality gate between layers | Great Expectations (Python library, no service) | stop a bad batch before Gold, as a second line after the rules (for example "no null value in a valid row" also catches R15's gap) |
| 4 | Silver and Gold as Delta tables | Delta Lake (one jar) | roll back a bad batch with time travel (`RESTORE`), and `MERGE` archive rows over stream rows instead of rebuilding months |
| 5 | stream faults: duplicates, reordering, late data past the 25-hour watermark, malformed and oversized messages, schema drift (v1 and v2 messages, renamed fields, values as text) | chaos producer replaying a month into a separate topic, dead-letter topic, Schema Registry with Avro | the consumer must not crash or silently drop; the truth stays in a side file keyed by message id, not in the message |
| 6 | infrastructure faults: kill a broker, a DataNode or the consumer mid-batch, network delay and loss | Pumba (Docker chaos), the existing drills | measured instead of shown: rows lost or duplicated, recovery time, reconciled against the replay's known input |
| 7 | load test: replay a month at 100x and 1000x | Prometheus, Grafana, Kafka exporter | consumer lag and throughput; shows where `shuffle.partitions=1` and 3 partitions stop scaling |
| 8 | scheduling | Airflow | daily Silver/Gold, chaos runs and the quality gate on a schedule, with alerts |

Phases 3–8 each get their own section here when they start.
