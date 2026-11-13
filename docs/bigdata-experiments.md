# Big-data experiments

The data-quality fault injection (docs/fault-injection.md) shows the cleaning rules work. These
five experiments test the other side: what happens to the pipeline under load, failures,
memory limits, reruns and changing message formats. Each one has a known input, so the result
is a count (messages lost, stored twice, rejected), not a screenshot.

Everything runs on one machine in Docker: Kafka (3 brokers, replication 3, min.insync 2), HDFS
(NameNode and 4 DataNodes, replication 3) and Spark. The "machines" share one computer and one
disk, so absolute speeds are this laptop's; the comparisons between settings are the result.

## The shared parts

- **Replay producer** (`src/chaos/replay.py`, service `replay` in docker-compose.producers.yml).
  Sends a slice of the 2025 Silver back into Kafka as live OpenAQ v2 messages, ordered by event
  time, on its own topic `replay-<run>`. Every message Kafka acknowledged is listed in
  `openaq_lake/replay/<run>/manifest.parquet`, with its Kafka partition and offset. Switchable
  stream faults: rate, producer-retry duplicates, late messages, event-time disorder, schema
  variants, and contract enforcement through the Schema Registry.
- **Consumer under test** (service `spark-replay` in docker-compose.spark.yml). The production
  consumer code (`src/streaming/consumer.py`), configured by `STREAM_*` variables to read the
  replay topic and write to `hdfs:///replay/<run>`. Its progress (rows, rates, batch durations,
  offsets behind, watermark drops) goes to `openaq_lake/replay/<run>/progress.jsonl`. Spark UI
  at http://localhost:4041.
- **Reconciliation** (`src/chaos/reconcile_stream.py`). The manifest against the archive (by
  key), the streaming Silver (exactly, by Kafka offset) and the dead-letter store. Writes
  `openaq_lake/reports/replay_<run>/{outcomes.csv, summary.json}`.
- **Failpoints** (`src/common/failpoint.py`). `CHAOS_CRASH_AT=<point>` ends a job at that point
  as a `kill -9` would. Points: `silver.after_stage`, `silver.after_delete`, `silver.mid_move`,
  `silver.before_done`.
- `scripts/experiments/common.sh` holds the helpers; each experiment is one script.

## 1. Velocity and backpressure — `scripts/experiments/velocity.sh`

- **Steady rates:** 300k messages at 2,000, 5,000 and 10,000 msg/s, and unthrottled. Measured:
  processed rows/s, batch duration (p50, p95), the most offsets the consumer fell behind.
- **Catch-up after an outage:** 1M messages queue up while the consumer is down. It then starts
  without a limit (the first micro-batch takes everything) and with `maxOffsetsPerTrigger`
  (50,000 per batch). Measured: batch durations, consumer memory, time to drain.
- What to expect: the streaming Silver checks each batch against every batch committed before
  (`silver_batch_writer.py`), so its batch time grows with the number of batches. That is a
  scaling limit of the current design, not of Kafka.

## 2. Exactly-once under failure — `scripts/experiments/exactly_once.sh`

600k messages at 3,000 msg/s, 1% sent twice (producer retries). During the replay:
`kill -9` the consumer, stop broker `kafka2`, stop DataNode `datanode2`, kill the consumer
again, bring the broker back. Pass: no acknowledged message missing from the archive or
Silver; no key stored twice. What makes it hold: the producer waits for `acks=all` and is
idempotent; the file sinks commit through their checkpoints; the Silver writer publishes each
batch atomically and skips a batch id it already committed; the archive drops duplicate keys
within its 25-hour watermark.

## 3. Memory pressure — `scripts/experiments/memory.sh`

- **A.** The full-month Silver build (33M rows) with the Spark driver at 1, 2, 4 and 8 GB. In
  local mode every task runs in the driver JVM, so this limits the whole job.
- **B.** R14 (a window over each sensor's month) at 2 GB with `--rows-per-task` 500,000 (its
  default memory knob) and 5,000,000.
- **C.** The streaming consumer at 1 GB after a 1M-message backlog, without and with
  `maxOffsetsPerTrigger`.
- Measured from the Spark event logs (`src/chaos/spark_events.py`): finished or failed, time,
  spill to memory and disk, GC share, peak execution memory per task. Results in
  `openaq_lake/reports/memory_<tag>.csv`.

## 4. Reprocessing safety — `scripts/experiments/reprocess.sh`

- **Plain Parquet (the current Silver):** a month exists; a rebuild crashes in the middle of
  moving files (`silver.mid_move`); a reader checks the month (`src/chaos/check_month.py`:
  files, rows, distinct keys); the same run id is rerun; then a crash just before the done
  marker and a rerun. Expected: the rerun repairs the month with nothing stored twice, but
  between the crash and the rerun readers see a broken month, because deleting the old files
  and moving the new ones in is not one atomic step.
- **Delta Lake (`src/chaos/delta_reprocess.py`):** the month as a Delta table; a bad rebuild
  (the chaos_01 Silver, full of faults) overwrites it; a quality gate compares the new version
  with the previous one (invalid share, row count) and fails; `RESTORE` brings the previous
  version back. Then an overwrite is killed while writing: readers keep the last committed
  version, because a Delta overwrite is one commit or nothing.

## 5. Schema evolution in the stream — `scripts/experiments/schema.sh`

100k messages with 2% each of: the legacy v1 format, an additive v3 (one new optional field),
a renamed value field, a value sent as text, missing keys, cut-off JSON, and 0.1% oversized
(2 MB, over Kafka's 1 MB limit).

- **A, no contract:** what the archive, Silver and the new dead-letter store do with each.
  Before this work the archive dropped unusable records silently and Silver rejected the
  additive v3 as legacy. Now the archive's rejects go to `stream/deadletter` with a reason,
  and Silver accepts schema versions 2 and 3.
- **B, contract:** the replay producer checks each message against its JSON Schema
  (`config/schemas/`), registered in a Confluent Schema Registry with BACKWARD compatibility.
  The registry accepts v3 as a new version and refuses the renamed schema. Messages that break
  the contract go to `replay-<run>-dlq` and never reach the topic.

## Results

Filled in from the reports after each run.
