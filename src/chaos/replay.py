"""Replay producer: a slice of Silver back into Kafka as live OpenAQ v2 messages.

Every streaming experiment (docs/bigdata-experiments.md) feeds the pipeline from here, so its
output can be reconciled exactly: openaq_lake/replay/<run>/manifest.parquet lists every message
Kafka acknowledged (one row per send, duplicates included) with the stream fault it carries.

Stream faults, all deterministic for a seed:
  --rate N            messages per second (0 = as fast as the producer goes)
  --dup-share p       a copy of the message is sent again a little later (producer retry)
  --late-share p      the message is held back until the replay is --late-hours past its event time
  --disorder-minutes  event times are shuffled within this jitter
  --variants a=p,...  schema variants instead of v2 (see VARIANTS)
  --registry URL      enforce the message contract (src/common/contract.py): each message is
                      checked against its JSON Schema, registered in the Schema Registry; a
                      message that breaks it goes to <topic>-dlq instead of the topic

Runs in the producer image on the Kafka network (scripts/experiments/*.sh):
  docker compose -f docker-compose.producers.yml run --rm replay --run-id r1 --start 2025-01-10 --hours 6
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import heapq
import json
import logging
from pathlib import Path
import random
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import KAFKA_BOOTSTRAP_SERVERS, LAKE

COLUMNS = ["location_id", "sensors_id", "location_name", "datetime_utc", "datetime_local_raw",
           "lat", "lon", "parameter_raw", "units_raw", "value_raw"]
# what each variant tests (the consumer's expected handling is in docs/bigdata-experiments.md)
VARIANTS = {
    "v2": "the current OpenAQ message",
    "v1": "legacy shared format (schema.make_message): no schema_version, no sensors_id",
    "v3_extra_field": "v2 plus a new optional field, schema_version 3 (an additive change)",
    "renamed_value": "value and value_raw renamed to measurement / measurement_raw",
    "string_value": "value sent as a JSON string, \"23.4\"",
    "missing_key": "station_id and sensors_id left out",
    "malformed": "JSON cut off halfway",
    "oversized": "a 2 MB padding field: larger than Kafka's 1 MB message limit",
}
# the schema a contract-enforcing producer would declare for each variant
SCHEMA_OF = {"v3_extra_field": "openaq_v3", "renamed_value": "openaq_renamed"}
log = logging.getLogger("replay")


def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def uniform(seed, *parts):
    """Deterministic number in [0, 1) for a seed and a message."""
    h = hashlib.blake2b(f"{seed}|{'|'.join(map(str, parts))}".encode(), digest_size=8).digest()
    return int.from_bytes(h, "big") / 2 ** 64


def load(month, start, hours, limit):
    """Silver rows of [start, start + hours), ordered by event time."""
    year, mon = map(int, month.split("-"))
    root = LAKE / "silver" / "measurements" / f"year={year}" / f"month={mon}"
    t0 = start.replace(tzinfo=None)
    flt = (pc.field("datetime_utc") >= pa.scalar(t0, pa.timestamp("ns"))) & \
          (pc.field("datetime_utc") < pa.scalar(t0 + timedelta(hours=hours), pa.timestamp("ns")))
    table = ds.dataset(root, format="parquet").to_table(columns=COLUMNS, filter=flt)
    table = table.sort_by([("datetime_utc", "ascending"), ("sensors_id", "ascending")])
    if limit:
        table = table.slice(0, limit)
    i = table.schema.get_field_index("datetime_utc")
    table = table.set_column(i, "datetime_utc", pc.cast(table["datetime_utc"], pa.timestamp("us", tz="UTC")))
    return table.to_pylist()


def payload(r, variant, now):
    """Message dict (or raw bytes for malformed/oversized) for one Silver row."""
    raw = r["value_raw"]
    try:
        value = float(raw) if raw is not None else None
    except ValueError:
        value = None
    when = iso(r["datetime_utc"])
    station = f"openaq-{r['location_id']}"
    if variant == "v1":
        return json.dumps({"source": "openaq", "station_id": station, "lat": r["lat"], "lon": r["lon"],
                           "parameter": r["parameter_raw"], "value": value, "unit": r["units_raw"],
                           "datetime": when, "ingested_at": now}, ensure_ascii=False).encode()
    msg = {"schema_version": 2, "source": "openaq", "station_id": station, "location_id": r["location_id"],
           "sensors_id": r["sensors_id"], "location_name": r["location_name"], "lat": r["lat"], "lon": r["lon"],
           "parameter": r["parameter_raw"], "parameter_raw": r["parameter_raw"], "value": value,
           "value_raw": raw, "unit": r["units_raw"], "units_raw": r["units_raw"], "datetime": when,
           "datetime_local_raw": r["datetime_local_raw"], "ingested_at": now}
    if variant == "v3_extra_field":
        msg.update(schema_version=3, sensor_grade="reference")
    elif variant == "renamed_value":
        msg["measurement"], msg["measurement_raw"] = msg.pop("value"), msg.pop("value_raw")
    elif variant == "string_value":
        msg["value"] = raw
    elif variant == "missing_key":
        del msg["station_id"], msg["sensors_id"]
    data = json.dumps(msg, ensure_ascii=False, default=str).encode()
    if variant == "malformed":
        return data[: len(data) // 2]
    if variant == "oversized":
        msg["padding"] = "x" * (2 << 20)
        return json.dumps(msg, default=str).encode()
    return data


def parse_variants(spec):
    shares = {}
    for part in filter(None, (spec or "").split(",")):
        name, share = part.split("=")
        if name not in VARIANTS or name == "v2":
            raise SystemExit(f"unknown variant {name}; choose from {sorted(VARIANTS)}")
        shares[name] = float(share)
    if sum(shares.values()) >= 1:
        raise SystemExit("variant shares must add up to less than 1")
    return shares


def pick_variant(u, shares):
    lo = 0.0
    for name, share in shares.items():
        if lo <= u < lo + share:
            return name
        lo += share
    return "v2"


def ensure_topic(topic, partitions):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        admin.create_topics([NewTopic(topic, partitions, 3, topic_configs={"min.insync.replicas": "2"})])
        log.info("created topic %s with %d partitions", topic, partitions)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", required=True)
    p.add_argument("--month", default="2025-01")
    p.add_argument("--start", default="2025-01-10T00:00:00", help="first event time (UTC)")
    p.add_argument("--hours", type=float, default=6, help="event-time span to replay")
    p.add_argument("--limit", type=int, help="at most this many Silver rows")
    p.add_argument("--topic", default="air-quality-replay")
    p.add_argument("--partitions", type=int, default=6, help="only when the topic is created")
    p.add_argument("--rate", type=float, default=0, help="messages per second; 0 = unthrottled")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--dup-share", type=float, default=0.0)
    p.add_argument("--late-share", type=float, default=0.0)
    p.add_argument("--late-hours", type=float, default=30.0)
    p.add_argument("--disorder-minutes", type=float, default=0.0)
    p.add_argument("--variants", help="e.g. v1=0.02,malformed=0.01")
    p.add_argument("--registry", help="Schema Registry URL, e.g. http://schema-registry:8081")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)

    from kafka import KafkaProducer
    out = LAKE / "replay" / args.run_id
    out.mkdir(parents=True, exist_ok=True)
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    shares = parse_variants(args.variants)
    rows = load(args.month, start, args.hours, args.limit)
    log.info("loaded %d Silver rows from %s, %.1f hours", len(rows), args.start, args.hours)

    # send order: event time (+ jitter); late messages move behind the event-time cursor
    plan = []
    for i, r in enumerate(rows):
        key = (r["sensors_id"], r["datetime_utc"])
        t = r["datetime_utc"].timestamp()
        jitter = uniform(args.seed, "jitter", *key) * args.disorder_minutes * 60
        late = uniform(args.seed, "late", *key) < args.late_share
        dup = uniform(args.seed, "dup", *key) < args.dup_share
        variant = pick_variant(uniform(args.seed, "variant", *key), shares)
        plan.append((t + jitter, i, late, dup, variant))
    plan.sort()

    ensure_topic(args.topic, args.partitions)
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, acks="all",
                             enable_idempotence=True, linger_ms=20, batch_size=256 * 1024,
                             max_request_size=1 << 20)
    contract = None
    if args.registry:
        from src.common.contract import Contract
        contract = Contract(args.registry, f"{args.topic}-value")
        if contract.register("openaq_v2") is None:
            raise SystemExit(f"openaq_v2 refused by the registry: {contract.errors}")
    manifest = []                # one entry per send attempt; acked filled by callbacks
    held, copies = [], []        # (release_at, ...) heaps: late messages and duplicate copies
    sent = failed = 0
    began, last_report = time.monotonic(), time.monotonic()

    def send(i, variant, late, copy):
        nonlocal sent
        r = rows[i]
        now = iso(datetime.now(timezone.utc))
        data = payload(r, variant, now)
        entry = {"seq": len(manifest), "row": i, "station_id": f"openaq-{r['location_id']}",
                 "location_id": r["location_id"], "sensors_id": r["sensors_id"],
                 "parameter": r["parameter_raw"], "event_time": r["datetime_utc"], "variant": variant,
                 "late": late, "copy": copy, "bytes": len(data), "acked": None, "error": None,
                 "partition": None, "offset": None}
        manifest.append(entry)
        if args.rate:
            wait = began + sent / args.rate - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        if contract:
            problem = contract.check(SCHEMA_OF.get(variant, "openaq_v2"), data) or                 ("too_large" if len(data) > 1 << 20 else None)
            if problem:
                entry.update(acked=False, error=f"contract:{problem}")
                producer.send(f"{args.topic}-dlq", key=entry["station_id"].encode(), value=data[: 1 << 19],
                              headers=[("reason", problem.encode())])
                return
        try:
            future = producer.send(args.topic, key=entry["station_id"].encode(), value=data)
        except Exception as exc:            # e.g. MessageSizeTooLargeError, raised before sending
            entry.update(acked=False, error=type(exc).__name__)
            return
        future.add_callback(lambda md, e=entry: e.update(acked=True, partition=md.partition, offset=md.offset))
        future.add_errback(lambda exc, e=entry: e.update(acked=False, error=type(exc).__name__))
        sent += 1

    for t, i, late, dup, variant in plan:
        # release held late messages and pending copies whose time has come
        while held and held[0][0] <= t:
            _, j, v = heapq.heappop(held)
            send(j, v, True, False)
        while copies and copies[0][0] <= t:
            _, j, v = heapq.heappop(copies)
            send(j, v, False, True)
        if late:
            heapq.heappush(held, (t + args.late_hours * 3600, i, variant))
            continue
        send(i, variant, False, False)
        if dup:
            heapq.heappush(copies, (t + 60, i, variant))     # a retry a minute of event time later
        if time.monotonic() - last_report > 10:
            last_report = time.monotonic()
            log.info("sent %d of %d planned (%.0f msg/s)", sent, len(plan), sent / (last_report - began))
    for _, j, v in sorted(held):
        send(j, v, True, False)
    for _, j, v in sorted(copies):
        send(j, v, False, True)
    producer.flush()
    producer.close()
    seconds = time.monotonic() - began

    for e in manifest:
        if e["acked"] is None:
            e.update(acked=False, error="no_ack")
    failed = sum(not e["acked"] for e in manifest)
    pq.write_table(pa.Table.from_pylist(manifest), out / "manifest.parquet")
    summary = {"run_id": args.run_id, "args": vars(args), "silver_rows": len(rows), "sends": len(manifest),
               "registry": {"ids": contract.ids, "refused": contract.errors} if contract else None,
               "acked": len(manifest) - failed, "failed": failed, "seconds": round(seconds, 1),
               "msgs_per_s": round(len(manifest) / seconds, 1) if seconds else None,
               "finished_utc": datetime.now(timezone.utc).isoformat()}
    (out / "replay.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log.info("done: %s", json.dumps({k: summary[k] for k in ["sends", "acked", "failed", "seconds", "msgs_per_s"]}))


if __name__ == "__main__":
    main()
