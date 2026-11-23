"""Stream validator: one replica of a Kafka consumer group that the autoscaler scales out and in.

Every message from --in is checked against its message contract (config/schemas, by its
schema_version: 2 -> openaq_v2, 3 -> openaq_v3) and forwarded to --out, or to --dlq with the reason
in a `reason` header. Producers we do not control (sensor fleets, partner feeds) get checked here,
before Spark, MongoDB or anyone else reads the data.

Scaling: all replicas share the group id, so Kafka splits the topic's partitions between them
and rebalances when one joins or leaves. More replicas than partitions sit idle, so the topic's
partition count is the ceiling (scripts/experiments/autoscale.sh creates 12).

Delivery: at least once. Offsets are committed only after the producer has every forwarded
message acknowledged, so a replica killed or scaled in mid-batch loses nothing; the messages it
had not committed are processed again by the replica that takes over its partitions (counted as
`redelivered` by the autoscale experiment). SIGTERM (docker stop, scale-in) finishes the batch,
commits and leaves the group first.

    docker compose -f docker-compose.autoscale.yml up -d --scale validator=1
"""
import argparse
import json
import logging
import signal
import socket
import time

from kafka import KafkaConsumer, KafkaProducer

from src.common.config import KAFKA_BOOTSTRAP_SERVERS
from src.common.contract import problem

SCHEMA_OF_VERSION = {2: "openaq_v2", 3: "openaq_v3"}
log = logging.getLogger("validator")


def classify(data):
    """None when the message is valid, else why it is not (a short reason for the dlq header)."""
    try:
        version = json.loads(data).get("schema_version")
    except (ValueError, AttributeError):
        return "not_json"
    name = SCHEMA_OF_VERSION.get(version)
    if name is None:
        return f"unknown_schema_version:{version}"
    return problem(name, data)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--in", dest="topic_in", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--dlq", required=True)
    p.add_argument("--group", default="validator")
    p.add_argument("--max-records", type=int, default=2000, help="messages per poll (one commit each)")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {socket.gethostname()} %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = KafkaConsumer(args.topic_in, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=args.group,
                             enable_auto_commit=False, auto_offset_reset="earliest",
                             max_poll_records=args.max_records)
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, acks="all", linger_ms=20,
                             batch_size=256 * 1024)
    done = bad = 0
    window_start, window_done = time.monotonic(), 0
    log.info("started: %s -> %s / %s, group %s", args.topic_in, args.out, args.dlq, args.group)
    try:
        while not stopping:
            batches = consumer.poll(timeout_ms=1000)
            for records in batches.values():
                for r in records:
                    reason = classify(r.value)
                    if reason is None:
                        producer.send(args.out, key=r.key, value=r.value)
                    else:
                        bad += 1
                        producer.send(args.dlq, key=r.key, value=r.value, headers=[("reason", reason.encode())])
                    done += 1
                    window_done += 1
            if batches:
                producer.flush()             # every forwarded message acknowledged ...
                consumer.commit()            # ... before its input offset is committed
            if time.monotonic() - window_start >= 10:
                parts = sorted(tp.partition for tp in consumer.assignment())
                log.info("%.0f msg/s, partitions %s, %d processed, %d to dlq",
                         window_done / (time.monotonic() - window_start), parts, done, bad)
                window_start, window_done = time.monotonic(), 0
    finally:
        producer.flush()
        consumer.close()                 # commits nothing new; leaves the group so it rebalances now
        producer.close()
        log.info("stopped: %d processed, %d to dlq", done, bad)


if __name__ == "__main__":
    main()
