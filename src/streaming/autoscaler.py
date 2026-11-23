"""Lag-driven autoscaler for a Kafka consumer group running as a Docker Compose service.

KEDA's Kafka rule on Kubernetes, on one machine with Docker Compose, plus a rate term:
  desired = max(ceil(lag / lag-per-replica), ceil(incoming msg/s / rate-per-replica)), within
  [--min, --max] and at most the partition count (an extra consumer would get no partition).
  Lag = messages in the topic the group has not committed yet.
  Lag alone is not enough: once enough replicas run, the lag falls to ~0 while the load is still
  high, the lag rule asks for fewer, and the lag comes back (measured: 3 -> 2 replicas mid-burst,
  lag 1,332 -> 24,700 in 10 s). The rate term keeps the replicas the incoming rate needs.
  scale out at once; scale in only to the highest replica count wanted during the last
  --scale-in-after seconds (a stabilization window, as Kubernetes' HPA), so a short dip in the
  load does not tear replicas down only to start them again a minute later.

Each tick prints and appends one CSV row (time, lag, produced and consumed msg/s, replicas).

    python -m src.streaming.autoscaler --topic autoscale-in --group validator \
        --compose-file docker-compose.autoscale.yml --service validator --max 6
"""
import argparse
import csv
from datetime import datetime, timezone
import math
from pathlib import Path
import subprocess
import time

from kafka.errors import KafkaError

from src.common.config import KAFKA_BOOTSTRAP_SERVERS


def desired_replicas(lag, lag_per_replica, lo, hi, partitions, rate_in=0.0, rate_per_replica=0):
    """Replicas wanted to drain this lag (KEDA's rule) and to keep up with the incoming rate
    (when rate_per_replica is set), within [lo, min(hi, partitions)]."""
    want = math.ceil(lag / lag_per_replica) if lag > 0 else 0
    if rate_per_replica:
        want = max(want, math.ceil(rate_in / rate_per_replica))
    return max(lo, min(want, hi, partitions))


def next_replicas(current, history, now, window_s):
    """Scale out to the newest wish at once; scale in only to the highest wish of the last window.

    history: [(time, desired), ...] oldest first, the newest last.
    """
    newest = history[-1][1]
    if newest >= current:
        return newest
    return min(current, max(d for t, d in history if now - t <= window_s))


class Lag:
    """Committed offsets of a group vs the topic's end offsets."""

    def __init__(self, topic, group):
        from kafka import KafkaConsumer, TopicPartition
        from kafka.admin import KafkaAdminClient
        self.topic, self.group = topic, group
        self.admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        self.reader = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        self.parts = [TopicPartition(topic, p) for p in sorted(self.reader.partitions_for_topic(topic) or [])]
        if not self.parts:
            raise SystemExit(f"topic {topic} does not exist")

    def read(self):
        """(lag, end offsets total, committed total)."""
        end = self.reader.end_offsets(self.parts)
        start = self.reader.beginning_offsets(self.parts)
        # kafka-python 3: list_group_offsets({group: partitions}) -> {group: {tp: OffsetAndMetadata}}
        done = {tp: om.offset for tp, om in
                self.admin.list_group_offsets({self.group: self.parts}).get(self.group, {}).items()}
        # a partition the group never committed counts from its first offset (auto_offset_reset=earliest)
        committed = {tp: done[tp] if done.get(tp, -1) >= 0 else start[tp] for tp in self.parts}
        return sum(end[tp] - committed[tp] for tp in self.parts), sum(end.values()), sum(committed.values())


def running(compose_file, service):
    out = subprocess.run(["docker", "compose", "-f", compose_file, "ps", "-q", service],
                         capture_output=True, text=True)
    return len(out.stdout.split())


def scale(compose_file, service, n):
    subprocess.run(["docker", "compose", "-f", compose_file, "up", "-d", "--no-recreate",
                    "--scale", f"{service}={n}", service], capture_output=True, text=True, check=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--topic", required=True)
    p.add_argument("--group", required=True)
    p.add_argument("--compose-file", required=True)
    p.add_argument("--service", required=True)
    p.add_argument("--min", type=int, default=1)
    p.add_argument("--max", type=int, default=6)
    p.add_argument("--lag-per-replica", type=int, default=20_000,
                   help="lag one replica is expected to absorb (KEDA's lagThreshold)")
    p.add_argument("--rate-per-replica", type=float, default=0,
                   help="msg/s one replica sustains; 0 = lag only (scales in under steady high load)")
    p.add_argument("--interval", type=float, default=10, help="seconds between checks")
    p.add_argument("--scale-in-after", type=float, default=60, help="stabilization window for scale-in, seconds")
    p.add_argument("--csv", help="append one row per check")
    args = p.parse_args()

    lag = Lag(args.topic, args.group)
    partitions = len(lag.parts)
    if args.max > partitions:
        print(f"--max {args.max} > {partitions} partitions: replicas above {partitions} would sit idle")
    current = running(args.compose_file, args.service)
    if current < args.min:
        scale(args.compose_file, args.service, args.min)
        current = args.min
    writer = None
    if args.csv:
        Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
        fresh = not Path(args.csv).exists()
        fh = open(args.csv, "a", newline="", encoding="utf-8")
        writer = csv.writer(fh)
        if fresh:
            writer.writerow(["time_utc", "lag", "produced_per_s", "consumed_per_s", "replicas", "desired"])
    history, last = [], None
    print(f"autoscaling {args.service}: lag per replica {args.lag_per_replica:,}, "
          f"rate per replica {args.rate_per_replica:,.0f} msg/s, replicas {args.min}..{args.max}, "
          f"{partitions} partitions, scale-in after {args.scale_in_after:.0f} s", flush=True)
    while True:
        now = time.monotonic()
        try:
            behind, produced, consumed = lag.read()
        except KafkaError as exc:
            # transient while the group forms or a broker restarts (NotCoordinator, leader moves):
            # skip this check, never scale on a reading that failed
            print(f"{datetime.now(timezone.utc):%H:%M:%S} lag unavailable ({type(exc).__name__}), retrying", flush=True)
            time.sleep(args.interval)
            continue
        rates = ((produced - last[1]) / (now - last[0]), (consumed - last[2]) / (now - last[0])) if last else (0, 0)
        last = (now, produced, consumed)
        want = desired_replicas(behind, args.lag_per_replica, args.min, args.max, partitions,
                                rates[0], args.rate_per_replica)
        history = [(t, d) for t, d in history if now - t <= args.scale_in_after] + [(now, want)]
        current = running(args.compose_file, args.service)
        target = next_replicas(current, history, now, args.scale_in_after)
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        note = ""
        if target != current:
            scale(args.compose_file, args.service, target)
            note = f"  -> SCALE {'OUT' if target > current else 'IN'} to {target}"
        print(f"{stamp} lag {behind:>9,}  in {rates[0]:>7,.0f}/s  out {rates[1]:>7,.0f}/s  "
              f"replicas {current} (wanted {want}){note}", flush=True)
        if writer:
            writer.writerow([stamp, behind, round(rates[0]), round(rates[1]), target, want])
            fh.flush()
        time.sleep(max(0.0, args.interval - (time.monotonic() - now)))


if __name__ == "__main__":
    main()
