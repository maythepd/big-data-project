"""Shared producer logic: rate limiting, the poll loop, dedupe and sending to Kafka.

Each source subclasses BaseProducer and only implements fetch(), which yields
messages already in the shared format (see schema.make_message).
"""
import argparse, json, logging, time
from datetime import datetime, timedelta, timezone
import requests
from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer, JsonSerializer
from .config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME


class RateLimiter:
    """At most one request every `min_interval` seconds; waits and retries on HTTP 429."""

    def __init__(self, min_interval=1.0):
        self.min_interval = min_interval
        self.last_call = 0.0

    def get(self, url, **kwargs):
        while True:
            wait = self.min_interval - (time.monotonic() - self.last_call)
            if wait > 0:
                time.sleep(wait)
            self.last_call = time.monotonic()
            r = requests.get(url, timeout=30, **kwargs)

            if r.status_code == 429:
                reset = int(r.headers.get("retry-after") or r.headers.get("x-ratelimit-reset") or 60)
                logging.warning("rate limited, sleeping %ss", reset)
                time.sleep(reset)
                continue
            r.raise_for_status()

            if r.headers.get("x-ratelimit-remaining") == "0":
                time.sleep(int(r.headers.get("x-ratelimit-reset", 60)))
            return r


class BaseProducer:
    source = None                    # e.g. "openaq"
    poll_seconds = 600               # time between cycles
    min_interval = 1.0               # seconds between API requests
    max_age = timedelta(days=1)      # skip readings older than this

    def __init__(self, dry_run=False):
        self.log = logging.getLogger(self.source)
        self.api = RateLimiter(self.min_interval)
        self.dry_run = dry_run
        self.last_seen = {}          # (station_id, parameter) -> datetime of last sent reading
        self.producer = None if dry_run else KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=DefaultSerializer(),      # str -> utf-8 bytes
            value_serializer=JsonSerializer(),       # dict -> JSON bytes
            acks="all",
        )

    def fetch(self):
        """Yield messages in the shared format (None values are ignored)."""
        raise NotImplementedError

    def safe_get(self, url, **kwargs):
        """GET through the rate limiter; logs and returns None on failure so one bad
        station doesn't stop the whole cycle."""
        try:
            return self.api.get(url, **kwargs)
        except Exception:
            self.log.exception("request failed: %s", url)
            return None

    def run_once(self):
        cutoff = datetime.now(timezone.utc) - self.max_age
        sent = 0
        for msg in self.fetch():
            if msg is None:
                continue
            if datetime.fromisoformat(msg["datetime"].replace("Z", "+00:00")) < cutoff:
                continue                 # stale reading
            key = (msg["station_id"], msg["parameter"])
            if self.last_seen.get(key) == msg["datetime"]:
                continue                 # already sent last cycle
            self.last_seen[key] = msg["datetime"]

            if self.dry_run:
                print(json.dumps(msg, ensure_ascii=False))
            else:
                self.producer.send(TOPIC_NAME, key=msg["station_id"], value=msg)
            sent += 1
        if self.producer:
            self.producer.flush()
        self.log.info("cycle done, sent %d new readings", sent)

    def run(self, once=False):
        while True:
            try:
                self.run_once()
            except Exception:
                self.log.exception("cycle failed, retrying next cycle")
            if once:
                return
            time.sleep(self.poll_seconds)

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser(description=f"{cls.source} producer")
        parser.add_argument("--once", action="store_true", help="run one cycle and exit")
        parser.add_argument("--dry-run", action="store_true",
                            help="print messages instead of sending them (implies --once)")
        args = parser.parse_args()

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        logging.getLogger("kafka").setLevel(logging.WARNING)
        cls(dry_run=args.dry_run).run(once=args.once or args.dry_run)
