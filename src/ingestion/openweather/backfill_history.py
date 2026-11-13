"""Hourly OpenWeather air pollution history (from 25/11/2020) for every catalog point -> Bronze.

    python -m src.ingestion.openweather.backfill_history [--run-id openweather_history_01]
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import threading
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src.common.config import DIM_DIR, require_env

LAKE = Path.home() / "openaq_lake"
OUT = LAKE / "bronze" / "openweather_history"
URL = "https://api.openweathermap.org/data/2.5/air_pollution/history"
START = datetime(2020, 11, 25, tzinfo=timezone.utc)      # first hour the API returns
COMPONENTS = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
SCHEMA = pa.schema([("station_id", pa.string()), ("dt", pa.timestamp("s", tz="UTC")),
                    ("owm_aqi", pa.int8())] + [(c, pa.float64()) for c in COMPONENTS])

log = logging.getLogger("openweather_history")


class SharedLimiter:
    """At most one request start every `interval` seconds across threads; waits on HTTP 429."""

    def __init__(self, interval):
        self.interval, self.next, self.lock = interval, 0.0, threading.Lock()

    def get(self, url, params):
        while True:
            with self.lock:
                now = time.monotonic()
                start = max(now, self.next)
                self.next = start + self.interval
            time.sleep(start - now)
            r = requests.get(url, params=params, timeout=120)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("retry-after") or 60))
                continue
            r.raise_for_status()
            return r


def windows(start, end, months):
    """[start, end) split at calendar boundaries every `months` months."""
    cur = start
    while cur < end:
        m = (cur.month - 1) // months * months + months       # next boundary, 0-based month count
        nxt = datetime(cur.year + m // 12, m % 12 + 1, 1, tzinfo=timezone.utc)
        yield cur, min(nxt, end)
        cur = nxt


def fetch(api, key, st, start, end, retries=3):
    params = {"lat": st["lat"], "lon": st["lon"], "start": int(start.timestamp()),
              "end": int(end.timestamp()), "appid": key}
    for attempt in range(retries):
        try:
            return api.get(URL, params=params).json().get("list", [])
        except Exception as exc:                  # 504 on large ranges, timeouts
            log.warning("%s %s..%s attempt %d: %s", st["station_id"], start.date(), end.date(), attempt + 1, exc)
            time.sleep(5 * (attempt + 1))
    return None


def year_rows(api, key, st, start, end):
    rows = fetch(api, key, st, start, end)
    if rows is None:                              # retry the year in quarters
        rows = []
        for a, b in windows(start, end, 3):
            part = fetch(api, key, st, a, b)
            if part is None:
                raise RuntimeError(f"{st['station_id']} {a.date()}..{b.date()} failed")
            rows += part
    return rows


def to_table(station_id, rows):
    seen = {}
    for e in rows:                                # the API can repeat an hour at chunk edges
        seen[e["dt"]] = e
    data = {"station_id": [station_id] * len(seen),
            "dt": [datetime.fromtimestamp(t, timezone.utc) for t in sorted(seen)],
            "owm_aqi": [seen[t]["main"]["aqi"] for t in sorted(seen)]}
    for c in COMPONENTS:
        data[c] = [seen[t]["components"].get(c) for t in sorted(seen)]
    return pa.table(data, schema=SCHEMA)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", default="openweather_history_01")
    p.add_argument("--limit", type=int, help="only the first N points (test)")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    key = require_env("OPENWEATHER_API_KEY")
    stations = pd.read_parquet(DIM_DIR / "openweather_stations.parquet").to_dict("records")[:args.limit]
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    years = list(windows(START, now, 12))
    api = SharedLimiter(1.1)                      # free tier: 60 calls/minute
    report = LAKE / "reports" / args.run_id
    report.mkdir(parents=True, exist_ok=True)
    meta = {"run_id": args.run_id, "started_utc": datetime.now(timezone.utc).isoformat(),
            "points": len(stations), "until_utc": now.isoformat(), "files": 0, "rows": 0, "skipped": 0}
    t0 = time.time()
    lock, done = threading.Lock(), [0]

    def one_point(st):
        files = rows = skipped = 0
        for start, end in years:
            path = OUT / f"year={start.year}" / f"{st['station_id']}.parquet"
            if path.exists() and start.year < now.year:       # past years are final
                skipped += 1
                continue
            table = to_table(st["station_id"], year_rows(api, key, st, start, end))
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            pq.write_table(table, tmp, compression="zstd")
            tmp.rename(path)
            files, rows = files + 1, rows + table.num_rows
        with lock:
            meta["files"] += files; meta["rows"] += rows; meta["skipped"] += skipped; done[0] += 1
            log.info("%d/%d %s done (%d rows so far, %.0f s)", done[0], len(stations), st["station_id"],
                     meta["rows"], time.time() - t0)

    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(one_point, stations))
    meta.update(finished_utc=datetime.now(timezone.utc).isoformat(), seconds=round(time.time() - t0, 1))
    (report / "run.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(json.dumps(meta), flush=True)


if __name__ == "__main__":
    main()
