import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]   # project root
load_dotenv(ROOT / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "air-quality"          # every source writes here, in the same format
DIM_DIR = ROOT / "data/dim"         # station catalogs written by each fetch_dimensions.py

# (lat_min, lon_min, lat_max, lon_max) around Vietnam; also covers parts of neighbours,
# so each source filters the stations it finds down to Vietnam
VIETNAM_BBOX = (8.4, 102.1, 23.4, 109.5)


def require_env(name):
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"{name} is missing: add it to .env")
    return value
