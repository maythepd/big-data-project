import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]   # project root
load_dotenv(ROOT / ".env")

# all 3 brokers, so producers can start while any one of them is down
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096")
TOPIC_NAME = "air-quality"          # every source writes here, in the same format

# serving MongoDB: MONGODB_URI (e.g. Atlas) from .env if set, else the local container with MONGO_PASSWORD
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = "airquality"
MONGO_USER = "airquality"
DIM_DIR = ROOT / "data/dim"         # station catalogs written by each fetch_dimensions.py

# local data lake (Near-Silver, Silver, Gold, reports, chaos runs); git-ignored.
# Inside this folder so the Docker Spark jobs see it at /app/openaq_lake.
LAKE = Path(os.getenv("LAKE_DIR") or ROOT / "openaq_lake").expanduser().absolute()

# (lat_min, lon_min, lat_max, lon_max); includes some neighbours, filtered per source
VIETNAM_BBOX = (8.4, 102.1, 23.4, 109.5)


def require_env(name):
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"{name} is missing: add it to .env")
    return value


def mongo_uri():
    """Connection string for the serving MongoDB: MONGODB_URI (e.g. Atlas) if set, else the local container."""
    if uri := os.getenv("MONGODB_URI"):
        return uri
    from urllib.parse import quote_plus
    password = quote_plus(require_env("MONGO_PASSWORD"))
    return f"mongodb://{MONGO_USER}:{password}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
