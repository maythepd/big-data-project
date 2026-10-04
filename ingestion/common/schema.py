"""The one message format every producer sends to Kafka.

One message = one measurement:
    {"source": "openaq", "station_id": "openaq-4946812", "lat": 21.0, "lon": 105.8,
     "parameter": "pm25", "value": 23.4, "unit": "µg/m³",
     "datetime": "2026-10-04T10:00:00Z", "ingested_at": "2026-10-04T10:05:12Z"}
"""
from datetime import datetime, timezone

# source spelling -> our spelling
NAME_MAP = {
    "pm2_5": "pm25",                 # OpenWeather
    "t": "temperature",              # WAQI
    "h": "relativehumidity",         # WAQI
}

# every parameter we keep, with the one unit it is stored in
UNITS = {
    "pm1": "µg/m³", "pm25": "µg/m³", "pm10": "µg/m³",
    "no": "µg/m³", "no2": "µg/m³", "o3": "µg/m³", "so2": "µg/m³", "co": "µg/m³", "nh3": "µg/m³",
    "temperature": "c", "relativehumidity": "%",
    # WAQI publishes pollutants as US AQI sub-indices, not concentrations,
    # so they are kept as separate parameters instead of being mixed with µg/m³ values
    "aqi_pm25": "us_aqi", "aqi_pm10": "us_aqi", "aqi_no2": "us_aqi",
    "aqi_o3": "us_aqi", "aqi_so2": "us_aqi", "aqi_co": "us_aqi",
}

# g/mol, for converting gas concentrations from ppm/ppb to µg/m³ (at 25 °C, 1 atm)
MOLECULAR_WEIGHT = {"no": 30.01, "no2": 46.01, "o3": 48.00, "so2": 64.07, "co": 28.01, "nh3": 17.03}


def to_utc_iso(dt):
    """Accepts a datetime, an ISO string (any offset) or a unix timestamp."""
    if isinstance(dt, (int, float)):
        dt = datetime.fromtimestamp(dt, timezone.utc)
    elif isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_message(source, station_id, lat, lon, parameter, value, unit, dt):
    """Translate one reading into the shared format. Returns None if we don't keep it."""
    parameter = NAME_MAP.get(parameter, parameter)
    if parameter not in UNITS or value is None:
        return None

    if unit in ("ppm", "ppb"):
        if parameter not in MOLECULAR_WEIGHT:
            return None
        factor = 1000 if unit == "ppm" else 1
        value = value * MOLECULAR_WEIGHT[parameter] * factor / 24.45
    elif unit != UNITS[parameter]:
        return None                      # unexpected unit: drop rather than mislabel

    return {
        "source": source,
        "station_id": f"{source}-{station_id}",
        "lat": float(lat),
        "lon": float(lon),
        "parameter": parameter,
        "value": round(float(value), 3),
        "unit": UNITS[parameter],
        "datetime": to_utc_iso(dt),
        "ingested_at": to_utc_iso(datetime.now(timezone.utc)),
    }
