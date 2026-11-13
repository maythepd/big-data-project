"""Dashboard helpers: cached MongoDB loaders, AQI bands, colours.

hour_utc is UTC (shown at +7); daily `date` is the station's local date stored as 00:00 UTC.
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

import pandas as pd
from pymongo import MongoClient
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common.config import MONGO_DB, mongo_uri  # noqa: E402

VN_OFFSET = timedelta(hours=7)
POLLUTANTS = ["pm25", "pm10", "o3", "no2", "so2", "co"]
POLLUTANT_LABEL = {"pm25": "PM2.5", "pm10": "PM10", "o3": "O₃", "no2": "NO₂", "so2": "SO₂", "co": "CO"}
UNIT_NOTE = "Đơn vị: bụi µg/m³, khí ppm (đơn vị chuẩn của Silver)."

# AQI bands in the standards' official colours
BANDS = {
    "vn": [(50, "Tốt", "#00e400"), (100, "Trung bình", "#ffff00"), (150, "Kém", "#ff7e00"),
           (200, "Xấu", "#ff0000"), (300, "Rất xấu", "#8f3f97"), (10_000, "Nguy hại", "#7e0023")],
    "us": [(50, "Good", "#00e400"), (100, "Moderate", "#ffff00"),
           (150, "Unhealthy for sensitive groups", "#ff7e00"), (200, "Unhealthy", "#ff0000"),
           (300, "Very unhealthy", "#8f3f97"), (10_000, "Hazardous", "#7e0023")],
}
NO_DATA = ("Không đủ dữ liệu", "#9e9e9e")
# categorical colours, fixed order
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
NEUTRAL = "#4a6fa5"
GRID = "rgba(128,128,128,0.2)"
# base map for the station maps: Esri raster tiles, free and keyless (OpenStreetMap's tile servers
# are unreachable from some networks, Carto now asks for an API key)
BASEMAP_LAYERS = [dict(below="traces", sourcetype="raster", sourceattribution="Tiles © Esri",
                       source=["https://services.arcgisonline.com/ArcGIS/rest/services/Canvas/"
                               "World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}"])]


def band(aqi, std):
    if aqi is None or pd.isna(aqi):
        return NO_DATA
    for top, name, color in BANDS[std]:
        if aqi <= top:
            return name, color
    return BANDS[std][-1][1:]


def band_names(std):
    return [n for _, n, _ in BANDS[std]]


def band_colors(std):
    return {n: c for _, n, c in BANDS[std]} | {NO_DATA[0]: NO_DATA[1]}


def std_state():
    """(std key, aqi column, main column, label) from the sidebar choice."""
    std = st.session_state.get("std", "vn")
    return std, f"aqi_{std}", f"aqi_{std}_main", "VN_AQI" if std == "vn" else "US EPA AQI"


def style(fig, height=340, **layout):
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=30, b=10), plot_bgcolor="rgba(0,0,0,0)",
                      paper_bgcolor="rgba(0,0,0,0)", **layout)
    fig.update_xaxes(gridcolor=GRID, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, zeroline=False)
    return fig


def as_day(t):
    return datetime(t.year, t.month, t.day)


# ------------------------------------------------------------------ MongoDB
@st.cache_resource
def db():
    return MongoClient(mongo_uri(), serverSelectionTimeoutMS=5000)[MONGO_DB]


def _flat(rows, time_field):
    out = []
    for r in rows:
        meta = r.pop("meta", {})
        r.pop("_id", None)
        out.append({**meta, **r})
    df = pd.DataFrame(out)
    if not df.empty and time_field in df:
        df[time_field] = pd.to_datetime(df[time_field])
    return df


@st.cache_data(ttl=600, show_spinner=False)
def stations():
    rows = list(db().stations.find({}, {"name": 1, "provider": 1, "location": 1}))
    return pd.DataFrame([{"location_id": r["_id"], "name": r["name"], "provider": r["provider"],
                          "lon": r["location"]["coordinates"][0], "lat": r["location"]["coordinates"][1]}
                         for r in rows])


@st.cache_data(ttl=600, show_spinner="Đang đọc AQI ngày của các trạm VN…")
def vn_daily_aqi():
    """Every VN station-day of station_daily_aqi (~14k documents)."""
    df = _flat(list(db().station_daily_aqi.find({})), "date")
    cols = ["location_id", "date"] + [f"aqi_{s}{x}" for s in ("vn", "us") for x in ("", "_main")] + \
           [f"aqi_{s}_{p}" for s in ("vn", "us") for p in POLLUTANTS] + \
           [f"{p}_{x}" for p in POLLUTANTS for x in ("mean", "max_1h", "max_8h", "hours", "valid")]
    df = df.reindex(columns=list(dict.fromkeys(list(df.columns) + cols)))
    names = stations().set_index("location_id")["name"]
    df["name"] = df["location_id"].map(names).fillna(df["location_id"].astype(str))
    return df


@st.cache_data(ttl=600, show_spinner="Đang đọc AQI giờ của các trạm VN…")
def vn_hourly_aqi():
    """Every VN station-hour of station_aqi_hourly (~300k documents), in Vietnam time."""
    fields = {"_id": 0, "meta": 1, "hour_utc": 1, "aqi_vn": 1, "aqi_us": 1, "pm25_1h": 1,
              "pm25_nowcast": 1, "aqi_vn_main": 1, "aqi_us_main": 1}
    df = _flat(list(db().station_aqi_hourly.find({}, fields)), "hour_utc")
    df = df.reindex(columns=list(dict.fromkeys(list(df.columns) + list(fields)[2:])))
    df["hour_vn"] = df["hour_utc"] + VN_OFFSET
    return df


@st.cache_data(ttl=600, show_spinner="Đang đọc nồng độ ngày của các trạm VN…")
def vn_daily(parameter):
    # filter by location ids to use the index (meta.is_vn has none)
    ids = [int(i) for i in stations()["location_id"]]
    rows = db().station_daily.find({"meta.location_id": {"$in": ids}, "meta.parameter": parameter},
                                   {"_id": 0, "meta.location_id": 1, "date": 1, "value_mean": 1,
                                    "value_max": 1, "hours": 1, "day_valid": 1})
    return _flat(list(rows), "date")


@st.cache_data(ttl=600, show_spinner=False)
def collection(name, sort=None):
    cur = db()[name].find({})
    if sort:
        cur = cur.sort(sort, 1)
    return pd.DataFrame(list(cur))


@st.cache_data(ttl=3600, show_spinner="Đang đọc toạ độ các location toàn cầu…")
def locations():
    rows = db().locations.find({}, {"name": 1, "location": 1, "is_vn": 1})
    return pd.DataFrame([{"location_id": r["_id"], "name": r["name"], "is_vn": r["is_vn"],
                          "lon": r["location"]["coordinates"][0], "lat": r["location"]["coordinates"][1]}
                         for r in rows])


@st.cache_data(ttl=600, show_spinner="Đang truy vấn station_daily toàn cầu…")
def world_day(parameter, day):
    rows = db().station_daily.find({"meta.parameter": parameter, "date": as_day(day), "day_valid": True},
                                   {"_id": 0, "meta.location_id": 1, "meta.is_vn": 1, "value_mean": 1,
                                    "value_max": 1, "hours": 1})
    return _flat(list(rows), "date")


@st.cache_data(ttl=600, show_spinner=False)
def meta():
    """Precomputed by load_dashboard_summaries.py: time range, collection sizes."""
    return db().dashboard_meta.find_one({"_id": "summaries"}) or {}


def world_days(parameter=None):
    """First and last day of the Gold daily tables (precomputed)."""
    m = meta()
    if not m:
        return datetime(2006, 11, 1).date(), datetime.now().date()
    return m["first_month"].date(), (m["end_exclusive"] - timedelta(days=1)).date()


def coll_sizes():
    """Collection sizes and counts (precomputed: counting is slow)."""
    return pd.DataFrame(meta().get("collections", []))


@st.cache_data(ttl=30, show_spinner=False)
def latest():
    return pd.DataFrame(list(db().latest_readings.find({}, {"_id": 0}).sort("event_time", -1).limit(500)))
