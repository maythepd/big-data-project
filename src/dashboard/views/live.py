"""Trực tiếp: latest_readings written every minute by the streaming consumer (speed layer)."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.common import BASEMAP_LAYERS, POLLUTANT_LABEL, latest


@st.fragment(run_every=60)
def live_block():
    lt = latest()
    if lt.empty:
        st.info("`latest_readings` đang trống: luồng streaming (producer → Kafka → Spark consumer) chưa chạy. "
                "Khi chạy, trang này tự làm mới mỗi phút.")
        st.code("docker compose -f docker-compose.serving.yml up -d   # MongoDB\n"
                "docker compose up -d                                 # Kafka\n"
                "docker compose -f docker-compose.hdfs.yml up -d      # HDFS\n"
                "docker compose -f docker-compose.spark.yml up -d --build\n"
                "python -m src.ingestion.openaq.producer", language="bash")
        return
    lt["event_time"] = pd.to_datetime(lt["event_time"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
    now = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh")
    lt["tuổi (phút)"] = ((now - lt["event_time"]).dt.total_seconds() / 60).round(0)
    k = st.columns(4)
    k[0].metric("Trạm × chất", len(lt))
    k[1].metric("Nguồn", ", ".join(sorted(lt["source"].unique())))
    k[2].metric("Số đo mới nhất", f"{lt['event_time'].max():%H:%M %d/%m}")
    k[3].metric("Cập nhật trong 2 giờ", f"{(lt['tuổi (phút)'] <= 120).sum()} / {len(lt)}")
    p = st.selectbox("Chất", sorted(lt["parameter"].unique()), format_func=lambda v: POLLUTANT_LABEL.get(v, v))
    sub = lt[(lt["parameter"] == p) & lt["lat"].notna()]
    fig = go.Figure(go.Scattermapbox(lat=sub["lat"], lon=sub["lon"], mode="markers",
                                     marker=dict(size=12, color=sub["value"], colorscale="YlOrRd", showscale=True),
                                     customdata=list(zip(sub["station_id"], sub["value"], sub["event_time"].dt.strftime("%H:%M %d/%m"))),
                                     hovertemplate="%{customdata[0]}: %{customdata[1]} (%{customdata[2]})<extra></extra>"))
    fig.update_layout(mapbox=dict(style="white-bg", layers=BASEMAP_LAYERS, center=dict(lat=16, lon=106), zoom=4.6), height=480,
                      margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(lt[["source", "station_id", "parameter", "value", "unit", "event_time", "tuổi (phút)"]],
                 hide_index=True, use_container_width=True)


def render():
    st.title("Trực tiếp (speed layer)")
    st.caption("`latest_readings`: số đo mới nhất mỗi trạm × chất, consumer upsert mỗi phút và chỉ ghi đè khi "
               "`event_time` mới hơn. Giờ hiển thị theo giờ Việt Nam.")
    live_block()
