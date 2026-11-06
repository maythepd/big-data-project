"""Toàn cầu: one day of a pollutant worldwide (station_daily, 70M documents) and VN vs world."""

from datetime import date

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.common import (POLLUTANT_LABEL, SERIES, UNIT_NOTE, collection, locations, style,
                                  world_day, world_days)

# sequential scale (one hue, light -> dark) for magnitude on the world map
SEQ = [[0, "#fde9c9"], [0.35, "#f6a35c"], [0.7, "#d4512b"], [1, "#7a1d12"]]


def render():
    st.title("Toàn cầu")
    st.caption("Collection `station_daily` (MongoDB time series, 69,9 triệu document): trung bình ngày theo giờ "
               "địa phương của mọi location OpenAQ có 6 chất AQI. Chỉ ngày hợp lệ (≥ 18 giờ).")
    c1, c2 = st.columns([1, 3])
    param = c1.selectbox("Chất", ["pm25", "pm10", "o3", "no2", "so2", "co"], format_func=POLLUTANT_LABEL.get)
    first, last = world_days(param)
    day = c2.date_input("Ngày", value=min(last, date(2025, 1, 15)), min_value=first, max_value=last,
                        format="DD/MM/YYYY")

    d = world_day(param, day)
    if d.empty:
        st.info("Không có dữ liệu cho ngày này.")
        return
    d = d.merge(locations().drop(columns=["is_vn"]), on="location_id", how="inner")
    v = d["value_mean"]
    cap = float(np.nanpercentile(v, 98)) if len(v) else 1.0

    k = st.columns(4)
    k[0].metric("Location có dữ liệu", f"{len(d):,}".replace(",", "."))
    k[1].metric("Trung vị toàn cầu", f"{v.median():.1f}")
    vn = d[d["is_vn"]]
    k[2].metric("Trung vị VN", f"{vn['value_mean'].median():.1f}" if len(vn) else "—", help=f"{len(vn)} trạm VN")
    k[3].metric("P98 (đỉnh thang màu)", f"{cap:.1f}")

    fig = go.Figure(go.Scattergeo(
        lat=d["lat"], lon=d["lon"], mode="markers",
        marker=dict(size=4, color=v.clip(upper=cap), colorscale=SEQ, cmin=0, cmax=cap, opacity=0.85,
                    colorbar=dict(title=POLLUTANT_LABEL[param], thickness=10)),
        customdata=list(zip(d["name"], v.round(2))),
        hovertemplate="%{customdata[0]}: %{customdata[1]}<extra></extra>"))
    fig.update_geos(projection_type="natural earth", showcountries=True, countrycolor="rgba(128,128,128,0.5)",
                    showland=True, landcolor="rgba(200,200,200,0.15)", showocean=False, bgcolor="rgba(0,0,0,0)")
    fig.update_layout(height=520, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)
    st.caption(UNIT_NOTE + f" Giá trị trên P98 gộp vào cột cuối của biểu đồ phân bố. Thang màu cắt ở P98 ({cap:.1f}) để vài điểm cực đại không làm mất tương phản.")

    a, b = st.columns(2)
    with a:
        st.subheader("Phân bố trong ngày")
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=d.loc[~d["is_vn"], "value_mean"].clip(upper=cap), name="Thế giới",
                                   histnorm="percent", marker_color=SERIES[0], opacity=0.75,
                                   xbins=dict(start=0, end=cap * 1.001, size=cap / 40)))
        if len(vn):
            fig.add_trace(go.Histogram(x=vn["value_mean"].clip(upper=cap), name="Việt Nam", histnorm="percent",
                                       marker_color=SERIES[1], opacity=0.75,
                                       xbins=dict(start=0, end=cap * 1.001, size=cap / 40)))
        style(fig, height=320, barmode="overlay", xaxis_title=POLLUTANT_LABEL[param], yaxis_title="% location",
              legend=dict(orientation="h", y=1.12))
        st.plotly_chart(fig, use_container_width=True)
    with b:
        st.subheader("Cao nhất trong ngày")
        top = d.sort_values("value_mean", ascending=False).head(15)[["name", "value_mean", "hours"]]
        top.columns = ["Location", "Trung bình ngày", "Số giờ"]
        top["Trung bình ngày"] = top["Trung bình ngày"].round(2)
        st.dataframe(top, hide_index=True, use_container_width=True, height=320)
        st.caption("Giá trị rất cao (hàng trăm, nghìn µg/m³ trong cả ngày) có thể là lỗi cảm biến mà luật "
                   "Silver chưa bắt (vượt ngưỡng cứng mới bị loại); xem như điểm cần kiểm tra, không phải kết luận.")

    st.subheader("PM2.5: Việt Nam so với phần còn lại của thế giới, theo tháng")
    m = collection("pm25_monthly", sort="month")
    if not m.empty:
        m["month"] = pd.to_datetime(m["month"])
        fig = go.Figure()
        for i, (flag, label) in enumerate([(False, "Thế giới (trừ VN)"), (True, "Việt Nam")]):
            s = m[m["is_vn"] == flag]
            fig.add_trace(go.Scatter(x=s["month"], y=s["median"], mode="lines", name=label,
                                     line=dict(width=2, color=SERIES[i]), customdata=s["stations"],
                                     hovertemplate="%{x|%m/%Y}: trung vị %{y:.1f} µg/m³ (%{customdata} trạm)<extra>" + label + "</extra>"))
        style(fig, height=340, yaxis_title="Trung vị PM2.5 ngày (µg/m³)", hovermode="x unified",
              legend=dict(orientation="h", y=1.12))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Từ collection tổng hợp `pm25_monthly` (`load_dashboard_summaries.py`), tính trên các trạm-ngày "
                   "hợp lệ của Gold. Số trạm VN rất ít trước 2024.")
