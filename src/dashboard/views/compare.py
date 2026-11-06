"""So sánh trạm: daily concentration lines, station x month heatmap, days per band."""

from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.common import (BANDS, POLLUTANT_LABEL, SERIES, UNIT_NOTE, band, band_names, stations,
                                  std_state, style, vn_daily, vn_daily_aqi)


def render():
    std, aqi_col, _, std_label = std_state()
    st.title("So sánh trạm")
    daily = vn_daily_aqi()
    with_aqi = daily[daily[aqi_col].notna()]
    days_per = with_aqi.groupby("location_id").size().sort_values(ascending=False)
    names = stations().set_index("location_id")["name"].to_dict()

    c1, c2, c3 = st.columns([3, 1, 2])
    lo, hi = with_aqi["date"].min().date(), with_aqi["date"].max().date()
    rng = c3.date_input("Khoảng ngày", value=(max(lo, date(2024, 1, 1)), min(hi, date(2025, 3, 31))),
                        min_value=lo, max_value=hi, format="DD/MM/YYYY")
    if isinstance(rng, tuple) and len(rng) == 2:      # default stations: most days inside the range
        inside = with_aqi[(with_aqi["date"] >= pd.Timestamp(rng[0])) & (with_aqi["date"] <= pd.Timestamp(rng[1]))]
        default = inside.groupby("location_id").size().sort_values(ascending=False).index[:4].tolist()
    else:
        default = days_per.index[:4].tolist()
    pick = c1.multiselect("Trạm (tối đa 5)", days_per.index.tolist(), default=default,
                          max_selections=5, format_func=lambda i: f"{names.get(i, i)} ({days_per[i]} ngày)")
    param = c2.selectbox("Chất", ["pm25", "pm10", "o3", "no2", "co", "so2"], format_func=POLLUTANT_LABEL.get)
    if not pick or not isinstance(rng, tuple) or len(rng) != 2:
        st.info("Chọn ít nhất một trạm và một khoảng ngày.")
        return
    start, end = (pd.Timestamp(x) for x in rng)

    conc = vn_daily(param)
    conc = conc[conc["location_id"].isin(pick) & (conc["date"] >= start) & (conc["date"] <= end) & conc["day_valid"]]
    st.subheader(f"{POLLUTANT_LABEL[param]} trung bình ngày")
    smooth = st.toggle("Làm mượt (trung bình trượt 7 ngày)", value=True)
    fig = go.Figure()
    for i, sid in enumerate(pick):                       # colour follows the station's slot in the pick
        s = conc[conc["location_id"] == sid].set_index("date")["value_mean"].sort_index()
        if s.empty:
            continue
        s = s.asfreq("D")
        y = s.rolling(7, min_periods=4).mean() if smooth else s
        fig.add_trace(go.Scatter(x=y.index, y=y, mode="lines", name=names.get(sid, sid),
                                 line=dict(width=2, color=SERIES[i]), connectgaps=False,
                                 hovertemplate="%{x|%d/%m/%Y}: %{y:.1f}<extra>" + str(names.get(sid, sid)) + "</extra>"))
    style(fig, height=380, yaxis_title=POLLUTANT_LABEL[param], hovermode="x unified",
          legend=dict(orientation="h", y=1.12))
    st.plotly_chart(fig, use_container_width=True)
    st.caption(UNIT_NOTE + " Chỉ ngày hợp lệ (≥ 18 giờ). Khoảng trống = không có dữ liệu.")

    # ---- station x month heatmap of median AQI
    st.subheader(f"Trung vị {std_label} theo tháng, mọi trạm")
    d = with_aqi[(with_aqi["date"] >= start) & (with_aqi["date"] <= end)]
    hm = d.assign(month=d["date"].dt.to_period("M").astype(str)).pivot_table(
        index="name", columns="month", values=aqi_col, aggfunc="median")
    hm = hm.loc[hm.notna().sum(axis=1).sort_values(ascending=False).index]
    edges = [0, 50, 100, 150, 200, 300, 500]
    cs = []
    for i, (_, _, col) in enumerate(BANDS[std]):
        cs += [[edges[i] / 500, col], [edges[i + 1] / 500, col]]
    fig = go.Figure(go.Heatmap(z=hm.values, x=hm.columns, y=hm.index, zmin=0, zmax=500, colorscale=cs,
                               xgap=1, ygap=1, showscale=False,
                               hovertemplate="%{y} · %{x}: trung vị %{z:.0f}<extra></extra>"))
    style(fig, height=max(300, 18 * len(hm) + 80), yaxis=dict(autorange="reversed", showgrid=False),
          xaxis=dict(showgrid=False))
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Màu theo mức AQI của chuẩn đang chọn. Ô trống = trạm không có ngày hợp lệ trong tháng.")

    # ---- days per band, per station
    st.subheader("Số ngày theo mức AQI trong khoảng đã chọn")
    tab = (d.assign(Mức=[band(v, std)[0] for v in d[aqi_col]]).pivot_table(
        index="name", columns="Mức", values=aqi_col, aggfunc="size", fill_value=0)
        .reindex(columns=band_names(std), fill_value=0))
    tab["Tổng ngày"] = tab.sum(axis=1)
    tab["% ngày > 100"] = (100 * tab[band_names(std)[2:]].sum(axis=1) / tab["Tổng ngày"]).round(1)
    tab.index.name = "Trạm"
    st.dataframe(tab.sort_values("Tổng ngày", ascending=False), use_container_width=True)
