"""Xu hướng & mùa vụ: long-term trend, AQI band shares per month, season, week and day cycles."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.common import (BANDS, NEUTRAL, band, band_colors, band_names, std_state, style,
                                  vn_daily_aqi, vn_hourly_aqi)

MONTHS_VI = ["Th1", "Th2", "Th3", "Th4", "Th5", "Th6", "Th7", "Th8", "Th9", "Th10", "Th11", "Th12"]
WEEKDAYS_VI = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]


def render():
    std, aqi_col, _, std_label = std_state()
    daily = vn_daily_aqi()
    daily = daily[daily[aqi_col].notna()]
    names = daily.groupby("location_id")["name"].first()
    days_per = daily.groupby("location_id").size().sort_values(ascending=False)

    st.title("Xu hướng & mùa vụ")
    scope = st.selectbox("Phạm vi", ["Tất cả trạm VN"] + days_per.index.tolist(),
                         format_func=lambda v: v if isinstance(v, str) else f"{names[v]} ({days_per[v]} ngày)")
    d = daily if isinstance(scope, str) else daily[daily["location_id"] == scope]
    st.caption("Mỗi trạm-ngày là một quan sát; khi gộp nhiều trạm, số trạm thay đổi theo thời gian "
               "(nhiều nhất cuối 2024), nên xu hướng dài hạn phản ánh cả thay đổi về độ phủ.")

    # ---- monthly trend + coverage (two charts, never a dual axis)
    d = d.assign(month=d["date"].dt.to_period("M").dt.to_timestamp())
    m = d.groupby("month").agg(median=(aqi_col, "median"), p90=(aqi_col, lambda s: s.quantile(0.9)),
                               stations=("location_id", "nunique"), days=(aqi_col, "size")).reset_index()
    c1, c2 = st.columns([3, 2])
    with c1:
        st.subheader(f"{std_label} theo tháng")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=m["month"], y=m["p90"], name="P90", mode="lines",
                                 line=dict(width=1, color=NEUTRAL, dash="dot"),
                                 hovertemplate="%{x|%m/%Y}: P90 %{y:.0f}<extra></extra>"))
        fig.add_trace(go.Scatter(x=m["month"], y=m["median"], name="Trung vị", mode="lines+markers",
                                 line=dict(width=2, color=NEUTRAL), marker=dict(size=6),
                                 hovertemplate="%{x|%m/%Y}: trung vị %{y:.0f}<extra></extra>"))
        style(fig, yaxis_title="AQI ngày", legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Số trạm có dữ liệu mỗi tháng")
        fig = go.Figure(go.Bar(x=m["month"], y=m["stations"], marker_color="#8c8c8c", marker_line_width=0,
                               hovertemplate="%{x|%m/%Y}: %{y} trạm<extra></extra>"))
        style(fig, yaxis_title="Trạm")
        st.plotly_chart(fig, use_container_width=True)

    # ---- share of days per band each month (100% stacked)
    st.subheader("Tỷ lệ ngày theo mức AQI mỗi tháng")
    d = d.assign(band=[band(v, std)[0] for v in d[aqi_col]])
    share = (d.groupby(["month", "band"]).size().unstack(fill_value=0)
             .reindex(columns=band_names(std), fill_value=0))
    pct = share.div(share.sum(axis=1), axis=0) * 100
    colors = band_colors(std)
    fig = go.Figure()
    for name in band_names(std):
        if share[name].sum() == 0:
            continue
        fig.add_trace(go.Bar(x=pct.index, y=pct[name], name=name, marker_color=colors[name],
                             marker_line=dict(width=0.5, color="rgba(255,255,255,0.6)"),
                             customdata=share[name],
                             hovertemplate="%{x|%m/%Y} · " + name + ": %{y:.0f}% (%{customdata} trạm-ngày)<extra></extra>"))
    style(fig, height=320, barmode="stack", yaxis=dict(title="% trạm-ngày", range=[0, 100]),
          legend=dict(orientation="h", y=1.12), bargap=0.15)
    st.plotly_chart(fig, use_container_width=True)

    # ---- seasonal / weekly / daily cycles
    a, b, c = st.columns(3)
    with a:
        st.subheader("Theo tháng trong năm")
        s = d.groupby(d["date"].dt.month)[aqi_col].agg(["median", "size"]).reindex(range(1, 13))
        fig = go.Figure(go.Bar(x=MONTHS_VI, y=s["median"],
                               marker_color=[band(v, std)[1] for v in s["median"]], marker_line_width=0,
                               customdata=s["size"],
                               hovertemplate="%{x}: trung vị %{y:.0f} (%{customdata} trạm-ngày)<extra></extra>"))
        style(fig, height=300, yaxis_title=f"Trung vị {std_label}")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Màu = mức của giá trị trung vị.")
    with b:
        st.subheader("Theo thứ trong tuần")
        s = d.groupby(d["date"].dt.dayofweek)[aqi_col].median().reindex(range(7))
        fig = go.Figure(go.Bar(x=WEEKDAYS_VI, y=s, marker_color=NEUTRAL, marker_line_width=0,
                               hovertemplate="%{x}: trung vị %{y:.0f}<extra></extra>"))
        style(fig, height=300, yaxis_title=f"Trung vị {std_label}")
        st.plotly_chart(fig, use_container_width=True)
    with c:
        st.subheader("Theo giờ trong ngày")
        h = vn_hourly_aqi()
        h = h[h[aqi_col].notna()] if isinstance(scope, str) else h[(h["location_id"] == scope) & h[aqi_col].notna()]
        s = h.groupby(h["hour_vn"].dt.hour)[aqi_col].median().reindex(range(24))
        fig = go.Figure(go.Scatter(x=list(range(24)), y=s, mode="lines+markers", line=dict(width=2, color=NEUTRAL),
                                   marker=dict(size=6), hovertemplate="%{x}:00 · trung vị %{y:.0f}<extra></extra>"))
        style(fig, height=300, yaxis_title=f"Trung vị {std_label} giờ", xaxis=dict(title="Giờ Việt Nam", dtick=3))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("AQI giờ: PM theo NowCast nên đỉnh trễ hơn nồng độ 1 giờ.")

    # ---- calendar heatmap: day x month for one year
    st.subheader("Lịch AQI theo ngày")
    years = sorted(d["date"].dt.year.unique())
    year = st.select_slider("Năm", options=years, value=years[-1] if len(years) else None)
    y = d[d["date"].dt.year == year]
    cal = y.groupby("date")[aqi_col].median()
    grid = pd.DataFrame({"date": pd.date_range(f"{year}-01-01", f"{year}-12-31")})
    grid["aqi"] = grid["date"].map(cal)
    grid["week"] = ((grid["date"] - pd.Timestamp(f"{year}-01-01")).dt.days + pd.Timestamp(f"{year}-01-01").dayofweek) // 7
    grid["dow"] = grid["date"].dt.dayofweek
    z = grid.pivot(index="dow", columns="week", values="aqi")
    txt = grid.pivot(index="dow", columns="week", values="date").map(lambda t: f"{t:%d/%m/%Y}" if pd.notna(t) else "")
    # discrete band colour scale on the 0-500 range
    edges = [0, 50, 100, 150, 200, 300, 500]
    cs = []
    for i, (_, _, col) in enumerate(BANDS[std]):
        lo, hi = edges[i] / 500, edges[i + 1] / 500
        cs += [[lo, col], [hi, col]]
    fig = go.Figure(go.Heatmap(z=z.values, x=z.columns, y=WEEKDAYS_VI, text=txt.values, zmin=0, zmax=500,
                               colorscale=cs, xgap=2, ygap=2, showscale=False,
                               hovertemplate="%{text}: trung vị AQI %{z:.0f}<extra></extra>"))
    style(fig, height=230, xaxis=dict(showticklabels=False, showgrid=False), yaxis=dict(autorange="reversed", showgrid=False))
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"Năm {year}: {cal.notna().sum()} ngày có dữ liệu. Ô trống = không có trạm nào đủ 18 giờ. "
               "Khi gộp nhiều trạm, mỗi ô là trung vị AQI của các trạm trong ngày.")

