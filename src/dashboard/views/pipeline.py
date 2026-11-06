"""Dữ liệu & pipeline: how the numbers on the other pages were produced, and their quality."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.common import NEUTRAL, SERIES, coll_sizes, collection, style

STATUS_COLORS = {"valid": "#1baf7a", "suspect": "#eda100", "invalid": "#e34948"}   # reserved status roles
STATUS_VI = {"valid": "Hợp lệ", "suspect": "Nghi vấn", "invalid": "Không hợp lệ"}


def fmt(n):
    n = float(n or 0)
    for div, unit, dec in ((1e9, " tỷ", 2), (1e6, " triệu", 1), (1e3, " nghìn", 1)):
        if abs(n) >= div:
            return f"{n / div:.{dec}f}".replace(".", ",") + unit
    return f"{n:.0f}"


def render():
    st.title("Dữ liệu & pipeline")
    st.markdown(
        "**Luồng dữ liệu (Lambda):** OpenAQ S3 (lịch sử) → Bronze → Near-Silver → **Silver** (sensor × thời điểm, "
        "luật R01–R14) → **Gold** (trạm × giờ, AQI, bảng ngày) → **MongoDB** (serving) → dashboard. "
        "Speed layer: API → Kafka → Spark Structured Streaming → `latest_readings` + Silver stream.")

    runs = collection("pipeline_runs", sort="order")
    monthly = collection("pipeline_monthly", sort="month")
    if runs.empty or monthly.empty:
        st.warning("Chưa có collection tổng hợp: chạy `python3 src/serving/load_dashboard_summaries.py`.")
        return
    nums = {r["stage"]: r["numbers"] for _, r in runs.iterrows()}
    silver = nums.get("Silver v1.2", {})
    gold1 = nums.get("Gold lượt 1 (station_hourly)", {})
    gold2 = nums.get("Gold lượt 2 (AQI, bảng ngày)", {})

    k = st.columns(5)
    k[0].metric("Dòng compact (đầu vào Silver)", fmt(silver.get("compact_rows", 0)))
    k[1].metric("Dòng Silver", fmt(silver.get("silver_rows", 0)),
                help=f"Đã loại {fmt(silver.get('dedup_removed', 0))} dòng trùng")
    k[2].metric("Gold station_hourly", fmt(gold1.get("station_hours", 0)))
    k[3].metric("Gold AQI giờ", fmt(gold2.get("station_hourly_aqi", 0)),
                help=f"AQI ngày: {fmt(gold2.get('station_daily_aqi', 0))}")
    sizes = coll_sizes()
    k[4].metric("Document trong MongoDB", fmt(sizes["documents"].sum()) if len(sizes) else "—")

    st.subheader("Các bước đã chạy")
    tab = runs[["stage", "run_id", "ok", "finished_utc"]].copy()
    tab["ok"] = tab["ok"].map({True: "Đạt", False: "Không đạt", None: "—"})
    tab.columns = ["Bước", "Run", "Kiểm định", "Xong lúc (UTC)"]
    st.dataframe(tab, hide_index=True, use_container_width=True)

    # ---- Silver quality over time
    monthly["month"] = pd.to_datetime(monthly["month"])
    st.subheader("Silver theo tháng: dòng hợp lệ / nghi vấn / không hợp lệ")
    mode = st.radio("Hiển thị", ["Số dòng", "Tỷ lệ %"], horizontal=True)
    m = monthly.assign(suspect=monthly["suspect_allowed"] + monthly["suspect_excluded"])
    fig = go.Figure()
    for s in ("valid", "suspect", "invalid"):
        y = m[s] / m["silver_rows"] * 100 if mode == "Tỷ lệ %" else m[s] / 1e6
        fig.add_trace(go.Bar(x=m["month"], y=y, name=STATUS_VI[s], marker_color=STATUS_COLORS[s], marker_line_width=0,
                             hovertemplate="%{x|%m/%Y}: %{y:.2f}" + ("%" if mode == "Tỷ lệ %" else " triệu") +
                                           "<extra>" + STATUS_VI[s] + "</extra>"))
    style(fig, height=340, barmode="stack", bargap=0.1, legend=dict(orientation="h", y=1.12),
          yaxis_title="% dòng" if mode == "Tỷ lệ %" else "Triệu dòng")
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Nghi vấn = cờ mềm (đơn vị, outlier theo thời gian R14…). Gold chỉ dùng hợp lệ + 2 cờ được phép "
               "(`unit_relabeled`, `negative_small`).")

    a, b = st.columns(2)
    with a:
        st.subheader("Chất lượng theo chất (Silver)")
        ps = collection("parameter_status")
        top = ps.groupby("parameter")["rows"].sum().sort_values(ascending=False).head(14).index
        ps = ps[ps["parameter"].isin(top)]
        piv = ps.pivot_table(index="parameter", columns="dq_status", values="rows", aggfunc="sum", fill_value=0)
        piv = piv.loc[top[::-1]]
        pct = piv.div(piv.sum(axis=1), axis=0) * 100
        fig = go.Figure()
        for s in ("valid", "suspect", "invalid"):
            if s in pct:
                fig.add_trace(go.Bar(y=pct.index, x=pct[s], orientation="h", name=STATUS_VI[s],
                                     marker_color=STATUS_COLORS[s], marker_line_width=0, customdata=piv[s],
                                     hovertemplate="%{y}: %{x:.1f}% (%{customdata:,} dòng)<extra>" + STATUS_VI[s] + "</extra>"))
        style(fig, height=420, barmode="stack", xaxis=dict(title="% dòng", range=[0, 100]),
              legend=dict(orientation="h", y=1.08))
        st.plotly_chart(fig, use_container_width=True)
    with b:
        st.subheader("Gold: trạm-giờ mỗi tháng")
        fig = go.Figure(go.Bar(x=monthly["month"], y=monthly["station_hours"] / 1e6, marker_color=NEUTRAL,
                               marker_line_width=0, hovertemplate="%{x|%m/%Y}: %{y:.1f} triệu<extra></extra>"))
        style(fig, height=200, yaxis_title="Triệu trạm-giờ")
        st.plotly_chart(fig, use_container_width=True)
        st.subheader("Gold: trạm-giờ của Việt Nam")
        fig = go.Figure(go.Bar(x=monthly["month"], y=monthly["vn_station_hours"], marker_color=SERIES[1],
                               marker_line_width=0, hovertemplate="%{x|%m/%Y}: %{y:,}<extra></extra>"))
        style(fig, height=200, yaxis_title="Trạm-giờ")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("MongoDB serving")
    if len(sizes):
        s = sizes.copy()
        s["Loại"] = s.pop("time_series").map({True: "time series", False: "thường"})
        for c in ("bson_mb", "disk_mb", "index_mb"):
            s[c] = s[c].round(1)
        s.columns = ["Collection", "Document", "BSON (MB)", "Trên đĩa (MB)", "Index (MB)", "Loại"]
        st.dataframe(s, hide_index=True, use_container_width=True)
    bench = nums.get("Đo nạp 1 tháng toàn cầu theo giờ")
    if bench:
        c = st.columns(4)
        c[0].metric("Nạp 1 tháng toàn cầu theo giờ", f"{bench['seconds']:.0f} giây",
                    help=f"{fmt(bench['rows'])} document (2024-11)")
        c[1].metric("Tốc độ", f"{bench['docs_per_second'] / 1000:.0f}k doc/s")
        c[2].metric("Parquet → Mongo trên đĩa", f"{bench['parquet_bytes'] / 1e6:.0f} → {bench['mongo_disk_bytes'] / 1e6:.0f} MB")
        c[3].metric("Thường → time series (VN giờ)",
                    f"{bench['regular_vn_disk_index_bytes'] / 1e6:.1f} → {bench['timeseries_vn_disk_index_bytes'] / 1e6:.1f} MB")
