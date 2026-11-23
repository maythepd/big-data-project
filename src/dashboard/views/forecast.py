"""Dự báo & cảnh báo sớm: AQI 24 giờ tới (GBT) and the next-24-h risk band (random forest).

Models: src/ml/aqi_forecast.py (Spark MLlib on Gold station_hourly_aqi); loaded by
src/serving/load_ml.py into ml_forecast, ml_warning, ml_backtest, ml_warning_backtest, ml_metrics.
"""

import math

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from src.dashboard.common import (BANDS, BASEMAP_LAYERS, NEUTRAL, SERIES, band, band_colors, band_names,
                                  ml_frame, ml_metrics, stations, std_state, style)

RUN_CMD = ("docker compose -f docker-compose.spark.yml run --rm spark-lake \\\n"
           "    /opt/spark/bin/spark-submit --driver-memory 4g src/ml/aqi_forecast.py\n"
           "python -m src.serving.load_ml")
FEATURE_VI = {"lag": "AQI {} giờ trước", "mean6": "TB 6 giờ qua", "mean24": "TB 24 giờ qua", "min24": "Thấp nhất 24 giờ qua",
              "max24": "Cao nhất 24 giờ qua", "d3": "Thay đổi trong 3 giờ", "d24": "Thay đổi trong 24 giờ",
              "pm25_1h": "PM2.5", "pm10_1h": "PM10", "o3_1h": "O₃", "no2_1h": "NO₂", "co_1h": "CO", "so2_1h": "SO₂",
              "hour_local": "Giờ trong ngày", "target_hour_local": "Giờ được dự báo", "dow": "Thứ trong tuần",
              "lat": "Vĩ độ", "lon": "Kinh độ", "h": "Số giờ tới"}
LEVELS = ["▲ Cảnh báo", "△ Theo dõi", "Bình thường"]


def feature_vi(name, std):
    if name == f"aqi_{std}":
        return "AQI hiện tại"
    key = name.removeprefix(f"{std}_")
    if key.startswith("lag"):
        return FEATURE_VI["lag"].format(key[3:])
    return FEATURE_VI.get(key, name)


def level(p, thr):
    return LEVELS[0] if p >= thr else LEVELS[1] if p >= thr / 2 else LEVELS[2]


def fmt_pct(v):
    return "—" if v is None or (isinstance(v, float) and math.isnan(v)) else f"{v:.0%}"


def band_backdrop(fig, std, top, row=None):
    """Faint AQI band stripes behind a chart, up to `top`."""
    lo = 0
    for hi, _, color in BANDS[std]:
        if lo >= top:
            break
        fig.add_hrect(y0=lo, y1=min(hi, top), fillcolor=color, opacity=0.08, line_width=0, layer="below",
                      **({"row": row, "col": 1} if row else {}))
        lo = hi


def station_map(per, std):
    fig = go.Figure()
    colors = band_colors(std)
    for name in band_names(std):
        part = per[per["band"] == name]
        if part.empty:
            continue
        size = part["level"].map({LEVELS[0]: 20, LEVELS[1]: 16}).fillna(13)
        fig.add_trace(go.Scattermapbox(lat=part["lat"], lon=part["lon"], mode="markers", showlegend=False,
                                       hoverinfo="skip", marker=dict(size=size + 4, color="#333333", opacity=0.9)))
        fig.add_trace(go.Scattermapbox(
            lat=part["lat"], lon=part["lon"], mode="markers", name=name, marker=dict(size=size, color=colors[name]),
            customdata=list(zip(part["name"], part["peak"].round(0), part["peak_vn"].dt.strftime("%H:%M %d/%m"),
                                part["p_warn"].map(fmt_pct), part["level"])),
            hovertemplate="<b>%{customdata[0]}</b><br>Cao nhất 24 giờ tới: AQI %{customdata[1]} lúc %{customdata[2]}"
                          "<br>Xác suất vượt ngưỡng: %{customdata[3]} · %{customdata[4]}<extra></extra>"))
    fig.update_layout(mapbox=dict(style="white-bg", layers=BASEMAP_LAYERS, center=dict(lat=16, lon=106), zoom=4.4),
                      height=470, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)",
                      legend=dict(title="Mức AQI cao nhất dự báo", orientation="h", y=-0.02))
    return fig


def render():
    std, aqi_col, _, std_label = std_state()
    st.title("Dự báo & cảnh báo sớm")
    mt = ml_metrics()
    if std not in mt:
        st.info("Chưa có kết quả mô hình trong MongoDB. Huấn luyện bằng Spark rồi nạp vào MongoDB:")
        st.code(RUN_CMD, language="bash")
        return
    run, m = mt["run"], mt[std]
    risk, thr, above = m["risk"], m["risk"]["threshold"], m["risk"]["warn_above"]
    d, sp = run["data"], run["split"]
    st.caption(f"Spark MLlib, lần chạy `{run['run_id']}`. Huấn luyện trên {d['rows']:,} trạm-giờ của "
               f"{d['stations']:,} trạm toàn cầu ({d['vn_stations']} trạm VN), {d['first_hour'][:10]} → "
               f"{d['last_hour'][:10]}; kiểm tra trên các giờ từ {sp['test_start'][:10]} mà mô hình chưa thấy. "
               "Dự báo xuất phát từ giờ cuối có dữ liệu của từng trạm.")

    fc, wn = ml_frame("ml_forecast", std), ml_frame("ml_warning", std)
    names = stations().set_index("location_id")
    peak = fc.loc[fc.groupby("location_id")["pred"].idxmax(), ["location_id", "pred", "target_vn"]]
    per = (wn.merge(peak.rename(columns={"pred": "peak", "target_vn": "peak_vn"}), on="location_id")
             .join(names[["name", "lat", "lon"]], on="location_id"))
    per["name"] = per["name"].fillna(per["location_id"].astype(str))
    per["band"] = [band(v, std)[0] for v in per["peak"]]
    per["level"] = [level(p, thr) for p in per["p_warn"]]
    per = per.sort_values(["p_warn", "peak"], ascending=False)

    # ---------------------------------------------------------------- 1. all stations
    st.header("Toàn bộ trạm: 24 giờ tới")
    k = st.columns(4)
    k[0].metric("Trạm cảnh báo", int((per["level"] == LEVELS[0]).sum()), help=f"Xác suất AQI vượt {above} ≥ {thr:.0%}")
    k[1].metric("Trạm theo dõi", int((per["level"] == LEVELS[1]).sum()), help=f"Xác suất ≥ {thr / 2:.0%}")
    top = per.sort_values("peak", ascending=False).iloc[0]
    k[2].metric("AQI dự báo cao nhất", f"{top['peak']:.0f}", help=f"{top['name']}, {top['peak_vn']:%H:%M %d/%m}")
    k[3].metric("Dự báo phát lúc", f"{per['issued_vn'].max():%H:%M %d/%m/%Y}")
    c1, c2 = st.columns([3, 2])
    with c1:
        st.plotly_chart(station_map(per, std), use_container_width=True)
        st.caption("Màu = mức AQI cao nhất dự báo trong 24 giờ tới; điểm lớn hơn = đang cảnh báo hoặc theo dõi.")
    with c2:
        table = per.assign(issued=per["issued_vn"].dt.strftime("%H:%M %d/%m"))[
            ["name", "level", "p_warn", "aqi_now", "peak", "band", "issued"]]
        st.dataframe(table, hide_index=True, use_container_width=True, height=470, column_config={
            "name": "Trạm", "level": "Cảnh báo",
            "p_warn": st.column_config.ProgressColumn(f"P(AQI > {above})", min_value=0, max_value=1, format="percent"),
            "aqi_now": st.column_config.NumberColumn("AQI lúc phát", format="%.0f"),
            "peak": st.column_config.NumberColumn("Cao nhất dự báo", format="%.0f"),
            "band": "Mức", "issued": "Phát lúc"})

    # ---------------------------------------------------------------- 2. one station
    st.header("Một trạm")
    sel = st.selectbox("Trạm", per["location_id"].tolist(), format_func=lambda i: per.set_index("location_id").at[i, "name"])
    row = per.set_index("location_id").loc[sel]
    f = fc[fc["location_id"] == sel].sort_values("h")
    k = st.columns(3)
    k[0].metric("AQI lúc phát", f"{row['aqi_now']:.0f}", help=band(row["aqi_now"], std)[0])
    k[1].metric("Cao nhất 24 giờ tới", f"{row['peak']:.0f}", help=f"{row['band']}, lúc {row['peak_vn']:%H:%M %d/%m}")
    k[2].metric(f"Xác suất vượt {above}", fmt_pct(row["p_warn"]), help=row["level"])
    a, b = st.columns([3, 2])
    with a:
        st.subheader(f"{std_label}: 48 giờ qua và 24 giờ tới")
        h = ml_frame("ml_recent", std)
        h = h[h["location_id"] == sel].sort_values("hour_vn").rename(columns={"aqi": aqi_col})
        fig = go.Figure()
        x_band = pd.concat([f["target_vn"], f["target_vn"][::-1]])
        fig.add_trace(go.Scatter(x=x_band, y=pd.concat([f["hi"], f["lo"][::-1]]), fill="toself", line_width=0,
                                 fillcolor="rgba(42,120,214,0.18)", name="Khoảng 10–90%", hoverinfo="skip"))
        fig.add_trace(go.Scatter(x=h["hour_vn"], y=h[aqi_col], mode="lines", name="Đo được",
                                 line=dict(width=2, color=NEUTRAL), hovertemplate="%{x|%H:%M %d/%m}: AQI %{y:.0f}<extra></extra>"))
        fig.add_trace(go.Scatter(x=[row["issued_vn"]] + f["target_vn"].tolist(), y=[row["aqi_now"]] + f["pred"].tolist(),
                                 mode="lines+markers", name="Dự báo", line=dict(width=2, color=SERIES[0], dash="dash"),
                                 marker=dict(size=6), hovertemplate="%{x|%H:%M %d/%m}: dự báo %{y:.0f}<extra></extra>"))
        top_y = max(f["hi"].max(), h[aqi_col].max() if len(h) else 0, 60) * 1.1
        band_backdrop(fig, std, top_y)
        fig.add_vline(x=row["issued_vn"], line=dict(width=1, color="#8c8c8c", dash="dot"))
        style(fig, height=360, yaxis=dict(title="AQI", range=[0, top_y]), legend=dict(orientation="h", y=1.12))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Khoảng 10–90%: sai số thực tế của mô hình ở cùng số giờ tới trên tập kiểm tra. Nền = các mức AQI.")
    with b:
        st.subheader("Mức AQI cao nhất 24 giờ tới")
        probs = list(row["band_probs"]) + [0.0] * (len(BANDS[std]) - len(row["band_probs"]))
        fig = go.Figure(go.Bar(x=band_names(std), y=probs, marker_color=[c for _, _, c in BANDS[std]],
                               marker_line=dict(width=1, color="#555555"),
                               hovertemplate="%{x}: %{y:.0%}<extra></extra>"))
        style(fig, height=360, yaxis=dict(title="Xác suất", tickformat=".0%", range=[0, 1]))
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Random forest phân loại mức của giờ tệ nhất trong 24 giờ tới. Cảnh báo khi tổng xác suất các mức "
                   f"trên {above} ≥ {thr:.0%} (ngưỡng chọn trên tập kiểm định, ưu tiên không bỏ sót).")

    # ---------------------------------------------------------------- 3. evaluation
    st.header("Mô hình đúng đến đâu")
    scope = st.radio("Đánh giá trên", ["vn", "all"], horizontal=True,
                     format_func=lambda s: "Trạm Việt Nam" if s == "vn" else "Tất cả trạm (mẫu)")
    bh = pd.DataFrame(m["forecast"]["by_h"])
    bh = bh[bh["scope"] == scope].sort_values("h")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Sai số dự báo theo số giờ tới")
        fig = go.Figure()
        for col, label, color, dash in [("mae", "Mô hình (GBT)", SERIES[0], "solid"),
                                        ("mae_persistence", "Giữ nguyên như hiện tại", SERIES[1], "dot"),
                                        ("mae_mean24", "Trung bình 24 giờ qua", SERIES[2], "dash")]:
            fig.add_trace(go.Scatter(x=bh["h"], y=bh[col], name=label, mode="lines+markers",
                                     line=dict(width=2, color=color, dash=dash), marker=dict(size=6),
                                     hovertemplate="%{x} giờ tới: sai số %{y:.1f}<extra>" + label + "</extra>"))
        style(fig, height=340, xaxis=dict(title="Số giờ tới", dtick=3),
              yaxis=dict(title="Sai số tuyệt đối TB (AQI)", rangemode="tozero"), legend=dict(orientation="h", y=1.15))
        st.plotly_chart(fig, use_container_width=True)
        if len(bh):
            last = bh.iloc[-1]
            gain = 1 - last["mae"] / last["mae_persistence"] if last["mae_persistence"] else 0
            st.caption(f"Ở {int(last['h'])} giờ tới: sai số {last['mae']:.1f} so với {last['mae_persistence']:.1f} khi "
                       f"giả định AQI giữ nguyên ({'thấp' if gain >= 0 else 'cao'} hơn {abs(gain):.0%}), "
                       f"trên {int(last['n']):,} lần dự báo.")
    with c2:
        st.subheader("Dự báo và thực tế trên giai đoạn kiểm tra")
        bt = ml_frame("ml_backtest", std)
        hsel = st.radio("Dự báo trước", [1, 6, 24], index=1, horizontal=True, format_func=lambda v: f"{v} giờ")
        bt = bt[(bt["location_id"] == sel) & (bt["h"] == hsel)].sort_values("target_vn")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=bt["target_vn"], y=bt["actual"], name="Đo được", mode="lines",
                                 line=dict(width=2, color=NEUTRAL), hovertemplate="%{x|%H:%M %d/%m}: %{y:.0f}<extra>Đo được</extra>"))
        fig.add_trace(go.Scatter(x=bt["target_vn"], y=bt["pred"], name=f"Dự báo trước {hsel} giờ", mode="lines",
                                 line=dict(width=2, color=SERIES[0]),
                                 hovertemplate="%{x|%H:%M %d/%m}: %{y:.0f}<extra>Dự báo</extra>"))
        style(fig, height=340, yaxis=dict(title="AQI", rangemode="tozero"), legend=dict(orientation="h", y=1.15))
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"{row['name']}: các giờ này không dùng khi huấn luyện." if len(bt)
                   else "Trạm này không có giờ nào trong giai đoạn kiểm tra.")

    st.subheader(f"Cảnh báo sớm: AQI vượt {above} trong 24 giờ tới")
    w, base, lead = risk["warning"].get(scope, {}), risk["baseline"].get(scope, {}), risk["lead"]
    rows = [dict(cach="Mô hình (random forest)", precision=w.get("precision"), recall=w.get("recall"), f1=w.get("f1")),
            dict(cach=f"Quy tắc: 24 giờ qua đã vượt {above}", precision=base.get("precision"), recall=base.get("recall"),
                 f1=base.get("f1"))]
    if scope == "vn":
        for r, key in zip(rows, ("model", "baseline")):
            r["episodes"] = f"{lead[key]['warned']}/{lead[key]['episodes']}"
            r["lead"] = lead[key]["median_lead_h"]
    c1, c2 = st.columns([3, 2])
    with c1:
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True, column_config={
            "cach": "Cách cảnh báo",
            "precision": st.column_config.NumberColumn("Đúng khi báo", format="percent", help="precision"),
            "recall": st.column_config.NumberColumn("Bắt được", format="percent", help="recall: giờ có vượt ngưỡng phía trước"),
            "f1": st.column_config.NumberColumn("F1", format="%.2f"),
            "episodes": st.column_config.TextColumn("Đợt ô nhiễm được báo trước", help="đợt bắt đầu khi AQI vượt ngưỡng"),
            "lead": st.column_config.NumberColumn("Báo trước (giờ, trung vị)", format="%.0f")})
        st.caption(f"Ngưỡng xác suất {thr:.0%}, chọn trên 3 ngày kiểm định ({'trạm VN' if risk.get('threshold_scope') == 'vn' else 'mọi trạm'}) "
                   "theo F2: bỏ sót một đợt ô nhiễm bị tính nặng hơn một lần báo nhầm.")
    with c2:
        cv = pd.DataFrame(risk["curve"].get(scope, []))
        fig = go.Figure()
        for col, label, color in [("precision", "Đúng khi báo", SERIES[0]), ("recall", "Bắt được", SERIES[1])]:
            if col in cv:
                fig.add_trace(go.Scatter(x=cv["threshold"], y=cv[col], name=label, mode="lines",
                                         line=dict(width=2, color=color),
                                         hovertemplate="ngưỡng %{x:.0%}: %{y:.0%}<extra>" + label + "</extra>"))
        fig.add_vline(x=thr, line=dict(width=1, color="#8c8c8c", dash="dot"))
        style(fig, height=280, xaxis=dict(title="Ngưỡng xác suất", tickformat=".0%"),
              yaxis=dict(tickformat=".0%", range=[0, 1]), legend=dict(orientation="h", y=1.2))
        st.plotly_chart(fig, use_container_width=True)

    wb = ml_frame("ml_warning_backtest", std)
    wb = wb[wb["location_id"] == sel].sort_values("hour_vn")
    if len(wb):
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.6, 0.4], vertical_spacing=0.06)
        fig.add_trace(go.Scatter(x=wb["hour_vn"], y=wb["aqi"], name="AQI đo được", mode="lines",
                                 line=dict(width=2, color=NEUTRAL), hovertemplate="%{x|%H:%M %d/%m}: AQI %{y:.0f}<extra></extra>"),
                      row=1, col=1)
        fig.add_hline(y=above, line=dict(width=1, color="#ff0000", dash="dash"), row=1, col=1)
        fig.add_trace(go.Scatter(x=wb["hour_vn"], y=wb["p_warn"], name="Xác suất vượt", mode="lines",
                                 line=dict(width=2, color=SERIES[0]), hovertemplate="%{x|%H:%M %d/%m}: %{y:.0%}<extra></extra>"),
                      row=2, col=1)
        on = wb[wb["warn"].astype(bool)]
        fig.add_trace(go.Scatter(x=on["hour_vn"], y=on["p_warn"], name="Giờ phát cảnh báo", mode="markers",
                                 marker=dict(size=8, color=SERIES[1], symbol="triangle-up"),
                                 hovertemplate="%{x|%H:%M %d/%m}: cảnh báo<extra></extra>"), row=2, col=1)
        fig.add_hline(y=thr, line=dict(width=1, color="#8c8c8c", dash="dot"), row=2, col=1)
        style(fig, height=380, legend=dict(orientation="h", y=1.1))
        fig.update_yaxes(title_text="AQI", rangemode="tozero", row=1, col=1)
        fig.update_yaxes(title_text="Xác suất", tickformat=".0%", range=[0, 1], row=2, col=1)
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Giai đoạn kiểm tra, trạm đã chọn: trên, AQI đo được và ngưỡng {above}; dưới, xác suất mô hình đưa ra "
                   "mỗi giờ cho 24 giờ sau đó. Cảnh báo tốt xuất hiện trước khi đường AQI vượt ngưỡng.")

    c1, c2, c3 = st.columns(3)
    conf = risk["confusion"].get(scope)
    with c1:
        st.subheader("Mức dự đoán và mức thật")
        if conf:
            mat = pd.DataFrame(conf, index=band_names(std), columns=band_names(std))
            keep = mat.sum(axis=1) > 0
            pct = mat.div(mat.sum(axis=1).where(keep), axis=0).loc[keep]
            fig = go.Figure(go.Heatmap(z=pct.values, x=pct.columns, y=pct.index, colorscale="Blues", zmin=0, zmax=1,
                                       text=mat.loc[keep].values, xgap=2, ygap=2, showscale=False,
                                       hovertemplate="thật %{y}, dự đoán %{x}: %{z:.0%} (%{text})<extra></extra>"))
            style(fig, height=320, xaxis=dict(title="Dự đoán", showgrid=False),
                  yaxis=dict(title="Thật", autorange="reversed", showgrid=False))
            st.plotly_chart(fig, use_container_width=True)
            sc = risk["bands"][scope]
            st.caption(f"Đúng mức {sc['accuracy']:.0%}, F1 trung bình các mức {sc['macro_f1']:.2f} "
                       f"({sc['n']:,} giờ). Mỗi hàng cộng lại 100%.")
    for col, (title, imp) in zip((c2, c3), [("Dự báo dựa vào gì", m["forecast"]["importances"]),
                                            ("Cảnh báo dựa vào gì", risk["importances"])]):
        with col:
            st.subheader(title)
            top = pd.DataFrame(imp[:10], columns=["f", "v"])[::-1]
            fig = go.Figure(go.Bar(x=top["v"], y=[feature_vi(n, std) for n in top["f"]], orientation="h",
                                   marker_color=NEUTRAL, marker_line_width=0,
                                   hovertemplate="%{y}: %{x:.1%}<extra></extra>"))
            style(fig, height=320, xaxis=dict(title="Mức quan trọng", tickformat=".0%"))
            st.plotly_chart(fig, use_container_width=True)
    comparison_block(mt.get("comparison"), std, above)


MODELS = [("spark", "Spark MLlib (GBT / random forest)", SERIES[0]), ("xgboost", "XGBoost (GPU)", SERIES[1]),
          ("lstm", "LSTM nhẹ (GPU)", SERIES[2])]


def comparison_block(cmp, std, above):
    """Spark models next to XGBoost and an LSTM on the same Vietnam test hours (src/ml/compare_models.py)."""
    st.header("So sánh với XGBoost và LSTM")
    if not cmp or std not in cmp.get("std", {}):
        st.info("Chưa có so sánh. Chạy `python -m src.ml.compare_models` sau khi huấn luyện, rồi nạp lại "
                "`python -m src.serving.load_ml`.")
        return
    r = cmp["std"][std]
    st.caption("Cùng dữ liệu, cùng đặc trưng (LSTM đọc trực tiếp 24 giờ gần nhất), cùng cách chia thời gian; "
               f"đánh giá trên các giờ kiểm tra của trạm Việt Nam. XGBoost và LSTM chạy trên GPU ({cmp['device']}).")
    c1, c2 = st.columns([3, 2])
    with c1:
        fig = go.Figure()
        first = None
        for key, label, color in MODELS:
            if key not in r:
                continue
            f = r[key]["forecast"] if key != "lstm" else r[key]
            bh = pd.DataFrame(f["by_h"]).sort_values("h")
            first = bh if first is None else first
            fig.add_trace(go.Scatter(x=bh["h"], y=bh["mae"], name=label, mode="lines+markers",
                                     line=dict(width=2, color=color), marker=dict(size=6),
                                     hovertemplate="%{x} giờ tới: %{y:.1f}<extra>" + label + "</extra>"))
        if first is not None:
            fig.add_trace(go.Scatter(x=first["h"], y=first["mae_persistence"], name="Giữ nguyên như hiện tại",
                                     mode="lines", line=dict(width=2, color="#8c8c8c", dash="dot"),
                                     hovertemplate="%{x} giờ tới: %{y:.1f}<extra>Giữ nguyên</extra>"))
        style(fig, height=340, xaxis=dict(title="Số giờ tới", dtick=3),
              yaxis=dict(title="Sai số tuyệt đối TB (AQI)", rangemode="tozero"), legend=dict(orientation="h", y=1.15))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        rows = []
        for key, label, _ in MODELS:
            if key not in r:
                continue
            f = r[key]["forecast"] if key != "lstm" else r[key]
            bh = {b["h"]: b["mae"] for b in f["by_h"]}
            w = r[key]["warning"]
            ld = w.get("lead") or {}
            rows.append(dict(model=label, h1=bh.get(1), h6=bh.get(6), h24=bh.get(24),
                             f1=(w.get("vn") or {}).get("f1"), recall=(w.get("vn") or {}).get("recall"),
                             episodes=f"{ld.get('warned', '—')}/{ld.get('episodes', '—')}",
                             seconds=f.get("seconds")))
        base = r["spark"].get("baseline_warning", {})
        rows.append(dict(model=f"Quy tắc: 24 giờ qua đã vượt {above}", f1=(base.get("vn") or {}).get("f1"),
                         recall=(base.get("vn") or {}).get("recall"),
                         episodes=f"{(base.get('lead') or {}).get('warned', '—')}/{(base.get('lead') or {}).get('episodes', '—')}"))
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True, column_config={
            "model": "Mô hình",
            "h1": st.column_config.NumberColumn("Sai số 1 giờ", format="%.1f"),
            "h6": st.column_config.NumberColumn("Sai số 6 giờ", format="%.1f"),
            "h24": st.column_config.NumberColumn("Sai số 24 giờ", format="%.1f"),
            "f1": st.column_config.NumberColumn("F1 cảnh báo", format="%.2f"),
            "recall": st.column_config.NumberColumn("Bắt được", format="percent"),
            "episodes": st.column_config.TextColumn("Đợt được báo trước"),
            "seconds": st.column_config.NumberColumn("Huấn luyện (giây)", format="%d")})
        st.caption("Cảnh báo của LSTM: giờ cao nhất trong 24 giờ dự báo vượt ngưỡng. Thời gian huấn luyện "
                   "không so trực tiếp được: Spark chạy CPU trong Docker, XGBoost và LSTM chạy GPU.")
