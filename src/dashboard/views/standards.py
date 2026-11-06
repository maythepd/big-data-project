"""VN_AQI vs US EPA: the same day scored by both standards."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.common import (BANDS, NEUTRAL, POLLUTANT_LABEL, POLLUTANTS, band, style,
                                  vn_daily_aqi)


def render():
    st.title("So sánh chuẩn VN_AQI và US EPA")
    st.caption("Cùng một trạm-ngày, cùng nồng độ, tính theo hai bảng ngưỡng: QĐ 1459/QĐ-TCMT (Bảng 2, µg/m³) "
               "và EPA Technical Assistance Document 05/2026 (Table 6). VN chỉ có AQI chung khi có PM2.5 hoặc PM10.")
    d = vn_daily_aqi()
    both = d[d["aqi_vn"].notna() & d["aqi_us"].notna()]

    k = st.columns(4)
    k[0].metric("Trạm-ngày có cả hai AQI", f"{len(both):,}".replace(",", "."))
    k[1].metric("Trung vị US − VN", f"{(both['aqi_us'] - both['aqi_vn']).median():+.0f}")
    same = sum(band(u, "us")[0] == dict(zip([n for _, n, _ in BANDS['vn']], [n for _, n, _ in BANDS['us']]))
               .get(band(v, "vn")[0]) for u, v in zip(both["aqi_us"], both["aqi_vn"]))
    k[2].metric("Cùng cấp (1–6)", f"{100 * same / max(len(both), 1):.0f}%")
    k[3].metric("Chất chi phối giống nhau", f"{100 * (both['aqi_us_main'] == both['aqi_vn_main']).mean():.0f}%")

    a, b = st.columns(2)
    with a:
        st.subheader("AQI ngày: VN theo US")
        fig = go.Figure(go.Scattergl(x=both["aqi_vn"], y=both["aqi_us"], mode="markers",
                                     marker=dict(size=5, color=NEUTRAL, opacity=0.35),
                                     customdata=list(zip(both["name"], both["date"].dt.strftime("%d/%m/%Y"))),
                                     hovertemplate="%{customdata[0]} %{customdata[1]}<br>VN %{x} · US %{y}<extra></extra>"))
        top = float(max(both["aqi_vn"].max(), both["aqi_us"].max()))
        fig.add_trace(go.Scatter(x=[0, top], y=[0, top], mode="lines", line=dict(width=1, dash="dot", color="#8c8c8c"),
                                 hoverinfo="skip", showlegend=False))
        style(fig, height=420, xaxis=dict(title="VN_AQI", range=[0, 520]),
              yaxis=dict(title="US EPA AQI", range=[0, 520]), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        over = int((both["aqi_us"] > 520).sum())
        st.caption("Đường chấm: hai chuẩn bằng nhau. Điểm nằm trên đường = US cao hơn VN. "
                   f"Trục cắt ở 520: {over} trạm-ngày có AQI US > 520 (EPA ngoại suy trên 500, VN dừng ở 500).")
    with b:
        st.subheader("Cấp VN (hàng) × cấp US (cột)")
        vn_names = [n for _, n, _ in BANDS["vn"]]
        us_names = [n for _, n, _ in BANDS["us"]]
        m = pd.crosstab(pd.Categorical([band(v, "vn")[0] for v in both["aqi_vn"]], categories=vn_names),
                        pd.Categorical([band(v, "us")[0] for v in both["aqi_us"]], categories=us_names), dropna=False)
        fig = go.Figure(go.Heatmap(z=m.values, x=[f"{i + 1}. {n}" for i, n in enumerate(us_names)],
                                   y=[f"{i + 1}. {n}" for i, n in enumerate(vn_names)], colorscale="Blues",
                                   text=m.values, texttemplate="%{text}", xgap=2, ygap=2, showscale=False,
                                   hovertemplate="VN %{y} · US %{x}: %{z} trạm-ngày<extra></extra>"))
        style(fig, height=420, yaxis=dict(autorange="reversed", showgrid=False), xaxis=dict(showgrid=False, tickangle=-25))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Đường chéo = cùng cấp. Bên phải đường chéo = US xếp nặng hơn VN.")

    st.subheader("Chỉ số con theo chất: trung vị US − VN")
    rows = []
    for p in POLLUTANTS:
        pair = d[[f"aqi_us_{p}", f"aqi_vn_{p}"]].dropna()
        if len(pair):
            diff = pair[f"aqi_us_{p}"] - pair[f"aqi_vn_{p}"]
            rows.append({"Chất": POLLUTANT_LABEL[p], "Trạm-ngày": len(pair), "Trung vị US": pair[f"aqi_us_{p}"].median(),
                         "Trung vị VN": pair[f"aqi_vn_{p}"].median(), "Trung vị US − VN": diff.median(),
                         "% US > VN": round(100 * (diff > 0).mean(), 1)})
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    st.caption("PM2.5: ngưỡng US (sửa năm 2024: 9 µg/m³ ở mức 50) chặt hơn VN (25 µg/m³), nên cùng nồng độ "
               "thì AQI US cao hơn. Đây là khác biệt do chuẩn, không phải do dữ liệu.")
