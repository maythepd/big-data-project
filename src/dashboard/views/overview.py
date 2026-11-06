"""Tổng quan: one day across Vietnam (map, ranking, headline numbers) and one station."""

from datetime import timedelta
import math

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.common import (BANDS, NEUTRAL, NO_DATA, POLLUTANT_LABEL, POLLUTANTS, UNIT_NOTE,
                                  band, band_colors, band_names, stations, std_state, style,
                                  vn_daily_aqi, vn_hourly_aqi)


def map_view(pts):
    span = max(pts["lat"].max() - pts["lat"].min(), pts["lon"].max() - pts["lon"].min(), 0.3)
    # Plotly's map zoom z shows 360 / 2^z degrees per 512 px; keep the span within
    # ~1/1.3 of the ~420 px of map height: 2^z = 360 * 420 / (512 * 1.3 * span)
    zoom = float(min(11.0, max(4.0, math.log2(227 / span))))
    return dict(lat=float((pts["lat"].max() + pts["lat"].min()) / 2),
                lon=float((pts["lon"].max() + pts["lon"].min()) / 2)), zoom


def aqi_map(view, std, aqi_col, main_col, height=560):
    fig = go.Figure()
    colors = band_colors(std)
    for name in band_names(std) + [NO_DATA[0]]:
        part = view[view["band"] == name]
        if part.empty:
            continue
        size = 14 if name != NO_DATA[0] else 9
        aqi_txt = part[aqi_col].map(lambda v: "—" if pd.isna(v) else str(int(v)))
        main_txt = part[main_col].map(lambda v: POLLUTANT_LABEL.get(v, "—") if isinstance(v, str) else "—")
        fig.add_trace(go.Scattermapbox(      # dark ring under each marker (yellow on a light map)
            lat=part["lat"], lon=part["lon"], mode="markers", showlegend=False, hoverinfo="skip",
            marker=dict(size=size + 4, color="#333333", opacity=0.9)))
        fig.add_trace(go.Scattermapbox(
            lat=part["lat"], lon=part["lon"], mode="markers", name=name,
            marker=dict(size=size, color=colors[name], opacity=1),
            customdata=list(zip(part["name"], aqi_txt, main_txt)),
            hovertemplate="<b>%{customdata[0]}</b><br>AQI %{customdata[1]} (" + name +
                          ")<br>Chất chi phối: %{customdata[2]}<extra></extra>"))
    has = view[view[aqi_col].notna()]
    center, zoom = map_view(has if len(has) else view)
    fig.update_layout(mapbox=dict(style="open-street-map", center=center, zoom=zoom), height=height,
                      margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)",
                      legend=dict(title="Mức AQI", orientation="h", y=-0.02))
    return fig


def render():
    std, aqi_col, main_col, std_label = std_state()
    daily = vn_daily_aqi()
    st_df = stations()
    counts = daily[daily[aqi_col].notna()].groupby("date").size()
    # default: latest day with >= 75% of the best coverage (best days are when stations started)
    default_day = counts[counts >= 0.75 * counts.max()].index.max().date()

    st.title("Tổng quan chất lượng không khí Việt Nam")
    c_day, c_info = st.columns([1, 3])
    day = c_day.date_input("Ngày (giờ địa phương)", value=st.session_state.get("day", default_day),
                           min_value=counts.index.min().date(), max_value=counts.index.max().date(),
                           format="DD/MM/YYYY", key="day")
    c_info.caption(f"Mặc định {default_day:%d/%m/%Y}: ngày gần nhất có ≥ 75% số trạm của ngày đông nhất "
                   f"({counts.max()} trạm). Có AQI từ {counts.index.min():%d/%m/%Y} đến "
                   f"{counts.index.max():%d/%m/%Y}. Ngày tính từ 01:00 đến 24:00 giờ địa phương.")

    today = daily[daily["date"].dt.date == day]
    view = st_df.merge(today.drop(columns=["name"]), on="location_id", how="left")
    view["band"] = [band(v, std)[0] for v in view[aqi_col]]
    has = view[view[aqi_col].notna()]

    prev = daily[daily["date"].dt.date == day - timedelta(days=1)]
    prev_med = prev[aqi_col].median() if len(prev) else None
    k = st.columns(5)
    k[0].metric("Trạm có AQI ngày", f"{len(has)} / {len(st_df)}")
    if len(has):
        worst = has.loc[has[aqi_col].idxmax()]
        med = has[aqi_col].median()
        k[1].metric(f"Trung vị {std_label}", int(med),
                    delta=None if prev_med is None or pd.isna(prev_med) else int(med - prev_med),
                    delta_color="inverse", help="So với ngày hôm trước")
        k[2].metric("Cao nhất", int(worst[aqi_col]), help=worst["name"])
        k[2].caption(f"{worst['name']} · {worst['band']}")
        bad = (has[aqi_col] > 100).sum()
        k[3].metric("Trạm vượt mức Trung bình (> 100)", f"{bad} / {len(has)}")
        main = has[main_col].value_counts()
        k[4].metric("Chất chi phối nhiều nhất", POLLUTANT_LABEL.get(main.index[0], main.index[0]),
                    help=f"{main.iloc[0]} / {len(has)} trạm")
    else:
        k[1].info("Không có trạm nào đủ 18 giờ dữ liệu trong ngày này.")

    left, right = st.columns([3, 2])
    with left:
        st.subheader(f"Bản đồ {std_label} ngày {day:%d/%m/%Y}")
        st.plotly_chart(aqi_map(view, std, aqi_col, main_col), use_container_width=True)
    with right:
        st.subheader("Phân bố mức AQI")
        dist = has["band"].value_counts().reindex(band_names(std)).fillna(0)
        colors = band_colors(std)
        fig = go.Figure(go.Bar(x=dist.values, y=dist.index, orientation="h",
                               marker_color=[colors[n] for n in dist.index], marker_line_width=0,
                               text=[int(v) if v else "" for v in dist.values], textposition="outside",
                               hovertemplate="%{y}: %{x} trạm<extra></extra>"))
        style(fig, height=230, xaxis_title="Số trạm", yaxis=dict(autorange="reversed"), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.subheader("Xếp hạng trạm")
        if len(has):
            rank = has.sort_values(aqi_col, ascending=False)[["name", aqi_col, "band", main_col]]
            rank.columns = ["Trạm", "AQI", "Mức", "Chất chi phối"]
            rank["Chất chi phối"] = rank["Chất chi phối"].map(lambda v: POLLUTANT_LABEL.get(v, v))
            st.dataframe(rank, hide_index=True, use_container_width=True, height=300)

    # ---- one station
    st.divider()
    ids = has.sort_values(aqi_col, ascending=False)["location_id"].tolist() or st_df["location_id"].tolist()
    names = st_df.set_index("location_id")["name"].to_dict()
    sid = st.selectbox("Chi tiết trạm", ids, format_func=lambda i: f"{names.get(i, i)} (#{i})")
    a, b = st.columns(2)
    with a:
        st.markdown(f"**{std_label} theo ngày, 365 ngày đến {day:%d/%m/%Y}**")
        s = daily[(daily["location_id"] == sid) & daily[aqi_col].notna()
                  & (daily["date"].dt.date > day - timedelta(days=365)) & (daily["date"].dt.date <= day)]
        if s.empty:
            st.write("Trạm không có AQI ngày trong khoảng này.")
        else:
            bands = [band(v, std) for v in s[aqi_col]]
            fig = go.Figure(go.Bar(x=s["date"], y=s[aqi_col], marker_color=[c for _, c in bands],
                                   marker_line_width=0, customdata=[n for n, _ in bands],
                                   hovertemplate="%{x|%d/%m/%Y}: AQI %{y} (%{customdata})<extra></extra>"))
            fig.update_traces(width=86_400_000 * 0.9)
            style(fig, yaxis_title="AQI", xaxis=dict(type="date", range=[pd.Timestamp(day - timedelta(days=365)),
                                                                         pd.Timestamp(day + timedelta(days=1))]))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{len(s)} ngày có AQI trong 365 ngày.")
    with b:
        st.markdown(f"**{std_label} theo giờ, 7 ngày đến {day:%d/%m/%Y} (giờ Việt Nam)**")
        h = vn_hourly_aqi()
        end = pd.Timestamp(day) + pd.Timedelta(days=1, hours=1)
        h = h[(h["location_id"] == sid) & (h["hour_vn"] >= end - pd.Timedelta(days=7)) & (h["hour_vn"] < end)]
        h = h.sort_values("hour_vn")
        if h.empty or h[aqi_col].isna().all():
            st.write("Trạm không có AQI giờ trong khoảng này.")
        else:
            fig = go.Figure(go.Scatter(x=h["hour_vn"], y=h[aqi_col], mode="lines", line=dict(width=2, color=NEUTRAL),
                                       hovertemplate="%{x|%d/%m %H:%M}: AQI %{y}<extra></extra>"))
            for top, name, color in BANDS[std][:4]:
                fig.add_hline(y=top, line=dict(width=1, color=color, dash="dot"), annotation_text=name,
                              annotation_position="top left", annotation_font_size=10)
            style(fig, yaxis_title="AQI", hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)

    row = today[today["location_id"] == sid]
    if len(row):
        r = row.iloc[0]
        st.markdown(f"**Chỉ số từng chất ngày {day:%d/%m/%Y}** (AQI chung = chỉ số lớn nhất)")
        sub = pd.DataFrame([{"Chất": POLLUTANT_LABEL[p], "AQI": r.get(f"aqi_{std}_{p}"),
                             "Trung bình ngày": r.get(f"{p}_mean"), "Max 1 giờ": r.get(f"{p}_max_1h"),
                             "Số giờ": r.get(f"{p}_hours")}
                            for p in POLLUTANTS if pd.notna(r.get(f"{p}_hours"))])
        sub["Mức"] = sub["AQI"].map(lambda v: band(v, std)[0])
        for c in ("Trung bình ngày", "Max 1 giờ"):
            sub[c] = sub[c].map(lambda v: None if pd.isna(v) else (round(v, 1) if v >= 1 else round(v, 4)))
        st.dataframe(sub, hide_index=True, use_container_width=True)
        st.caption(UNIT_NOTE + " Chất không đủ 18 giờ thì không có AQI.")
