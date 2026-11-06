"""Vietnam air quality dashboard on MongoDB (Gold collections + latest_readings).

    streamlit run src/dashboard/app.py      # http://localhost:8501
"""

from pathlib import Path
import sys

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.dashboard import common  # noqa: E402
from src.dashboard.views import compare, live, overview, pipeline, standards, trends, world  # noqa: E402

st.set_page_config(page_title="Chất lượng không khí Việt Nam", layout="wide")

try:
    common.db().command("ping")
except Exception as exc:
    st.error(f"Không kết nối được MongoDB: {exc}. Bật bằng `docker compose -f docker-compose.serving.yml up -d`.")
    st.stop()

with st.sidebar:
    st.markdown("### Không khí Việt Nam")
    choice = st.radio("Chuẩn AQI", ["VN_AQI (QĐ 1459)", "US EPA"], key="std_label")
    st.session_state["std"] = "vn" if choice.startswith("VN") else "us"
    st.caption("Nguồn: OpenAQ (lịch sử S3 + API). Gold v1 → MongoDB. Ngày theo giờ địa phương (01:00–24:00), "
               "giờ hiển thị theo giờ Việt Nam.")

nav = st.navigation({
    "Việt Nam": [st.Page(overview.render, title="Tổng quan", icon=":material/map:", default=True),
                 st.Page(trends.render, title="Xu hướng & mùa vụ", icon=":material/trending_up:", url_path="xu-huong"),
                 st.Page(compare.render, title="So sánh trạm", icon=":material/compare_arrows:", url_path="so-sanh"),
                 st.Page(standards.render, title="Chuẩn VN và US", icon=":material/balance:", url_path="chuan-aqi")],
    "Mở rộng": [st.Page(world.render, title="Toàn cầu", icon=":material/public:", url_path="toan-cau"),
                st.Page(live.render, title="Trực tiếp", icon=":material/sensors:", url_path="truc-tiep"),
                st.Page(pipeline.render, title="Dữ liệu & pipeline", icon=":material/database:", url_path="pipeline")],
})
nav.run()
