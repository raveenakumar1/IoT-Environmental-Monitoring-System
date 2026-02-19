from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "pipeline.db"

st.set_page_config(page_title="Hydroficient Secure Water Monitor", layout="wide")

st.markdown(
    """
    <style>
    :root {
        --bg: #0b0f17;
        --card: #111827;
        --accent: #00e5ff;
        --muted: #9aa4b2;
        --border: #1f2937;
    }
    .stApp { background: var(--bg); color: #e5e7eb; }
    .block-container { padding-top: 2.5rem; }
    header[data-testid="stHeader"] { background: transparent; }
    div[data-testid="stToolbar"] { visibility: hidden; height: 0; }
    .docker-card {
        background: linear-gradient(180deg, rgba(17,24,39,0.95), rgba(13,18,30,0.95));
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 16px;
        box-shadow: 0 0 24px rgba(0,229,255,0.08);
    }
    .header-wrap {
        background: linear-gradient(135deg, rgba(17,24,39,0.95), rgba(11,15,23,0.95));
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 14px 18px;
        margin-bottom: 14px;
        box-shadow: 0 0 28px rgba(0,229,255,0.12);
    }
    .header-glow {
        color: #e5e7eb;
        text-shadow: 0 0 16px rgba(0,229,255,0.35);
        font-size: 28px;
        font-weight: 700;
        margin: 0;
    }
    .metric-title { color: var(--muted); font-size: 12px; text-transform: uppercase; }
    .metric-value { font-size: 24px; font-weight: 700; color: #e5e7eb; }
    .status-pill {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 12px;
        border: 1px solid var(--border);
        background: rgba(0,229,255,0.08);
        color: #9ff3ff;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


CSV_FALLBACKS = {
    "telemetry": DATA_DIR / "mock_telemetry.csv",
    "security_events": DATA_DIR / "mock_security_events.csv",
    "tls_metrics": DATA_DIR / "mock_tls_metrics.csv",
}


def load_table(table: str) -> pd.DataFrame:
    df = pd.DataFrame()
    if DB_PATH.exists():
        try:
            with sqlite3.connect(DB_PATH) as conn:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        except Exception:
            df = pd.DataFrame()

    if df.empty and table in CSV_FALLBACKS and CSV_FALLBACKS[table].exists():
        df = pd.read_csv(CSV_FALLBACKS[table])
    return df


telemetry = load_table("telemetry")
security_events = load_table("security_events")
tls_metrics = load_table("tls_metrics")

st.markdown(
    "<div class='header-wrap'><div class='header-glow'>Hydroficient Secure Water Monitor</div></div>",
    unsafe_allow_html=True,
)

col_status, col_time = st.columns([3, 1])
with col_status:
    st.markdown("<span class='status-pill'>MQTT TLS ACTIVE • DEVICE CERTS</span>", unsafe_allow_html=True)
with col_time:
    st.caption("Real-time pipeline security overview")

metric_cols = st.columns(5)

if telemetry.empty:
    total_msgs = 0
    anomalies = 0
    devices = 0
else:
    total_msgs = len(telemetry)
    anomalies = int((telemetry["status"] == "anomaly").sum())
    devices = telemetry["device_id"].nunique()

replays = 0
if not security_events.empty:
    replays = int((security_events["event_type"] == "replay_detected").sum())

with metric_cols[0]:
    st.markdown("<div class='docker-card'><div class='metric-title'>Total Messages</div>"
                f"<div class='metric-value'>{total_msgs}</div></div>", unsafe_allow_html=True)
with metric_cols[1]:
    st.markdown("<div class='docker-card'><div class='metric-title'>Active Devices</div>"
                f"<div class='metric-value'>{devices}</div></div>", unsafe_allow_html=True)
with metric_cols[2]:
    st.markdown("<div class='docker-card'><div class='metric-title'>Anomalies</div>"
                f"<div class='metric-value'>{anomalies}</div></div>", unsafe_allow_html=True)
with metric_cols[3]:
    st.markdown("<div class='docker-card'><div class='metric-title'>Replay Attempts</div>"
                f"<div class='metric-value'>{replays}</div></div>", unsafe_allow_html=True)
with metric_cols[4]:
    tls_ok = int(tls_metrics["success"].sum()) if not tls_metrics.empty else 0
    st.markdown("<div class='docker-card'><div class='metric-title'>TLS Handshakes</div>"
                f"<div class='metric-value'>{tls_ok}</div></div>", unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

left, right = st.columns([2, 1])
with left:
    st.markdown("<div class='docker-card'>", unsafe_allow_html=True)
    st.subheader("Temporal Water Quality Trends")
    if telemetry.empty:
        st.info("No telemetry yet. Run the simulator or seed mock data.")
    else:
        telemetry["ts"] = pd.to_datetime(telemetry["ts"], errors="coerce")
        telemetry = telemetry.sort_values("ts")
        fig = px.line(
            telemetry,
            x="ts",
            y=["ph", "turbidity", "temperature"],
            color_discrete_sequence=["#00e5ff", "#8b5cf6", "#22c55e"],
        )
        fig.update_layout(
            paper_bgcolor="#111827",
            plot_bgcolor="#0f172a",
            font_color="#e5e7eb",
            height=340,
        )
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown("<div class='docker-card'>", unsafe_allow_html=True)
    st.subheader("Pipeline Health")
    if telemetry.empty:
        st.info("Awaiting data stream.")
    else:
        recent = telemetry.tail(30)
        health_score = max(0, 100 - int((recent["status"] == "anomaly").mean() * 100))
        st.metric("Health Score", f"{health_score}%")
        st.progress(health_score / 100)
        st.caption("Derived from anomaly rate in latest samples.")
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

col_map, col_sec = st.columns([2, 1])
with col_map:
    st.markdown("<div class='docker-card'>", unsafe_allow_html=True)
    st.subheader("Spatial Sensor Coverage")
    if telemetry.empty:
        st.info("No geospatial data available.")
    else:
        map_df = telemetry[["lat", "lon", "device_id", "status"]].dropna()
        st.map(map_df.rename(columns={"lat": "latitude", "lon": "longitude"}))
    st.markdown("</div>", unsafe_allow_html=True)

with col_sec:
    st.markdown("<div class='docker-card'>", unsafe_allow_html=True)
    st.subheader("Security Events")
    if security_events.empty:
        st.info("No security events logged.")
    else:
        security_events["ts"] = pd.to_datetime(security_events["ts"], errors="coerce")
        st.dataframe(security_events.sort_values("ts", ascending=False).head(10), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

st.markdown("<div class='docker-card'>", unsafe_allow_html=True)
st.subheader("TLS Performance")
if tls_metrics.empty:
    st.info("No TLS benchmarks recorded.")
else:
    tls_metrics["ts"] = pd.to_datetime(tls_metrics["ts"], errors="coerce")
    fig = px.histogram(
        tls_metrics,
        x="handshake_ms",
        nbins=30,
        color="success",
        color_discrete_sequence=["#00e5ff", "#ef4444"],
    )
    fig.update_layout(
        paper_bgcolor="#111827",
        plot_bgcolor="#0f172a",
        font_color="#e5e7eb",
        height=280,
    )
    st.plotly_chart(fig, use_container_width=True)

st.markdown("</div>", unsafe_allow_html=True)
