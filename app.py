"""
Streaming Weather Dashboard
Pipeline: Open-Meteo API -> Kafka -> Spark -> MongoDB -> Streamlit

Tab 1  Live view      : consumes Kafka directly, auto-refreshes (default 15 s)
Tab 2  Historical view: queries MongoDB, filters + aggregation + data export
"""

import io
import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import KafkaError
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    from kafka.errors import KafkaConnectionError as NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Weather Streaming Dashboard", page_icon="🌦️",
                   layout="wide", initial_sidebar_state="expanded")

TZ = "Asia/Manila"
BUFFER_SIZE = 400          # live records kept in memory
MAX_DOCS = 50_000          # safety cap for history queries
UNITS = {"temperature": "°C", "humidity": "%", "pressure": "hPa", "wind_speed": "km/h"}
TIME_RANGES = {"1h": timedelta(hours=1), "24h": timedelta(hours=24),
               "7d": timedelta(days=7), "30d": timedelta(days=30), "All": None}


# ----------------------------------------------------------------- helpers
def to_local(series: pd.Series) -> pd.Series:
    """Convert (naive-UTC or aware) timestamps to naive Manila time for display."""
    return pd.to_datetime(series, utc=True).dt.tz_convert(TZ).dt.tz_localize(None)


def label(metric: str) -> str:
    return f"{metric.replace('_', ' ').title()} ({UNITS.get(metric, '')})"


# ----------------------------------------------------------------- sidebar
def setup_sidebar():
    st.sidebar.title("Dashboard Controls")

    st.sidebar.subheader("Kafka")
    broker = st.sidebar.text_input("Kafka Broker", value="localhost:9092")
    topic = st.sidebar.text_input("Kafka Topic", value="streaming-data")

    st.sidebar.subheader("Storage")
    st.sidebar.selectbox("Storage Type", ["MongoDB"], help="This pipeline uses MongoDB.")
    mongo_uri = st.sidebar.text_input("MongoDB URI", value="mongodb://localhost:27017")
    mongo_db = st.sidebar.text_input("Database", value="weather_db")

    st.sidebar.subheader("Refresh Settings")
    auto = st.sidebar.checkbox("Enable Auto Refresh", value=True)
    interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 15) if auto else 15
    if st.sidebar.button("🔄 Manual Refresh"):
        st.rerun()

    return {"kafka_broker": broker, "kafka_topic": topic, "mongo_uri": mongo_uri,
            "mongo_db": mongo_db, "auto_refresh": auto, "refresh_interval": interval}


# ------------------------------------------------------------------- Kafka
def get_consumer(broker: str, topic: str):
    key = f"consumer::{broker}::{topic}"
    if key not in st.session_state:
        try:
            st.session_state[key] = KafkaConsumer(
                topic,
                bootstrap_servers=[broker],
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id=None,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
        except NoBrokersAvailable:
            st.error(f"Kafka broker not available at {broker}. Is Kafka running?")
            return None
        except KafkaError as e:
            st.error(f"Kafka error: {e}")
            return None
    return st.session_state[key]


def parse_message(data: dict):
    try:
        return {
            "timestamp": pd.to_datetime(data["timestamp"], utc=True),
            "value": float(data["value"]),
            "metric_type": str(data["metric_type"]),
            "sensor_id": str(data["sensor_id"]),
            "unit": data.get("unit", ""),
        }
    except (KeyError, ValueError, TypeError) as e:
        st.warning(f"Skipped invalid message ({e}): {data}")
        return None


def consume_kafka_data(config) -> pd.DataFrame:
    """Poll Kafka and append new records to a rolling in-memory buffer."""
    buf = st.session_state.setdefault("live_buffer", [])
    consumer = get_consumer(config["kafka_broker"], config["kafka_topic"])
    if consumer is not None:
        try:
            batches = consumer.poll(timeout_ms=3000, max_records=200)
            for records in batches.values():
                for msg in records:
                    rec = parse_message(msg.value)
                    if rec:
                        buf.append(rec)
        except KafkaError as e:
            st.error(f"Kafka error while polling: {e}")
        del buf[:-BUFFER_SIZE]
        st.session_state["last_refresh"] = datetime.now()
    return pd.DataFrame(buf)


# ----------------------------------------------------------------- MongoDB
@st.cache_resource
def get_mongo(uri: str):
    return MongoClient(uri, serverSelectionTimeoutMS=3000)


def get_collection(config, name="readings"):
    return get_mongo(config["mongo_uri"])[config["mongo_db"]][name]


def distinct_values(config, field):
    try:
        return sorted(get_collection(config).distinct(field))
    except PyMongoError:
        return []


def query_historical_data(config, time_range="24h", metric=None, sensors=None):
    """Query raw readings from MongoDB. Returns a DataFrame (local time) or None on error."""
    query = {}
    delta = TIME_RANGES.get(time_range)
    if delta:
        query["timestamp"] = {"$gte": datetime.now(timezone.utc) - delta}
    if metric:
        query["metric_type"] = metric
    if sensors:
        query["sensor_id"] = {"$in": sensors}
    try:
        docs = list(get_collection(config).find(query, {"_id": 0})
                    .sort("timestamp", 1).limit(MAX_DOCS))
    except PyMongoError as e:
        st.error(f"MongoDB error: {e}")
        return None
    df = pd.DataFrame(docs)
    if not df.empty:
        df["timestamp"] = to_local(df["timestamp"])
    return df


def query_spark_aggregates(config, limit=200):
    try:
        docs = list(get_collection(config, "aggregates").find({}, {"_id": 0})
                    .sort("batch_time", -1).limit(limit))
    except PyMongoError:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    if not df.empty:
        df["batch_time"] = to_local(df["batch_time"])
    return df


def aggregate_data(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "raw" or df.empty:
        return df
    rule = {"hourly": "h", "daily": "D", "weekly": "W"}[mode]
    out = (df.set_index("timestamp").groupby(["sensor_id", "metric_type"])
           .resample(rule)["value"].agg(["mean", "min", "max", "count"]).reset_index())
    out = out[out["count"] > 0].rename(columns={"mean": "value"})
    out["value"] = out["value"].round(2)
    return out


# ------------------------------------------------------------------ export
def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="History")
        ws = writer.sheets["History"]
        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = max(len(str(c.value or "")) for c in col) + 2
    return buf.getvalue()


# ------------------------------------------------------------------- views
def display_real_time_view(config):
    st.header("📈 Real-time Streaming Dashboard")
    state = "🟢 Enabled" if config["auto_refresh"] else "🔴 Disabled"
    st.info(f"**Auto-refresh:** {state} - updates every {config['refresh_interval']} seconds")

    with st.spinner("Fetching real-time data from Kafka..."):
        df = consume_kafka_data(config)

    if df.empty:
        st.warning("Waiting for messages from Kafka... make sure `producer.py` is running "
                   "(the first message can take up to one producer interval).")
        return

    df["timestamp"] = to_local(df["timestamp"])
    age = (datetime.now() - st.session_state["last_refresh"]).total_seconds()
    st.success(f"Data updated {age:.0f} seconds ago")

    metrics = sorted(df["metric_type"].unique())
    metric = st.selectbox("Metric", metrics, format_func=label, key="live_metric")
    view = df[df["metric_type"] == metric]

    c1, c2, c3 = st.columns(3)
    c1.metric("Records in buffer", len(df))
    c2.metric("Cities", df["sensor_id"].nunique())
    c3.metric("Latest reading", df["timestamp"].max().strftime("%H:%M:%S"))

    st.subheader("Latest value per city")
    latest = (view.sort_values("timestamp").groupby("sensor_id").tail(1)
              [["sensor_id", "value", "unit", "timestamp"]].set_index("sensor_id"))
    st.dataframe(latest, width="stretch")

    fig = px.line(view, x="timestamp", y="value", color="sensor_id", markers=True,
                  title=f"Live {label(metric)}", template="plotly_white",
                  labels={"value": label(metric), "timestamp": "Time (PHT)", "sensor_id": "City"})
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, width="stretch")

    with st.expander("📋 View Raw Data"):
        st.dataframe(df.sort_values("timestamp", ascending=False), width="stretch", height=300)


def display_historical_view(config):
    st.header("📊 Historical Data Analysis")

    sensors_all = distinct_values(config, "sensor_id")
    metrics_all = distinct_values(config, "metric_type")
    if not metrics_all:
        st.warning("No historical data found in MongoDB yet. Start `producer.py` and "
                   "`spark_stream.py`, then wait for a few batches.")
        return

    st.subheader("Data Filters")
    c1, c2, c3, c4 = st.columns(4)
    time_range = c1.selectbox("Time Range", list(TIME_RANGES), index=1)
    metric = c2.selectbox("Metric Type", metrics_all, format_func=label)
    sensors = c3.multiselect("Cities", sensors_all, default=sensors_all)
    mode = c4.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"])

    df = query_historical_data(config, time_range, metric, sensors)
    if df is None or df.empty:
        st.info("No records match the selected filters.")
        return
    shown = aggregate_data(df, mode)

    st.subheader("Data Summary")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Records", len(df))
    m2.metric("Average", f"{df['value'].mean():.2f}")
    m3.metric("Min / Max", f"{df['value'].min():.1f} / {df['value'].max():.1f}")
    m4.metric("Std. Deviation", f"{df['value'].std():.2f}")

    st.subheader("Historical Trend")
    fig = px.line(shown, x="timestamp", y="value", color="sensor_id", template="plotly_white",
                  title=f"{label(metric)} - {mode} ({time_range})",
                  labels={"value": label(metric), "timestamp": "Time (PHT)", "sensor_id": "City"})
    st.plotly_chart(fig, width="stretch")

    st.subheader("Average by City")
    by_city = df.groupby("sensor_id")["value"].agg(["mean", "min", "max"]).round(2).reset_index()
    st.plotly_chart(px.bar(by_city, x="sensor_id", y="mean", template="plotly_white",
                           labels={"mean": f"Avg {label(metric)}", "sensor_id": "City"}),
                    width="stretch")

    st.subheader("Historical Data Table")
    st.dataframe(shown, width="stretch", hide_index=True)

    st.subheader("⬇️ Export")
    e1, e2, e3 = st.columns(3)
    e1.download_button("CSV", shown.to_csv(index=False).encode("utf-8"),
                       "weather_history.csv", "text/csv")
    e2.download_button("Excel (.xlsx)", to_excel_bytes(shown), "weather_history.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    e3.download_button("Chart (HTML)", fig.to_html(include_plotlyjs="cdn").encode("utf-8"),
                       "weather_chart.html", "text/html")

    with st.expander("⚡ Spark micro-batch aggregates (from `aggregates` collection)"):
        agg = query_spark_aggregates(config)
        if agg.empty:
            st.info("No Spark aggregates stored yet.")
        else:
            st.dataframe(agg, width="stretch", hide_index=True)


# -------------------------------------------------------------------- main
def main():
    st.title("🌦️ Philippine Weather - Big Data Streaming Dashboard")
    st.caption("Open-Meteo API → Kafka → Spark Structured Streaming → MongoDB → Streamlit")

    config = setup_sidebar()
    if config["auto_refresh"]:
        st_autorefresh(interval=config["refresh_interval"] * 1000, key="auto_refresh")

    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    with tab1:
        display_real_time_view(config)
    with tab2:
        display_historical_view(config)


if __name__ == "__main__":
    main()
