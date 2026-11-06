# 🌦️ Philippine Weather: Big Data Streaming Dashboard

A real-time big data pipeline that streams live weather data from a public API, processes it with Spark, stores it in MongoDB, and visualizes it in a two-tab Streamlit dashboard.

```
Open-Meteo API ──► Kafka Producer ──► Kafka ──┬──► Streamlit (Live view)
                                              │
                                              └──► Spark Structured Streaming ──► MongoDB ──► Streamlit (History view)
```

**Pipeline used:** MongoDB-Kafka-Spark

## Data Source

[Open-Meteo](https://open-meteo.com/) is a free weather API that needs no API key. Every 15 seconds the producer requests current conditions for 5 Philippine cities in a single call:

| City | Metrics |
|------|---------|
| Quezon City, Manila, Cebu City, Davao City, Baguio | temperature (°C), humidity (%), pressure (hPa), wind speed (km/h) |

Each reading is sent to Kafka as one JSON message:

```json
{
  "timestamp": "2026-11-06T01:23:34+00:00",
  "value": 29.1,
  "metric_type": "temperature",
  "sensor_id": "Quezon City",
  "location": "14.676,121.0437",
  "unit": "celsius"
}
```

> Open-Meteo refreshes its own readings roughly every 15 minutes, so values stay constant between updates even though the pipeline polls every 15 seconds.

## Components

| File | Role |
|------|------|
| `producer.py` | Calls the Open-Meteo API and publishes JSON messages to the Kafka topic `streaming-data`. Logs API errors and retries on the next cycle. |
| `spark_stream.py` | Spark Structured Streaming job. Reads from Kafka, stores raw readings in MongoDB (`readings`), and computes avg / min / max / count per city and metric for every micro-batch (`aggregates`). |
| `app.py` | Streamlit dashboard with a **Real-time** tab (Kafka, auto-refresh) and a **Historical** tab (MongoDB). |

## Dashboard Features

**📈 Real-time tab**
- Consumes Kafka directly; auto-refreshes every 15 seconds (adjustable 5 to 60 s)
- Metric selector, latest value per city, live line chart, raw data table
- Data freshness indicator

**📊 Historical tab**
- Queries MongoDB with filters: time range, metric, cities
- **Data aggregation:** raw, hourly, daily, weekly
- Summary statistics, trend chart, average-by-city bar chart, data table
- Spark micro-batch aggregates viewer
- **Export:** CSV, Excel (.xlsx), interactive chart (HTML)

Timestamps are displayed in Philippine Time (Asia/Manila).

## Prerequisites

- Python 3.10+
- Java 17 (required by Spark; newer versions such as Java 25 are not compatible)
- Docker (for Kafka and MongoDB)

## Setup and Run

### 1. Install Python packages

```bash
pip install -r requirements.txt
```

### 2. Start MongoDB and Kafka

```bash
docker run -d --name mongo -p 27017:27017 mongo:7
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
```

Wait about 10 seconds for Kafka to start. Check with `docker ps`.

### 3. Start the producer (Terminal 1)

```bash
python3 producer.py
```

Expected output: `sent 20/20 messages` every 15 seconds.

### 4. Start Spark (Terminal 2)

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
python3 spark_stream.py
```

The first run downloads the Spark-Kafka connector, which takes a minute or two. Expected output: `[batch N] stored 20 readings, 20 aggregates`.

Optional: `python3 spark_stream.py --interval "1 minute"` for larger micro-batches.

### 5. Start the dashboard (Terminal 3)

```bash
python3 -m streamlit run app.py
```

Open the URL shown in the terminal (port 8501).

## Configuration

| Setting | Default | Where |
|---------|---------|-------|
| Kafka broker | `localhost:9092` | `--bootstrap-servers` flag, dashboard sidebar |
| Kafka topic | `streaming-data` | `--topic` flag, dashboard sidebar |
| Poll interval | 15 s | `producer.py --interval` |
| Spark batch trigger | 15 s | `spark_stream.py --interval` |
| MongoDB URI / database | `mongodb://localhost:27017` / `weather_db` | `--mongo-uri` / `--db` flags, dashboard sidebar |

## Error Handling

- Producer: API timeouts and HTTP errors are logged and retried on the next cycle; Kafka connection failures are reported clearly.
- Dashboard: shows readable messages when Kafka or MongoDB is unavailable instead of crashing; invalid messages are skipped with a warning.
- Spark: malformed or incomplete messages are filtered out before storage.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `cannot import name 'NoBrokersAvailable'` | Handled in the code: newer `kafka-python` (3.x) renamed the error class. |
| Spark fails to start or hangs | Make sure `JAVA_HOME` points to Java 17 and give the first run time to download its jars. |
| Dashboard says "Waiting for messages" | Check that `producer.py` is running; the first message can take up to 15 seconds. |
| `streamlit: command not found` | Use `python3 -m streamlit run app.py`. |

## Tech Stack

Python · Apache Kafka · Apache Spark (Structured Streaming) · MongoDB · Streamlit · Plotly · Pandas · Docker
