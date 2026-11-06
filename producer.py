"""
Kafka Producer: Open-Meteo Weather API -> Kafka

Every --interval seconds it calls the free Open-Meteo API (no API key needed)
for several Philippine cities and publishes one JSON message per metric.

Message schema (required by the assignment):
    timestamp    ISO-8601 string (UTC)
    value        numeric
    metric_type  string  (temperature | humidity | pressure | wind_speed)
    sensor_id    string  (city name)
Extra fields: location (lat,lon) and unit.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    from kafka.errors import KafkaConnectionError as NoBrokersAvailable

API_URL = "https://api.open-meteo.com/v1/forecast"

# (city, latitude, longitude)
CITIES = [
    ("Quezon City", 14.6760, 121.0437),
    ("Manila", 14.5995, 120.9842),
    ("Cebu City", 10.3157, 123.8854),
    ("Davao City", 7.1907, 125.4553),
    ("Baguio", 16.4023, 120.5960),
]

# Open-Meteo variable -> (metric_type, unit)
VARIABLES = {
    "temperature_2m": ("temperature", "celsius"),
    "relative_humidity_2m": ("humidity", "percent"),
    "surface_pressure": ("pressure", "hPa"),
    "wind_speed_10m": ("wind_speed", "km/h"),
}


class StreamingDataProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer: Optional[KafkaProducer] = None
        try:
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            print(f"Kafka producer ready: {bootstrap_servers} -> topic '{topic}'")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka broker available at {bootstrap_servers}")
        except Exception as e:
            print(f"ERROR: Could not start Kafka producer: {e}")

    # ------------------------------------------------------------------ API
    def fetch_weather_data(self) -> List[Dict[str, Any]]:
        """Call the API once (all cities in one request) and flatten to records."""
        params = {
            "latitude": ",".join(str(c[1]) for c in CITIES),
            "longitude": ",".join(str(c[2]) for c in CITIES),
            "current": ",".join(VARIABLES.keys()),
            "timezone": "UTC",
        }
        try:
            resp = requests.get(API_URL, params=params, timeout=10)
            resp.raise_for_status()
            payload = resp.json()
        except requests.RequestException as e:
            print(f"API error: {e}")
            return []

        if isinstance(payload, dict):  # single-location responses are not lists
            payload = [payload]

        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        records = []
        for (city, lat, lon), item in zip(CITIES, payload):
            current = item.get("current", {})
            for var, (metric, unit) in VARIABLES.items():
                value = current.get(var)
                if value is None:
                    continue
                records.append({
                    "timestamp": now,
                    "value": round(float(value), 2),
                    "metric_type": metric,
                    "sensor_id": city,
                    "location": f"{lat},{lon}",
                    "unit": unit,
                })
        return records

    # ---------------------------------------------------------------- Kafka
    def serialize_data(self, data: Dict[str, Any]) -> Optional[bytes]:
        try:
            return json.dumps(data).encode("utf-8")
        except (TypeError, ValueError) as e:
            print(f"Serialization error: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False
        payload = self.serialize_data(data)
        if payload is None:
            return False
        try:
            self.producer.send(self.topic, value=payload).get(timeout=10)
            return True
        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return False

    def produce_stream(self, interval: float = 15.0, duration: Optional[int] = None):
        print(f"Producing every {interval}s, duration: {duration or 'infinite'}")
        start = time.time()
        sent = 0
        try:
            while True:
                if duration and time.time() - start >= duration:
                    print("Duration limit reached")
                    break
                records = self.fetch_weather_data()
                ok = sum(self.send_message(r) for r in records)
                sent += ok
                print(f"[{datetime.now():%H:%M:%S}] sent {ok}/{len(records)} messages (total {sent})")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nStopped by user")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {sent}")

    def close(self):
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
            except Exception as e:
                print(f"Error closing producer: {e}")


def parse_arguments():
    p = argparse.ArgumentParser(description="Open-Meteo -> Kafka producer")
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="streaming-data")
    p.add_argument("--interval", type=float, default=15.0, help="seconds between API calls")
    p.add_argument("--duration", type=int, default=None, help="run time in seconds (default: forever)")
    return p.parse_args()


def main():
    args = parse_arguments()
    producer = StreamingDataProducer(args.bootstrap_servers, args.topic)
    producer.produce_stream(interval=args.interval, duration=args.duration)


if __name__ == "__main__":
    main()
