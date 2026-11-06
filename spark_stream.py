"""
Spark Structured Streaming job:  Kafka  ->  Spark  ->  MongoDB

For every micro-batch (default every 15 s) it:
  1. parses the JSON messages from Kafka,
  2. stores the raw readings in MongoDB   (collection: readings),
  3. computes per-city / per-metric aggregates with Spark
     (avg, min, max, count) and stores them (collection: aggregates).

Run:   python spark_stream.py
Needs: Java 8/11/17, pyspark, pymongo. The Kafka connector jar is downloaded
       automatically on first run (internet required).
"""

import argparse
import os
from datetime import datetime, timezone

import pyspark
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("value", DoubleType()),
    StructField("metric_type", StringType()),
    StructField("sensor_id", StringType()),
    StructField("location", StringType()),
    StructField("unit", StringType()),
])


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="streaming-data")
    p.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    p.add_argument("--db", default="weather_db")
    p.add_argument("--interval", default="15 seconds", help="micro-batch trigger")
    p.add_argument("--checkpoint", default=os.path.join(os.getcwd(), "spark_checkpoint"))
    return p.parse_args()


def build_spark():
    version = pyspark.__version__
    scala = "2.13" if int(version.split(".")[0]) >= 4 else "2.12"
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"
    return (
        SparkSession.builder.appName("WeatherKafkaToMongo")
        .config("spark.jars.packages", kafka_pkg)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def main():
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    mongo = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    db = mongo[args.db]
    readings, aggregates = db["readings"], db["aggregates"]

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
        .select("d.*")
        .where(F.col("value").isNotNull() & F.col("timestamp").isNotNull())
    )

    def write_batch(df, batch_id):
        rows = df.collect()
        if not rows:
            return

        # 1) raw readings -> MongoDB
        docs = []
        for r in rows:
            d = r.asDict()
            try:
                d["timestamp"] = datetime.fromisoformat(d["timestamp"].replace("Z", "+00:00"))
            except ValueError:
                continue
            docs.append(d)
        if docs:
            readings.insert_many(docs)

        # 2) Spark aggregation of this micro-batch -> MongoDB
        agg = (
            df.groupBy("sensor_id", "metric_type")
            .agg(
                F.round(F.avg("value"), 2).alias("avg_value"),
                F.min("value").alias("min_value"),
                F.max("value").alias("max_value"),
                F.count("*").alias("n"),
            )
            .collect()
        )
        now = datetime.now(timezone.utc)
        agg_docs = [{**a.asDict(), "batch_id": batch_id, "batch_time": now} for a in agg]
        if agg_docs:
            aggregates.insert_many(agg_docs)

        print(f"[batch {batch_id}] stored {len(docs)} readings, {len(agg_docs)} aggregates")

    query = (
        parsed.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.interval)
        .start()
    )
    print("Spark streaming started. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
