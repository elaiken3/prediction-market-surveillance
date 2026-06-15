"""PySpark Structured Streaming job -- market surveillance.

  1. INGEST   - read canonical market events from Kafka.
  2. CONTRACT - validate against the schema; violations -> dead-letter with reason.
  3. DETECT   - per-asset windowed price-range and traded-volume aggregates flag
                real-time dislocations; cross-outcome coherence is handled in dbt
                (it needs the latest price across all of a market's outcomes).
  4. SINK     - valid / flagged / dlq land as Parquet for dbt + the dashboard.

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        streaming/spark_job.py
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

EVENT_SCHEMA = StructType([
    StructField("source", StringType()),
    StructField("event_type", StringType()),
    StructField("event_id", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("market_id", StringType()),
    StructField("asset_id", StringType()),
    StructField("outcome", StringType()),
    StructField("price", DoubleType()),
    StructField("size", DoubleType()),
    StructField("side", StringType()),
])

EVENT_TYPES = ["trade", "quote", "book"]


def reject_reason(df):
    reason = (
        F.when(F.col("event_id").isNull() | (F.length("event_id") < 6), "bad event_id")
        .when(~F.col("event_type").isin(EVENT_TYPES), "bad event_type")
        .when(F.col("event_ts").isNull(), "unparseable event_ts")
        .when(F.col("market_id").isNull() | (F.length(F.trim("market_id")) == 0), "blank market_id")
        .when(F.col("asset_id").isNull() | (F.length(F.trim("asset_id")) == 0), "blank asset_id")
        .when(F.col("price").isNull() | (F.col("price") < 0) | (F.col("price") > 1), "price outside [0,1]")
        .when(F.col("size") < 0, "negative size")
        .otherwise(F.lit(None).cast(StringType()))
    )
    return df.withColumn("reject_reason", reason)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--topic", default="markets.events")
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--checkpoints", default="data/checkpoints")
    ap.add_argument("--price-range", type=float, default=0.15)
    ap.add_argument("--trigger-seconds", type=int, default=60,
                    help="micro-batch interval; larger = fewer, bigger parquet files (key for multi-day runs)")
    args = ap.parse_args()
    trigger = {"processingTime": f"{args.trigger_seconds} seconds"}

    spark = (
        SparkSession.builder.appName("market-surveillance")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(F.col("value").cast("string").alias("raw_json"))
        .withColumn("d", F.from_json("raw_json", EVENT_SCHEMA))
        .select("raw_json", "d.*")
    )
    checked = reject_reason(parsed)

    valid = (
        checked.where(F.col("reject_reason").isNull())
        .drop("reject_reason", "raw_json")
        .withColumn("event_date", F.to_date("event_ts"))
    )
    rejected = (
        checked.where(F.col("reject_reason").isNotNull())
        .select("raw_json", "reject_reason", F.current_timestamp().alias("rejected_at"))
        .withColumn("rejected_date", F.to_date("rejected_at"))
    )

    q_valid = (
        valid.writeStream.format("parquet")
        .option("path", f"{args.lake}/valid")
        .option("checkpointLocation", f"{args.checkpoints}/valid")
        .partitionBy("event_date")
        .trigger(**trigger)
        .outputMode("append").start()
    )

    q_dlq = (
        rejected.writeStream.format("parquet")
        .option("path", f"{args.lake}/dlq")
        .option("checkpointLocation", f"{args.checkpoints}/dlq")
        .partitionBy("rejected_date")
        .trigger(**trigger)
        .outputMode("append").start()
    )

    # Per-asset 60s sliding windows: price range + traded volume.
    windowed = (
        valid.withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "60 seconds", "30 seconds"), F.col("asset_id"), F.col("market_id"))
        .agg(
            (F.max("price") - F.min("price")).alias("price_range"),
            F.sum(F.when(F.col("event_type") == "trade", F.col("size")).otherwise(F.lit(0.0))).alias("volume"),
            F.count("*").alias("tick_count"),
        )
        .where(F.col("price_range") >= args.price_range)
        .select(
            "asset_id", "market_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "price_range", "volume", "tick_count",
            F.lit("price_range").alias("rule"),
        )
        .withColumn("window_date", F.to_date("window_start"))
    )
    q_flagged = (
        windowed.writeStream.format("parquet")
        .option("path", f"{args.lake}/flagged")
        .option("checkpointLocation", f"{args.checkpoints}/flagged")
        .partitionBy("window_date")
        .trigger(**trigger)
        .outputMode("append").start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
