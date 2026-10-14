"""Spark batch + structured-streaming processing.

Spark is used where its execution model earns its place:
  * Distributed batch aggregation over large Silver datasets (backfill / replay).
  * Scalable Silver->Gold rebuilds when the local single-process path is not
    enough.
  * Optional Iceberg table writes when the local warehouse is configured.

We deliberately do NOT reimplement the streaming validation logic in Spark:
that single implementation lives in ``apexbet.silver.transform`` and is reused.
Spark handles the *large-scale aggregation* layer, driven by the same Gold
grain definitions.

Run inside the Spark container:

    spark-submit --master local[4] src/apexbet/spark/jobs.py --mode gold --input s3a://apexbet-lake/silver

Two modes:
  * ``gold``     -- batch: read Silver parquet, rebuild Gold marts as parquet.
  * ``stream``   -- structured streaming: micro-batch read from a directory or
                    Kafka, write parquet with checkpointing (at-least-once).
"""

from __future__ import annotations

import argparse
import os


def _spark(app: str):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    # LocalStack/MinIO S3A config is applied when endpoint env vars are present.
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
            .config(
                "spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    return builder.getOrCreate()


def run_gold(input_path: str, output_path: str) -> None:
    from pyspark.sql import functions as F

    spark = _spark("apexbet-gold")
    df = spark.read.parquet(input_path)
    df.createOrReplaceTempView("silver_events")

    # Bet activity by date / sport / event type.
    activity = spark.sql(
        """
        SELECT event_date,
               sport_code,
               event_type,
               COUNT(*) AS event_count,
               SUM(CASE WHEN event_type='BetPlaced' THEN payload.stake_minor ELSE 0 END) AS staked_minor,
               SUM(CASE WHEN event_type='BetSettled' THEN payload.payout_minor ELSE 0 END) AS payout_minor
        FROM silver_events
        WHERE event_category IN ('BETTING','SETTLEMENT')
        GROUP BY 1,2,3
        """
    )
    activity.write.mode("overwrite").partitionBy("event_date").parquet(
        f"{output_path}/gold_betting_activity"
    )

    outcomes = spark.sql(
        """
        SELECT payload.result AS result,
               COUNT(*) AS bet_count,
               SUM(payload.settled_stake_minor) AS settled_stake_minor,
               SUM(payload.payout_minor) AS payout_minor
        FROM silver_events
        WHERE event_type='BetSettled'
        GROUP BY 1
        """
    )
    outcomes.write.mode("overwrite").parquet(f"{output_path}/gold_bet_outcomes")

    volume = spark.sql(
        """
        SELECT substr(event_timestamp,1,16) AS event_minute, event_type, COUNT(*) AS event_count
        FROM silver_events GROUP BY 1,2
        """
    )
    volume.write.mode("overwrite").parquet(f"{output_path}/gold_event_volume_by_minute")

    latency = (
        df.filter(F.col("processing_latency_ms").isNotNull())
        .groupBy("event_type")
        .agg(
            F.count("*").alias("n"),
            F.expr("percentile_cont(0.5) WITHIN GROUP (ORDER BY processing_latency_ms)").alias(
                "p50_ms"
            ),
            F.expr("percentile_cont(0.95) WITHIN GROUP (ORDER BY processing_latency_ms)").alias(
                "p95_ms"
            ),
            F.expr("percentile_cont(0.99) WITHIN GROUP (ORDER BY processing_latency_ms)").alias(
                "p99_ms"
            ),
        )
    )
    latency.write.mode("overwrite").parquet(f"{output_path}/gold_latency_distribution")

    print("[spark] gold marts written to", output_path)
    spark.stop()


def run_stream(input_path: str, output_path: str, checkpoint: str) -> None:
    """Structured streaming: read new files, write micro-batches with checkpointing."""
    from pyspark.sql import functions as F

    spark = _spark("apexbet-stream")
    schema = spark.read.parquet(input_path).schema
    stream = (
        spark.readStream.schema(schema)
        .parquet(input_path)
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window("event_timestamp", "1 minute").alias("w"),
            F.col("event_type"),
        )
        .count()
    )
    query = (
        stream.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


def run_iceberg(input_path: str, table: str) -> None:
    """Optional: write Silver to an Iceberg table (schema evolution + snapshots).

    Requires the Iceberg runtime jar on the Spark classpath. Documented as an
    optional extension; not part of the minimum demo.
    """
    spark = _spark("apexbet-iceberg")
    df = spark.read.parquet(input_path)
    spark.sql("CREATE DATABASE IF NOT EXISTS apexbet")
    df.writeTo(table).using("iceberg").createOrReplace()
    print("[spark] iceberg table written:", table)
    spark.stop()


def main() -> None:
    ap = argparse.ArgumentParser(description="ApexBet Spark jobs")
    ap.add_argument("--mode", choices=["gold", "stream", "iceberg"], default="gold")
    ap.add_argument("--input", default="s3a://apexbet-lake/silver")
    ap.add_argument("--output", default="s3a://apexbet-lake/gold")
    ap.add_argument("--checkpoint", default="s3a://apexbet-lake/checkpoints/spark")
    ap.add_argument("--table", default="apexbet.silver_events")
    args = ap.parse_args()
    if args.mode == "gold":
        run_gold(args.input, args.output)
    elif args.mode == "stream":
        run_stream(args.input, args.output, args.checkpoint)
    else:
        run_iceberg(args.input, args.table)


if __name__ == "__main__":
    main()
