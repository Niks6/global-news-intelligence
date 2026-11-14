"""Spark Structured Streaming job: GDELT Mentions stream.

Pipeline:
  Kafka (gdelt.mentions.raw)
    → parse JSON (GDELT Mentions schema)
    → normalize / validate
    → write raw to HDFS /gdelt/raw/mentions/date=YYYY-MM-DD/hour=HH/
    → write curated to HDFS /gdelt/curated/mentions/date=YYYY-MM-DD/

Checkpoint: /gdelt/checkpoints/mentions_stream/
Trigger: processingTime '30 seconds'
"""

import logging
import os

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql import functions as F  # type: ignore
from pyspark.sql.types import (  # type: ignore
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GDELT Mentions schema
# ---------------------------------------------------------------------------
MENTIONS_SCHEMA = StructType([
    StructField("GlobalEventID", LongType(), True),
    StructField("EventTimeDate", LongType(), True),
    StructField("MentionTimeDate", LongType(), True),
    StructField("MentionType", IntegerType(), True),
    StructField("MentionSourceName", StringType(), True),
    StructField("MentionIdentifier", StringType(), True),
    StructField("SentenceID", IntegerType(), True),
    StructField("Actor1CharOffset", IntegerType(), True),
    StructField("Actor2CharOffset", IntegerType(), True),
    StructField("ActionCharOffset", IntegerType(), True),
    StructField("InRawText", IntegerType(), True),
    StructField("Confidence", IntegerType(), True),
    StructField("MentionDocLen", IntegerType(), True),
    StructField("MentionDocTone", DoubleType(), True),
    StructField("MentionDocTranslationInfo", StringType(), True),
    StructField("Extras", StringType(), True),
])


def run(spark: SparkSession) -> None:
    hdfs_base = os.environ.get("HDFS_BASE", "hdfs://namenode:8020/gdelt")
    kafka_brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    checkpoint_base = f"{hdfs_base}/checkpoints/mentions_stream"

    # Read from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "gdelt.mentions.raw")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "5000")
        .option("kafka.group.id", "spark-mentions-consumer")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON
    parsed = raw_stream.select(
        F.from_json(F.col("value").cast("string"), MENTIONS_SCHEMA).alias("m"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("m.*", "kafka_timestamp")

    # Derive event_time from MentionTimeDate (format: YYYYMMDDHHMMSS)
    with_time = parsed.withColumn(
        "event_time",
        F.to_timestamp(F.col("MentionTimeDate").cast("string"), "yyyyMMddHHmmss"),
    ).withColumn(
        "date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd")
    ).withColumn(
        "hour_partition", F.date_format(F.col("event_time"), "HH")
    )

    # Filter invalid rows
    valid = with_time.filter(
        F.col("GlobalEventID").isNotNull() & F.col("MentionTimeDate").isNotNull()
    )

    # Watermark for state management
    watermarked = valid.withWatermark("event_time", "2 hours")

    # Normalize: trim strings, cast nulls
    normalized = watermarked.withColumn(
        "MentionSourceName", F.trim(F.col("MentionSourceName"))
    ).withColumn(
        "MentionIdentifier", F.trim(F.col("MentionIdentifier"))
    )

    # Write raw
    raw_query = (
        normalized.writeStream
        .format("parquet")
        .option("path", f"{hdfs_base}/raw/mentions")
        .option("checkpointLocation", f"{checkpoint_base}/raw_hdfs")
        .partitionBy("date_partition", "hour_partition")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    # Write curated
    curated_query = (
        normalized.writeStream
        .format("parquet")
        .option("path", f"{hdfs_base}/curated/mentions")
        .option("checkpointLocation", f"{checkpoint_base}/curated_hdfs")
        .partitionBy("date_partition")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    logger.info("Mentions stream started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    from src.processing.spark_session import create_spark_session  # type: ignore

    _spark = create_spark_session(app_name="GDELT-MentionsStream")
    run(_spark)
