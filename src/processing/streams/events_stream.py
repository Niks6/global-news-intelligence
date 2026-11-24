"""Spark Structured Streaming job: GDELT Events stream.

Pipeline:
  Kafka (gdelt.events.raw)
    → parse JSON (61-field GDELT Event schema)
    → validate required fields
    → deduplicate by GlobalEventID (watermark 1 hour)
    → enrich (category name, cooperation_flag)
    → write raw to HDFS /gdelt/raw/events/date=YYYY-MM-DD/hour=HH/
    → write curated to HDFS /gdelt/curated/events/date=YYYY-MM-DD/
    → write to HBase events_by_id and events_by_country_time

Trigger: processingTime '30 seconds'
Checkpoint: /gdelt/checkpoints/events_stream/
"""

import json
import logging
import os
import sys
from typing import Iterator

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql import functions as F  # type: ignore
from pyspark.sql.types import (  # type: ignore
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GDELT Events schema (61 fields)
# ---------------------------------------------------------------------------
EVENTS_SCHEMA = StructType([
    StructField("GlobalEventID", LongType(), True),
    StructField("SQLDATE", StringType(), True),
    StructField("MonthYear", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("FractionDate", DoubleType(), True),
    StructField("Actor1Code", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor1CountryCode", StringType(), True),
    StructField("Actor1KnownGroupCode", StringType(), True),
    StructField("Actor1EthnicCode", StringType(), True),
    StructField("Actor1Religion1Code", StringType(), True),
    StructField("Actor1Religion2Code", StringType(), True),
    StructField("Actor1Type1Code", StringType(), True),
    StructField("Actor1Type2Code", StringType(), True),
    StructField("Actor1Type3Code", StringType(), True),
    StructField("Actor2Code", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("Actor2CountryCode", StringType(), True),
    StructField("Actor2KnownGroupCode", StringType(), True),
    StructField("Actor2EthnicCode", StringType(), True),
    StructField("Actor2Religion1Code", StringType(), True),
    StructField("Actor2Religion2Code", StringType(), True),
    StructField("Actor2Type1Code", StringType(), True),
    StructField("Actor2Type2Code", StringType(), True),
    StructField("Actor2Type3Code", StringType(), True),
    StructField("IsRootEvent", IntegerType(), True),
    StructField("EventCode", StringType(), True),
    StructField("EventBaseCode", StringType(), True),
    StructField("EventRootCode", StringType(), True),
    StructField("QuadClass", IntegerType(), True),
    StructField("GoldsteinScale", DoubleType(), True),
    StructField("NumMentions", IntegerType(), True),
    StructField("NumSources", IntegerType(), True),
    StructField("NumArticles", IntegerType(), True),
    StructField("AvgTone", DoubleType(), True),
    StructField("Actor1Geo_Type", IntegerType(), True),
    StructField("Actor1Geo_FullName", StringType(), True),
    StructField("Actor1Geo_CountryCode", StringType(), True),
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    StructField("Actor1Geo_ADM2Code", StringType(), True),
    StructField("Actor1Geo_Lat", DoubleType(), True),
    StructField("Actor1Geo_Long", DoubleType(), True),
    StructField("Actor1Geo_FeatureID", StringType(), True),
    StructField("Actor2Geo_Type", IntegerType(), True),
    StructField("Actor2Geo_FullName", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True),
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_ADM2Code", StringType(), True),
    StructField("Actor2Geo_Lat", DoubleType(), True),
    StructField("Actor2Geo_Long", DoubleType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),
    StructField("ActionGeo_Type", IntegerType(), True),
    StructField("ActionGeo_FullName", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_ADM2Code", StringType(), True),
    StructField("ActionGeo_Lat", DoubleType(), True),
    StructField("ActionGeo_Long", DoubleType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),
    StructField("DATEADDED", LongType(), True),
    StructField("SOURCEURL", StringType(), True),
])

# ---------------------------------------------------------------------------
# CAMEO root code → category mapping (cooperation/conflict classification)
# ---------------------------------------------------------------------------
CAMEO_CATEGORY = {
    "01": "Make Public Statement",
    "02": "Appeal",
    "03": "Express Intent to Cooperate",
    "04": "Consult",
    "05": "Engage in Diplomatic Cooperation",
    "06": "Engage in Material Cooperation",
    "07": "Provide Aid",
    "08": "Yield",
    "09": "Investigate",
    "10": "Demand",
    "11": "Disapprove",
    "12": "Reject",
    "13": "Threaten",
    "14": "Protest",
    "15": "Exhibit Military Posture",
    "16": "Reduce Relations",
    "17": "Coerce",
    "18": "Assault",
    "19": "Fight",
    "20": "Use Unconventional Mass Violence",
}

# QuadClass: 1=Verbal Cooperation, 2=Material Cooperation, 3=Verbal Conflict, 4=Material Conflict
COOPERATION_QUAD_CLASSES = {1, 2}
CONFLICT_QUAD_CLASSES = {3, 4}


def build_category_map_expr():
    """Build a Spark SQL CASE expression for CAMEO root codes."""
    cases = " ".join(
        f"WHEN EventRootCode = '{code}' THEN '{name}'"
        for code, name in CAMEO_CATEGORY.items()
    )
    return f"CASE {cases} ELSE 'Unknown' END"


# ---------------------------------------------------------------------------
# HBase foreachBatch writer
# ---------------------------------------------------------------------------

def write_to_hbase(batch_df: DataFrame, batch_id: int) -> None:
    """Write a micro-batch DataFrame to HBase (events_by_id + events_by_country_time)."""
    import sys
    import os

    # Ensure src package is on the path inside executor
    src_root = os.environ.get("SRC_ROOT", "/app")
    if src_root not in sys.path:
        sys.path.insert(0, src_root)

    from src.storage.hbase_client import HBaseClient  # type: ignore

    hbase_host = os.environ.get("HBASE_HOST", "localhost")
    hbase_port = int(os.environ.get("HBASE_PORT", "9090"))

    rows = batch_df.collect()
    if not rows:
        return

    client = HBaseClient(host=hbase_host, port=hbase_port, auto_connect=True)
    try:
        for row in rows:
            event = row.asDict()
            client.put_event(event)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Main streaming job
# ---------------------------------------------------------------------------

def run(spark: SparkSession) -> None:
    hdfs_base = os.environ.get("HDFS_BASE", "hdfs://namenode:9000/gdelt")
    kafka_brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    checkpoint_base = f"{hdfs_base}/checkpoints/events_stream"

    # 1. Read from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "gdelt.events.raw")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "5000")
        .option("kafka.group.id", "spark-events-consumer")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON payload
    parsed = raw_stream.select(
        F.from_json(F.col("value").cast("string"), EVENTS_SCHEMA).alias("ev"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("ev.*", "kafka_timestamp")

    # 3. Add event_time from SQLDATE (format YYYYMMDD → timestamp)
    with_event_time = parsed.withColumn(
        "event_time",
        F.to_timestamp(F.col("SQLDATE").cast("string"), "yyyyMMdd"),
    )

    # 4. Filter rows missing required fields
    valid = with_event_time.filter(
        F.col("GlobalEventID").isNotNull()
        & F.col("SQLDATE").isNotNull()
        & F.col("EventRootCode").isNotNull()
    )

    # 5. Watermark + deduplication by GlobalEventID (1-hour late data tolerance)
    deduped = (
        valid.withWatermark("event_time", "1 hour")
        .dropDuplicates(["GlobalEventID"])
    )

    # 6. Enrichment: category name + cooperation_flag
    enriched = (
        deduped
        .withColumn("category_name", F.expr(build_category_map_expr()))
        .withColumn(
            "cooperation_flag",
            F.when(F.col("QuadClass").isin(1, 2), F.lit("cooperation"))
            .when(F.col("QuadClass").isin(3, 4), F.lit("conflict"))
            .otherwise(F.lit("unknown")),
        )
        .withColumn("date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
        .withColumn("hour_partition", F.date_format(F.col("event_time"), "HH"))
    )

    # 7a. Write raw to HDFS (partitioned by date/hour)
    raw_hdfs_query = (
        enriched.writeStream
        .format("parquet")
        .option("path", f"{hdfs_base}/raw/events")
        .option("checkpointLocation", f"{checkpoint_base}/raw_hdfs")
        .partitionBy("date_partition", "hour_partition")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    # 7b. Write curated to HDFS (partitioned by date)
    curated_hdfs_query = (
        enriched.writeStream
        .format("parquet")
        .option("path", f"{hdfs_base}/curated/events")
        .option("checkpointLocation", f"{checkpoint_base}/curated_hdfs")
        .partitionBy("date_partition")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    # 7c. Write to HBase via foreachBatch
    hbase_query = (
        enriched.writeStream
        .foreachBatch(write_to_hbase)
        .option("checkpointLocation", f"{checkpoint_base}/hbase")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    logger.info("Events stream started. Awaiting termination.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    from src.processing.spark_session import create_spark_session  # type: ignore

    _spark = create_spark_session(app_name="GDELT-EventsStream")
    run(_spark)
