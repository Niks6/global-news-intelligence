"""Unified Distributed Spark Pipeline Job.

Runs on the 3-node Spark cluster (spark://spark-master:7077) with conservative
memory settings (--driver-memory 512m --executor-memory 512m) and zero Python UDFs:

  1. Consume incremental Kafka topics (availableNow=True, HDFS-checkpointed):
       - gdelt.events.raw   -> HDFS /gdelt/raw/events, /gdelt/curated/events,
                               Kafka gdelt.events.normalized,
                               HBase events_by_id & events_by_country_time
       - gdelt.mentions.raw -> HDFS /gdelt/raw/mentions, /gdelt/curated/mentions
       - gdelt.gkg.raw      -> HDFS /gdelt/raw/gkg, /gdelt/curated/gkg
  2. Compute distributed Spark aggregations over HDFS curated Parquet:
       - Country metrics    -> HDFS /gdelt/analytics/country_metrics + HBase country_metrics_by_time
       - Category metrics   -> HDFS /gdelt/analytics/category_metrics + HBase category_metrics_by_time
       - Topic metrics      -> HDFS /gdelt/analytics/topic_metrics + HBase topic_metrics_by_time
       - Rolling anomalies  -> HDFS /gdelt/analytics/anomaly_metrics + HBase anomalies_by_time
       - Analytics summary  -> Kafka gdelt.analytics + data/metrics/processing_metrics.json
"""
from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession  # type: ignore
from pyspark.sql import functions as F  # type: ignore
from pyspark.sql.types import (  # type: ignore
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Ensure /app is importable
_APP_ROOT = os.environ.get("SRC_ROOT", "/app")
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

from src.common.cameo_mapping import CAMEO_ROOT_CATEGORIES  # noqa: E402
from src.storage.hbase_client import HBaseClient  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_pipeline")

# ---------------------------------------------------------------------------
# Schemas matching src/ingestion/parsers/*.py JSON payloads
# ---------------------------------------------------------------------------
EVENTS_SCHEMA = StructType([
    StructField("GlobalEventID", LongType(), True),
    StructField("SQLDATE", LongType(), True),
    StructField("MonthYear", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("FractionDate", DoubleType(), True),
    StructField("Actor1Code", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor1CountryCode", StringType(), True),
    StructField("Actor2Code", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("Actor2CountryCode", StringType(), True),
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
    StructField("ActionGeo_Type", IntegerType(), True),
    StructField("ActionGeo_FullName", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_Lat", DoubleType(), True),
    StructField("ActionGeo_Long", DoubleType(), True),
    StructField("DATEADDED", LongType(), True),
    StructField("SOURCEURL", StringType(), True),
])

MENTIONS_SCHEMA = StructType([
    StructField("GlobalEventID", LongType(), True),
    StructField("EventTimeDate", LongType(), True),
    StructField("MentionTimeDate", LongType(), True),
    StructField("MentionType", IntegerType(), True),
    StructField("MentionSourceName", StringType(), True),
    StructField("MentionIdentifier", StringType(), True),
    StructField("Confidence", IntegerType(), True),
    StructField("MentionDocLen", IntegerType(), True),
    StructField("MentionDocTone", DoubleType(), True),
])

GKG_SCHEMA = StructType([
    StructField("GKGRecordID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("SourceCollectionIdentifier", IntegerType(), True),
    StructField("SourceCommonName", StringType(), True),
    StructField("DocumentIdentifier", StringType(), True),
    StructField("Themes", ArrayType(StringType()), True),
    StructField("Locations", ArrayType(StructType([
        StructField("type", IntegerType(), True),
        StructField("fullname", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ])), True),
    StructField("Persons", ArrayType(StringType()), True),
    StructField("Organizations", ArrayType(StringType()), True),
    StructField("Tone", DoubleType(), True),
    StructField("TonePositive", DoubleType(), True),
    StructField("ToneNegative", DoubleType(), True),
    StructField("TonePolarity", DoubleType(), True),
    StructField("ToneWordCount", DoubleType(), True),
])

FIPS_NAMES = {
    "US": "United States", "UK": "United Kingdom", "FR": "France", "GM": "Germany",
    "RS": "Russia", "CH": "China", "JA": "Japan", "IN": "India", "BR": "Brazil",
    "MX": "Mexico", "AU": "Australia", "CA": "Canada", "IT": "Italy", "SP": "Spain",
    "NL": "Netherlands", "SW": "Sweden", "NO": "Norway", "DA": "Denmark", "FI": "Finland",
    "PL": "Poland", "TU": "Turkey", "IR": "Iran", "IZ": "Iraq", "SY": "Syria",
    "SA": "Saudi Arabia", "EG": "Egypt", "SF": "South Africa", "NI": "Nigeria",
    "KE": "Kenya", "ID": "Indonesia", "TH": "Thailand", "VM": "Vietnam", "PH": "Philippines",
    "MY": "Malaysia", "PK": "Pakistan", "BD": "Bangladesh", "AF": "Afghanistan",
    "UP": "Ukraine", "IS": "Israel", "AR": "Argentina", "CO": "Colombia",
}


def _hdfs_path_exists(spark: SparkSession, path: str) -> bool:
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        jpath = sc._jvm.org.apache.hadoop.fs.Path(path)
        fs = jpath.getFileSystem(hadoop_conf)
        return bool(fs.exists(jpath))
    except Exception:
        return False


def _category_sql_expr() -> str:
    cases = " ".join(
        f"WHEN EventRootCode = '{code}' THEN '{name.replace(chr(39), '')}'"
        for code, (name, _) in CAMEO_ROOT_CATEGORIES.items()
    )
    return f"CASE {cases} ELSE 'Unknown' END"


def create_spark(app_name: str = "GDELT-UnifiedPipeline") -> SparkSession:
    master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
    hdfs_url = os.environ.get("HDFS_NAMENODE_URL", os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000"))
    ivy_dir = os.environ.get("SPARK_IVY_DIR", "/tmp/ivy")
    kafka_pkg = os.environ.get("SPARK_KAFKA_PKG", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "spark-pipeline"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "500m"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "500m"))
        .config("spark.driver.memoryOverhead", "64m")
        .config("spark.executor.memoryOverhead", "64m")
        .config("spark.driver.extraJavaOptions", "-XX:+UseSerialGC")
        .config("spark.executor.extraJavaOptions", "-XX:+UseSerialGC")
        .config("spark.cores.max", os.environ.get("SPARK_CORES_MAX", "1"))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.memory.fraction", "0.5")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.jars.packages", kafka_pkg)
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.defaultFS", hdfs_url)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Stage 1: Consume incremental Kafka streams -> HDFS + HBase
# ---------------------------------------------------------------------------

def process_kafka_streams(
    spark: SparkSession,
    kafka_brokers: str,
    hdfs_base: str,
    hbase_host: str,
    hbase_port: int,
) -> Dict[str, int]:
    stats = {"events_written": 0, "mentions_written": 0, "gkg_written": 0}
    checkpoints = f"{hdfs_base}/checkpoints/pipeline"

    # ---- 1a. Events Stream ----
    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "gdelt.events.raw")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5000")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed_events = raw_events.select(
        F.from_json(F.col("value").cast("string"), EVENTS_SCHEMA).alias("ev"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("ev.*", "kafka_timestamp")

    # Derive accurate timestamp using DATEADDED when same date as SQLDATE
    sqldate_str = F.col("SQLDATE").cast("string")
    dateadded_str = F.col("DATEADDED").cast("string")
    event_time_col = (
        F.when(
            (F.length(dateadded_str) >= 14) & (F.substring(dateadded_str, 1, 8) == sqldate_str),
            F.to_timestamp(F.substring(dateadded_str, 1, 14), "yyyyMMddHHmmss"),
        )
        .otherwise(F.to_timestamp(sqldate_str, "yyyyMMdd") + F.expr("INTERVAL 12 HOURS"))
    )

    enriched_events = (
        parsed_events
        .filter(F.col("GlobalEventID").isNotNull() & F.col("SQLDATE").isNotNull())
        .withColumn("event_time", event_time_col)
        .filter(F.col("event_time").isNotNull())
        .withColumn(
            "EventRootCode",
            F.coalesce(F.col("EventRootCode"), F.substring(F.col("EventCode"), 1, 2)),
        )
        .withColumn("category_name", F.expr(_category_sql_expr()))
        .withColumn(
            "cooperation_flag",
            F.when(F.col("QuadClass").isin(1, 2), F.lit("cooperation"))
            .when(F.col("QuadClass").isin(3, 4), F.lit("conflict"))
            .otherwise(F.lit("unknown")),
        )
        .withColumn("date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
        .withColumn("hour_partition", F.date_format(F.col("event_time"), "HH"))
    )

    def _events_batch(batch_df: DataFrame, batch_id: int) -> None:
        deduped = batch_df.dropDuplicates(["GlobalEventID"]).persist()
        try:
            count = deduped.count()
            if count == 0:
                return
            logger.info("Events micro-batch %d: %d records", batch_id, count)

            # Write raw & curated Parquet to HDFS
            (
                deduped.write.mode("append")
                .partitionBy("date_partition", "hour_partition")
                .parquet(f"{hdfs_base}/raw/events")
            )
            (
                deduped.write.mode("append")
                .partitionBy("date_partition")
                .parquet(f"{hdfs_base}/curated/events")
            )

            # Publish normalized events back to Kafka
            (
                deduped.select(
                    F.col("GlobalEventID").cast("string").alias("key"),
                    F.to_json(F.struct(*[c for c in deduped.columns if c != "kafka_timestamp"])).alias("value"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", kafka_brokers)
                .option("topic", "gdelt.events.normalized")
                .save()
            )

            # Stream rows to HBase in bounded batches to keep driver memory flat
            client = HBaseClient(host=hbase_host, port=hbase_port, auto_connect=True)
            try:
                written = 0
                buf: List[Dict[str, Any]] = []
                for row in deduped.toLocalIterator(prefetchPartitions=False):
                    buf.append(row.asDict())
                    if len(buf) >= 200:
                        written += client.put_events_batch(buf, batch_size=200)
                        buf.clear()
                if buf:
                    written += client.put_events_batch(buf, batch_size=200)
                    buf.clear()
                stats["events_written"] += written
                logger.info("HBase batch write: %d events persisted", written)
            finally:
                client.close()
        finally:
            deduped.unpersist()

    q_events = (
        enriched_events.writeStream
        .foreachBatch(_events_batch)
        .option("checkpointLocation", f"{checkpoints}/events_raw")
        .trigger(availableNow=True)
        .start()
    )
    q_events.awaitTermination()

    # ---- 1b. Mentions Stream ----
    raw_mentions = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "gdelt.mentions.raw")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "10000")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed_mentions = (
        raw_mentions.select(
            F.from_json(F.col("value").cast("string"), MENTIONS_SCHEMA).alias("m"),
        )
        .select("m.*")
        .filter(F.col("GlobalEventID").isNotNull() & F.col("MentionTimeDate").isNotNull())
        .withColumn("event_time", F.to_timestamp(F.col("MentionTimeDate").cast("string"), "yyyyMMddHHmmss"))
        .filter(F.col("event_time").isNotNull())
        .withColumn("date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
        .withColumn("hour_partition", F.date_format(F.col("event_time"), "HH"))
    )

    def _mentions_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        cnt = batch_df.count()
        logger.info("Mentions micro-batch %d: %d records", batch_id, cnt)
        (
            batch_df.write.mode("append")
            .partitionBy("date_partition", "hour_partition")
            .parquet(f"{hdfs_base}/raw/mentions")
        )
        (
            batch_df.write.mode("append")
            .partitionBy("date_partition")
            .parquet(f"{hdfs_base}/curated/mentions")
        )
        stats["mentions_written"] += cnt

    q_mentions = (
        parsed_mentions.writeStream
        .foreachBatch(_mentions_batch)
        .option("checkpointLocation", f"{checkpoints}/mentions_raw")
        .trigger(availableNow=True)
        .start()
    )
    q_mentions.awaitTermination()

    # ---- 1c. GKG Stream ----
    raw_gkg = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "gdelt.gkg.raw")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5000")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed_gkg = (
        raw_gkg.select(
            F.from_json(F.col("value").cast("string"), GKG_SCHEMA).alias("g"),
        )
        .select("g.*")
        .filter(F.col("GKGRecordID").isNotNull() & F.col("DATE").isNotNull())
        .withColumn("event_time", F.to_timestamp(F.substring(F.col("DATE"), 1, 14), "yyyyMMddHHmmss"))
        .filter(F.col("event_time").isNotNull())
        .withColumn("date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
        .withColumn("hour_partition", F.date_format(F.col("event_time"), "HH"))
        # Normalize themes with pure Spark SQL expressions (strip GDELT noisy prefixes)
        .withColumn(
            "topics",
            F.array_distinct(
                F.filter(
                    F.transform(
                        F.coalesce(F.col("Themes"), F.array().cast("array<string>")),
                        lambda t: F.regexp_replace(
                            F.upper(F.trim(t)),
                            r"^(TAX_FNCACT_|TAX_WORLDLANGUAGES_|TAX_WORLDMAMMALS_|TAX_ETHNICITY_|TAX_|WB_\d+_|WB_|USPEC_|GENERAL_|MEDIA_|INTERNET_|ECON_|CRISISLEX_[A-Z0-9]+_|CRISISLEX_|EPU_CATS_|EPU_POLICY_|EPU_|MANMADE_|ENV_)",
                            "",
                        ),
                    ),
                    lambda t: (
                        (F.length(t) >= 4)
                        & (~t.rlike("^(GENERAL|GENERAL_HEALTH|TAX_|WB_|USPEC_|UNGP_)"))
                        & (~t.rlike("^[0-9_]+$"))
                    ),
                )
            ),
        )
        .withColumn(
            "country_codes",
            F.array_distinct(
                F.filter(
                    F.coalesce(F.col("Locations.country_code"), F.array().cast("array<string>")),
                    lambda c: c.isNotNull() & (F.length(c) == 2),
                )
            ),
        )
    )

    def _gkg_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        cnt = batch_df.count()
        logger.info("GKG micro-batch %d: %d records", batch_id, cnt)
        (
            batch_df.write.mode("append")
            .partitionBy("date_partition", "hour_partition")
            .parquet(f"{hdfs_base}/raw/gkg")
        )
        curated_gkg = batch_df.drop("Persons", "Organizations", "Locations")
        (
            curated_gkg.write.mode("append")
            .partitionBy("date_partition")
            .parquet(f"{hdfs_base}/curated/gkg")
        )
        stats["gkg_written"] += cnt

    q_gkg = (
        parsed_gkg.writeStream
        .foreachBatch(_gkg_batch)
        .option("checkpointLocation", f"{checkpoints}/gkg_raw")
        .trigger(availableNow=True)
        .start()
    )
    q_gkg.awaitTermination()

    return stats


# ---------------------------------------------------------------------------
# Stage 2: Distributed Spark Analytics over HDFS Curated Parquet -> HBase
# ---------------------------------------------------------------------------

def compute_analytics(
    spark: SparkSession,
    kafka_brokers: str,
    hdfs_base: str,
    hbase_host: str,
    hbase_port: int,
) -> Dict[str, int]:
    counts = {"country_rows": 0, "category_rows": 0, "topic_rows": 0, "anomaly_rows": 0}
    curated_events_path = f"{hdfs_base}/curated/events"
    curated_gkg_path = f"{hdfs_base}/curated/gkg"

    if not _hdfs_path_exists(spark, curated_events_path):
        logger.info("No curated events in HDFS yet — skipping analytics")
        return counts

    events_df = (
        spark.read.parquet(curated_events_path)
        .dropDuplicates(["GlobalEventID"])
        # Limit window to events within recent 90 days for active metrics
        .filter(F.col("event_time") >= F.expr("current_timestamp() - INTERVAL 90 DAYS"))
        .persist()
    )
    if events_df.isEmpty():
        events_df.unpersist()
        return counts

    client = HBaseClient(host=hbase_host, port=hbase_port, auto_connect=True)
    try:
        # ---- 2a. Country Metrics (1-hour windows) ----
        country_agg = (
            events_df
            .filter(F.col("ActionGeo_CountryCode").isNotNull() & (F.length("ActionGeo_CountryCode") == 2))
            .groupBy(
                F.window("event_time", "1 hour").alias("w"),
                F.col("ActionGeo_CountryCode").alias("country_code"),
            )
            .agg(
                F.count("GlobalEventID").alias("event_count"),
                F.coalesce(F.sum("NumMentions"), F.lit(0)).alias("mention_count"),
                F.coalesce(F.sum("NumArticles"), F.lit(0)).alias("article_count"),
                F.round(F.avg("AvgTone"), 3).alias("avg_tone"),
                F.round(F.avg("GoldsteinScale"), 3).alias("avg_goldstein"),
                F.sum(F.when(F.col("QuadClass").isin(1, 2), 1).otherwise(0)).alias("cooperation_count"),
                F.sum(F.when(F.col("QuadClass").isin(3, 4), 1).otherwise(0)).alias("conflict_count"),
            )
            .select(
                "country_code",
                F.date_format("w.start", "yyyy-MM-dd").alias("date"),
                F.hour("w.start").alias("hour"),
                F.unix_timestamp("w.start").alias("epoch_ts"),
                "event_count", "mention_count", "article_count",
                "avg_tone", "avg_goldstein",
                "cooperation_count", "conflict_count",
            )
        )
        country_agg.write.mode("overwrite").parquet(f"{hdfs_base}/analytics/country_metrics")

        country_rows: List[Tuple[str, int, Dict[str, Any]]] = []
        for r in country_agg.collect():
            cc = r["country_code"]
            ts = int(r["epoch_ts"])
            country_rows.append((cc, ts, {
                "country_code": cc,
                "country_name": FIPS_NAMES.get(cc, cc),
                "date": r["date"],
                "hour": r["hour"],
                "event_count": int(r["event_count"]),
                "mention_count": int(r["mention_count"]),
                "article_count": int(r["article_count"]),
                "avg_tone": float(r["avg_tone"]) if r["avg_tone"] is not None else 0.0,
                "avg_goldstein": float(r["avg_goldstein"]) if r["avg_goldstein"] is not None else 0.0,
                "cooperation_count": int(r["cooperation_count"]),
                "conflict_count": int(r["conflict_count"]),
            }))
        counts["country_rows"] = client.put_country_metrics_batch(country_rows)

        # ---- 2b. Category Metrics (1-hour windows) ----
        cat_agg = (
            events_df
            .filter(F.col("EventRootCode").isNotNull())
            .groupBy(
                F.window("event_time", "1 hour").alias("w"),
                F.col("EventRootCode").alias("root_code"),
            )
            .agg(
                F.count("GlobalEventID").alias("count"),
                F.round(F.avg("GoldsteinScale"), 3).alias("avg_goldstein"),
                F.round(F.avg("AvgTone"), 3).alias("avg_tone"),
            )
            .withColumn("is_conflict", F.col("root_code").cast("int") >= 10)
            .select(
                "root_code",
                F.date_format("w.start", "yyyy-MM-dd").alias("date"),
                F.hour("w.start").alias("hour"),
                F.unix_timestamp("w.start").alias("epoch_ts"),
                "count", "avg_goldstein", "avg_tone", "is_conflict",
            )
        )
        cat_agg.write.mode("overwrite").parquet(f"{hdfs_base}/analytics/category_metrics")

        cat_rows: List[Tuple[str, int, Dict[str, Any]]] = []
        for r in cat_agg.collect():
            rc = r["root_code"]
            ts = int(r["epoch_ts"])
            cat_rows.append((rc, ts, {
                "root_code": rc,
                "date": r["date"],
                "hour": r["hour"],
                "count": int(r["count"]),
                "avg_goldstein": float(r["avg_goldstein"]) if r["avg_goldstein"] is not None else 0.0,
                "avg_tone": float(r["avg_tone"]) if r["avg_tone"] is not None else 0.0,
                "is_conflict": bool(r["is_conflict"]),
            }))
        counts["category_rows"] = client.put_category_metrics_batch(cat_rows)

        # ---- 2c. Topic Metrics (from GKG curated) ----
        topic_agg = None
        if _hdfs_path_exists(spark, curated_gkg_path):
            gkg_df = (
                spark.read.parquet(curated_gkg_path)
                .dropDuplicates(["GKGRecordID"])
                .filter(F.col("event_time") >= F.expr("current_timestamp() - INTERVAL 90 DAYS"))
            )
            exploded_topics = (
                gkg_df
                .filter(F.size(F.col("topics")) > 0)
                .select(
                    "event_time",
                    "SourceCommonName",
                    "Tone",
                    "country_codes",
                    F.explode("topics").alias("topic"),
                )
            )
            # Keep top 50 topics by overall frequency so tables stay meaningful
            top_topic_names = [
                r["topic"]
                for r in exploded_topics.groupBy("topic").count().orderBy(F.desc("count")).limit(50).collect()
            ]
            if top_topic_names:
                filtered_topics = exploded_topics.filter(F.col("topic").isin(top_topic_names))
                topic_agg = (
                    filtered_topics
                    .groupBy(F.window("event_time", "1 hour").alias("w"), "topic")
                    .agg(
                        F.count("*").alias("count"),
                        F.countDistinct("SourceCommonName").alias("article_count"),
                        F.round(F.avg("Tone"), 3).alias("avg_tone"),
                        F.flatten(F.collect_list("country_codes")).alias("all_countries"),
                    )
                    .select(
                        "topic",
                        F.date_format("w.start", "yyyy-MM-dd").alias("date"),
                        F.hour("w.start").alias("hour"),
                        F.unix_timestamp("w.start").alias("epoch_ts"),
                        "count", "article_count", "avg_tone", "all_countries",
                    )
                )
                topic_agg.drop("all_countries").write.mode("overwrite").parquet(f"{hdfs_base}/analytics/topic_metrics")

                topic_rows: List[Tuple[str, int, Dict[str, Any]]] = []
                for r in topic_agg.collect():
                    dist: Dict[str, int] = {}
                    for cc in (r["all_countries"] or []):
                        if cc:
                            dist[cc] = dist.get(cc, 0) + 1
                    top_dist = dict(sorted(dist.items(), key=lambda x: x[1], reverse=True)[:15])
                    topic_rows.append((r["topic"], int(r["epoch_ts"]), {
                        "topic": r["topic"],
                        "date": r["date"],
                        "hour": r["hour"],
                        "count": int(r["count"]),
                        "article_count": int(r["article_count"]),
                        "avg_tone": float(r["avg_tone"]) if r["avg_tone"] is not None else 0.0,
                        "country_distribution": json.dumps(top_dist),
                    }))
                counts["topic_rows"] = client.put_topic_metrics_batch(topic_rows)

        # ---- 2d. Anomaly Detection (Temporal leave-one-out or peer baseline Z >= 2.5) ----
        now_epoch = int(datetime.now(timezone.utc).timestamp())
        now_iso = datetime.now(timezone.utc).isoformat()
        anomaly_rows: List[Tuple[str, str, int, Dict[str, Any]]] = []

        def _detect_entity_anomalies(
            series: List[Tuple[str, int, int]],
            entity_type: str,
            label_fn: Any,
            z_thresh: float = 2.5,
            min_count: int = 10,
        ) -> List[Tuple[str, str, int, Dict[str, Any]]]:
            by_key: Dict[str, List[Tuple[int, int]]] = {}
            totals: Dict[str, int] = {}
            for key, ts, cnt in series:
                by_key.setdefault(key, []).append((ts, cnt))
                totals[key] = totals.get(key, 0) + cnt
            if not by_key:
                return []

            # Trimmed peer baseline (excludes top 5% extremes so a single mega-hub doesn't mask spikes)
            sorted_totals = sorted(totals.values())
            trim_n = max(1, len(sorted_totals) // 20) if len(sorted_totals) >= 10 else 0
            peer_sample = sorted_totals[: len(sorted_totals) - trim_n] if trim_n else sorted_totals
            peer_mean = sum(peer_sample) / len(peer_sample)
            peer_var = sum((v - peer_mean) ** 2 for v in peer_sample) / max(len(peer_sample), 1)
            peer_std = (peer_var ** 0.5) + 0.5

            found: List[Tuple[str, str, int, Dict[str, Any]]] = []
            for key, pts in by_key.items():
                pts.sort(key=lambda x: x[0])
                cur_win = pts[-1][1]
                if len(pts) >= 6:
                    prior = [c for _, c in pts[:-1]]
                    base = sum(prior) / len(prior)
                    var = sum((c - base) ** 2 for c in prior) / len(prior)
                    std = (var ** 0.5) + 0.5
                    cur = float(cur_win)
                else:
                    base = peer_mean
                    std = peer_std
                    cur = float(totals[key])

                z = (cur - base) / std
                if z >= z_thresh and cur >= min_count:
                    pct = ((cur - base) / (base + 1.0)) * 100.0
                    found.append((
                        entity_type,
                        key,
                        now_epoch,
                        {
                            "anomaly_type": entity_type,
                            "label": label_fn(key),
                            "country_code": key if entity_type == "country" else "",
                            "topic": key if entity_type == "topic" else "",
                            "baseline_avg": round(base, 2),
                            "current_count": round(cur, 0),
                            "pct_change": round(pct, 1),
                            "z_score": round(z, 2),
                            "severity": "critical" if z >= 3.5 else "high",
                            "detected_at": now_iso,
                        },
                    ))
            found.sort(key=lambda item: item[3]["z_score"], reverse=True)
            return found[:25]

        anomaly_rows.extend(
            _detect_entity_anomalies(
                [(d.get("country_code", cc), ts, int(d["event_count"])) for cc, ts, d in country_rows],
                "country",
                lambda cc: FIPS_NAMES.get(cc, cc),
            )
        )
        if counts.get("topic_rows", 0) > 0:
            anomaly_rows.extend(
                _detect_entity_anomalies(
                    [(d["topic"], ts, int(d["count"])) for _, ts, d in topic_rows],
                    "topic",
                    lambda t: t.replace("_", " ").title(),
                )
            )

        if anomaly_rows:
            anomaly_df = spark.createDataFrame(
                [{k: v for k, v in payload.items()} for _, _, _, payload in anomaly_rows]
            )
            anomaly_df.write.mode("overwrite").parquet(f"{hdfs_base}/analytics/anomaly_metrics")
            counts["anomaly_rows"] = client.put_anomalies_batch(anomaly_rows)

        # Publish analytics summary to gdelt.analytics Kafka topic
        summary_df = spark.createDataFrame([{
            "key": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "value": json.dumps({
                "timestamp": now_iso,
                **counts,
            }),
        }])
        (
            summary_df.write.format("kafka")
            .option("kafka.bootstrap.servers", kafka_brokers)
            .option("topic", "gdelt.analytics")
            .save()
        )
    finally:
        client.close()
        events_df.unpersist()

    return counts


def _write_processing_metrics(stream_stats: Dict[str, int], analytics_stats: Dict[str, int]) -> None:
    metrics_path = Path(os.environ.get("GDELT_PROCESSING_METRICS_FILE", "data/metrics/processing_metrics.json"))
    try:
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_spark_run": datetime.now(timezone.utc).isoformat(),
            **stream_stats,
            **analytics_stats,
        }
        tmp_fd, tmp_name = tempfile.mkstemp(dir=metrics_path.parent, prefix=".proc_", suffix=".json")
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        os.chmod(tmp_name, 0o644)
        os.replace(tmp_name, metrics_path)
    except Exception as exc:
        logger.warning("Could not write processing metrics: %s", exc)


def main() -> None:
    kafka_brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    hdfs_url = os.environ.get("HDFS_NAMENODE_URL", "hdfs://namenode:9000").rstrip("/")
    hdfs_base = f"{hdfs_url}/gdelt"
    hbase_host = os.environ.get("HBASE_HOST", "hbase-master")
    hbase_port = int(os.environ.get("HBASE_THRIFT_PORT", "9090"))

    logger.info("Starting Spark pipeline run: kafka=%s hdfs=%s hbase=%s:%d",
                kafka_brokers, hdfs_base, hbase_host, hbase_port)

    # Ensure all HBase tables exist before streaming starts
    try:
        hb = HBaseClient(host=hbase_host, port=hbase_port, auto_connect=True)
        hb.create_tables(drop_if_exists=False)
        hb.close()
    except Exception as exc:
        logger.warning("Pre-flight HBase table check failed (continuing): %s", exc)

    spark = create_spark()
    try:
        stream_stats = process_kafka_streams(spark, kafka_brokers, hdfs_base, hbase_host, hbase_port)
        analytics_stats = compute_analytics(spark, kafka_brokers, hdfs_base, hbase_host, hbase_port)
        _write_processing_metrics(stream_stats, analytics_stats)
        logger.info("Spark pipeline run complete — stream=%s analytics=%s", stream_stats, analytics_stats)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
