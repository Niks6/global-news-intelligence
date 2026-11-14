"""Spark Structured Streaming job: GDELT GKG stream.

Pipeline:
  Kafka (gdelt.gkg.raw)
    → parse JSON (GKG V2 schema subset)
    → parse V2Themes (semicolon-delimited) → array of theme strings
    → parse V2Tone (comma-delimited 7-field string) → tone components
    → extract topic list from themes (normalized, deduplicated)
    → write raw to HDFS /gdelt/raw/gkg/date=YYYY-MM-DD/hour=HH/
    → write curated to HDFS /gdelt/curated/gkg/date=YYYY-MM-DD/

Checkpoint: /gdelt/checkpoints/gkg_stream/
Trigger: processingTime '30 seconds'
"""

import logging
import os

from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql import functions as F  # type: ignore
from pyspark.sql.types import (  # type: ignore
    StructType, StructField,
    StringType, ArrayType, DoubleType, LongType, IntegerType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GKG V2 schema (main fields we care about)
# ---------------------------------------------------------------------------
GKG_SCHEMA = StructType([
    StructField("GKGRECORDID", StringType(), True),
    StructField("DATE", LongType(), True),              # YYYYMMDDHHMMSS
    StructField("SourceCollectionIdentifier", IntegerType(), True),
    StructField("SourceCommonName", StringType(), True),
    StructField("DocumentIdentifier", StringType(), True),
    StructField("Counts", StringType(), True),           # delimited sub-fields
    StructField("V2Counts", StringType(), True),
    StructField("Themes", StringType(), True),           # legacy themes
    StructField("V2Themes", StringType(), True),         # semicolon-delimited GDELT themes
    StructField("Locations", StringType(), True),
    StructField("V2Locations", StringType(), True),
    StructField("Persons", StringType(), True),
    StructField("V2Persons", StringType(), True),
    StructField("Organizations", StringType(), True),
    StructField("V2Organizations", StringType(), True),
    StructField("V2Tone", StringType(), True),           # comma: tone,pos,neg,polarity,activity,selfref,wordcount
    StructField("Dates", StringType(), True),
    StructField("GCAM", StringType(), True),
    StructField("SharingImage", StringType(), True),
    StructField("RelatedImages", StringType(), True),
    StructField("SocialImageEmbeds", StringType(), True),
    StructField("SocialVideoEmbeds", StringType(), True),
    StructField("Quotations", StringType(), True),
    StructField("AllNames", StringType(), True),
    StructField("Amounts", StringType(), True),
    StructField("TranslationInfo", StringType(), True),
    StructField("Extras", StringType(), True),
])

# V2Tone field order: tone,pos_score,neg_score,polarity,activity_ref_density,self_group_ref_density,word_count
_V2TONE_FIELDS = ["tone", "pos_score", "neg_score", "polarity",
                  "activity_density", "self_ref_density", "word_count"]

# Theme prefixes to strip during normalization
_GDELT_PREFIXES = ("WB_", "TAX_", "ECON_", "CRISISLEX_", "EPU_", "MANMADE_", "ENV_")

# Noise themes to exclude
_NOISE_THEMES = frozenset(["GENERAL", "GENERAL_HEALTH", "TYPE:GENERAL"])


def _normalize_theme_udf():
    """UDF: lowercase, strip GDELT prefixes, return None for noise themes."""
    from pyspark.sql.functions import udf  # type: ignore
    from pyspark.sql.types import StringType as ST  # type: ignore

    def _normalize(theme: str) -> str:
        if not theme:
            return None
        t = theme.upper().strip()
        for prefix in _GDELT_PREFIXES:
            if t.startswith(prefix):
                t = t[len(prefix):]
                break
        t_lower = t.lower()
        return None if t_lower.upper() in _NOISE_THEMES else t_lower

    return udf(_normalize, ST())


def run(spark: SparkSession) -> None:
    hdfs_base = os.environ.get("HDFS_BASE", "hdfs://namenode:8020/gdelt")
    kafka_brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    checkpoint_base = f"{hdfs_base}/checkpoints/gkg_stream"

    normalize_theme = _normalize_theme_udf()

    # Read from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "gdelt.gkg.raw")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "2000")
        .option("kafka.group.id", "spark-gkg-consumer")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw_stream.select(
        F.from_json(F.col("value").cast("string"), GKG_SCHEMA).alias("g"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("g.*", "kafka_timestamp")

    # Derive event_time
    with_time = parsed.withColumn(
        "event_time",
        F.to_timestamp(F.col("DATE").cast("string"), "yyyyMMddHHmmss"),
    ).withColumn(
        "date_partition", F.date_format(F.col("event_time"), "yyyy-MM-dd")
    ).withColumn(
        "hour_partition", F.date_format(F.col("event_time"), "HH")
    )

    valid = with_time.filter(F.col("GKGRECORDID").isNotNull() & F.col("DATE").isNotNull())
    watermarked = valid.withWatermark("event_time", "2 hours")

    # Parse V2Themes: "THEME1;THEME2;..." → array, then normalize
    with_themes = watermarked.withColumn(
        "themes_raw", F.split(F.col("V2Themes"), ";")
    ).withColumn(
        "topics",
        F.array_distinct(
            F.filter(
                F.transform(F.col("themes_raw"), normalize_theme),
                lambda t: t.isNotNull(),
            )
        ),
    )

    # Parse V2Tone: "tone,pos,neg,polarity,activity,selfref,wordcount"
    tone_cols = with_themes
    for i, field_name in enumerate(_V2TONE_FIELDS):
        tone_cols = tone_cols.withColumn(
            f"tone_{field_name}",
            F.split(F.col("V2Tone"), ",").getItem(i).cast("double"),
        )

    enriched = tone_cols.drop("themes_raw")

    # Write raw
    raw_query = (
        enriched.writeStream
        .format("parquet")
        .option("path", f"{hdfs_base}/raw/gkg")
        .option("checkpointLocation", f"{checkpoint_base}/raw_hdfs")
        .partitionBy("date_partition", "hour_partition")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    # Write curated (drop heavy raw string columns)
    curated = enriched.drop("Counts", "Locations", "Persons", "Organizations",
                             "GCAM", "Quotations", "AllNames", "Extras")
    curated_query = (
        curated.writeStream
        .format("parquet")
        .option("path", f"{hdfs_base}/curated/gkg")
        .option("checkpointLocation", f"{checkpoint_base}/curated_hdfs")
        .partitionBy("date_partition")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    logger.info("GKG stream started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    from src.processing.spark_session import create_spark_session  # type: ignore

    _spark = create_spark_session(app_name="GDELT-GKGStream")
    run(_spark)
