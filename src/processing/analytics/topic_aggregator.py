"""
Topic-level aggregation from GKG themes.
Normalizes themes: lowercases, strips GDELT noise prefixes (TAX_, WB_, USPEC_, etc.).
Writes to HDFS /gdelt/analytics/topic_metrics/ and HBase topic_metrics_by_time.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Prefixes to strip from GDELT themes to get cleaner topic names
NOISE_PREFIXES = ["TAX_", "WB_", "USPEC_", "GENERAL_", "MEDIA_", "INTERNET_"]


def normalize_theme(theme_col):
    """Strip noise prefixes from a theme string, lowercase, and trim."""
    result = F.lower(F.trim(theme_col))
    for prefix in NOISE_PREFIXES:
        result = F.regexp_replace(result, f"^{prefix.lower()}", "")
    return result


def aggregate_topic_metrics(gkg_df: DataFrame) -> DataFrame:
    """
    Aggregate topics from GKG records.

    Input: GKG records with:
      DocumentDateTime (timestamp), Themes (array of strings),
      SourceCommonName, Tone, CountryCode (extracted from locations)

    Output: topic metrics per 1-hour window.
    """
    # Explode themes array into individual rows
    exploded = (
        gkg_df
        .filter(F.size(F.col("Themes")) > 0)
        .select(
            F.col("DocumentDateTime"),
            F.explode(F.col("Themes")).alias("raw_theme"),
            F.col("SourceCommonName"),
            F.col("Tone"),
        )
        .withColumn("topic", normalize_theme(F.col("raw_theme")))
        .filter(F.length("topic") > 2)  # drop very short/empty themes
        .filter(~F.col("topic").rlike("^(tax_|wb_|uspec_)"))  # extra safety
    )

    return (
        exploded
        .withWatermark("DocumentDateTime", "1 hour")
        .groupBy(
            F.window("DocumentDateTime", "1 hour").alias("time_window"),
            "topic",
        )
        .agg(
            F.count("*").alias("count"),
            F.countDistinct("SourceCommonName").alias("article_count"),
            F.avg("Tone").alias("avg_tone"),
        )
        .select(
            "topic",
            F.date_format(F.col("time_window.start"), "yyyy-MM-dd").alias("date"),
            F.hour(F.col("time_window.start")).alias("hour"),
            "count", "article_count",
            F.round("avg_tone", 3).alias("avg_tone"),
        )
    )

