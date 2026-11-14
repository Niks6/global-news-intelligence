"""
Country-level aggregation — Spark Structured Streaming micro-batch.
Tumbling window: 1 hour. Aggregates by ActionGeo_CountryCode.
Writes to HDFS /gdelt/analytics/country_metrics/ and HBase country_metrics_by_time.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def aggregate_country_metrics(events_df: DataFrame) -> DataFrame:
    """
    Aggregate events by country and 1-hour tumbling window.

    Input: normalized events with columns:
      EventDateTime (timestamp), ActionGeo_CountryCode, QuadClass,
      GoldsteinScale, AvgTone, NumMentions, NumArticles, GlobalEventID

    Output: country metrics per hour.
    """
    return (
        events_df
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .withWatermark("EventDateTime", "1 hour")
        .groupBy(
            F.window("EventDateTime", "1 hour").alias("time_window"),
            F.col("ActionGeo_CountryCode").alias("country_code"),
        )
        .agg(
            F.count("GlobalEventID").alias("event_count"),
            F.sum("NumMentions").alias("mention_count"),
            F.sum("NumArticles").alias("article_count"),
            F.avg("AvgTone").alias("avg_tone"),
            F.avg("GoldsteinScale").alias("avg_goldstein"),
            F.sum(F.when(F.col("QuadClass").isin(1, 2), 1).otherwise(0)).alias("cooperation_count"),
            F.sum(F.when(F.col("QuadClass").isin(3, 4), 1).otherwise(0)).alias("conflict_count"),
        )
        .select(
            F.col("country_code"),
            F.date_format(F.col("time_window.start"), "yyyy-MM-dd").alias("date"),
            F.hour(F.col("time_window.start")).alias("hour"),
            "event_count", "mention_count", "article_count",
            F.round("avg_tone", 3).alias("avg_tone"),
            F.round("avg_goldstein", 3).alias("avg_goldstein"),
            "cooperation_count", "conflict_count",
        )
    )

