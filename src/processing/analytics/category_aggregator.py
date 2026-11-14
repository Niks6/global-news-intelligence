"""
CAMEO event category aggregation.
Groups by EventRootCode (01-20), computes counts, Goldstein avg, tone avg.
Writes to HDFS /gdelt/analytics/category_metrics/ and HBase category_metrics_by_time.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# CAMEO root codes to human-readable names (broadcast-friendly)
CAMEO_NAMES = {
    "01": "Make Public Statement", "02": "Appeal", "03": "Express Intent to Cooperate",
    "04": "Consult", "05": "Diplomatic Cooperation", "06": "Material Cooperation",
    "07": "Provide Aid", "08": "Yield", "09": "Investigate", "10": "Demand",
    "11": "Disapprove", "12": "Reject", "13": "Threaten", "14": "Protest",
    "15": "Exhibit Military Posture", "16": "Reduce Relations", "17": "Coerce",
    "18": "Assault", "19": "Fight", "20": "Unconventional Mass Violence",
}


def aggregate_category_metrics(events_df: DataFrame) -> DataFrame:
    """
    Aggregate events by CAMEO root code and 1-hour tumbling window.

    Input: normalized events with EventRootCode, QuadClass, GoldsteinScale, AvgTone.
    Output: per-category metrics.
    """
    return (
        events_df
        .filter(F.col("EventRootCode").isNotNull())
        .withWatermark("EventDateTime", "1 hour")
        .groupBy(
            F.window("EventDateTime", "1 hour").alias("time_window"),
            F.col("EventRootCode").alias("root_code"),
        )
        .agg(
            F.count("GlobalEventID").alias("count"),
            F.avg("GoldsteinScale").alias("avg_goldstein"),
            F.avg("AvgTone").alias("avg_tone"),
        )
        .withColumn(
            "is_conflict",
            F.col("root_code").cast("int") >= 10
        )
        .select(
            "root_code",
            F.date_format(F.col("time_window.start"), "yyyy-MM-dd").alias("date"),
            F.hour(F.col("time_window.start")).alias("hour"),
            "count",
            F.round("avg_goldstein", 3).alias("avg_goldstein"),
            F.round("avg_tone", 3).alias("avg_tone"),
            "is_conflict",
        )
    )

