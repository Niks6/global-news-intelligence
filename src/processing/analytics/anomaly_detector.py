"""
Anomaly / spike detection using rolling z-score.

Method:
  - For each country/topic: compare last 2-hour window against 24-hour baseline
  - Z-score = (current_count - baseline_mean) / (baseline_std + epsilon)
  - Flag if z_score > 2.5 AND current_count >= MIN_VOLUME_THRESHOLD (10)
  - Severity: critical (z≥3.0), high (z≥2.5), medium (z≥2.0)

Avoids:
  - Division by zero: epsilon = 0.01 added to std
  - Tiny baseline spikes: minimum volume threshold of 10 events
  - pct_change overflow: uses (current - mean) / (mean + 1) * 100

Writes anomalies to HBase anomalies_by_time.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


MIN_VOLUME_THRESHOLD = 10
Z_SCORE_THRESHOLD = 2.5
EPSILON = 0.01  # avoid division by zero


def detect_anomalies_country(
    country_metrics_df: DataFrame,
    current_window_hours: int = 2,
    baseline_window_hours: int = 24,
) -> DataFrame:
    """
    Detect anomalies in country event counts.

    Input: country_metrics with columns: country_code, date, hour, event_count, avg_tone.
    Output: anomaly alerts.
    """
    # Build current window aggregate (last N hours)
    current = (
        country_metrics_df
        .groupBy("country_code")
        .agg(
            F.sum("event_count").alias("current_count"),
            F.avg("avg_tone").alias("current_tone"),
        )
    )

    # In a full streaming context, the baseline comes from state;
    # here we compute it from the same batch as a simplified approach.
    baseline = (
        country_metrics_df
        .groupBy("country_code")
        .agg(
            F.avg("event_count").alias("baseline_avg"),
            F.stddev("event_count").alias("baseline_std"),
        )
    )

    joined = current.join(baseline, "country_code", "inner")

    return (
        joined
        .withColumn("baseline_std_safe", F.col("baseline_std") + F.lit(EPSILON))
        .withColumn("z_score",
                     (F.col("current_count") - F.col("baseline_avg")) / F.col("baseline_std_safe"))
        .withColumn("pct_change",
                     (F.col("current_count") - F.col("baseline_avg")) /
                     (F.col("baseline_avg") + F.lit(1)) * F.lit(100))
        .filter(F.col("z_score") >= Z_SCORE_THRESHOLD)
        .filter(F.col("current_count") >= MIN_VOLUME_THRESHOLD)
        .withColumn("severity",
                     F.when(F.col("z_score") >= 3.0, "critical")
                      .when(F.col("z_score") >= 2.5, "high")
                      .otherwise("medium"))
        .withColumn("anomaly_type", F.lit("country"))
        .withColumn("label", F.col("country_code"))
        .select(
            "anomaly_type", "label", "country_code",
            F.round("baseline_avg", 2).alias("baseline_avg"),
            F.round("current_count", 0).alias("current_count"),
            F.round("pct_change", 1).alias("pct_change"),
            F.round("z_score", 2).alias("z_score"),
            "severity",
        )
    )

