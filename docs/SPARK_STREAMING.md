# Spark Structured Streaming Design

## Overview

Three Spark Structured Streaming jobs consume Kafka topics, process GDELT data, and write to HDFS (raw + curated) and HBase (serving layer).

## Streaming Jobs

| Job | Kafka Topic | Output HDFS | Output HBase | Trigger |
|-----|-------------|-------------|--------------|---------|
| `events_stream.py` | `gdelt.events.raw` | `/gdelt/raw/events/`, `/gdelt/curated/events/` | `events_by_id`, `events_by_country_time` | 30 seconds |
| `mentions_stream.py` | `gdelt.mentions.raw` | `/gdelt/raw/mentions/`, `/gdelt/curated/mentions/` | — | 30 seconds |
| `gkg_stream.py` | `gdelt.gkg.raw` | `/gdelt/raw/gkg/`, `/gdelt/curated/gkg/` | `topic_metrics_by_time` | 30 seconds |

## Processing Pipeline (Events)

1. **Consume** from Kafka `gdelt.events.raw` — JSON-serialized event records
2. **Parse** JSON → typed DataFrame with all 61 event columns
3. **Validate** required fields: `GlobalEventID`, `SQLDATE`, `EventCode`, `EventRootCode`, `QuadClass`
4. **Quarantine** invalid rows → log + skip (do not crash)
5. **Deduplicate** by `GlobalEventID` using watermark (1 hour) + `dropDuplicates`
6. **Enrich:**
   - Map `EventRootCode` → `EventCategoryName` (from CAMEO codebook)
   - Derive `IsConflict` from `QuadClass` (1,2 = cooperation; 3,4 = conflict)
   - Convert `SQLDATE` to proper date/timestamp
   - Determine `GeoResolution` from `ActionGeo_Type`
7. **Write raw** to HDFS `/gdelt/raw/events/date=YYYY-MM-DD/hour=HH/` (Parquet)
8. **Write curated** (enriched) to HDFS `/gdelt/curated/events/date=YYYY-MM-DD/` (Parquet)
9. **Write to HBase** `events_by_id` and `events_by_country_time`

## Watermark and Late Data

- Watermark: **1 hour** — events arriving more than 1 hour late (by event time) may be dropped from deduplication state, but are still written to raw HDFS.
- Event time source: `DATEADDED` field (YYYYMMDDHHMMSS), converted to timestamp.
- Trade-off: Longer watermark = more state memory. 1 hour is conservative for GDELT's 15-minute update cycle.

## Checkpoint Strategy

- Each job writes checkpoints to HDFS: `/gdelt/checkpoints/{job_name}/`
- Checkpoints store: Kafka offsets, watermark state, deduplication state
- On restart, Spark resumes from last checkpoint — no data loss or double-counting
- **Never delete checkpoint directories** unless you want to reprocess from scratch

## Memory Configuration

For 8 GB machines:
```
--driver-memory 2g
--executor-memory 2g
--executor-cores 2
```

Spark uses ~60% of executor memory for execution/storage, leaving headroom for JVM overhead.

## Analytics (Computed in Micro-Batches)

After raw processing, analytics aggregators run as foreachBatch operations:

| Aggregator | Input | Window | Output |
|-----------|-------|--------|--------|
| `country_aggregator` | Curated events | 1-hour tumbling | `country_metrics_by_time` (HBase) |
| `topic_aggregator` | GKG themes | 1-hour tumbling | `topic_metrics_by_time` (HBase) |
| `category_aggregator` | Events by EventRootCode | 1-hour tumbling | `category_metrics_by_time` (HBase) |
| `anomaly_detector` | Country/topic metrics | 2h vs 24h baseline | `anomalies_by_time` (HBase) |

## Anomaly Detection Method

**Rolling Z-Score:**
- Current window: last 2 hours of event counts
- Baseline: last 24 hours mean and standard deviation
- Z-score = (current - baseline_mean) / (baseline_std + 0.01)
- Flagged if: z_score ≥ 2.5 AND current_count ≥ 10 (minimum volume threshold)
- Severity: critical (z ≥ 3.0), high (z ≥ 2.5), medium (z ≥ 2.0)
- Epsilon (0.01) prevents division by zero on low-variance baselines

## Restart / Recovery

1. Stop Spark job: `spark-submit --kill <app-id>`
2. Restart: re-run `spark-submit` with the same checkpoint directory
3. Spark reads checkpoint → resumes from last committed Kafka offset
4. No manual offset management needed

## Submitting Jobs

```bash
bash src/processing/submit_jobs.sh
```

