# HDFS & HBase Design

## HDFS Directory Layout

```
/gdelt/
  raw/                              # Immutable raw ingested data
    events/date=YYYY-MM-DD/hour=HH/   # Parquet, partitioned by date+hour
    mentions/date=YYYY-MM-DD/hour=HH/
    gkg/date=YYYY-MM-DD/hour=HH/
  curated/                          # Enriched/normalized data
    events/date=YYYY-MM-DD/          # Parquet, partitioned by date
    mentions/date=YYYY-MM-DD/
    gkg/date=YYYY-MM-DD/
  analytics/                        # Pre-computed aggregates
    country_metrics/date=YYYY-MM-DD/
    topic_metrics/date=YYYY-MM-DD/
    category_metrics/date=YYYY-MM-DD/
    anomaly_metrics/date=YYYY-MM-DD/
    tone_metrics/date=YYYY-MM-DD/
  checkpoints/                      # Spark streaming checkpoints
    events_stream/
    mentions_stream/
    gkg_stream/
  hbase/                            # HBase data (managed by HBase)
```

### Storage Format
- **Raw:** Parquet (columnar, compressed, efficient for analytical queries)
- **Curated:** Parquet with enriched columns (category names, conflict flags, geo resolution)
- **Analytics:** Parquet with pre-aggregated metrics

### Replication
- Factor **2** for a 3-node cluster: tolerates 1 DataNode failure
- Trade-off: 2× storage usage. Factor 1 saves space but any single node failure = data loss.
- Block size: 64 MB (smaller than default 128 MB, appropriate for GDELT file sizes)

---

## HBase Table Designs

### 1. `events_by_id`

**Purpose:** Look up any event by its GlobalEventID.

| Property | Value |
|----------|-------|
| Row Key | `{GlobalEventID}` (string, zero-padded to 10 digits) |
| Column Family: `event` | All 61 event fields as qualifiers |
| Column Family: `meta` | source_file, ingestion_time, processing_time |

**Access pattern:** Point GET by event ID — O(1) lookup.

### 2. `events_by_country_time`

**Purpose:** Scan events for a country within a time range (most common query).

| Property | Value |
|----------|-------|
| Row Key | `{salt}_{CountryCode}_{reverse_timestamp}_{GlobalEventID}` |
| Salt | `hash(CountryCode) % 8` as single hex char (0-7) — prevents hot-spotting |
| Reverse timestamp | `sys.maxsize - epoch_seconds`, zero-padded to 19 digits |
| Column Family: `data` | Key event fields (subset for serving) |

**Access pattern:** Prefix scan `{salt}_{CC}_{rev_ts_start}` to `{salt}_{CC}_{rev_ts_end}` — returns events in time-descending order.

### 3. `country_metrics_by_time`

**Purpose:** Country-level aggregate metrics per time window.

| Property | Value |
|----------|-------|
| Row Key | `{CountryCode}_{reverse_timestamp}` |
| Column Family: `metrics` | country_code, date, hour, event_count, mention_count, article_count, avg_tone, avg_goldstein, cooperation_count, conflict_count |

**Access pattern:** Scan by country prefix + time range.

### 4. `topic_metrics_by_time`

**Purpose:** Topic frequency and distribution over time.

| Property | Value |
|----------|-------|
| Row Key | `{topic}_{reverse_timestamp}` |
| Column Family: `metrics` | topic, date, hour, count, article_count, avg_tone, country_distribution (JSON) |

### 5. `category_metrics_by_time`

**Purpose:** CAMEO category counts over time.

| Property | Value |
|----------|-------|
| Row Key | `{root_code}_{reverse_timestamp}` |
| Column Family: `metrics` | root_code, category_name, date, count, avg_goldstein, avg_tone, is_conflict |

### 6. `anomalies_by_time`

**Purpose:** Detected anomaly alerts, sorted by time (newest first).

| Property | Value |
|----------|-------|
| Row Key | `{reverse_timestamp}_{anomaly_type}_{label}` |
| Column Family: `alert` | anomaly_type, label, country_code, topic, baseline_avg, current_count, pct_change, z_score, severity, detected_at |

### Row Key Design: Reverse Timestamp

All time-series tables use reverse timestamps: `sys.maxsize - epoch_seconds` (19-digit zero-padded).

This ensures that HBase scans (which read in lexicographic order) return **newest records first** without requiring a reverse scan — critical for dashboard queries that show "latest N" results.

### TTL / Retention

| Table | TTL | Notes |
|-------|-----|-------|
| `events_by_id` | None (keep forever) | Primary event lookup |
| `events_by_country_time` | 90 days | Serving layer; historical data in HDFS |
| `country_metrics_by_time` | 90 days | Aggregate cache |
| `topic_metrics_by_time` | 90 days | Aggregate cache |
| `category_metrics_by_time` | 90 days | Aggregate cache |
| `anomalies_by_time` | 30 days | Short-lived alerts |

### Connection

Backend connects to HBase via **Thrift** server using the `happybase` Python library:
```python
import happybase
conn = happybase.Connection(host='node3', port=9090)
table = conn.table('events_by_id')
row = table.row(b'1234567890')
```

