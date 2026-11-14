# Architecture — Global News Intelligence Platform

## 1. System Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                    GDELT Project (External Source)                    │
│  https://data.gdeltproject.org/gdeltv2/lastupdate.txt                │
│  Files: *.export.CSV.zip (Events) | *.mentions.CSV.zip | *.gkg.csv.zip │
└───────────────────────────────┬──────────────────────────────────────┘
                                │  HTTPS download (every ~15 min)
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER  (node1)                           │
│  src/ingestion/poller.py          — polls lastupdate.txt              │
│  src/ingestion/downloader.py      — download + validate + checksum    │
│  src/ingestion/checkpoint.py      — persist processed file IDs        │
│  src/ingestion/parsers/           — per-dataset TSV/CSV parsers       │
│  src/ingestion/producers/         — Kafka producers (per topic)       │
└───────────────────────────────┬──────────────────────────────────────┘
                                │  Kafka produce (JSON messages)
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       KAFKA  (node1)                                  │
│  Broker: node1:9092          ZooKeeper: node1:2181                   │
│                                                                       │
│  Topics (3 partitions, RF=1):                                         │
│  ┌─────────────────────┐  ┌──────────────────────┐                   │
│  │ gdelt.events.raw    │  │ gdelt.mentions.raw   │                   │
│  └─────────────────────┘  └──────────────────────┘                   │
│  ┌─────────────────────┐  ┌──────────────────────┐                   │
│  │ gdelt.gkg.raw       │  │ gdelt.events.normalized│                 │
│  └─────────────────────┘  └──────────────────────┘                   │
│  ┌─────────────────────┐  ┌──────────────────────┐                   │
│  │ gdelt.analytics     │  │ gdelt.dead-letter    │                   │
│  └─────────────────────┘  └──────────────────────┘                   │
└───────────────────────────────┬──────────────────────────────────────┘
                                │  Kafka consume (Spark structured streaming)
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│               SPARK STRUCTURED STREAMING  (node2)                     │
│  Master: spark://node2:7077                                           │
│  src/processing/streaming_job.py                                      │
│                                                                       │
│  Per-topic consumers → Parse → Validate → Normalize                   │
│         │                                                             │
│         ├─→ Deduplication (GlobalEventID watermark)                  │
│         ├─→ Enrichment (CAMEO taxonomy, country names)               │
│         ├─→ Time-window aggregates:                                   │
│         │       country_metrics (1h, 24h windows)                    │
│         │       topic_metrics   (1h, 24h windows)                    │
│         │       category_metrics                                      │
│         │       tone_metrics                                          │
│         └─→ Anomaly detection (rolling z-score vs 7-day baseline)    │
└────────────────────┬───────────────────────────────┬─────────────────┘
                     │  Parquet writes (HDFS)         │  HBase puts
                     ▼                                ▼
┌──────────────────────────┐       ┌──────────────────────────────────┐
│   HADOOP HDFS  (node3)   │       │       HBASE  (node3)             │
│                          │       │                                   │
│  /gdelt/                 │       │  events_by_id                    │
│    raw/                  │       │  events_by_country_time          │
│      events/YYYY-MM-DD/  │       │  topic_metrics_by_time          │
│      mentions/YYYY-MM-DD/│       │  country_metrics_by_time        │
│      gkg/YYYY-MM-DD/     │       │  category_metrics_by_time       │
│    curated/              │       │  anomalies_by_time              │
│      events/YYYY-MM-DD/  │       │                                   │
│      mentions/YYYY-MM-DD/│       │  ZooKeeper quorum:               │
│    analytics/            │       │  node1,node2,node3:2181          │
│      country_metrics/    │       └──────────────────────────────────┘
│      topic_metrics/      │                      │
│      anomaly_metrics/    │                      │
│    checkpoints/          │                      │
└──────────────────────────┘                      │
                     │                            │  Thrift2
                     └─────────────────┬──────────┘
                                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    FASTAPI BACKEND  (node1 or node2)                  │
│  backend/app/main.py         Port: 8000                              │
│                                                                       │
│  Reads:  HBase (Thrift2 via happybase) for event/metric lookups      │
│          HDFS (hdfs3 or WebHDFS) for historical analytics            │
│                                                                       │
│  Endpoints: /api/overview, /api/events, /api/topics,                 │
│             /api/countries, /api/analytics, /api/map, ...            │
└───────────────────────────────┬──────────────────────────────────────┘
                                │  REST / JSON
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      REACT FRONTEND  (dev machine)                    │
│  frontend/        Port: 3000                                          │
│                                                                       │
│  Pages:                                                               │
│  1. Global Overview   2. Live Feed/Events   3. Topics & Trends       │
│  4. Country Intel     5. Country Comparison  6. Geographic Explorer  │
│  7. Conflict/Coop     8. Sentiment/Tone      9. Anomalies/Spikes     │
│  10. Pipeline Health                                                  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Node Role Assignment

| Machine | Hostname | IP (example) | Services |
|---|---|---|---|
| Machine 1 | node1 | 192.168.1.101 | ZooKeeper, Kafka, GDELT ingestion workers, FastAPI backend, HDFS DataNode |
| Machine 2 | node2 | 192.168.1.102 | Spark Master+Worker, YARN ResourceManager+NodeManager, HDFS DataNode |
| Machine 3 | node3 | 192.168.1.103 | HDFS NameNode+DataNode, HBase Master+RegionServer, ZooKeeper |

> Replace the example IPs with your actual LAN IPs. See `THREE_NODE_CLUSTER_SETUP.md`.

---

## 3. Data Flow Detail

### 3.1 Ingestion Flow

```
1. Poller wakes every 15 minutes (configurable via GDELT_POLL_INTERVAL_SECONDS)
2. Downloads lastupdate.txt → parses 3 file entries (Events, Mentions, GKG)
3. Checks checkpoint store: skip if file already processed
4. For each new file:
   a. Download to data/raw/<filename>.tmp (atomic)
   b. Verify ZIP integrity (zipfile.testzip())
   c. Rename to data/raw/<filename> on success
   d. Parse TSV/CSV rows → Python dicts
   e. Publish each row to its Kafka topic (key=GlobalEventID or doc hash)
   f. Record file as processed in checkpoint with metadata
5. Malformed rows → gdelt.dead-letter topic
6. Log every step (file discovered / downloaded / published / failed)
```

### 3.2 Kafka Topic Design

| Topic | Key | Partition Strategy | Retention |
|---|---|---|---|
| `gdelt.events.raw` | `GlobalEventID` (string) | Hash of key | 7 days |
| `gdelt.mentions.raw` | `GlobalEventID` | Hash of key | 7 days |
| `gdelt.gkg.raw` | GKGRECORDID (hash) | Hash of key | 7 days |
| `gdelt.events.normalized` | `GlobalEventID` | Hash of key | 7 days |
| `gdelt.analytics` | `<metric_type>:<date>` | Hash of key | 30 days |
| `gdelt.dead-letter` | original topic+offset | Round-robin | 30 days |

> Kafka ordering is guaranteed **within a partition** only. All records for a given `GlobalEventID` land in the same partition, ensuring per-event ordering.

### 3.3 Spark Processing Windows

| Aggregate | Window | Slide | Output |
|---|---|---|---|
| Country event counts | 1 hour | 15 min | HBase `country_metrics_by_time` |
| Topic frequency | 1 hour | 15 min | HBase `topic_metrics_by_time` |
| Category distribution | 24 hours | 1 hour | HBase `category_metrics_by_time` |
| Tone average | 1 hour | 15 min | HBase `country_metrics_by_time` |
| Anomaly baseline | 7 days rolling | 1 hour | HBase `anomalies_by_time` |

**Watermark:** 2 hours (late events older than 2 hours are dropped from window state)

### 3.4 HDFS Layout

```
hdfs://node3:8020/gdelt/
├── raw/
│   ├── events/
│   │   └── date=YYYY-MM-DD/hour=HH/
│   │       └── <timestamp>.export.CSV.gz
│   ├── mentions/
│   │   └── date=YYYY-MM-DD/hour=HH/
│   └── gkg/
│       └── date=YYYY-MM-DD/hour=HH/
├── curated/
│   ├── events/
│   │   └── date=YYYY-MM-DD/
│   │       └── part-*.parquet
│   ├── mentions/
│   │   └── date=YYYY-MM-DD/
│   └── gkg/
│       └── date=YYYY-MM-DD/
├── analytics/
│   ├── country_metrics/
│   │   └── date=YYYY-MM-DD/
│   │       └── part-*.parquet
│   ├── topic_metrics/
│   │   └── date=YYYY-MM-DD/
│   ├── category_metrics/
│   │   └── date=YYYY-MM-DD/
│   ├── tone_metrics/
│   │   └── date=YYYY-MM-DD/
│   └── anomaly_metrics/
│       └── date=YYYY-MM-DD/
└── checkpoints/
    └── spark/
        ├── events_stream/
        ├── mentions_stream/
        └── gkg_stream/
```

**Format:** Raw files stored as-is (compressed CSV/ZIP). Curated and analytics data stored as **Parquet** (columnar, Snappy compressed).

**Replication:** Factor 2 (not default 3) to conserve disk on academic cluster.

---

## 4. HBase Table Designs

### Table: `events_by_id`
- **Row key:** `GlobalEventID` (zero-padded 10 digits for lexicographic ordering, e.g., `0000987654`)
- **Column family:** `e` (events data)
- **Qualifiers (selected):** `dt`, `ec`, `er`, `gs`, `tone`, `a1c`, `a2c`, `agcc`, `lat`, `lon`, `src`
- **Purpose:** Single-event lookup by ID
- **TTL:** 90 days (older events archived in HDFS)

### Table: `events_by_country_time`
- **Row key:** `<CountryCode>#<YYYYMMDDHH>#<GlobalEventID>` (e.g., `US#2026110715#0000987654`)
  - Scan by country+time range is efficient; no hot-spotting because events are distributed across countries
- **Column family:** `e`
- **Purpose:** Country timeline queries; country dashboard
- **TTL:** 90 days

### Table: `topic_metrics_by_time`
- **Row key:** `<TopicSlug>#<YYYYMMDDHH>` (e.g., `CLIMATE_CHANGE#2026110715`)
- **Column family:** `m`
- **Qualifiers:** `count`, `mention_count`, `avg_tone`, `top_countries`
- **Purpose:** Trending topics, topic time series
- **TTL:** 180 days

### Table: `country_metrics_by_time`
- **Row key:** `<CountryCode>#<YYYYMMDDHH>` (e.g., `US#2026110715`)
- **Column family:** `m`
- **Qualifiers:** `event_count`, `mention_count`, `avg_tone`, `coop_count`, `conflict_count`, `top_topics`
- **Purpose:** Country dashboard KPIs
- **TTL:** 180 days

### Table: `category_metrics_by_time`
- **Row key:** `<RootCode>#<YYYYMMDD>` (e.g., `01#20261107`)
- **Column family:** `m`
- **Qualifiers:** `count`, `avg_goldstein`, `top_countries`
- **Purpose:** CAMEO category distribution, cooperation/conflict analysis
- **TTL:** 365 days

### Table: `anomalies_by_time`
- **Row key:** `<YYYYMMDDHH>#<entity_type>#<entity_key>` (e.g., `2026110715#country#US`)
  - Leading timestamp enables time-range scans for recent anomalies
- **Column family:** `a`
- **Qualifiers:** `current_count`, `baseline_count`, `z_score`, `pct_change`, `entity_type`, `entity_key`
- **Purpose:** Anomaly/spike detection feed
- **TTL:** 30 days

> **Hot-spotting mitigation:** Row keys beginning with a timestamp would cause all writes to hit the same RegionServer (monotonically increasing key problem). For `anomalies_by_time`, this is acceptable because the write rate is low (hourly). For `events_by_country_time`, the country code prefix distributes load across regions. For `events_by_id`, salting with the last 2 digits of GlobalEventID as a prefix can be added if write throughput becomes a bottleneck.

---

## 5. Kafka Topic Strategy Summary

```
Ingestion worker                    Spark Streaming
     │                                    │
     ├─→ gdelt.events.raw   ────────────→ │ parse, normalize, deduplicate
     ├─→ gdelt.mentions.raw ────────────→ │ parse, enrich
     ├─→ gdelt.gkg.raw      ────────────→ │ parse, extract themes
     │                                    │
     │                                    ├─→ gdelt.events.normalized
     │                                    │   (re-published for downstream)
     │                                    │
     │                                    ├─→ HDFS (Parquet)
     │                                    └─→ HBase (serving)
     │
     └─→ gdelt.dead-letter  ← malformed records from any stage
```

**Partitioning:** 3 partitions per topic (matches 3-node cluster, 1 Kafka broker).
**Keying:** `GlobalEventID` for events/mentions (ensures all updates to an event land in the same partition, preserving per-event ordering).
**Consumer groups:**
- `spark-events-consumer` — Spark events stream
- `spark-mentions-consumer` — Spark mentions stream
- `spark-gkg-consumer` — Spark GKG stream
- `monitoring-consumer` — health/lag monitoring

---

## 6. Backend API Architecture

```
FastAPI Application
├── /api/health           ← liveness check
├── /api/health/pipeline  ← Kafka lag, HDFS status, HBase status, ingestion freshness
├── /api/overview         ← global KPIs for time range
├── /api/events           ← paginated event list with filters
├── /api/topics           ← trending topics
├── /api/countries        ← country list + overviews
├── /api/compare/countries← side-by-side country comparison
├── /api/analytics/       ← category, tone, cooperation, anomalies
├── /api/map/             ← map data: event points, country choropleth
└── /api/search           ← cross-entity search
```

**Data sources per endpoint:**
- Event lookups: HBase `events_by_id`, `events_by_country_time`
- Aggregates: HBase `country_metrics_by_time`, `topic_metrics_by_time`
- Anomalies: HBase `anomalies_by_time`
- Historical analysis (deep time range): HDFS Parquet via Spark or SparkSQL

---

## 7. Geographic Data Strategy

- **Country boundaries:** Natural Earth GeoJSON (public domain, no API key required)
- **Country codes:** GDELT uses FIPS 10-4 codes (not ISO 3166). Mapping table in `src/common/constants.py`.
- **Sub-country resolution:**
  - Use GDELT `ActionGeo_ADM1Code` (admin 1 / state) where available
  - Admin1 names from GeoNames dataset (Creative Commons Attribution)
- **Coordinate resolution labeling:** Expose `geo_resolution` field in API responses:
  - `exact` — lat/lon from article text
  - `approximate` — centroid of admin region
  - `country` — only country code available, using country centroid
  - `unknown` — missing or zero coordinates

---

## 8. Observability

| Metric | Source | Destination |
|---|---|---|
| Last poll time | Ingestion worker | Checkpoint JSON / health endpoint |
| Files processed / failed | Ingestion worker | Structured log / health endpoint |
| Kafka consumer lag | Kafka admin client | `/api/health/pipeline` |
| Spark batch duration | Spark query listener | Structured log |
| Spark input rows/sec | Spark query listener | Structured log |
| HDFS capacity | HDFS admin client | `/api/health/pipeline` |
| HBase master status | HBase admin | `/api/health/pipeline` |
| API latency | FastAPI middleware | Structured log |
| Data freshness | Last event timestamp in HBase | `/api/health/pipeline` |
