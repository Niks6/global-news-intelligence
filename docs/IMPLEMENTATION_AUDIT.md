# Implementation Audit — Global News Intelligence Platform

> **Generated:** Phase 0 (baseline audit)  
> **Status:** Fresh start — no pre-existing source code in this repository  
> **Auditor:** phase01 agent

---

## 1. What Exists (Baseline)

### Directory Structure Created
The following directory tree has been initialized:

```
global-news-intelligence/
├── backend/              ← empty, reserved for FastAPI (phase06 worker)
├── data/
│   ├── raw/              ← GDELT ZIP downloads (not committed)
│   └── processed/        ← checkpoint files, extracted data
├── docs/                 ← this file and all architecture docs
├── frontend/             ← empty, reserved for React (frontend worker)
├── infrastructure/
│   ├── hadoop/           ← Hadoop XML config templates
│   ├── hbase/            ← HBase XML config templates
│   ├── kafka/            ← Kafka topic scripts and server.properties
│   └── spark/            ← Spark defaults config template
└── src/
    ├── common/           ← shared schemas, config, constants, GDELT schema
    ├── ingestion/        ← GDELT polling and download workers (Phase 2)
    ├── processing/       ← Spark streaming jobs (Phase 4)
    └── storage/          ← HDFS/HBase write helpers (Phase 5)
```

### Pre-existing Sample Work (from user's earlier exploration)
- **GDELT sample ZIP:** `data/raw/events_sample.zip` (~43 KB)
- **Parser script:** `parse_events.py` (not committed — personal workspace script)
- **Sample output:** `events_sample.jsonl` (749 records, 61 columns)
- **Validated results:**
  - 749 unique records (0 malformed)
  - 8 records missing lat/lon (741 map-plottable)
  - Date range: 2025-11-07 to 2026-11-07
  - Top countries: US(129), IN(72), MP(58), NI(57), UK(43)
  - Category distribution: Verbal Cooperation 65.82%, Verbal Conflict 14.29%, Material Conflict 10.01%, Material Cooperation 9.88%

> ⚠️ The 749-record sample is a development validation artifact only. It must NOT be used as production metrics or hardcoded into dashboards.

---

## 2. What Must Be Built — All 10 Phases

### Phase 0 ✅ — Audit and Baseline (THIS PHASE)
- [x] Inspect repository and initialize structure
- [x] Create `docs/IMPLEMENTATION_AUDIT.md` (this file)
- [x] Create root README and architecture documentation
- [x] Create `.env.example` with all environment variables
- [x] Create `.gitignore`
- [x] Create `docker-compose.yml` for local dev

### Phase 1 ✅ — Data Contracts and Configuration (THIS PHASE)
- [x] `src/common/__init__.py`
- [x] `src/common/config.py` — Pydantic Settings
- [x] `src/common/schemas.py` — all Pydantic v2 models
- [x] `src/common/constants.py` — CAMEO codes, country codes
- [x] `src/common/gdelt_schema.py` — full GDELT column definitions
- [x] `src/common/cameo_mapping.py` — full CAMEO taxonomy mapping
- [x] Infrastructure config templates (Kafka, Hadoop, HBase, Spark)
- [x] Kafka topic creation scripts

### Phase 2 🔲 — Robust GDELT Ingestion
- [ ] `src/ingestion/poller.py` — master list polling loop
- [ ] `src/ingestion/downloader.py` — file download with retry/checksum
- [ ] `src/ingestion/checkpoint.py` — persistent processed-file tracking
- [ ] `src/ingestion/parsers/events_parser.py` — 61-column TSV parser
- [ ] `src/ingestion/parsers/mentions_parser.py` — mentions TSV parser
- [ ] `src/ingestion/parsers/gkg_parser.py` — GKG delimited parser
- [ ] `src/ingestion/metadata.py` — ingestion metadata store
- [ ] Unit tests for each parser
- [ ] CLI: `python -m src.ingestion.poller --once` and `--continuous`

### Phase 3 🔲 — Kafka Integration
- [ ] `src/ingestion/producers/events_producer.py`
- [ ] `src/ingestion/producers/mentions_producer.py`
- [ ] `src/ingestion/producers/gkg_producer.py`
- [ ] Dead-letter producer
- [ ] Producer error handling and retries
- [ ] Consumer group verification

### Phase 4 🔲 — Spark Structured Streaming
- [ ] `src/processing/streaming_job.py` — main entry point
- [ ] `src/processing/consumers/events_consumer.py`
- [ ] `src/processing/consumers/mentions_consumer.py`
- [ ] `src/processing/consumers/gkg_consumer.py`
- [ ] `src/processing/transforms/normalizer.py`
- [ ] `src/processing/transforms/enricher.py`
- [ ] `src/processing/transforms/deduplicator.py`
- [ ] `src/processing/aggregations/country_metrics.py`
- [ ] `src/processing/aggregations/topic_metrics.py`
- [ ] `src/processing/aggregations/category_metrics.py`
- [ ] `src/processing/aggregations/anomaly_detector.py`
- [ ] Checkpoint configuration for HDFS
- [ ] Restart/recovery testing

### Phase 5 🔲 — HDFS and HBase Storage
- [ ] `src/storage/hdfs_writer.py`
- [ ] `src/storage/hbase_writer.py`
- [ ] `src/storage/hbase_schema.py` — table creation DDL
- [ ] HDFS partition layout validation
- [ ] HBase table schemas and row-key design
- [ ] Read verification tests

### Phase 6 🔲 — Backend APIs
- [ ] `backend/app/main.py` — FastAPI app
- [ ] `backend/app/routers/events.py`
- [ ] `backend/app/routers/topics.py`
- [ ] `backend/app/routers/countries.py`
- [ ] `backend/app/routers/analytics.py`
- [ ] `backend/app/routers/map.py`
- [ ] `backend/app/routers/health.py`
- [ ] `backend/app/services/hbase_service.py`
- [ ] `backend/app/services/hdfs_service.py`
- [ ] Pagination, validation, error handling, CORS
- [ ] OpenAPI documentation
- [ ] Tests for core endpoints

### Phase 7 🔲 — Frontend
- [ ] React app setup (Vite or CRA)
- [ ] 10 pages as specified in brief Section 6
- [ ] World map (Leaflet or Mapbox GL JS)
- [ ] Charts (Recharts or Chart.js)
- [ ] API integration for every dashboard metric
- [ ] Empty/loading/error states
- [ ] Date range filter component (reusable)
- [ ] No hardcoded production metrics

### Phase 8 🔲 — Geography and Intelligence Quality
- [ ] Country boundary GeoJSON integration
- [ ] Reverse geocoding (city → state → country hierarchy)
- [ ] Topic normalization and stop-word filtering
- [ ] Spike detection threshold tuning
- [ ] Tone aggregation validation

### Phase 9 🔲 — Three-Machine Deployment
- [ ] Node role configuration per machine
- [ ] LAN hosts file setup on all 3 nodes
- [ ] Firewall rules per node
- [ ] JVM heap tuning per service
- [ ] End-to-end verification across all nodes
- [ ] Shutdown procedure documentation

### Phase 10 🔲 — Final Validation and Handoff
- [ ] All acceptance tests passing (Sections 11.1–11.6 of brief)
- [ ] Feature completion matrix with evidence
- [ ] Final README update with start/stop commands
- [ ] Architecture diagram updated
- [ ] Limitations documented

---

## 3. Technology Stack — Pinned Versions

| Component | Version | Notes |
|---|---|---|
| Python | 3.11.x | All ingestion, processing, backend workers |
| Java | 11 LTS | Required by Hadoop, HBase, Kafka, Spark |
| Apache Kafka | 3.6.1 | Message broker |
| Apache Spark | 3.5.1 | Scala 2.12 build |
| Hadoop | 3.3.6 | HDFS, YARN |
| Apache HBase | 2.5.7 | NoSQL serving layer |
| Node.js | 20 LTS | React build tooling |
| React | 18.x | Frontend |
| FastAPI | 0.111+ | Backend REST API |
| Pydantic | v2 (2.6+) | Schema validation |
| confluent-kafka | 2.4.0 | Python Kafka client |
| pyspark | 3.5.1 | Python Spark bindings |
| happybase | 1.2.0 | Python HBase Thrift client |

### Compatibility Matrix

```
Spark 3.5.1  ←→  Hadoop 3.3.6  (Spark ships hadoop-3.3 jars)
Spark 3.5.1  ←→  Scala 2.12    (use spark-3.5.1-bin-hadoop3-scala2.12.tgz)
HBase 2.5.7  ←→  Hadoop 3.3.6  (HBase 2.5.x supports Hadoop 3.3.x)
Kafka 3.6.1  ←→  Java 11       (Kafka 3.6.x requires Java 11+)
```

> ⚠️ **Version pinning is critical.** Mixing Hadoop 3.3 with HBase 2.4 or Spark 3.4 can cause subtle binary incompatibilities. Install the exact versions listed above.

---

## 4. Resource Considerations — 3 × 8 GB PC Cluster

### Memory Budget per Machine

Each machine has approximately 8 GB RAM. With OS overhead (~1.5 GB) and other processes, available memory is **~6–6.5 GB** per node.

| Service | Recommended Heap | Node |
|---|---|---|
| ZooKeeper | 512 MB | node1 |
| Kafka broker | 2 GB | node1 |
| GDELT ingestion worker | 512 MB | node1 |
| Spark Master | 512 MB | node2 |
| Spark Worker / Executor | 2 GB | node2 |
| YARN ResourceManager | 512 MB | node2 |
| HDFS DataNode | 512 MB | node2 |
| HDFS NameNode | 1 GB | node3 |
| HBase Master | 1 GB | node3 |
| HBase RegionServer | 2 GB | node3 |
| HDFS DataNode | 512 MB | node3 |

> Total per node stays within ~5.5 GB with conservative settings. Leave ~0.5 GB slack for OS + buffers.

### HDFS Replication Factor
Use **replication factor 2** (not the default 3) to conserve storage on a 3-node academic cluster. With 3 nodes and RF=2, 1 node can fail without data loss. RF=3 would use 3× storage and is not recommended here.

### Kafka Partitions
Use **3 partitions** per topic to match the 3-node cluster. With 1 Kafka broker in this setup, replication factor is 1 (no redundancy). This is acceptable for an academic project — document the limitation.

### Spark Parallelism
Set `spark.default.parallelism=6` (2 × 3 cores = 6 total tasks). Executor memory 2g, 2 cores per executor, 1 executor per worker node.

---

## 5. Version and Compatibility Risks

| Risk | Mitigation |
|---|---|
| Java version mismatch | Pin Java 11 on ALL nodes. Do not mix Java 8 and Java 11. |
| Spark-Hadoop binary mismatch | Use `spark-3.5.1-bin-hadoop3-scala2.12.tgz` exactly |
| HBase ZooKeeper conflict | HBase ships its own ZooKeeper; configure to use external ZK |
| WSL2 networking | WSL2 hosts are NAT'd; cluster peers cannot reach WSL2 services on a fixed IP. Use native Ubuntu on cluster nodes. See `THREE_NODE_CLUSTER_SETUP.md`. |
| Wi-Fi client isolation | Many home routers block peer-to-peer traffic between Wi-Fi clients. See `THREE_NODE_CLUSTER_SETUP.md` for diagnosis. |
| Kafka advertised.listeners | Must be set to the LAN IP/hostname, not `localhost`, or remote consumers will fail. |
| Python 3.12 incompatibility | PySpark 3.5.1 supports Python 3.8–3.11. Use Python 3.11, not 3.12. |

---

## 6. Decisions Recorded

| Decision | Choice | Reason |
|---|---|---|
| Primary ingestion method | Poll raw GDELT files (not DOC API) | Raw files are bulk, free, 15-min cadence; DOC API is rate-limited to 1 req/min |
| Storage format (curated) | Parquet | Columnar, Spark-native, efficient for analytics queries |
| HBase client library | happybase (Thrift) | Pure Python, stable, works over Thrift2 |
| Kafka Python client | confluent-kafka | Most feature-complete; supports delivery guarantees and retry |
| Country filtering field | `ActionGeo_CountryCode` for location; `Actor1CountryCode` / `Actor2CountryCode` for actor origin | These are semantically different fields |
| HDFS replication | Factor 2 | 3-node academic cluster; RF=3 wastes 3× storage |
| Kafka replication | Factor 1 | 1 broker; RF=1 is correct. Document as academic trade-off. |
| OS for cluster nodes | Native Ubuntu 22.04 LTS preferred | WSL2 NAT networking makes multi-PC cluster impractical |
