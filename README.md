# Global News Intelligence & Monitoring Platform

A big-data distributed stream-processing platform that continuously ingests global news/event data from GDELT, processes it through Apache Kafka and Spark Structured Streaming, stores historical data in Hadoop HDFS and queryable data in Apache HBase, and presents actionable global intelligence through a modern React dashboard.

## Project Vision

Answer questions such as:
- What major events are happening around the world right now?
- Which topics are trending globally or in a particular country?
- Is coverage of a topic increasing unusually quickly?
- What is the sentiment/tone of coverage around an event, topic, or country?
- How do countries compare over a selected period?

This is a **big-data / distributed-stream-processing academic project** using real distributed technologies.

---

## Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Data source | GDELT Project | v2 |
| Ingestion | Python workers / scheduled polling | Python 3.11 |
| Message broker | Apache Kafka | 3.6.1 |
| Stream processing | Apache Spark Structured Streaming | 3.5.1 (Scala 2.12) |
| Distributed storage | Hadoop HDFS | 3.3.6 |
| NoSQL serving layer | Apache HBase | 2.5.7 |
| Backend API | FastAPI | 0.111+ |
| Frontend | React | 18 |
| Runtime | Java | 11 (LTS) |
| Node.js | Node.js | 20 LTS |

---

## Directory Structure

```
global-news-intelligence/
├── backend/              # FastAPI application (owned by backend worker)
├── data/
│   ├── raw/              # Raw downloaded GDELT files (not committed)
│   └── processed/        # Locally processed/checkpoint files
├── docs/                 # All architecture and operations documentation
│   ├── IMPLEMENTATION_AUDIT.md
│   ├── ARCHITECTURE.md
│   ├── DATA_DICTIONARY.md
│   ├── GDELT_INGESTION.md
│   ├── THREE_NODE_CLUSTER_SETUP.md
│   └── ...
├── frontend/             # React application (owned by frontend worker)
├── infrastructure/
│   ├── hadoop/           # Hadoop config templates
│   ├── hbase/            # HBase config templates
│   ├── kafka/            # Kafka config and topic scripts
│   └── spark/            # Spark config templates
└── src/
    ├── common/           # Shared schemas, config, constants
    ├── ingestion/        # GDELT polling and download workers
    ├── processing/       # Spark streaming jobs
    └── storage/          # HDFS and HBase write utilities
```

---

## Pipeline Overview

```
GDELT published files (Events / Mentions / GKG, every ~15 min)
                   │
                   ▼
       Ingestion Workers (src/ingestion/)
       - poll lastupdate.txt every 15 min
       - discover & download new files only
       - validate checksums, deduplicate
       - checkpoint processed file IDs
                   │
                   ▼
                Kafka (3 brokers / 1 broker dev mode)
       Topics: gdelt.events.raw | gdelt.mentions.raw
               gdelt.gkg.raw | gdelt.events.normalized
               gdelt.analytics | gdelt.dead-letter
                   │
                   ▼
       Spark Structured Streaming (src/processing/)
       - parse actual GDELT schemas
       - normalize, enrich, deduplicate (watermark)
       - compute window aggregates, topic trends, anomalies
       - write to HDFS (Parquet) + HBase (serving)
                   │
           ┌───────┴────────┐
           ▼                ▼
        HDFS             HBase
     /gdelt/raw/       events_by_id
     /gdelt/curated/   events_by_country_time
     /gdelt/analytics/ topic_metrics_by_time
                       country_metrics_by_time
                       anomalies_by_time
           │                │
           └───────┬────────┘
                   ▼
         FastAPI Backend (backend/)
                   │
                   ▼
           React Frontend (frontend/)
     Global Overview | Live Feed | Topics & Trends
     Country Intel | Comparison | Geographic Explorer
     Conflict/Cooperation | Sentiment | Anomalies | Health
```

---

## Cluster Roles (3 × 8 GB PCs)

| Machine | Hostname | Main Services |
|---|---|---|
| Machine 1 | node1 | GDELT ingestion workers, ZooKeeper, Kafka broker |
| Machine 2 | node2 | Spark Master + Worker, YARN ResourceManager, HDFS DataNode |
| Machine 3 | node3 | HDFS NameNode, HBase Master, ZooKeeper, HDFS DataNode |

> See `docs/THREE_NODE_CLUSTER_SETUP.md` for complete step-by-step setup.

---

## Quick Start (Local Dev Mode)

Local dev mode uses Docker Compose for Kafka/ZooKeeper. HDFS and HBase require the full 3-node cluster.

### Prerequisites

- Docker Desktop (Windows) or Docker Engine (Ubuntu)
- Python 3.11
- Java 11

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env with your LAN IP addresses and paths
```

### 2. Start Kafka (local dev)

```bash
docker compose up -d kafka zookeeper
```

### 3. Create Kafka topics

```bash
# Linux/Mac
bash infrastructure/kafka/create-topics.sh

# Windows
infrastructure\kafka\create-topics.bat
```

### 4. Set up Python environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate        # Linux
# .venv\Scripts\activate.bat    # Windows

pip install -r src/requirements.txt
```

### 5. Run ingestion worker (once)

```bash
python -m src.ingestion.poller --once
```

### 6. Start Spark processing

```bash
# Requires SPARK_HOME to be set
spark-submit src/processing/streaming_job.py
```

### 7. Start backend API

```bash
cd backend && uvicorn app.main:app --reload --port 8000
```

### 8. Start frontend

```bash
cd frontend && npm install && npm start
# Opens http://localhost:3000
```

---

## Full Cluster Start Order

> **Always start services in this order to avoid dependency failures.**

1. ZooKeeper (all 3 nodes)
2. Kafka broker (node1)
3. HDFS NameNode (node3), then DataNodes (node1, node2, node3)
4. YARN ResourceManager (node2), then NodeManagers
5. HBase Master (node3), then RegionServers (node1, node2)
6. Spark Master (node2), then Workers
7. FastAPI backend (node1 or node2)
8. React frontend (dev machine)

See `docs/THREE_NODE_CLUSTER_SETUP.md` for exact commands.

---

## Documentation

| Document | Description |
|---|---|
| [IMPLEMENTATION_AUDIT.md](docs/IMPLEMENTATION_AUDIT.md) | Phase-by-phase build plan, what exists, what must be built |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Full data pipeline architecture |
| [DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) | All schemas: GDELT, Pydantic models, HBase tables |
| [GDELT_INGESTION.md](docs/GDELT_INGESTION.md) | Ingestion design: polling, checksums, deduplication |
| [THREE_NODE_CLUSTER_SETUP.md](docs/THREE_NODE_CLUSTER_SETUP.md) | Complete beginner-friendly cluster setup |

---

## Data Source

**GDELT Project** — Global Database of Events, Language, and Tone  
- Raw files updated every ~15 minutes  
- Master list: https://data.gdeltproject.org/gdeltv2/lastupdate.txt  
- Events codebook: https://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf  
- GKG codebook: https://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook.pdf  

> **Note:** GDELT data is free for research/academic use. See the GDELT Project website for terms.

---

## Important Notes

- **Data quality:** GDELT event coordinates can be approximate or missing. Country of an actor ≠ country where event occurred.
- **Tone:** GDELT `AvgTone` is a text/reporting-tone signal, not ground-truth public sentiment.
- **Kafka ordering:** Guaranteed per partition, not globally across all partitions.
- **Cluster grade:** This 3-PC academic cluster is not production-grade. HDFS replication factor 2 is used; a 2-node failure loses data.
- **DOC API:** Rate-limited to 1 req/min. Bulk raw file download is the primary ingestion route.
- **Sample data:** The 749-record sample in `data/raw/` is for development validation only, not representative of live totals.
