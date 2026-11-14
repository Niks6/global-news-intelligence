# Testing Guide

## Running Unit Tests

```bash
# All tests
pytest src/ backend/ tests/ -v --tb=short

# Specific module
pytest tests/ingestion/ -v
pytest backend/tests/ -v
```

## Test the Ingestion Pipeline

### 1. One-shot ingestion (dry run — no Kafka needed)
```bash
python -m src.ingestion.cli once --dry-run
```
This polls `lastupdate.txt`, downloads new files, parses them, but does NOT publish to Kafka.

### 2. Check ingestion status
```bash
python -m src.ingestion.cli status
```

### 3. Replay a specific file
```bash
python -m src.ingestion.cli replay data/raw/FILENAME.zip --dry-run
```

## Verify Kafka Messages

```bash
# List topics
kafka-topics.sh --list --bootstrap-server node2:9092

# Read first 5 events
kafka-console-consumer.sh \
  --bootstrap-server node2:9092 \
  --topic gdelt.events.raw \
  --from-beginning \
  --max-messages 5

# Check consumer group lag
kafka-consumer-groups.sh \
  --describe \
  --group spark-events-consumer \
  --bootstrap-server node2:9092
```

## Verify HDFS Data

```bash
# List directories
hdfs dfs -ls /gdelt/
hdfs dfs -ls /gdelt/raw/events/

# Check file count and sizes
hdfs dfs -du -h /gdelt/

# Read a Parquet file (via Spark shell)
spark-shell --master local
# scala> spark.read.parquet("/gdelt/curated/events/date=2026-11-14").show(5)
```

## Verify HBase Tables

```bash
hbase shell
> list                                    # List all tables
> describe 'events_by_id'                 # Schema details
> count 'events_by_id'                    # Row count
> get 'events_by_id', '1234567890'        # Lookup by event ID
> scan 'country_metrics_by_time', {LIMIT => 5}  # First 5 rows
```

## Test Backend API

```bash
# Health
curl http://localhost:8000/api/health
curl http://localhost:8000/api/health/pipeline

# Overview
curl "http://localhost:8000/api/overview?from_dt=2026-11-01T00:00:00Z&to_dt=2026-11-14T00:00:00Z"

# Events
curl "http://localhost:8000/api/events?from_dt=2026-11-01T00:00:00Z&to_dt=2026-11-14T00:00:00Z&limit=5"

# Countries
curl http://localhost:8000/api/countries

# Topics
curl "http://localhost:8000/api/topics?from_dt=2026-11-01T00:00:00Z&to_dt=2026-11-14T00:00:00Z"

# OpenAPI docs
open http://localhost:8000/api/docs
```

## Verify Frontend Builds

```bash
cd frontend
npm install
npm run build      # TypeScript compilation + Vite production build
npm run preview    # Preview production build locally
```

## Acceptance Criteria Checklist

### Ingestion (Section 11.1)
- [ ] Polling discovers new GDELT files from lastupdate.txt
- [ ] Files are downloaded, validated (size + hash), and recorded
- [ ] Repeated polls do not duplicate already-processed data
- [ ] Interrupted downloads are safely retried
- [ ] Malformed records are quarantined, not crashed
- [ ] Events, Mentions, GKG have separate parsers

### Kafka & Spark (Section 11.2)
- [ ] Ingestion publishes messages to intended Kafka topics
- [ ] Kafka topics and partitions can be inspected
- [ ] Spark consumes actual Kafka messages
- [ ] Valid records are normalized and processed
- [ ] Duplicate/late/malformed records handled
- [ ] Checkpoint restart works
- [ ] Aggregates written to HDFS and HBase

### Storage (Section 11.3)
- [ ] HDFS contains raw and curated partitioned data
- [ ] HBase tables exist with documented schemas
- [ ] Record can be looked up by event ID
- [ ] Country/time and topic/time aggregates queryable
- [ ] Restarting services does not erase data

### Backend (Section 11.4)
- [ ] Health endpoints report actual component status
- [ ] Overview returns computed metrics
- [ ] All endpoints return valid results
- [ ] Pagination and filters work
- [ ] Invalid inputs return clean errors

### Frontend (Section 11.5)
- [ ] Every dashboard page loads API data
- [ ] Date filters update charts and tables
- [ ] Country selection opens country details
- [ ] Map markers reflect filtered data
- [ ] Empty/loading/error states work
- [ ] No fake metrics in production mode

### Three-Machine Operation (Section 11.6)
- [ ] Each node has documented role and configuration
- [ ] All nodes reach required services over LAN
- [ ] Kafka advertised listeners work from remote nodes
- [ ] HDFS and HBase cluster health verified
- [ ] Service restart recovers without corruption

