# Kafka Design

## Topics

| Topic | Purpose | Key | Partitions | Retention |
|-------|---------|-----|------------|-----------|
| `gdelt.events.raw` | Raw parsed GDELT event records | `GlobalEventID` (string) | 3 | 7 days |
| `gdelt.mentions.raw` | Raw parsed mention records | `MentionIdentifier` (string) | 3 | 7 days |
| `gdelt.gkg.raw` | Raw parsed GKG records | `GKGRecordID` (string) | 3 | 7 days |
| `gdelt.events.normalized` | Enriched/normalized events (post-Spark) | `GlobalEventID` | 3 | 7 days |
| `gdelt.analytics` | Computed aggregate metrics | Composite key | 3 | 7 days |
| `gdelt.dead-letter` | Malformed/failed records for debugging | Source identifier | 1 | 14 days |

## Key Strategy

- **Events:** Keyed by `GlobalEventID` (string). Ensures all records for the same event land in the same partition, enabling per-event ordering and deduplication.
- **Mentions:** Keyed by `MentionIdentifier` (URL/doc ID).
- **GKG:** Keyed by `GKGRecordID`.

> **Important:** Kafka ordering is guaranteed **within a partition**, not globally across partitions. Events with different keys may be processed out of order across partitions. The system does not rely on global ordering.

## Consumer Groups

| Group | Consumers | Topics |
|-------|-----------|--------|
| `spark-events-consumer` | Spark events_stream job | `gdelt.events.raw` |
| `spark-mentions-consumer` | Spark mentions_stream job | `gdelt.mentions.raw` |
| `spark-gkg-consumer` | Spark gkg_stream job | `gdelt.gkg.raw` |

## Producer Configuration

```python
{
    'bootstrap.servers': 'node2:9092',
    'client.id': 'gdelt-ingestion-producer',
    'acks': 'all',              # Wait for all replicas (ISR)
    'enable.idempotence': True, # Exactly-once semantics
    'retries': 5,
    'retry.backoff.ms': 1000,
    'batch.size': 32768,        # 32 KB
    'linger.ms': 100,           # Wait up to 100ms to batch
    'compression.type': 'lz4',
}
```

## Offset Commit Strategy

- Spark Structured Streaming manages its own offsets via checkpoint files in HDFS (`/gdelt/checkpoints/`).
- Offsets are committed **after** successful processing and writing to sinks (HDFS/HBase).
- On restart, Spark resumes from the last committed checkpoint — no data loss or double-counting.

## Dead Letter Queue (DLQ)

- Records that fail parsing (malformed TSV, missing required fields) are published to `gdelt.dead-letter`.
- DLQ messages include: original record, error message, source file, timestamp.
- DLQ retention is 14 days for debugging.
- DLQ records do NOT crash the pipeline — they are logged and skipped.

## Replication Factor

- **1** for this academic cluster (single Kafka broker).
- In production, use RF=3 with 3+ brokers for fault tolerance.
- Trade-off: RF=1 means if the broker fails, all data in Kafka is lost. Acceptable for academic use because raw files remain on disk and can be replayed.

## Topic Creation

Run the provided script:
```bash
bash infrastructure/kafka/create-topics.sh node2:9092
```

## Monitoring

- Consumer lag: `kafka-consumer-groups.sh --describe --group spark-events-consumer --bootstrap-server node2:9092`
- Topic contents: `kafka-console-consumer.sh --bootstrap-server node2:9092 --topic gdelt.events.raw --from-beginning --max-messages 5`

