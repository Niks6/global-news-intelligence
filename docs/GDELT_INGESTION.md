# GDELT Ingestion Design

## Primary Ingestion Route

Poll GDELT's published raw data files (NOT the DOC API):
- URL: `https://data.gdeltproject.org/gdeltv2/lastupdate.txt`
- Format: 3 lines, each `<size> <md5hash> <url>`
- Updated every ~15 minutes
- File types: `.export.CSV.zip` (Events), `.mentions.CSV.zip` (Mentions), `.gkg.csv.zip` (GKG)

## Polling Strategy

1. Poll `lastupdate.txt` every `POLL_INTERVAL_MINUTES` (default: 15, configurable via env)
2. Parse response → extract file URLs, sizes, MD5 hashes
3. Check each URL against checkpoint DB → skip if already processed
4. Download new files only

## Download Process

1. Download to temporary file: `{filename}.tmp`
2. Verify file size matches `lastupdate.txt` declared size
3. Verify MD5 hash matches
4. Atomic rename: `.tmp` → final filename (prevents partial downloads being read)
5. Validate ZIP archive integrity before extraction

## Retry Strategy

- **Max retries:** 5 (configurable)
- **Backoff:** Exponential: 1s, 2s, 4s, 8s, 16s
- **HTTP 429:** Honor `Retry-After` header, fall back to 60s
- **5xx errors:** Retry with backoff
- **4xx errors (except 429):** Do not retry, mark as failed
- **Network errors:** Retry with backoff

## Deduplication

- File level: Checkpoint DB tracks every URL; already-processed URLs are skipped
- Record level: `GlobalEventID` uniqueness for events; `MentionIdentifier` for mentions; `GKGRecordID` for GKG
- Kafka: Idempotent producer prevents duplicate message delivery
- Spark: `dropDuplicates` with watermark removes duplicate events within the watermark window

## Checkpoint Database

SQLite at `data/checkpoints/ingestion.db`:

```sql
CREATE TABLE ingestion_files (
    file_url TEXT PRIMARY KEY,
    file_type TEXT NOT NULL,       -- events | mentions | gkg
    file_hash TEXT,                -- MD5 from lastupdate.txt
    file_size INTEGER,
    download_path TEXT,
    status TEXT DEFAULT 'pending', -- pending | downloaded | processed | failed
    discovered_at TEXT,
    downloaded_at TEXT,
    processed_at TEXT,
    kafka_published_at TEXT,
    error_msg TEXT,
    retry_count INTEGER DEFAULT 0
);
```

## Parsing

- **Format:** TAB-separated values (NOT comma-separated)
- **Events:** 61 columns per row
- **Mentions:** 16 columns per row
- **GKG:** 27 columns; multi-value fields use `;` and `,` sub-delimiters
- **Malformed rows:** Quarantined to dead-letter (logged, not crashed)
- **Required fields:** Events must have GlobalEventID, SQLDATE, EventCode, EventRootCode, QuadClass

## Rate Limiting (DOC API)

The DOC 2.0 API is supplementary, not primary:
- Global rate limit: 1 request per minute
- Max 1 concurrent request
- Rate limiter: `asyncio.Semaphore(1)` + timestamp tracking

## Idempotency

- Restarting the poller does NOT reprocess already-completed files (checkpoint check)
- Replaying a file (`ingest replay FILE`) re-parses and re-publishes to Kafka
- Kafka idempotent producer prevents duplicate messages even on retry

## Metrics Tracked

- `last_poll_time` — when lastupdate.txt was last fetched
- `last_file_discovered` — newest file URL found
- `files_downloaded` / `files_processed` — counters
- `records_accepted` / `records_rejected` — per dataset type
- `kafka_messages_sent` — total
- `errors` — total error counter
- Written to `data/metrics/ingestion_metrics.json` for the health endpoint

