# API Reference

Base URL: `http://localhost:8000`  
OpenAPI docs: `http://localhost:8000/api/docs`

All time parameters use ISO 8601 format: `2026-11-01T00:00:00Z`  
Default time range: last 7 days. Maximum: 90 days.

---

## Health

### `GET /api/health`
Basic health check.

**Response:**
```json
{"status": "ok", "timestamp": "2026-11-14T12:00:00Z", "version": "1.0.0"}
```

### `GET /api/health/pipeline`
Pipeline component health and data freshness.

**Response:**
```json
{
  "hbase_connected": true,
  "last_ingestion_time": "2026-11-14T11:45:00Z",
  "freshness_status": "fresh",
  "files_processed_today": 42,
  "records_accepted_today": 18500,
  "records_rejected_today": 12,
  "kafka_status": "unknown",
  "spark_status": "unknown",
  "hdfs_status": "unknown"
}
```

---

## Overview

### `GET /api/overview?from_dt=&to_dt=`
Global dashboard metrics for a time range.

**Parameters:** `from_dt` (ISO 8601), `to_dt` (ISO 8601)

**Response:** total_events, total_mentions, active_countries, avg_tone, top_countries, category_distribution, cooperation/conflict counts.

---

## Events

### `GET /api/events?from_dt=&to_dt=&country=&category=&tone_min=&tone_max=&page=&limit=`
Paginated, filtered event list.

| Param | Type | Required | Default | Constraint |
|-------|------|----------|---------|------------|
| from_dt | datetime | No | 7 days ago | ISO 8601 |
| to_dt | datetime | No | now | ISO 8601 |
| country | string | No | — | FIPS code (e.g. US) |
| category | string | No | — | CAMEO root code 01-20 |
| tone_min | float | No | — | -100 to 100 |
| tone_max | float | No | — | -100 to 100 |
| page | int | No | 1 | ≥ 1 |
| limit | int | No | 50 | 1-500 |

### `GET /api/events/{global_event_id}`
Single event by ID.

### `GET /api/events/{global_event_id}/mentions`
Mentions for a specific event.

---

## Topics

### `GET /api/topics?from_dt=&to_dt=&country=&limit=`
Top topics ranked by frequency.

### `GET /api/topics/{topic}/trend?from_dt=&to_dt=`
Time series of a topic's frequency.

### `GET /api/topics/{topic}/countries?from_dt=&to_dt=`
Country distribution for a topic.

---

## Countries

### `GET /api/countries`
List of all country codes with names.

### `GET /api/countries/{country_code}/overview?from_dt=&to_dt=`
Country-level metrics summary.

### `GET /api/countries/{country_code}/events?from_dt=&to_dt=&page=&limit=`
Events for a specific country.

### `GET /api/countries/{country_code}/topics?from_dt=&to_dt=`
Top topics in a country.

### `GET /api/countries/{country_code}/regions?from_dt=&to_dt=`
Region-level breakdown (best-effort, depends on ADM1 data quality).

---

## Comparison

### `GET /api/compare/countries?codes=US,UK,FR&from_dt=&to_dt=`
Compare 2-6 countries side by side.

| Param | Type | Required | Constraint |
|-------|------|----------|------------|
| codes | string | Yes | Comma-separated, 2-6 codes |

---

## Analytics

### `GET /api/analytics/categories?from_dt=&to_dt=&country=`
CAMEO category distribution.

### `GET /api/analytics/tone?from_dt=&to_dt=&country=&topic=`
Tone trends over time.

### `GET /api/analytics/cooperation-conflict?from_dt=&to_dt=&country=`
Cooperation vs conflict breakdown.

### `GET /api/analytics/anomalies?from_dt=&to_dt=&country=&min_z_score=`
Detected anomalies/spikes.

---

## Map

### `GET /api/map/events?from_dt=&to_dt=&country=&topic=&category=&max_points=`
Geo-located events for map display (max 2000 points default).

### `GET /api/map/countries?from_dt=&to_dt=`
Country-level event intensity for choropleth.

---

## Search

### `GET /api/search?q=&from_dt=&to_dt=&page=&limit=`
Keyword search across actor names, locations, and source URLs.

---

## Regions

### `GET /api/regions/{region_id}/overview?from_dt=&to_dt=`
Region-level overview (requires ADM1 data integration).

---

## Error Format

All errors return:
```json
{"detail": "Error description"}
```

HTTP status codes: 422 (validation), 404 (not found), 503 (HBase unavailable).

