"""HBase client wrapper using happybase for the Global News Intelligence Platform.

Row-key designs:
  events_by_id:             GlobalEventID (string)
  events_by_country_time:   SALT_COUNTRYCODE_REVERSETIMESTAMP_EVENTID
                            SALT = hex(hash(country_code) % 8) -> avoids hot-spotting
  topic_metrics_by_time:    TOPIC_REVERSETIMESTAMP
  country_metrics_by_time:  COUNTRY_REVERSETIMESTAMP
  category_metrics_by_time: CATEGORY_REVERSETIMESTAMP
  anomalies_by_time:        REVERSETIMESTAMP_TYPE_KEY

Reverse-timestamp = str(sys.maxsize - epoch_seconds) -> newest records sort first.
"""

import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import happybase  # type: ignore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table name constants
# ---------------------------------------------------------------------------
TABLE_EVENTS_BY_ID = "events_by_id"
TABLE_EVENTS_BY_COUNTRY_TIME = "events_by_country_time"
TABLE_TOPIC_METRICS = "topic_metrics_by_time"
TABLE_COUNTRY_METRICS = "country_metrics_by_time"
TABLE_CATEGORY_METRICS = "category_metrics_by_time"
TABLE_ANOMALIES = "anomalies_by_time"

# Column-family names
CF_EVENT = b"event"
CF_META = b"meta"
CF_DATA = b"data"
CF_METRICS = b"metrics"
CF_ALERT = b"alert"

# Default TTL values (in seconds)
TTL_EVENTS = 365 * 24 * 3600          # 1 year
TTL_METRICS = 90 * 24 * 3600          # 90 days
TTL_ANOMALIES = 30 * 24 * 3600        # 30 days

# Salt bucket count for events_by_country_time
SALT_BUCKETS = 8


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _reverse_ts(epoch_seconds: int) -> str:
    """Return a zero-padded reverse timestamp string for time-descending scans."""
    return str(sys.maxsize - int(epoch_seconds)).zfill(19)


def _salt(country_code: str) -> str:
    """Deterministic single hex char (0-7) from country_code -> avoids hot-spotting across processes."""
    digest = hashlib.md5(country_code.encode("utf-8")).digest()
    return format(digest[0] % SALT_BUCKETS, "x")


def _encode(value: Any) -> bytes:
    """Encode a Python value to bytes for HBase storage."""
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (int, float, bool)):
        return str(value).encode("utf-8")
    return json.dumps(value, default=str).encode("utf-8")


def _decode(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    return value.decode("utf-8", errors="replace")


def _event_epoch(event: Dict[str, Any]) -> int:
    """Determine accurate UTC epoch seconds for an event using SQLDATE + DATEADDED."""
    date_str = str(event.get("SQLDATE") or "")
    dateadded_str = str(event.get("DATEADDED") or "")
    try:
        if len(dateadded_str) >= 14 and len(date_str) >= 8 and dateadded_str[:8] == date_str[:8]:
            dt = datetime.strptime(dateadded_str[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        if len(date_str) >= 8:
            dt = datetime.strptime(date_str[:8], "%Y%m%d").replace(hour=12, tzinfo=timezone.utc)
            return int(dt.timestamp())
    except (ValueError, TypeError):
        pass
    return int(datetime.now(timezone.utc).timestamp())


def _build_event_records(event: Dict[str, Any]) -> Tuple[bytes, Dict[bytes, bytes], bytes, Dict[bytes, bytes]]:
    """Build (row_key_id, id_data, row_key_ct, ct_data) for a single GDELT event."""
    event_id = str(event.get("GlobalEventID", ""))
    date_str = str(event.get("SQLDATE", ""))
    country = str(event.get("ActionGeo_CountryCode") or "UN")
    source_url = str(event.get("SOURCEURL") or "")
    epoch = _event_epoch(event)
    rev_ts = _reverse_ts(epoch)
    now_iso = datetime.now(timezone.utc).isoformat()

    row_key_id = _encode(event_id)
    event_data: Dict[bytes, bytes] = {}
    for field, val in event.items():
        if val is not None:
            col = f"{CF_EVENT.decode()}:{field}".encode()
            event_data[col] = _encode(val)
    event_data[b"meta:source_url"] = _encode(source_url)
    event_data[b"meta:ingested_at"] = _encode(now_iso)

    salt = _salt(country)
    row_key_ct = _encode(f"{salt}_{country}_{rev_ts}_{event_id}")
    ct_data: Dict[bytes, bytes] = {
        b"data:event_id": _encode(event_id),
        b"data:country": _encode(country),
        b"data:date": _encode(date_str),
        b"data:event_code": _encode(str(event.get("EventCode") or "")),
        b"data:event_root_code": _encode(str(event.get("EventRootCode") or "")),
        b"data:goldstein": _encode(str(event.get("GoldsteinScale") if event.get("GoldsteinScale") is not None else "")),
        b"data:avg_tone": _encode(str(event.get("AvgTone") if event.get("AvgTone") is not None else "")),
        b"data:quad_class": _encode(str(event.get("QuadClass") if event.get("QuadClass") is not None else "")),
        b"data:num_mentions": _encode(str(event.get("NumMentions") if event.get("NumMentions") is not None else "")),
        b"data:num_sources": _encode(str(event.get("NumSources") if event.get("NumSources") is not None else "")),
        b"data:num_articles": _encode(str(event.get("NumArticles") if event.get("NumArticles") is not None else "")),
        b"data:actor1_name": _encode(str(event.get("Actor1Name") or "")),
        b"data:actor1_country": _encode(str(event.get("Actor1CountryCode") or "")),
        b"data:actor2_name": _encode(str(event.get("Actor2Name") or "")),
        b"data:actor2_country": _encode(str(event.get("Actor2CountryCode") or "")),
        b"data:full_name": _encode(str(event.get("ActionGeo_FullName") or "")),
        b"data:geo_type": _encode(str(event.get("ActionGeo_Type") if event.get("ActionGeo_Type") is not None else "")),
        b"data:source_url": _encode(source_url),
        b"data:lat": _encode(str(event.get("ActionGeo_Lat") if event.get("ActionGeo_Lat") is not None else "")),
        b"data:lon": _encode(str(event.get("ActionGeo_Long") if event.get("ActionGeo_Long") is not None else "")),
    }
    return row_key_id, event_data, row_key_ct, ct_data


# ---------------------------------------------------------------------------
# Main client
# ---------------------------------------------------------------------------

class HBaseClient:
    """Wrapper around happybase.Connection providing domain-level methods."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9090,
        timeout: Optional[int] = 15_000,
        table_prefix: Optional[str] = None,
        auto_connect: bool = True,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.table_prefix = table_prefix
        self._connection: Optional[happybase.Connection] = None
        if auto_connect:
            self.connect()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open (or reopen) the Thrift connection."""
        try:
            self._connection = happybase.Connection(
                host=self.host,
                port=self.port,
                timeout=self.timeout,
                table_prefix=self.table_prefix,
                autoconnect=True,
            )
            logger.info("HBase connected to %s:%s", self.host, self.port)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("HBase connection failed: %s", exc)
            raise

    def close(self) -> None:
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

    def _conn(self) -> happybase.Connection:
        if self._connection is None:
            self.connect()
        return self._connection  # type: ignore[return-value]

    def _table(self, name: str) -> happybase.Table:
        return self._conn().table(name)

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def create_tables(self, drop_if_exists: bool = False) -> None:
        """Create all HBase tables required by the platform."""
        conn = self._conn()
        existing = {t.decode() for t in conn.tables()}

        table_specs: Dict[str, Dict[bytes, Dict[str, Any]]] = {
            TABLE_EVENTS_BY_ID: {
                CF_EVENT: {"max_versions": 1, "time_to_live": TTL_EVENTS, "bloom_filter_type": "ROW"},
                CF_META: {"max_versions": 1, "time_to_live": TTL_EVENTS},
            },
            TABLE_EVENTS_BY_COUNTRY_TIME: {
                CF_DATA: {"max_versions": 1, "time_to_live": TTL_EVENTS, "bloom_filter_type": "ROW"},
            },
            TABLE_TOPIC_METRICS: {
                CF_METRICS: {"max_versions": 3, "time_to_live": TTL_METRICS},
            },
            TABLE_COUNTRY_METRICS: {
                CF_METRICS: {"max_versions": 3, "time_to_live": TTL_METRICS},
            },
            TABLE_CATEGORY_METRICS: {
                CF_METRICS: {"max_versions": 3, "time_to_live": TTL_METRICS},
            },
            TABLE_ANOMALIES: {
                CF_ALERT: {"max_versions": 1, "time_to_live": TTL_ANOMALIES},
            },
        }

        for table_name, families in table_specs.items():
            if table_name in existing:
                if drop_if_exists:
                    logger.warning("Disabling and dropping table: %s", table_name)
                    conn.disable_table(table_name)
                    conn.delete_table(table_name)
                else:
                    logger.info("Table already exists (skipping): %s", table_name)
                    continue
            conn.create_table(table_name, families)
            logger.info("Created HBase table: %s", table_name)

    # ------------------------------------------------------------------
    # Write methods
    # ------------------------------------------------------------------

    def put_event(self, event: Dict[str, Any]) -> None:
        """Write a GDELT event to events_by_id and events_by_country_time."""
        rk_id, ev_data, rk_ct, ct_data = _build_event_records(event)
        self._table(TABLE_EVENTS_BY_ID).put(rk_id, ev_data)
        self._table(TABLE_EVENTS_BY_COUNTRY_TIME).put(rk_ct, ct_data)

    def put_events_batch(self, events: Iterable[Dict[str, Any]], batch_size: int = 200) -> int:
        """Batch-write events to events_by_id and events_by_country_time."""
        written = 0
        t_id = self._table(TABLE_EVENTS_BY_ID)
        t_ct = self._table(TABLE_EVENTS_BY_COUNTRY_TIME)
        with t_id.batch(batch_size=batch_size) as b_id, t_ct.batch(batch_size=batch_size) as b_ct:
            for ev in events:
                if not ev.get("GlobalEventID"):
                    continue
                rk_id, ev_data, rk_ct, ct_data = _build_event_records(ev)
                b_id.put(rk_id, ev_data)
                b_ct.put(rk_ct, ct_data)
                written += 1
        return written

    def put_country_metric(
        self,
        country: str,
        epoch_seconds: int,
        metrics: Dict[str, Any],
    ) -> None:
        """Write country aggregate metrics."""
        rev_ts = _reverse_ts(epoch_seconds)
        row_key = _encode(f"{country}_{rev_ts}")
        payload = {"country_code": country, **metrics}
        data: Dict[bytes, bytes] = {
            f"metrics:{k}".encode(): _encode(v) for k, v in payload.items()
        }
        self._table(TABLE_COUNTRY_METRICS).put(row_key, data)

    def put_country_metrics_batch(self, rows: Iterable[Tuple[str, int, Dict[str, Any]]], batch_size: int = 200) -> int:
        written = 0
        with self._table(TABLE_COUNTRY_METRICS).batch(batch_size=batch_size) as batch:
            for country, epoch_seconds, metrics in rows:
                rev_ts = _reverse_ts(epoch_seconds)
                row_key = _encode(f"{country}_{rev_ts}")
                payload = {"country_code": country, **metrics}
                batch.put(row_key, {f"metrics:{k}".encode(): _encode(v) for k, v in payload.items()})
                written += 1
        return written

    def put_topic_metric(
        self,
        topic: str,
        epoch_seconds: int,
        metrics: Dict[str, Any],
    ) -> None:
        """Write topic aggregate metrics."""
        rev_ts = _reverse_ts(epoch_seconds)
        row_key = _encode(f"{topic}_{rev_ts}")
        payload = {"topic": topic, **metrics}
        data: Dict[bytes, bytes] = {
            f"metrics:{k}".encode(): _encode(v) for k, v in payload.items()
        }
        self._table(TABLE_TOPIC_METRICS).put(row_key, data)

    def put_topic_metrics_batch(self, rows: Iterable[Tuple[str, int, Dict[str, Any]]], batch_size: int = 200) -> int:
        written = 0
        with self._table(TABLE_TOPIC_METRICS).batch(batch_size=batch_size) as batch:
            for topic, epoch_seconds, metrics in rows:
                rev_ts = _reverse_ts(epoch_seconds)
                row_key = _encode(f"{topic}_{rev_ts}")
                payload = {"topic": topic, **metrics}
                batch.put(row_key, {f"metrics:{k}".encode(): _encode(v) for k, v in payload.items()})
                written += 1
        return written

    def put_category_metric(
        self,
        category: str,
        epoch_seconds: int,
        metrics: Dict[str, Any],
    ) -> None:
        """Write CAMEO category aggregate metrics."""
        rev_ts = _reverse_ts(epoch_seconds)
        row_key = _encode(f"{category}_{rev_ts}")
        payload = {"root_code": category, **metrics}
        data: Dict[bytes, bytes] = {
            f"metrics:{k}".encode(): _encode(v) for k, v in payload.items()
        }
        self._table(TABLE_CATEGORY_METRICS).put(row_key, data)

    def put_category_metrics_batch(self, rows: Iterable[Tuple[str, int, Dict[str, Any]]], batch_size: int = 200) -> int:
        written = 0
        with self._table(TABLE_CATEGORY_METRICS).batch(batch_size=batch_size) as batch:
            for category, epoch_seconds, metrics in rows:
                rev_ts = _reverse_ts(epoch_seconds)
                row_key = _encode(f"{category}_{rev_ts}")
                payload = {"root_code": category, **metrics}
                batch.put(row_key, {f"metrics:{k}".encode(): _encode(v) for k, v in payload.items()})
                written += 1
        return written

    def put_anomaly(
        self,
        anomaly_type: str,
        key: str,
        epoch_seconds: int,
        alert_data: Dict[str, Any],
    ) -> None:
        """Write an anomaly/spike alert."""
        rev_ts = _reverse_ts(epoch_seconds)
        row_key = _encode(f"{rev_ts}_{anomaly_type}_{key}")
        data: Dict[bytes, bytes] = {
            f"alert:{k}".encode(): _encode(v) for k, v in alert_data.items()
        }
        self._table(TABLE_ANOMALIES).put(row_key, data)

    def put_anomalies_batch(self, rows: Iterable[Tuple[str, str, int, Dict[str, Any]]], batch_size: int = 200) -> int:
        written = 0
        with self._table(TABLE_ANOMALIES).batch(batch_size=batch_size) as batch:
            for anomaly_type, key, epoch_seconds, alert_data in rows:
                rev_ts = _reverse_ts(epoch_seconds)
                row_key = _encode(f"{rev_ts}_{anomaly_type}_{key}")
                batch.put(row_key, {f"alert:{k}".encode(): _encode(v) for k, v in alert_data.items()})
                written += 1
        return written

    # ------------------------------------------------------------------
    # Read methods
    # ------------------------------------------------------------------

    def get_event(self, event_id: str) -> Optional[Dict[str, str]]:
        """Fetch a single event by GlobalEventID."""
        row = self._table(TABLE_EVENTS_BY_ID).row(_encode(event_id))
        if not row:
            return None
        return {k.decode(): _decode(v) for k, v in row.items()}

    def scan_country_events(
        self,
        country: str,
        from_ts: int,
        to_ts: int,
        limit: int = 100,
    ) -> List[Dict[str, str]]:
        """Scan events_by_country_time for a country in a time range."""
        results = []
        for bucket in range(SALT_BUCKETS):
            salt = format(bucket, "x")
            row_start = _encode(f"{salt}_{country}_{_reverse_ts(to_ts)}")
            row_stop = _encode(f"{salt}_{country}_{_reverse_ts(from_ts)}\xff")
            scanner = self._table(TABLE_EVENTS_BY_COUNTRY_TIME).scan(
                row_start=row_start,
                row_stop=row_stop,
                limit=limit,
            )
            for _rk, data in scanner:
                results.append({k.decode(): _decode(v) for k, v in data.items()})
        return results[:limit]

    def scan_topic_metrics(
        self,
        topic: str,
        from_ts: int,
        to_ts: int,
    ) -> List[Dict[str, str]]:
        """Scan topic_metrics_by_time for a topic in epoch range."""
        row_start = _encode(f"{topic}_{_reverse_ts(to_ts)}")
        row_stop = _encode(f"{topic}_{_reverse_ts(from_ts)}\xff")
        scanner = self._table(TABLE_TOPIC_METRICS).scan(
            row_start=row_start, row_stop=row_stop
        )
        return [{k.decode(): _decode(v) for k, v in data.items()} for _, data in scanner]

    def get_country_metrics(
        self,
        country: str,
        from_ts: int,
        to_ts: int,
    ) -> List[Dict[str, str]]:
        """Scan country_metrics_by_time for a country in epoch range."""
        row_start = _encode(f"{country}_{_reverse_ts(to_ts)}")
        row_stop = _encode(f"{country}_{_reverse_ts(from_ts)}\xff")
        scanner = self._table(TABLE_COUNTRY_METRICS).scan(
            row_start=row_start, row_stop=row_stop
        )
        return [{k.decode(): _decode(v) for k, v in data.items()} for _, data in scanner]

    def get_category_metrics(
        self,
        from_ts: int,
        to_ts: int,
    ) -> List[Dict[str, str]]:
        """Scan category_metrics_by_time across categories in epoch range."""
        rows = []
        for _, data in self._table(TABLE_CATEGORY_METRICS).scan():
            rows.append({k.decode(): _decode(v) for k, v in data.items()})
        return rows

    def scan_anomalies(
        self,
        from_ts: int,
        to_ts: int,
    ) -> List[Dict[str, str]]:
        """Scan anomalies_by_time for alerts in epoch range."""
        row_start = _encode(_reverse_ts(to_ts))
        row_stop = _encode(f"{_reverse_ts(from_ts)}\xff")
        scanner = self._table(TABLE_ANOMALIES).scan(
            row_start=row_start, row_stop=row_stop
        )
        return [{k.decode(): _decode(v) for k, v in data.items()} for _, data in scanner]
