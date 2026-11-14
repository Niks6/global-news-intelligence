"""HBase client wrapper using happybase for the Global News Intelligence Platform.

Row-key designs:
  events_by_id:            GlobalEventID (string)
  events_by_country_time:  SALT_COUNTRYCODE_REVERSETIMESTAMP_EVENTID
                           SALT = hex(hash(country_code) % 8) → avoids hot-spotting
  topic_metrics_by_time:   TOPIC_REVERSETIMESTAMP
  country_metrics_by_time: COUNTRY_REVERSETIMESTAMP
  category_metrics_by_time:CATEGORY_REVERSETIMESTAMP
  anomalies_by_time:       REVERSETIMESTAMP_TYPE_KEY

Reverse-timestamp = str(sys.maxsize - epoch_seconds)  → newest records sort first.
"""

import sys
import json
import logging
import struct
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple, Any

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
    """Single hex char (0-7) derived from country_code hash → avoids hot-spotting."""
    return format(hash(country_code) % SALT_BUCKETS, "x")


def _encode(value: Any) -> bytes:
    """Encode a Python value to bytes for HBase storage."""
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
    return value.decode("utf-8")


# ---------------------------------------------------------------------------
# Main client
# ---------------------------------------------------------------------------

class HBaseClient:
    """Wrapper around happybase.Connection providing domain-level methods."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9090,
        timeout: Optional[int] = 10_000,
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
            self._connection.close()
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
        event_id = str(event.get("GlobalEventID", ""))
        date_str = str(event.get("SQLDATE", ""))
        country = str(event.get("ActionGeo_CountryCode", "UN") or "UN")
        source_url = str(event.get("SOURCEURL", "") or "")

        # Determine epoch from SQLDATE (YYYYMMDD) + optional time
        try:
            epoch = int(datetime.strptime(date_str[:8], "%Y%m%d").timestamp())
        except (ValueError, TypeError):
            epoch = int(datetime.utcnow().timestamp())

        rev_ts = _reverse_ts(epoch)

        # --- events_by_id ---
        row_key_id = _encode(event_id)
        event_data: Dict[bytes, bytes] = {}
        for field, val in event.items():
            col = f"{CF_EVENT.decode()}:{field}".encode()
            event_data[col] = _encode(val)
        event_data[b"meta:source_url"] = _encode(source_url)
        event_data[b"meta:ingested_at"] = _encode(str(datetime.utcnow().isoformat()))
        self._table(TABLE_EVENTS_BY_ID).put(row_key_id, event_data)

        # --- events_by_country_time ---
        salt = _salt(country)
        row_key_ct = _encode(f"{salt}_{country}_{rev_ts}_{event_id}")
        ct_data: Dict[bytes, bytes] = {
            b"data:event_id": _encode(event_id),
            b"data:country": _encode(country),
            b"data:date": _encode(date_str),
            b"data:event_root_code": _encode(str(event.get("EventRootCode", ""))),
            b"data:goldstein": _encode(str(event.get("GoldsteinScale", ""))),
            b"data:avg_tone": _encode(str(event.get("AvgTone", ""))),
            b"data:quad_class": _encode(str(event.get("QuadClass", ""))),
            b"data:actor1_country": _encode(str(event.get("Actor1CountryCode", ""))),
            b"data:actor2_country": _encode(str(event.get("Actor2CountryCode", ""))),
            b"data:source_url": _encode(source_url),
            b"data:lat": _encode(str(event.get("ActionGeo_Lat", ""))),
            b"data:lon": _encode(str(event.get("ActionGeo_Long", ""))),
        }
        self._table(TABLE_EVENTS_BY_COUNTRY_TIME).put(row_key_ct, ct_data)

    def put_country_metric(
        self,
        country: str,
        epoch_seconds: int,
        metrics: Dict[str, Any],
    ) -> None:
        """Write country aggregate metrics."""
        rev_ts = _reverse_ts(epoch_seconds)
        row_key = _encode(f"{country}_{rev_ts}")
        data: Dict[bytes, bytes] = {
            f"metrics:{k}".encode(): _encode(v) for k, v in metrics.items()
        }
        self._table(TABLE_COUNTRY_METRICS).put(row_key, data)

    def put_topic_metric(
        self,
        topic: str,
        epoch_seconds: int,
        metrics: Dict[str, Any],
    ) -> None:
        """Write topic aggregate metrics."""
        rev_ts = _reverse_ts(epoch_seconds)
        row_key = _encode(f"{topic}_{rev_ts}")
        data: Dict[bytes, bytes] = {
            f"metrics:{k}".encode(): _encode(v) for k, v in metrics.items()
        }
        self._table(TABLE_TOPIC_METRICS).put(row_key, data)

    def put_category_metric(
        self,
        category: str,
        epoch_seconds: int,
        metrics: Dict[str, Any],
    ) -> None:
        """Write CAMEO category aggregate metrics."""
        rev_ts = _reverse_ts(epoch_seconds)
        row_key = _encode(f"{category}_{rev_ts}")
        data: Dict[bytes, bytes] = {
            f"metrics:{k}".encode(): _encode(v) for k, v in metrics.items()
        }
        self._table(TABLE_CATEGORY_METRICS).put(row_key, data)

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
        """Scan events_by_country_time for a country in a time range.

        Because of reverse timestamps the scan start is the more-recent (larger) epoch.
        from_ts < to_ts (from_ts is older); rows are ordered newest-first.
        """
        results = []
        # We scan all 8 salt buckets
        for bucket in range(SALT_BUCKETS):
            salt = format(bucket, "x")
            # Reverse: older epoch → larger rev_ts → scan from rev_ts(to_ts) → rev_ts(from_ts)
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
        """Scan category_metrics_by_time across all categories in epoch range."""
        # No category prefix filter — return everything in the time band
        row_start = _encode(_reverse_ts(to_ts))
        row_stop = _encode(f"{_reverse_ts(from_ts)}\xff")
        scanner = self._table(TABLE_CATEGORY_METRICS).scan(
            row_start=row_start, row_stop=row_stop
        )
        return [{k.decode(): _decode(v) for k, v in data.items()} for _, data in scanner]

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
