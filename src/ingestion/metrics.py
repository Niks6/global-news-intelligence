"""
Operational metrics for the ingestion pipeline.
Exposes a singleton IngestionMetrics instance that all modules update.
The health/status endpoint reads metrics.as_dict().
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class IngestionMetrics:
    """Thread-safe counters and timestamps for ingestion operations."""

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # Timestamps (Unix epoch floats)
    last_poll_time: Optional[float] = None
    last_file_discovered: Optional[float] = None

    # Counters
    files_downloaded: int = 0
    files_processed: int = 0
    records_accepted: int = 0
    records_rejected: int = 0
    kafka_messages_sent: int = 0
    errors: int = 0

    # Per-type breakdowns
    events_accepted: int = 0
    mentions_accepted: int = 0
    gkg_accepted: int = 0

    def touch_poll(self) -> None:
        with self._lock:
            self.last_poll_time = time.time()

    def touch_discovered(self) -> None:
        with self._lock:
            self.last_file_discovered = time.time()

    def inc_downloaded(self, n: int = 1) -> None:
        with self._lock:
            self.files_downloaded += n

    def inc_processed(self, n: int = 1) -> None:
        with self._lock:
            self.files_processed += n

    def inc_accepted(self, n: int = 1, file_type: str = "") -> None:
        with self._lock:
            self.records_accepted += n
            if file_type == "events":
                self.events_accepted += n
            elif file_type == "mentions":
                self.mentions_accepted += n
            elif file_type == "gkg":
                self.gkg_accepted += n

    def inc_rejected(self, n: int = 1) -> None:
        with self._lock:
            self.records_rejected += n

    def inc_kafka(self, n: int = 1) -> None:
        with self._lock:
            self.kafka_messages_sent += n

    def inc_errors(self, n: int = 1) -> None:
        with self._lock:
            self.errors += n

    def as_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "last_poll_time": self.last_poll_time,
                "last_file_discovered": self.last_file_discovered,
                "files_downloaded": self.files_downloaded,
                "files_processed": self.files_processed,
                "records_accepted": self.records_accepted,
                "records_rejected": self.records_rejected,
                "kafka_messages_sent": self.kafka_messages_sent,
                "errors": self.errors,
                "breakdown": {
                    "events_accepted": self.events_accepted,
                    "mentions_accepted": self.mentions_accepted,
                    "gkg_accepted": self.gkg_accepted,
                },
            }


# Module-level singleton
metrics = IngestionMetrics()
