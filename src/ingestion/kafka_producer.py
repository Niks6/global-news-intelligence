"""
Kafka producer for GDELT ingestion pipeline.
Publishes events, mentions, and GKG records to their respective topics.
Sends malformed records to the dead-letter topic.
Implements an idempotent producer with delivery callbacks.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

try:
    from confluent_kafka import Producer, KafkaError, KafkaException
    CONFLUENT_AVAILABLE = True
except ImportError:
    CONFLUENT_AVAILABLE = False
    Producer = None  # type: ignore[misc,assignment]

from src.ingestion.metrics import metrics

logger = logging.getLogger(__name__)

# Topic names
TOPIC_EVENTS    = "gdelt.events.raw"
TOPIC_MENTIONS  = "gdelt.mentions.raw"
TOPIC_GKG       = "gdelt.gkg.raw"
TOPIC_DEAD_LETTER = "gdelt.dead-letter"

FLUSH_TIMEOUT = 30          # seconds to wait for flush
BATCH_SIZE    = 500         # flush every N messages


def _default_delivery_cb(err: Optional[Any], msg: Any) -> None:
    if err is not None:
        logger.error(
            "Delivery failed for %s [key=%s]: %s",
            msg.topic() if msg else "?",
            msg.key() if msg else "?",
            err,
        )
        metrics.inc_errors()
    else:
        metrics.inc_kafka()


class GDELTKafkaProducer:
    """
    Wraps confluent-kafka Producer for GDELT pipeline.

    If Kafka is not available (import fails or no broker config) it
    operates in a 'dry-run' mode, logging messages instead of producing.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        extra_config: Optional[Dict] = None,
        dry_run: bool = False,
    ) -> None:
        self.dry_run = dry_run or not CONFLUENT_AVAILABLE
        self._pending = 0

        if not self.dry_run:
            cfg: Dict[str, Any] = {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "retries": 10,
                "max.in.flight.requests.per.connection": 5,
                "compression.type": "lz4",
                "linger.ms": 20,
                "batch.size": 65536,
            }
            if extra_config:
                cfg.update(extra_config)
            self._producer = Producer(cfg)
            logger.info("Kafka producer connected to %s", bootstrap_servers)
        else:
            self._producer = None
            logger.warning(
                "confluent-kafka not available or dry_run=True; "
                "messages will be logged but not produced."
            )

    # ------------------------------------------------------------------
    # Public produce methods
    # ------------------------------------------------------------------

    def produce_events(
        self,
        records: List[Dict],
        on_delivery: Optional[Callable] = None,
    ) -> None:
        """Produce a list of GDELTEvent dicts to gdelt.events.raw."""
        for rec in records:
            key = str(rec.get("GlobalEventID", ""))
            self._produce(TOPIC_EVENTS, key, rec, on_delivery)

    def produce_mentions(
        self,
        records: List[Dict],
        on_delivery: Optional[Callable] = None,
    ) -> None:
        """Produce a list of GDELTMention dicts to gdelt.mentions.raw."""
        for rec in records:
            key = str(rec.get("MentionIdentifier", rec.get("GlobalEventID", "")))
            self._produce(TOPIC_MENTIONS, key, rec, on_delivery)

    def produce_gkg(
        self,
        records: List[Dict],
        on_delivery: Optional[Callable] = None,
    ) -> None:
        """Produce a list of GDELTGKGRecord dicts to gdelt.gkg.raw."""
        for rec in records:
            key = str(rec.get("GKGRecordID", ""))
            self._produce(TOPIC_GKG, key, rec, on_delivery)

    def produce_dead_letter(
        self,
        source_topic: str,
        raw_row: str,
        error: str,
        file_url: str = "",
    ) -> None:
        """Send a malformed record to the dead-letter topic."""
        payload = {
            "source_topic": source_topic,
            "file_url": file_url,
            "error": error,
            "raw_row": raw_row,
            "ts": time.time(),
        }
        self._produce(TOPIC_DEAD_LETTER, source_topic, payload, None)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _produce(
        self,
        topic: str,
        key: str,
        value: Dict,
        on_delivery: Optional[Callable],
    ) -> None:
        cb = on_delivery or _default_delivery_cb
        encoded_value = json.dumps(value, default=str).encode("utf-8")
        encoded_key = key.encode("utf-8") if key else None

        if self.dry_run:
            logger.debug("DRY-RUN produce → %s key=%s", topic, key)
            metrics.inc_kafka()
            return

        try:
            self._producer.produce(  # type: ignore[union-attr]
                topic=topic,
                key=encoded_key,
                value=encoded_value,
                on_delivery=cb,
            )
            self._pending += 1
            # Poll to trigger delivery callbacks
            self._producer.poll(0)  # type: ignore[union-attr]

            if self._pending >= BATCH_SIZE:
                self.flush()
        except KafkaException as exc:
            logger.error("KafkaException producing to %s: %s", topic, exc)
            metrics.inc_errors()

    def flush(self, timeout: float = FLUSH_TIMEOUT) -> int:
        """Flush all pending messages. Returns number still in queue."""
        if self.dry_run or self._producer is None:
            return 0
        remaining = self._producer.flush(timeout)
        self._pending = 0
        if remaining > 0:
            logger.warning("%d messages not delivered within %.1fs flush timeout", remaining, timeout)
        return remaining

    def close(self) -> None:
        self.flush()
        logger.info("Kafka producer closed")
