"""
GDELT Master Last-Update Poller.
Polls https://data.gdeltproject.org/gdeltv2/lastupdate.txt every POLL_INTERVAL_MINUTES.
Discovers new Events, Mentions, and GKG archive files.
Downloads, verifies, parses, publishes to Kafka, and persists real metrics.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

from src.ingestion.checkpoint import CheckpointDB
from src.ingestion.downloader import Downloader, DownloadError
from src.ingestion.kafka_producer import GDELTKafkaProducer
from src.ingestion.metrics import metrics
from src.ingestion.parsers.events_parser import EventsParser
from src.ingestion.parsers.mentions_parser import MentionsParser
from src.ingestion.parsers.gkg_parser import GKGParser

logger = logging.getLogger(__name__)

LASTUPDATE_URL = os.environ.get(
    "GDELT_LASTUPDATE_URL",
    "https://data.gdeltproject.org/gdeltv2/lastupdate.txt",
)
LASTUPDATE_HTTP_FALLBACK = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Environment variable names
ENV_POLL_INTERVAL = "POLL_INTERVAL_MINUTES"
ENV_DOWNLOAD_DIR = "GDELT_DOWNLOAD_DIR"
ENV_CHECKPOINT_DB = "GDELT_CHECKPOINT_DB"
ENV_DEAD_LETTER_DIR = "GDELT_DEAD_LETTER_DIR"
ENV_KAFKA_BROKERS = "KAFKA_BOOTSTRAP_SERVERS"
ENV_DRY_RUN = "GDELT_DRY_RUN"
ENV_METRICS_FILE = "GDELT_METRICS_FILE"

# File type identification substrings
FILE_TYPE_MAP = {
    ".export.CSV.zip": "events",
    ".mentions.CSV.zip": "mentions",
    ".gkg.csv.zip": "gkg",
}


def _detect_file_type(url: str) -> Optional[str]:
    for suffix, ftype in FILE_TYPE_MAP.items():
        if url.endswith(suffix) or suffix.lower() in url.lower():
            return ftype
    return None


def _parse_lastupdate(text: str) -> List[Dict]:
    """
    Parse the lastupdate.txt format.
    Each line: <size> <md5hash> <url>
    Returns list of dicts with keys: size, hash, url, file_type
    """
    entries = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            logger.warning("Unexpected lastupdate.txt line format: %r", line)
            continue
        size_str, md5hash, url = parts[0], parts[1], parts[2]
        try:
            size = int(size_str)
        except ValueError:
            size = None
        file_type = _detect_file_type(url)
        entries.append({
            "size": size,
            "hash": md5hash,
            "url": url,
            "file_type": file_type,
        })
    return entries


class GDELTPoller:
    """
    Polls GDELT lastupdate.txt and orchestrates download → parse → produce pipeline.
    """

    def __init__(
        self,
        poll_interval_minutes: Optional[int] = None,
        download_dir: Optional[str | Path] = None,
        checkpoint_db: Optional[str | Path] = None,
        dead_letter_dir: Optional[str | Path] = None,
        kafka_brokers: Optional[str] = None,
        dry_run: Optional[bool] = None,
        metrics_file: Optional[str | Path] = None,
    ) -> None:
        # Resolve configuration from args or environment variables
        self.poll_interval = int(
            poll_interval_minutes
            or os.environ.get(ENV_POLL_INTERVAL, "15")
        )
        self.download_dir = Path(
            download_dir
            or os.environ.get(ENV_DOWNLOAD_DIR, "data/raw")
        )
        checkpoint_path = Path(
            checkpoint_db
            or os.environ.get(ENV_CHECKPOINT_DB, "data/checkpoints/ingestion.db")
        )
        self.dead_letter_dir = Path(
            dead_letter_dir
            or os.environ.get(ENV_DEAD_LETTER_DIR, "data/dead_letter")
        )
        self.metrics_file = Path(
            metrics_file
            or os.environ.get(ENV_METRICS_FILE, "data/metrics/ingestion_metrics.json")
        )
        brokers = kafka_brokers or os.environ.get(ENV_KAFKA_BROKERS, "localhost:9092")

        if dry_run is None:
            dry_run = os.environ.get(ENV_DRY_RUN, "false").lower() in ("1", "true", "yes")
        self.dry_run = dry_run

        # Instantiate sub-components
        self.checkpoint = CheckpointDB(checkpoint_path)
        self.downloader = Downloader(self.download_dir)
        self.producer = GDELTKafkaProducer(
            bootstrap_servers=brokers,
            dry_run=dry_run,
        )
        self.events_parser = EventsParser(dead_letter_dir=self.dead_letter_dir)
        self.mentions_parser = MentionsParser(dead_letter_dir=self.dead_letter_dir)
        self.gkg_parser = GKGParser(dead_letter_dir=self.dead_letter_dir)

        logger.info(
            "GDELTPoller initialised: poll_interval=%dm, download_dir=%s, dry_run=%s, brokers=%s",
            self.poll_interval, self.download_dir, self.dry_run, brokers,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def poll_once(self) -> List[Dict]:
        """
        Fetch lastupdate.txt, discover new files, download+process each one.
        Returns list of result dicts.
        """
        metrics.touch_poll()
        text = self._fetch_lastupdate()
        if text is None:
            self._write_metrics_snapshot()
            return []

        entries = _parse_lastupdate(text)
        logger.info("lastupdate.txt listed %d files", len(entries))

        results = []
        for entry in entries:
            url = entry["url"]
            file_type = entry.get("file_type")

            if file_type is None:
                logger.debug("Skipping unknown file type: %s", url)
                continue

            # Register in checkpoint (idempotent)
            self.checkpoint.register_file(
                url=url,
                file_type=file_type,
                file_hash=entry.get("hash"),
                file_size=entry.get("size"),
            )

            if self.checkpoint.is_processed(url):
                logger.debug("Already processed, skipping: %s", url)
                continue

            metrics.touch_discovered()
            result = self._process_entry(entry)
            results.append(result)

        self._write_metrics_snapshot()
        return results

    def run_forever(self) -> None:
        """Continuous polling loop — runs until interrupted."""
        logger.info("Starting continuous GDELT polling (interval=%dm)", self.poll_interval)
        while True:
            try:
                self.poll_once()
            except KeyboardInterrupt:
                logger.info("Polling interrupted by user")
                break
            except Exception as exc:
                logger.error("Unexpected error in polling loop: %s", exc, exc_info=True)
                metrics.inc_errors()
                self._write_metrics_snapshot()
            wait_secs = self.poll_interval * 60
            logger.info("Sleeping %ds until next poll", wait_secs)
            time.sleep(wait_secs)
        self.producer.close()

    def replay_file(self, local_path: str | Path, file_type: Optional[str] = None) -> Dict:
        """Re-parse and re-publish a previously downloaded file."""
        local_path = Path(local_path)
        if file_type is None:
            file_type = _detect_file_type(local_path.name)
        if file_type is None:
            raise ValueError(f"Cannot detect file type for {local_path}")
        logger.info("Replaying %s (type=%s)", local_path, file_type)
        res = self._parse_and_produce(local_path, file_type, url=str(local_path))
        self._write_metrics_snapshot()
        return res

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch_lastupdate(self) -> Optional[str]:
        urls = [LASTUPDATE_URL]
        if LASTUPDATE_HTTP_FALLBACK not in urls:
            urls.append(LASTUPDATE_HTTP_FALLBACK)
        for url in urls:
            try:
                logger.info("Fetching %s", url)
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                return resp.text
            except Exception as exc:
                logger.warning("Failed to fetch %s: %s", url, exc)
        logger.error("All lastupdate.txt endpoints failed")
        metrics.inc_errors()
        return None

    def _write_metrics_snapshot(self) -> None:
        """Persist real SQLite-backed + runtime ingestion telemetry atomically."""
        try:
            self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
            db_metrics = self.checkpoint.get_pipeline_metrics()
            runtime = metrics.as_dict()
            db_metrics["last_poll_time"] = (
                datetime.fromtimestamp(runtime["last_poll_time"], tz=timezone.utc).isoformat()
                if runtime.get("last_poll_time")
                else datetime.now(timezone.utc).isoformat()
            )
            db_metrics["kafka_messages_sent"] = runtime.get("kafka_messages_sent", 0)
            db_metrics["total_errors"] = max(
                db_metrics.get("total_errors", 0),
                runtime.get("errors", 0),
            )

            tmp_fd, tmp_name = tempfile.mkstemp(
                dir=self.metrics_file.parent, prefix=".metrics_", suffix=".json"
            )
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                json.dump(db_metrics, fh, indent=2)
            os.chmod(tmp_name, 0o644)
            os.replace(tmp_name, self.metrics_file)
        except Exception as exc:
            logger.warning("Could not write ingestion metrics snapshot: %s", exc)

    def _process_entry(self, entry: Dict) -> Dict:
        url = entry["url"]
        file_type = entry["file_type"]
        result = {"url": url, "file_type": file_type, "status": "error", "records": 0}

        # Download
        try:
            local_path = self.downloader.download(
                url=url,
                expected_size=entry.get("size"),
                expected_md5=entry.get("hash"),
            )
            self.checkpoint.mark_downloaded(url, str(local_path))
            metrics.inc_downloaded()
        except DownloadError as exc:
            self.checkpoint.mark_failed(url, str(exc))
            metrics.inc_errors()
            result["error"] = str(exc)
            return result

        # Validate ZIP
        if not zipfile.is_zipfile(local_path):
            err = f"Downloaded file is not a valid ZIP: {local_path}"
            logger.error(err)
            self.checkpoint.mark_failed(url, err)
            metrics.inc_errors()
            result["error"] = err
            return result

        # Parse and produce
        parse_result = self._parse_and_produce(local_path, file_type, url=url)
        result.update(parse_result)
        return result

    def _parse_and_produce(
        self, local_path: Path, file_type: str, url: str
    ) -> Dict:
        result: Dict = {"url": url, "file_type": file_type, "status": "ok", "records": 0, "rejected": 0}

        try:
            if file_type == "events":
                accepted, rejected = self.events_parser.parse_zip(local_path)
                self.producer.produce_events([r for r in accepted])
                result["records"] = len(accepted)
                result["rejected"] = len(rejected)
                metrics.inc_accepted(len(accepted), "events")

            elif file_type == "mentions":
                accepted, rejected = self.mentions_parser.parse_zip(local_path)
                self.producer.produce_mentions([r for r in accepted])
                result["records"] = len(accepted)
                result["rejected"] = len(rejected)
                metrics.inc_accepted(len(accepted), "mentions")

            elif file_type == "gkg":
                accepted, rejected = self.gkg_parser.parse_zip(local_path)
                self.producer.produce_gkg([r for r in accepted])
                result["records"] = len(accepted)
                result["rejected"] = len(rejected)
                metrics.inc_accepted(len(accepted), "gkg")

            else:
                result["status"] = "skipped"
                return result

            metrics.inc_rejected(result.get("rejected", 0))
            remaining = self.producer.flush()
            if remaining > 0 and not self.dry_run:
                raise RuntimeError(f"Kafka flush incomplete: {remaining} messages remaining")

            self.checkpoint.mark_processed(
                url,
                kafka_published=not self.dry_run,
                records_accepted=result["records"],
                records_rejected=result["rejected"],
            )
            metrics.inc_processed()
            logger.info(
                "Processed %s: %d records accepted, %d rejected",
                url, result["records"], result.get("rejected", 0),
            )

        except Exception as exc:
            logger.error("Failed to parse/produce %s: %s", local_path, exc, exc_info=True)
            self.checkpoint.mark_failed(url, str(exc))
            metrics.inc_errors()
            result["status"] = "error"
            result["error"] = str(exc)

        return result
