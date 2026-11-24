"""
SQLite-based checkpoint store for GDELT ingestion deduplication.
Tracks every file URL seen from lastupdate.txt and its processing status,
including accepted/rejected record counts for real pipeline telemetry.
Thread-safe via a per-connection threading.local pattern.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ingestion_files (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    file_url            TEXT    NOT NULL UNIQUE,
    file_type           TEXT    NOT NULL,          -- 'events' | 'mentions' | 'gkg'
    file_hash           TEXT,                      -- MD5 from lastupdate.txt
    file_size           INTEGER,                   -- bytes from lastupdate.txt
    download_path       TEXT,                      -- local path after download
    status              TEXT    NOT NULL DEFAULT 'discovered',
                                                   -- discovered | downloaded | processed | failed
    discovered_at       REAL    NOT NULL,          -- Unix timestamp
    downloaded_at       REAL,
    processed_at        REAL,
    kafka_published_at  REAL,
    records_accepted    INTEGER NOT NULL DEFAULT 0,
    records_rejected    INTEGER NOT NULL DEFAULT 0,
    error_msg           TEXT,
    retry_count         INTEGER NOT NULL DEFAULT 0
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_ingestion_files_status ON ingestion_files(status);
"""


class CheckpointDB:
    """Thread-safe SQLite checkpoint store."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        """Return a per-thread SQLite connection (created lazily)."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self.db_path), timeout=30, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=10000")
            self._local.conn = conn
        return self._local.conn

    @contextmanager
    def _tx(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _init_db(self) -> None:
        with self._tx() as conn:
            conn.execute(CREATE_TABLE_SQL)
            conn.execute(CREATE_INDEX_SQL)
            # Migrate existing DBs that lack record count columns
            cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(ingestion_files)").fetchall()
            }
            if "records_accepted" not in cols:
                conn.execute(
                    "ALTER TABLE ingestion_files ADD COLUMN records_accepted INTEGER NOT NULL DEFAULT 0"
                )
            if "records_rejected" not in cols:
                conn.execute(
                    "ALTER TABLE ingestion_files ADD COLUMN records_rejected INTEGER NOT NULL DEFAULT 0"
                )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_processed(self, url: str) -> bool:
        """Return True if the URL has been fully processed (status='processed')."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT status FROM ingestion_files WHERE file_url = ?", (url,)
        ).fetchone()
        return row is not None and row["status"] == "processed"

    def is_known(self, url: str) -> bool:
        """Return True if the URL is in the DB (any status)."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT id FROM ingestion_files WHERE file_url = ?", (url,)
        ).fetchone()
        return row is not None

    def register_file(
        self,
        url: str,
        file_type: str,
        file_hash: Optional[str] = None,
        file_size: Optional[int] = None,
    ) -> None:
        """Insert a newly discovered file (idempotent — ignores conflicts)."""
        with self._tx() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO ingestion_files
                    (file_url, file_type, file_hash, file_size, status, discovered_at)
                VALUES (?, ?, ?, ?, 'discovered', ?)
                """,
                (url, file_type, file_hash, file_size, time.time()),
            )

    def mark_downloaded(self, url: str, path: str) -> None:
        """Mark a file as downloaded and record its local path."""
        with self._tx() as conn:
            conn.execute(
                """
                UPDATE ingestion_files
                SET status = 'downloaded', download_path = ?, downloaded_at = ?
                WHERE file_url = ?
                """,
                (path, time.time(), url),
            )

    def mark_processed(
        self,
        url: str,
        kafka_published: bool = False,
        records_accepted: int = 0,
        records_rejected: int = 0,
    ) -> None:
        """Mark a file as fully processed (and record accepted/rejected counts)."""
        now = time.time()
        with self._tx() as conn:
            conn.execute(
                """
                UPDATE ingestion_files
                SET status = 'processed',
                    processed_at = ?,
                    kafka_published_at = ?,
                    records_accepted = ?,
                    records_rejected = ?,
                    error_msg = NULL
                WHERE file_url = ?
                """,
                (now, now if kafka_published else None, records_accepted, records_rejected, url),
            )

    def mark_failed(self, url: str, error: str) -> None:
        """Mark a file as failed and increment retry_count."""
        with self._tx() as conn:
            conn.execute(
                """
                UPDATE ingestion_files
                SET status = 'failed',
                    error_msg = ?,
                    retry_count = retry_count + 1
                WHERE file_url = ?
                """,
                (error, url),
            )

    def reset_to_discovered(self, url: str) -> None:
        """Reset a failed file so it will be retried."""
        with self._tx() as conn:
            conn.execute(
                "UPDATE ingestion_files SET status = 'discovered', error_msg = NULL WHERE file_url = ?",
                (url,),
            )

    def get_pending_files(self) -> List[Dict]:
        """Return all files with status 'discovered' or 'downloaded' (not yet processed)."""
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT file_url, file_type, file_hash, file_size, download_path, status,
                   discovered_at, retry_count
            FROM ingestion_files
            WHERE status IN ('discovered', 'downloaded')
            ORDER BY discovered_at ASC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def get_stats(self) -> Dict:
        """Return summary statistics from the checkpoint DB."""
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT status, file_type, COUNT(*) as cnt
            FROM ingestion_files
            GROUP BY status, file_type
            """
        ).fetchall()
        stats: Dict = {}
        for row in rows:
            status = row["status"]
            ftype = row["file_type"]
            stats.setdefault(status, {})[ftype] = row["cnt"]
        total = conn.execute("SELECT COUNT(*) FROM ingestion_files").fetchone()[0]
        stats["total"] = total
        return stats

    def get_pipeline_metrics(self) -> Dict[str, Any]:
        """
        Compute live ingestion telemetry from the checkpoint SQLite store.
        No hardcoded values — every figure comes directly from processed/failed rows.
        """
        conn = self._get_conn()
        now_dt = datetime.now(timezone.utc)
        today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        week_start = (now_dt - timedelta(days=7)).timestamp()

        today_row = conn.execute(
            """
            SELECT
                COUNT(*) AS files_processed_today,
                COALESCE(SUM(records_accepted), 0) AS records_accepted_today,
                COALESCE(SUM(records_rejected), 0) AS records_rejected_today
            FROM ingestion_files
            WHERE status = 'processed' AND processed_at >= ?
            """,
            (today_start,),
        ).fetchone()

        week_row = conn.execute(
            """
            SELECT
                COUNT(*) AS files_week,
                COALESCE(SUM(records_accepted), 0) AS total_records_accepted,
                COALESCE(SUM(records_rejected), 0) AS total_records_rejected
            FROM ingestion_files
            WHERE status = 'processed' AND processed_at >= ?
            """,
            (week_start,),
        ).fetchone()

        latest_row = conn.execute(
            "SELECT MAX(processed_at) AS last_ts FROM ingestion_files WHERE status = 'processed'"
        ).fetchone()

        err_row = conn.execute(
            "SELECT COUNT(*) AS err_cnt FROM ingestion_files WHERE status = 'failed'"
        ).fetchone()

        breakdown_rows = conn.execute(
            """
            SELECT file_type,
                   COUNT(*) AS files,
                   COALESCE(SUM(records_accepted), 0) AS accepted,
                   COALESCE(SUM(records_rejected), 0) AS rejected
            FROM ingestion_files
            WHERE status = 'processed' AND processed_at >= ?
            GROUP BY file_type
            """,
            (today_start,),
        ).fetchall()

        breakdown = {
            "events_accepted": 0,
            "mentions_accepted": 0,
            "gkg_accepted": 0,
        }
        for r in breakdown_rows:
            ft = r["file_type"]
            if ft in ("events", "mentions", "gkg"):
                breakdown[f"{ft}_accepted"] = int(r["accepted"])

        last_ts = latest_row["last_ts"] if latest_row else None
        last_ingestion_iso: Optional[str] = None
        lag_minutes: Optional[float] = None
        if last_ts is not None:
            last_dt = datetime.fromtimestamp(float(last_ts), tz=timezone.utc)
            last_ingestion_iso = last_dt.isoformat()
            lag_minutes = round(max(0.0, (now_dt - last_dt).total_seconds() / 60.0), 1)

        history_rows = conn.execute(
            """
            SELECT strftime('%Y-%m-%d', processed_at, 'unixepoch') AS day,
                   COUNT(*) AS files,
                   COALESCE(SUM(records_accepted), 0) AS accepted,
                   COALESCE(SUM(records_rejected), 0) AS rejected
            FROM ingestion_files
            WHERE status = 'processed' AND processed_at >= ?
            GROUP BY day
            ORDER BY day ASC
            """,
            (week_start,),
        ).fetchall()

        error_rows = conn.execute(
            """
            SELECT file_url, error_msg, discovered_at
            FROM ingestion_files
            WHERE status = 'failed'
            ORDER BY discovered_at DESC
            LIMIT 10
            """
        ).fetchall()

        return {
            "last_ingestion_time": last_ingestion_iso,
            "pipeline_lag_minutes": lag_minutes,
            "files_processed_today": int(today_row["files_processed_today"]),
            "files_week": int(week_row["files_week"]),
            "records_accepted_today": int(today_row["records_accepted_today"]),
            "records_rejected_today": int(today_row["records_rejected_today"]),
            "total_records_accepted": int(week_row["total_records_accepted"]),
            "total_records_rejected": int(week_row["total_records_rejected"]),
            "total_errors": int(err_row["err_cnt"]),
            "breakdown": breakdown,
            "ingestion_history": [
                {
                    "date": r["day"],
                    "files": int(r["files"]),
                    "accepted": int(r["accepted"]),
                    "rejected": int(r["rejected"]),
                }
                for r in history_rows
                if r["day"]
            ],
            "error_logs": [
                {
                    "timestamp": datetime.fromtimestamp(float(r["discovered_at"]), tz=timezone.utc).isoformat(),
                    "level": "ERROR",
                    "message": r["error_msg"] or "Failed",
                    "source": r["file_url"],
                }
                for r in error_rows
            ],
        }
