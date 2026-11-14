"""HDFS client for the Global News Intelligence Platform.

Uses pyarrow.fs.HadoopFileSystem for Parquet I/O and directory operations.
Falls back to pywebhdfs for simple file operations when pyarrow HDFS is unavailable.

Partition path conventions:
  Raw events:    /gdelt/raw/events/date=YYYY-MM-DD/hour=HH/
  Curated events:/gdelt/curated/events/date=YYYY-MM-DD/
  Analytics:     /gdelt/analytics/<metric_type>/date=YYYY-MM-DD/
"""

import logging
import os
from datetime import datetime
from pathlib import PurePosixPath
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional HDFS filesystem backend
# ---------------------------------------------------------------------------
try:
    from pyarrow.fs import HadoopFileSystem  # type: ignore
    _HAVE_HADOOP = True
except ImportError:
    _HAVE_HADOOP = False
    logger.warning("pyarrow HadoopFileSystem unavailable; using local filesystem fallback.")

try:
    from pywebhdfs.webhdfs import PyWebHdfsClient  # type: ignore
    _HAVE_WEBHDFS = True
except ImportError:
    _HAVE_WEBHDFS = False


# ---------------------------------------------------------------------------
# HDFS layout constants
# ---------------------------------------------------------------------------
HDFS_BASE = "/gdelt"
RAW_EVENTS = f"{HDFS_BASE}/raw/events"
RAW_MENTIONS = f"{HDFS_BASE}/raw/mentions"
RAW_GKG = f"{HDFS_BASE}/raw/gkg"
CURATED_EVENTS = f"{HDFS_BASE}/curated/events"
CURATED_MENTIONS = f"{HDFS_BASE}/curated/mentions"
CURATED_GKG = f"{HDFS_BASE}/curated/gkg"
ANALYTICS_COUNTRY = f"{HDFS_BASE}/analytics/country_metrics"
ANALYTICS_TOPIC = f"{HDFS_BASE}/analytics/topic_metrics"
ANALYTICS_CATEGORY = f"{HDFS_BASE}/analytics/category_metrics"
ANALYTICS_TONE = f"{HDFS_BASE}/analytics/tone_metrics"
ANALYTICS_ANOMALY = f"{HDFS_BASE}/analytics/anomaly_metrics"
CHECKPOINTS = f"{HDFS_BASE}/checkpoints"


def raw_events_path(date: str, hour: str) -> str:
    """Return HDFS path for raw events partition. date=YYYY-MM-DD, hour=HH."""
    return f"{RAW_EVENTS}/date={date}/hour={hour}/"


def curated_events_path(date: str) -> str:
    """Return HDFS path for curated events partition. date=YYYY-MM-DD."""
    return f"{CURATED_EVENTS}/date={date}/"


def analytics_path(metric_type: str, date: str) -> str:
    """Return HDFS path for analytics output."""
    base_map = {
        "country": ANALYTICS_COUNTRY,
        "topic": ANALYTICS_TOPIC,
        "category": ANALYTICS_CATEGORY,
        "tone": ANALYTICS_TONE,
        "anomaly": ANALYTICS_ANOMALY,
    }
    base = base_map.get(metric_type, f"{HDFS_BASE}/analytics/{metric_type}")
    return f"{base}/date={date}/"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class HDFSClient:
    """HDFS client wrapping pyarrow.fs.HadoopFileSystem.

    When HDFS is unavailable (development mode), operations transparently
    fall back to the local filesystem under *local_fallback_root*.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8020,
        user: Optional[str] = None,
        local_fallback_root: Optional[str] = None,
        webhdfs_port: int = 9870,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user or os.environ.get("HADOOP_USER_NAME", "hadoop")
        self.local_fallback_root = local_fallback_root
        self.webhdfs_port = webhdfs_port
        self._fs: Optional[pa.fs.FileSystem] = None
        self._init_fs()

    def _init_fs(self) -> None:
        if _HAVE_HADOOP:
            try:
                self._fs = HadoopFileSystem(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                )
                logger.info("Connected to HDFS at %s:%s", self.host, self.port)
                return
            except Exception as exc:
                logger.warning("HDFS connection failed (%s); using local fallback.", exc)

        # Local filesystem fallback
        if self.local_fallback_root:
            self._fs = pa.fs.LocalFileSystem()
            logger.info("HDFS: local fallback root = %s", self.local_fallback_root)
        else:
            raise RuntimeError(
                "HDFS unavailable and no local_fallback_root configured. "
                "Set HDFS_LOCAL_FALLBACK_ROOT or ensure pyarrow HadoopFileSystem is available."
            )

    def _resolve(self, path: str) -> str:
        """Translate an HDFS path to local fallback path when needed."""
        if isinstance(self._fs, pa.fs.LocalFileSystem) and self.local_fallback_root:
            # Strip leading slash so os.path.join works
            rel = path.lstrip("/")
            return os.path.join(self.local_fallback_root, rel)
        return path

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    def mkdir(self, path: str) -> None:
        """Create directory (and parents) if it doesn't exist."""
        resolved = self._resolve(path)
        try:
            self._fs.create_dir(resolved, recursive=True)
            logger.debug("Created HDFS dir: %s", path)
        except Exception as exc:
            logger.warning("mkdir %s: %s", path, exc)

    def file_exists(self, path: str) -> bool:
        """Return True if path exists in HDFS."""
        try:
            info = self._fs.get_file_info(self._resolve(path))
            return info.type != pa.fs.FileType.NotFound
        except Exception:
            return False

    def list_files(self, path: str) -> List[str]:
        """List files under a directory path."""
        try:
            selector = pa.fs.FileSelector(self._resolve(path), recursive=False)
            infos = self._fs.get_file_info(selector)
            return [i.path for i in infos if i.type == pa.fs.FileType.File]
        except Exception as exc:
            logger.error("list_files %s: %s", path, exc)
            return []

    # ------------------------------------------------------------------
    # Parquet I/O
    # ------------------------------------------------------------------

    def write_parquet(
        self,
        df: pa.Table,
        path: str,
        partition_cols: Optional[List[str]] = None,
        compression: str = "snappy",
    ) -> None:
        """Write a PyArrow Table as Parquet to HDFS (or local fallback).

        Args:
            df: PyArrow Table to write.
            path: Target HDFS directory (a _SUCCESS file is not written here;
                  use write_to_dataset for partitioned writes).
            partition_cols: Optional list of columns to partition by.
            compression: Parquet compression codec (default: snappy).
        """
        self.mkdir(path)
        resolved = self._resolve(path)
        if partition_cols:
            pq.write_to_dataset(
                df,
                root_path=resolved,
                partition_cols=partition_cols,
                filesystem=self._fs,
                compression=compression,
                use_legacy_dataset=False,
            )
        else:
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
            file_path = f"{resolved}/part-{ts}.parquet".replace("\\", "/")
            with self._fs.open_output_stream(file_path) as out:
                pq.write_table(df, out, compression=compression)
        logger.info("Written Parquet to HDFS path: %s", path)

    def read_parquet(self, path: str) -> pa.Table:
        """Read Parquet files from an HDFS path (directory or single file)."""
        resolved = self._resolve(path)
        dataset = pq.ParquetDataset(resolved, filesystem=self._fs, use_legacy_dataset=False)
        return dataset.read()

    # ------------------------------------------------------------------
    # Convenience wrappers matching brief API
    # ------------------------------------------------------------------

    def write_raw_events(self, df: pa.Table, date: str, hour: str) -> None:
        self.write_parquet(df, raw_events_path(date, hour))

    def write_curated_events(self, df: pa.Table, date: str) -> None:
        self.write_parquet(df, curated_events_path(date))

    def write_analytics(self, df: pa.Table, metric_type: str, date: str) -> None:
        self.write_parquet(df, analytics_path(metric_type, date))
