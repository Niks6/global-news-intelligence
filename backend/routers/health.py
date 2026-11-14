"""Health and pipeline status endpoints."""
from __future__ import annotations
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter

router = APIRouter()


def _load_metrics() -> Dict[str, Any]:
    metrics_file = Path(os.environ.get("GDELT_METRICS_FILE", "data/metrics/ingestion_metrics.json"))
    if metrics_file.exists():
        try:
            return json.loads(metrics_file.read_text())
        except Exception:
            pass
    return {}


def _hbase_connected() -> bool:
    try:
        import happybase
        from src.common.config import get_settings
        s = get_settings()
        conn = happybase.Connection(host=s.hbase_host, port=s.hbase_thrift_port, timeout=3000)
        conn.open()
        conn.close()
        return True
    except Exception:
        return False


@router.get("/health")
def health_check():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0",
        "service": "Global News Intelligence API",
    }


@router.get("/health/pipeline")
def pipeline_health():
    metrics = _load_metrics()
    hbase_ok = _hbase_connected()

    last_ingestion = metrics.get("last_ingestion_time")
    freshness = "unknown"
    if last_ingestion:
        try:
            last_dt = datetime.fromisoformat(last_ingestion)
            age_minutes = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60
            if age_minutes < 20:
                freshness = "fresh"
            elif age_minutes < 60:
                freshness = "stale"
            else:
                freshness = "old"
        except Exception:
            pass

    return {
        "hbase_connected": hbase_ok,
        "last_ingestion_time": last_ingestion,
        "freshness_status": freshness,
        "files_processed_today": metrics.get("files_processed_today", 0),
        "records_accepted_today": metrics.get("records_accepted_today", 0),
        "records_rejected_today": metrics.get("records_rejected_today", 0),
        "total_errors": metrics.get("total_errors", 0),
        "kafka_status": "unknown",   # Requires Kafka JMX integration
        "spark_status": "unknown",   # Requires Spark REST API integration
        "hdfs_status": "unknown",    # Requires HDFS REST integration
        "pipeline_lag_minutes": metrics.get("pipeline_lag_minutes"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

