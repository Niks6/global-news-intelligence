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


def _kafka_reachable() -> bool:
    """Check if Kafka broker is reachable via socket."""
    import socket
    try:
        from src.common.config import get_settings
        s = get_settings()
        host = s.kafka_bootstrap_servers.split(",")[0]
        h, p = host.split(":")
        sock = socket.create_connection((h, int(p)), timeout=3)
        sock.close()
        return True
    except Exception:
        return False


def _hdfs_reachable() -> bool:
    """Check if HDFS NameNode web UI is reachable."""
    import urllib.request
    try:
        from src.common.config import get_settings
        s = get_settings()
        url = s.hdfs_webhdfs_url  # e.g. "http://node3:9870"
        req = urllib.request.Request(url, method="HEAD")
        urllib.request.urlopen(req, timeout=3)
        return True
    except Exception:
        return False


@router.get("/health")
def health_check():
    hbase_ok = _hbase_connected()
    kafka_ok = _kafka_reachable()
    hdfs_ok = _hdfs_reachable()

    components = {
        "kafka": {"status": "up" if kafka_ok else "down"},
        "hbase": {"status": "up" if hbase_ok else "down"},
        "hdfs": {"status": "up" if hdfs_ok else "down"},
        "spark": {"status": "unknown"},
    }

    all_up = all(c["status"] == "up" for c in components.values() if c["status"] != "unknown")
    any_up = any(c["status"] == "up" for c in components.values())

    if all_up:
        overall = "healthy"
    elif any_up:
        overall = "degraded"
    else:
        overall = "unhealthy"

    return {
        "status": overall,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0",
        "service": "Global News Intelligence API",
        "components": components,
        "demo_mode": not any_up,
    }


@router.get("/health/ready")
def readiness_check():
    """Readiness probe — returns ready=true if backend can serve requests."""
    return {"ready": True, "timestamp": datetime.now(timezone.utc).isoformat()}


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

