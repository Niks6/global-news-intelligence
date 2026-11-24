"""Health and pipeline status endpoints — reads real telemetry from
CheckpointDB, ingestion_metrics.json, processing_metrics.json, and live cluster probes.
Zero hardcoded metrics."""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter

router = APIRouter()

_probe_cache: Dict[str, Any] = {
    "hbase": False,
    "kafka": False,
    "spark": False,
    "hdfs": False,
    "spark_workers": 0,
    "hdfs_files": 0,
    "updated_at": None,
}
_probe_lock = threading.Lock()
_probe_started = False


def _hbase_connected() -> bool:
    try:
        import happybase
        from src.common.config import get_settings
        s = get_settings()
        conn = happybase.Connection(host=s.hbase_host, port=s.hbase_thrift_port, timeout=3000)
        conn.open()
        conn.tables()
        conn.close()
        return True
    except Exception:
        return False


def _kafka_reachable() -> bool:
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


def _hdfs_probe() -> Dict[str, Any]:
    import urllib.request
    try:
        from src.common.config import get_settings
        s = get_settings()
        base = s.hdfs_webhdfs_url.rstrip("/")
        req = urllib.request.Request(f"{base}/webhdfs/v1/?op=LISTSTATUS", method="GET")
        resp = urllib.request.urlopen(req, timeout=3)
        data = json.loads(resp.read().decode())
        top = data.get("FileStatuses", {}).get("FileStatus", [])
        # Also count curated/analytics dirs if /gdelt exists
        file_count = len(top)
        try:
            r2 = urllib.request.urlopen(f"{base}/webhdfs/v1/gdelt/curated?op=LISTSTATUS", timeout=3)
            d2 = json.loads(r2.read().decode())
            file_count = len(d2.get("FileStatuses", {}).get("FileStatus", []))
        except Exception:
            pass
        return {"up": True, "files": file_count}
    except Exception:
        return {"up": False, "files": 0}


def _spark_probe() -> Dict[str, Any]:
    import urllib.request
    try:
        from src.common.config import get_settings
        s = get_settings()
        # Extract host from spark://spark-master:7077
        master_host = s.spark_master.replace("spark://", "").split(":")[0]
        if master_host in ("local", "local[*]"):
            master_host = "spark-master"
        url = f"http://{master_host}:8080/json/"
        req = urllib.request.Request(url, method="GET")
        resp = urllib.request.urlopen(req, timeout=3)
        data = json.loads(resp.read().decode())
        alive_workers = sum(1 for w in data.get("workers", []) if w.get("state") == "ALIVE")
        return {
            "up": True,
            "workers": alive_workers,
            "active_apps": len(data.get("activeapps", [])),
            "completed_apps": len(data.get("completedapps", [])),
        }
    except Exception:
        return {"up": False, "workers": 0, "active_apps": 0, "completed_apps": 0}


def _run_probes_once() -> Dict[str, Any]:
    hbase = _hbase_connected()
    kafka = _kafka_reachable()
    spark = _spark_probe()
    hdfs = _hdfs_probe()
    snapshot = {
        "hbase": hbase,
        "kafka": kafka,
        "spark": spark["up"],
        "hdfs": hdfs["up"],
        "spark_workers": spark["workers"],
        "spark_completed_apps": spark.get("completed_apps", 0),
        "hdfs_files": hdfs["files"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with _probe_lock:
        _probe_cache.update(snapshot)
    return snapshot


def _probe_loop() -> None:
    while True:
        try:
            _run_probes_once()
        except Exception:
            pass
        time.sleep(20)


def _ensure_probes() -> Dict[str, Any]:
    global _probe_started
    if not _probe_started:
        with _probe_lock:
            if not _probe_started:
                _probe_started = True
                # Run initial probe lazily without blocking TestClient unnecessarily
                t = threading.Thread(target=_probe_loop, daemon=True, name="health-probe")
                t.start()
        _run_probes_once()
    with _probe_lock:
        return dict(_probe_cache)


def _load_real_ingestion_metrics() -> Dict[str, Any]:
    """Load real metrics from ingestion_metrics.json first, falling back to CheckpointDB."""
    metrics_file = Path(os.environ.get("GDELT_METRICS_FILE", "data/metrics/ingestion_metrics.json"))
    if metrics_file.exists():
        try:
            data = json.loads(metrics_file.read_text(encoding="utf-8"))
            if data:
                return data
        except Exception:
            pass

    db_path = Path(os.environ.get("GDELT_CHECKPOINT_DB", "data/checkpoints/ingestion.db"))
    if db_path.exists():
        try:
            from src.ingestion.checkpoint import CheckpointDB
            db = CheckpointDB(db_path)
            return db.get_pipeline_metrics()
        except Exception:
            pass
    return {}


def _load_processing_metrics() -> Dict[str, Any]:
    proc_file = Path(os.environ.get("GDELT_PROCESSING_METRICS_FILE", "data/metrics/processing_metrics.json"))
    if proc_file.exists():
        try:
            return json.loads(proc_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


@router.get("/health")
def health_check():
    probes = _ensure_probes()

    components = {
        "kafka": {"status": "up" if probes.get("kafka") else "down"},
        "hbase": {"status": "up" if probes.get("hbase") else "down"},
        "hdfs": {"status": "up" if probes.get("hdfs") else "down"},
        "spark": {"status": "up" if probes.get("spark") else "down", "workers": probes.get("spark_workers", 0)},
    }

    all_up = all(c["status"] == "up" for c in components.values())
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
        "demo_mode": not probes.get("hbase", False),
    }


@router.get("/health/ready")
def readiness_check():
    return {"ready": True, "timestamp": datetime.now(timezone.utc).isoformat()}


@router.get("/health/pipeline")
def pipeline_health():
    metrics = _load_real_ingestion_metrics()
    proc = _load_processing_metrics()
    probes = _ensure_probes()

    last_ingestion = metrics.get("last_ingestion_time")
    freshness = "unknown"
    lag_minutes = metrics.get("pipeline_lag_minutes")
    if last_ingestion:
        try:
            last_dt = datetime.fromisoformat(last_ingestion.replace("Z", "+00:00"))
            age_minutes = max(0.0, (datetime.now(timezone.utc) - last_dt).total_seconds() / 60.0)
            lag_minutes = round(age_minutes, 1)
            if age_minutes < 20:
                freshness = "fresh"
            elif age_minutes < 60:
                freshness = "stale"
            else:
                freshness = "old"
        except Exception:
            pass

    return {
        "hbase_connected": probes.get("hbase", False),
        "last_ingestion_time": last_ingestion,
        "last_poll_time": metrics.get("last_poll_time"),
        "freshness_status": freshness,
        "files_processed_today": metrics.get("files_processed_today", 0),
        "files_week": metrics.get("files_week", 0),
        "records_accepted_today": metrics.get("records_accepted_today", 0),
        "records_rejected_today": metrics.get("records_rejected_today", 0),
        "total_errors": metrics.get("total_errors", 0),
        "breakdown": metrics.get("breakdown", {}),
        "ingestion_history": metrics.get("ingestion_history", []),
        "error_logs": metrics.get("error_logs", []),
        "kafka_status": "online" if probes.get("kafka") else "offline",
        "spark_status": "online" if probes.get("spark") else "offline",
        "spark_workers": probes.get("spark_workers", 0),
        "spark_completed_apps": probes.get("spark_completed_apps", 0),
        "hdfs_status": "online" if probes.get("hdfs") else "offline",
        "processing": proc,
        "pipeline_lag_minutes": lag_minutes,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
