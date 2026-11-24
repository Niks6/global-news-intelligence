"""
Lightweight Spark Pipeline Supervisor.

Watches the Kafka raw topics (`gdelt.events.raw`, `gdelt.mentions.raw`,
`gdelt.gkg.raw`) for new messages and submits `src/processing/spark_pipeline.py`
to the distributed Spark Master (`spark://spark-master:7077`) whenever new
data arrives (or on startup if unread offsets exist).

When Spark finishes a batch, the JVM exits cleanly so memory is returned to the
cluster between 15-minute GDELT drops.
"""
from __future__ import annotations

import logging
import os
import socket
import subprocess
import sys
import time
from typing import Dict

from confluent_kafka import Consumer, TopicPartition  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] spark-runner — %(message)s",
)
logger = logging.getLogger("spark_runner")

TOPICS = ["gdelt.events.raw", "gdelt.mentions.raw", "gdelt.gkg.raw"]


def _all_brokers_reachable(brokers: str) -> bool:
    for b in brokers.split(","):
        try:
            h, p = b.strip().split(":")
            s = socket.create_connection((h, int(p)), timeout=3)
            s.close()
        except Exception:
            return False
    return True


def _topic_high_watermarks(brokers: str) -> Dict[str, int]:
    """Return total message high-watermark summed across partitions per topic."""
    totals: Dict[str, int] = {}
    conf = {
        "bootstrap.servers": brokers,
        "group.id": "gnip-spark-watermark-probe",
        "enable.auto.commit": False,
        "socket.timeout.ms": 5000,
    }
    consumer = Consumer(conf)
    try:
        md = consumer.list_topics(timeout=10)
        for topic in TOPICS:
            t_meta = md.topics.get(topic)
            if not t_meta:
                totals[topic] = 0
                continue
            total = 0
            for pid in t_meta.partitions.keys():
                try:
                    _lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=5)
                    if hi > 0:
                        total += hi
                except Exception:
                    pass
            totals[topic] = total
    finally:
        consumer.close()
    return totals


def _submit_spark_job() -> int:
    spark_master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
    ivy_dir = os.environ.get("SPARK_IVY_DIR", "/tmp/ivy")
    kafka_pkg = os.environ.get("SPARK_KAFKA_PKG", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "500m")
    exec_mem = os.environ.get("SPARK_EXECUTOR_MEMORY", "500m")
    cores_max = os.environ.get("SPARK_CORES_MAX", "1")

    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", spark_master,
        "--deploy-mode", "client",
        "--driver-memory", driver_mem,
        "--executor-memory", exec_mem,
        "--total-executor-cores", cores_max,
        "--packages", kafka_pkg,
        "--conf", f"spark.jars.ivy={ivy_dir}",
        "--conf", "spark.ui.enabled=false",
        "/app/src/processing/spark_pipeline.py",
    ]
    logger.info("Submitting Spark pipeline: %s", " ".join(cmd))
    proc = subprocess.run(cmd, check=False)
    logger.info("Spark job exited with code %d", proc.returncode)
    return proc.returncode


def main() -> None:
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
    poll_secs = int(os.environ.get("SPARK_POLL_SECONDS", "30"))
    last_watermarks: Dict[str, int] = {}

    logger.info("Spark pipeline runner started (brokers=%s, poll=%ds)", brokers, poll_secs)

    while True:
        try:
            if not _all_brokers_reachable(brokers):
                logger.info("Waiting for all Kafka brokers to be reachable...")
                time.sleep(10)
                continue
            current = _topic_high_watermarks(brokers)
            has_data = sum(current.values()) > 0
            changed = current != last_watermarks

            if has_data and changed:
                logger.info("Kafka offsets updated (%s) — launching Spark job", current)
                rc = _submit_spark_job()
                if rc == 0:
                    last_watermarks = current
                else:
                    logger.warning("Spark job returned %d — will retry next cycle", rc)
            else:
                logger.debug("No new Kafka messages (offsets=%s)", current)
        except Exception as exc:
            logger.error("Error in Spark runner loop: %s", exc)

        time.sleep(poll_secs)


if __name__ == "__main__":
    main()
