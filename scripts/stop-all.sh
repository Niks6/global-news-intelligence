#!/bin/bash
# Stop all services safely — reverse order of start.
# Usage: ./scripts/stop-all.sh {storage|processing|ingestion|all}

ROLE="${1:-all}"

echo "=== Global News Intelligence — Stopping role: $ROLE ==="

stop_ingestion() {
  echo "Stopping ingestion poller..."
  pkill -f "src.ingestion.cli" 2>/dev/null || true
  echo "Stopping FastAPI backend..."
  pkill -f "uvicorn backend.main" 2>/dev/null || true
}

stop_processing() {
  echo "Stopping Spark streaming jobs..."
  # Cancel running Spark apps via spark-submit --kill
  pkill -f "spark-submit" 2>/dev/null || true
  sleep 2

  echo "Stopping Spark worker + master..."
  $SPARK_HOME/sbin/stop-worker.sh 2>/dev/null || true
  $SPARK_HOME/sbin/stop-master.sh 2>/dev/null || true

  echo "Stopping Kafka broker..."
  $KAFKA_HOME/bin/kafka-server-stop.sh 2>/dev/null || true
}

stop_storage() {
  echo "Stopping HBase Thrift server..."
  $HBASE_HOME/bin/hbase-daemon.sh stop thrift 2>/dev/null || true

  echo "Stopping HBase..."
  $HBASE_HOME/bin/stop-hbase.sh 2>/dev/null || true
  sleep 3

  echo "Stopping HDFS..."
  $HADOOP_HOME/sbin/stop-dfs.sh 2>/dev/null || true

  echo "Stopping ZooKeeper..."
  $HBASE_HOME/bin/hbase-daemon.sh stop zookeeper 2>/dev/null || true
  $KAFKA_HOME/bin/zookeeper-server-stop.sh 2>/dev/null || true
}

case "$ROLE" in
  ingestion)  stop_ingestion ;;
  processing) stop_processing ;;
  storage)    stop_storage ;;
  all)
    stop_ingestion
    stop_processing
    stop_storage
    ;;
  *)
    echo "Usage: $0 {storage|processing|ingestion|all}"
    exit 1
    ;;
esac

echo "=== Stopped ==="

