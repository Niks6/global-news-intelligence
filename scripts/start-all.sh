#!/bin/bash
# Start all services for the Global News Intelligence Platform.
# Run from the project root. Adjust paths for your cluster node.
#
# Machine 1 (Ingestion):  ./scripts/start-all.sh ingestion
# Machine 2 (Processing): ./scripts/start-all.sh processing
# Machine 3 (Storage):    ./scripts/start-all.sh storage
# All-in-one dev:         ./scripts/start-all.sh dev

set -e
ROLE="${1:-dev}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== Global News Intelligence — Starting role: $ROLE ==="

case "$ROLE" in
  storage)
    echo "[1/4] Starting ZooKeeper..."
    $HBASE_HOME/bin/hbase-daemon.sh start zookeeper 2>/dev/null || \
      $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
    sleep 5

    echo "[2/4] Starting HDFS NameNode + DataNode..."
    $HADOOP_HOME/sbin/start-dfs.sh
    sleep 5

    echo "[3/4] Starting HBase Master + RegionServer..."
    $HBASE_HOME/bin/start-hbase.sh
    sleep 5

    echo "[4/4] Starting HBase Thrift server (for Python happybase client)..."
    $HBASE_HOME/bin/hbase-daemon.sh start thrift

    echo "Storage node started. Verify:"
    echo "  HDFS:  http://$(hostname):9870"
    echo "  HBase: http://$(hostname):16010"
    ;;

  processing)
    echo "[1/2] Starting Kafka broker..."
    $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
    sleep 5

    echo "[2/2] Starting Spark master + worker..."
    $SPARK_HOME/sbin/start-master.sh
    sleep 3
    $SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077

    echo "Processing node started. Verify:"
    echo "  Kafka:  port 9092"
    echo "  Spark:  http://$(hostname):8080"
    ;;

  ingestion)
    echo "Starting GDELT ingestion poller..."
    cd "$PROJECT_DIR"
    python -m src.ingestion.cli poll &
    echo "Ingestion poller started (PID: $!)."

    echo "Starting FastAPI backend..."
    uvicorn backend.main:app --host 0.0.0.0 --port 8000 &
    echo "Backend started (PID: $!)."
    ;;

  dev)
    echo "Starting local development mode..."
    echo "  Backend: http://localhost:8000"
    echo "  Frontend: http://localhost:5173"
    echo ""
    echo "Start in separate terminals:"
    echo "  Terminal 1: uvicorn backend.main:app --reload --port 8000"
    echo "  Terminal 2: cd frontend && npm run dev"
    echo "  Terminal 3: python -m src.ingestion.cli poll --dry-run"
    ;;

  *)
    echo "Usage: $0 {storage|processing|ingestion|dev}"
    exit 1
    ;;
esac

echo ""
echo "=== Done ==="

