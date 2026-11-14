#!/bin/bash
# Submit all Spark Structured Streaming jobs.
# Run from Machine 2 (processing node).
# Prerequisites: Kafka topics created, HDFS running, HBase running.

SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
CHECKPOINT_DIR="/gdelt/checkpoints"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "Submitting Spark streaming jobs..."
echo "  Kafka: $KAFKA_BOOTSTRAP"
echo "  Checkpoints: $CHECKPOINT_DIR"

# Events stream
echo ""
echo "[1/3] Submitting events_stream..."
$SPARK_SUBMIT \
  --master spark://$(hostname):7077 \
  --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.sql.streaming.checkpointLocation=$CHECKPOINT_DIR/events_stream \
  "$PROJECT_DIR/src/processing/streams/events_stream.py" &

sleep 5

# Mentions stream
echo "[2/3] Submitting mentions_stream..."
$SPARK_SUBMIT \
  --master spark://$(hostname):7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.sql.streaming.checkpointLocation=$CHECKPOINT_DIR/mentions_stream \
  "$PROJECT_DIR/src/processing/streams/mentions_stream.py" &

sleep 5

# GKG stream
echo "[3/3] Submitting gkg_stream..."
$SPARK_SUBMIT \
  --master spark://$(hostname):7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.sql.streaming.checkpointLocation=$CHECKPOINT_DIR/gkg_stream \
  "$PROJECT_DIR/src/processing/streams/gkg_stream.py" &

echo ""
echo "All streaming jobs submitted. Check Spark UI: http://$(hostname):4040"
echo "PIDs: $(jobs -p)"
wait

