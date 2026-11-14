#!/bin/bash
# Create all required Kafka topics for the GDELT pipeline
# Usage: ./create-topics.sh [bootstrap-server]
#
# Default bootstrap server: localhost:9092
# Topics: 3 partitions, replication-factor 1 (academic cluster)

BOOTSTRAP="${1:-localhost:9092}"
KAFKA_BIN="${KAFKA_HOME:-/opt/kafka}/bin"

echo "Creating Kafka topics on $BOOTSTRAP..."

TOPICS=(
  "gdelt.events.raw"
  "gdelt.mentions.raw"
  "gdelt.gkg.raw"
  "gdelt.events.normalized"
  "gdelt.analytics"
  "gdelt.dead-letter"
)

for TOPIC in "${TOPICS[@]}"; do
  echo "  Creating topic: $TOPIC"
  "$KAFKA_BIN/kafka-topics.sh" \
    --create \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete
done

echo ""
echo "Listing all topics:"
"$KAFKA_BIN/kafka-topics.sh" --list --bootstrap-server "$BOOTSTRAP"
echo ""
echo "Done."

