#!/usr/bin/env bash
# Health check script for the Global News Intelligence platform
# Usage: ./scripts/check-health.sh [HOST] [PORT]

set -euo pipefail

HOST="${1:-localhost}"
API_PORT="${2:-8000}"
FRONTEND_PORT="${3:-5173}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $1"; }

echo "============================================"
echo "  Global News Intelligence - Health Check"
echo "  Host: $HOST"
echo "============================================"
echo ""

TOTAL=0
PASSED=0

# --- Check FastAPI Backend ---
echo "🔧 Backend API (port $API_PORT)"
TOTAL=$((TOTAL + 1))
if curl -sf "http://$HOST:$API_PORT/api/health" > /dev/null 2>&1; then
    pass "FastAPI backend is running"
    PASSED=$((PASSED + 1))

    # Detailed component check
    HEALTH_JSON=$(curl -sf "http://$HOST:$API_PORT/api/health")
    STATUS=$(echo "$HEALTH_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','unknown'))" 2>/dev/null || echo "unknown")
    if [ "$STATUS" = "healthy" ]; then
        pass "Overall status: $STATUS"
    elif [ "$STATUS" = "degraded" ]; then
        warn "Overall status: $STATUS (some components unavailable)"
    else
        fail "Overall status: $STATUS"
    fi
else
    fail "FastAPI backend is NOT running on port $API_PORT"
fi

# --- Check Frontend ---
echo ""
echo "🌐 Frontend (port $FRONTEND_PORT)"
TOTAL=$((TOTAL + 1))
if curl -sf "http://$HOST:$FRONTEND_PORT" > /dev/null 2>&1; then
    pass "Frontend dev server is running"
    PASSED=$((PASSED + 1))
else
    fail "Frontend is NOT running on port $FRONTEND_PORT"
fi

# --- Check Kafka ---
echo ""
echo "📨 Apache Kafka"
TOTAL=$((TOTAL + 1))
KAFKA_PORT="${KAFKA_PORT:-9092}"
if (echo > /dev/tcp/$HOST/$KAFKA_PORT) 2>/dev/null; then
    pass "Kafka broker is reachable on port $KAFKA_PORT"
    PASSED=$((PASSED + 1))
else
    fail "Kafka is NOT reachable on port $KAFKA_PORT"
fi

# --- Check HBase Thrift ---
echo ""
echo "🗄️  Apache HBase (Thrift)"
TOTAL=$((TOTAL + 1))
HBASE_PORT="${HBASE_THRIFT_PORT:-9090}"
if (echo > /dev/tcp/$HOST/$HBASE_PORT) 2>/dev/null; then
    pass "HBase Thrift is reachable on port $HBASE_PORT"
    PASSED=$((PASSED + 1))
else
    fail "HBase Thrift is NOT reachable on port $HBASE_PORT"
fi

# --- Check HDFS NameNode ---
echo ""
echo "📁 HDFS NameNode"
TOTAL=$((TOTAL + 1))
HDFS_PORT="${HDFS_WEB_PORT:-9870}"
if curl -sf "http://$HOST:$HDFS_PORT" > /dev/null 2>&1; then
    pass "HDFS NameNode web UI is reachable on port $HDFS_PORT"
    PASSED=$((PASSED + 1))
else
    fail "HDFS NameNode is NOT reachable on port $HDFS_PORT"
fi

# --- Summary ---
echo ""
echo "============================================"
echo "  Results: $PASSED / $TOTAL checks passed"
echo "============================================"

if [ "$PASSED" -eq "$TOTAL" ]; then
    echo -e "  ${GREEN}All systems operational!${NC}"
    exit 0
elif [ "$PASSED" -gt 0 ]; then
    echo -e "  ${YELLOW}Some services are down.${NC}"
    exit 1
else
    echo -e "  ${RED}All services are down!${NC}"
    exit 2
fi
