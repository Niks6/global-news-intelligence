# Operations Runbook

## Service Startup Procedure

**Order matters.** Start in this sequence:

```
1. ZooKeeper (node2)     → kafka/bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
2. Kafka (node2)         → kafka/bin/kafka-server-start.sh -daemon config/server.properties
3. HDFS (node3)          → hadoop/sbin/start-dfs.sh
4. HBase (node3)         → hbase/bin/start-hbase.sh
5. HBase Thrift (node3)  → hbase/bin/hbase-daemon.sh start thrift
6. Spark Master (node2)  → spark/sbin/start-master.sh
7. Spark Worker (node2)  → spark/sbin/start-worker.sh spark://node2:7077
8. Spark Jobs (node2)    → bash src/processing/submit_jobs.sh
9. Backend (node1)       → uvicorn backend.main:app --host 0.0.0.0 --port 8000
10. Frontend (node1)     → cd frontend && npm run dev
11. Ingestion (node1)    → python -m src.ingestion.cli poll
```

## Service Shutdown Procedure

**Reverse order of startup to prevent data corruption:**

```
1. Ingestion poller      → Ctrl+C or kill PID
2. Frontend              → Ctrl+C
3. Backend               → Ctrl+C
4. Spark Jobs            → spark-submit --kill <app-id>
5. Spark Worker/Master   → spark/sbin/stop-worker.sh && spark/sbin/stop-master.sh
6. HBase Thrift          → hbase/bin/hbase-daemon.sh stop thrift
7. HBase                 → hbase/bin/stop-hbase.sh   (wait for graceful stop)
8. HDFS                  → hadoop/sbin/stop-dfs.sh
9. Kafka                 → kafka/bin/kafka-server-stop.sh
10. ZooKeeper            → kafka/bin/zookeeper-server-stop.sh
```

> **Warning:** Killing HBase or HDFS without graceful stop may corrupt data. Always use the stop scripts.

## Health Checks

```bash
# API health
curl http://node1:8000/api/health
curl http://node1:8000/api/health/pipeline

# ZooKeeper
echo ruok | nc node2 2181

# Kafka — list topics
kafka-topics.sh --list --bootstrap-server node2:9092
# Kafka — consumer lag
kafka-consumer-groups.sh --describe --group spark-events-consumer --bootstrap-server node2:9092

# HDFS
hdfs dfs -df -h
hdfs dfsadmin -report

# HBase
echo "status 'detailed'" | hbase shell

# Spark
curl http://node2:8080/json/   # Spark Master API
```

## Troubleshooting

### Kafka consumer lag too high
- Check Spark streaming job is running: `curl http://node2:4040/api/v1/applications`
- Check Spark logs: `$SPARK_HOME/logs/`
- If job died, restart: `bash src/processing/submit_jobs.sh`

### HDFS out of space
- Check usage: `hdfs dfs -du -h /gdelt/`
- Delete old raw data: `hdfs dfs -rm -r /gdelt/raw/events/date=2026-01-01/`
- Check DataNode reports: `hdfs dfsadmin -report`

### HBase RegionServer crash
- Check logs: `$HBASE_HOME/logs/hbase-*-regionserver-*.log`
- Restart RegionServer: `hbase-daemon.sh start regionserver`
- If master crashed too: `hbase-daemon.sh start master`

### Spark streaming job stopped
- Check Spark UI: `http://node2:4040`
- Check driver logs in `$SPARK_HOME/logs/` or `$SPARK_HOME/work/`
- Common cause: OutOfMemoryError → reduce `spark.executor.memory` or increase parallelism
- Restart: `bash src/processing/submit_jobs.sh` (resumes from checkpoint)

### Ingestion worker failed
- Check logs at stdout or configure log file
- Run `python -m src.ingestion.cli status` to see checkpoint stats
- Retry failed files: `python -m src.ingestion.cli replay data/raw/FILENAME.zip`
- Reset checkpoint (WARNING: will reprocess all files):
  ```bash
  rm data/checkpoints/ingestion.db  # deletes ALL checkpoint state
  ```

## Log File Locations

| Component | Log Location |
|-----------|-------------|
| Ingestion | stdout / `data/logs/ingestion.log` (if configured) |
| FastAPI | stdout |
| Kafka | `$KAFKA_HOME/logs/` |
| Spark | `$SPARK_HOME/logs/` and `$SPARK_HOME/work/` |
| HDFS | `$HADOOP_HOME/logs/` |
| HBase | `$HBASE_HOME/logs/` |

## Replay / Reprocess

Replay a specific file through the pipeline:
```bash
python -m src.ingestion.cli replay data/raw/20261114120000.export.CSV.zip --file-type events
```

## Backup

- **HDFS:** Use `hdfs dfs -copyToLocal /gdelt/curated/ /backup/` for important curated data
- **HBase:** Export tables: `hbase org.apache.hadoop.hbase.mapreduce.Export events_by_id /backup/hbase/events`
- **Checkpoint DB:** Copy `data/checkpoints/ingestion.db`
- **Kafka:** Data is ephemeral (7-day retention); raw files on disk are the source of truth

