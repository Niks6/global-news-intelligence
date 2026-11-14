# Three-Node Cluster Setup Guide

## Network Overview

| Machine | Role | Services |
|---------|------|----------|
| Machine 1 (node1) | Ingestion | GDELT Poller, FastAPI Backend, React Frontend |
| Machine 2 (node2) | Processing | Kafka Broker, ZooKeeper, Spark Master+Worker |
| Machine 3 (node3) | Storage | HDFS NameNode+DataNode, HBase Master+RegionServer, HBase Thrift |

> **Note:** Each machine has ~8 GB RAM. JVM heaps are configured conservatively.

## 1. Find Each Machine's LAN IP

**Ubuntu:**
```bash
ip addr show | grep "inet " | grep -v 127.0.0.1
# Look for something like 192.168.1.100/24
```

**Windows (PowerShell):**
```powershell
Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notlike "*Loopback*" } | Select IPAddress
# Or: ipconfig | findstr "IPv4"
```

## 2. Configure Hostnames

Add these to `/etc/hosts` (Ubuntu) or `C:\Windows\System32\drivers\etc\hosts` (Windows, edit as Admin):

```
192.168.1.100  node1
192.168.1.101  node2
192.168.1.102  node3
```

Replace IPs with your actual LAN IPs. All 3 machines need ALL 3 entries.

## 3. Verify Connectivity

From each machine, ping the other two:
```bash
ping node1
ping node2
ping node3
```

If pings fail, check:
- All machines on the **same Wi-Fi network**
- **Wi-Fi client isolation** is DISABLED on your router (check router admin → Wireless → AP Isolation / Client Isolation → OFF)
- Windows Firewall allows ICMP (ping): `netsh advfirewall firewall add rule name="Allow Ping" dir=in action=allow protocol=icmpv4`

## 4. Firewall Rules

Open these ports on each machine:

| Port | Service | Machine |
|------|---------|---------|
| 2181 | ZooKeeper | node2 |
| 9092 | Kafka | node2 |
| 7077 | Spark Master | node2 |
| 8080 | Spark Web UI | node2 |
| 4040 | Spark App UI | node2 |
| 9000 | HDFS NameNode RPC | node3 |
| 9870 | HDFS Web UI | node3 |
| 16000 | HBase Master | node3 |
| 16010 | HBase Master Web UI | node3 |
| 16020 | HBase RegionServer | node3 |
| 9090 | HBase Thrift | node3 |
| 8000 | FastAPI Backend | node1 |
| 3000/5173 | React Frontend | node1 |

**Ubuntu:**
```bash
sudo ufw allow 9092/tcp  # example for Kafka
sudo ufw allow 9000/tcp
sudo ufw allow 9870/tcp
# etc.
```

**Windows:**
```powershell
New-NetFirewallRule -DisplayName "Kafka" -Direction Inbound -Protocol TCP -LocalPort 9092 -Action Allow
```

## 5. Software Versions (pin these exactly)

| Software | Version | Notes |
|----------|---------|-------|
| Java | OpenJDK 11 | Required by Hadoop, HBase, Kafka, Spark |
| Hadoop | 3.3.6 | HDFS only (no YARN needed for this project) |
| HBase | 2.5.7 | Must match Hadoop version |
| Kafka | 3.6.1 (Scala 2.12) | Single broker for academic use |
| Spark | 3.5.1 (Scala 2.12) | Structured Streaming |
| Python | 3.11 | For ingestion, FastAPI |
| Node.js | 20 LTS | For React frontend |

## 6. Kafka Configuration (Machine 2)

Edit `$KAFKA_HOME/config/server.properties`:
```properties
# CRITICAL: set to Machine 2's actual LAN IP
advertised.listeners=PLAINTEXT://192.168.1.101:9092
listeners=PLAINTEXT://0.0.0.0:9092
```

If `advertised.listeners` is wrong, remote machines cannot produce/consume messages.

## 7. HDFS Configuration (Machine 3)

Copy templates from `infrastructure/hadoop/` to `$HADOOP_HOME/etc/hadoop/`:
- Replace `<NAMENODE_HOST>` with `node3` (or its IP) in `core-site.xml` and `hdfs-site.xml`
- Format NameNode (first time only): `hdfs namenode -format`

## 8. HBase Configuration (Machine 3)

Copy template from `infrastructure/hbase/` to `$HBASE_HOME/conf/`:
- Set `hbase.zookeeper.quorum` to `node2` (or wherever ZooKeeper runs)
- Set `hbase.rootdir` to `hdfs://node3:9000/hbase`
- Start HBase Thrift for Python client: `hbase-daemon.sh start thrift`

## 9. Service Startup Order

**Start in this exact order:**

1. **Machine 2:** ZooKeeper → `$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon config/zookeeper.properties`
2. **Machine 2:** Kafka → `$KAFKA_HOME/bin/kafka-server-start.sh -daemon config/server.properties`
3. **Machine 3:** HDFS → `$HADOOP_HOME/sbin/start-dfs.sh`
4. **Machine 3:** HBase → `$HBASE_HOME/bin/start-hbase.sh`
5. **Machine 3:** HBase Thrift → `$HBASE_HOME/bin/hbase-daemon.sh start thrift`
6. **Machine 2:** Spark → `$SPARK_HOME/sbin/start-master.sh && $SPARK_HOME/sbin/start-worker.sh spark://node2:7077`
7. **Machine 2:** Submit Spark jobs → `bash src/processing/submit_jobs.sh`
8. **Machine 1:** Backend → `uvicorn backend.main:app --host 0.0.0.0 --port 8000`
9. **Machine 1:** Frontend → `cd frontend && npm run dev`
10. **Machine 1:** Ingestion → `python -m src.ingestion.cli poll`

## 10. Health Verification

```bash
# ZooKeeper (Machine 2)
echo ruok | nc node2 2181    # should respond "imok"

# Kafka (from any machine)
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server node2:9092

# HDFS (from any machine)
hdfs dfs -ls /
curl http://node3:9870     # Web UI

# HBase (Machine 3)
echo "status" | hbase shell
curl http://node3:16010     # Web UI

# Spark (from any machine)
curl http://node2:8080      # Web UI

# Backend API (from any machine)
curl http://node1:8000/api/health
```

## 11. WSL2 Networking Caveats

If any machine runs Windows with WSL2:

- WSL2 uses a **virtual NAT network** — its IP is different from the Windows host IP
- Other machines on the LAN cannot reach WSL2 services directly
- **Recommended:** Use native Ubuntu for cluster nodes (Machine 2 and 3)
- **Workaround:** Set up port forwarding: `netsh interface portproxy add v4tov4 listenport=9092 listenaddress=0.0.0.0 connectport=9092 connectaddress=$(wsl hostname -I)`
- WSL2 IP changes on every restart — unreliable for a persistent cluster

## 12. Memory Tuning (8 GB machines)

| Machine | Service | Heap |
|---------|---------|------|
| node2 | ZooKeeper | 256 MB |
| node2 | Kafka | 1 GB (`KAFKA_HEAP_OPTS="-Xmx1g"`) |
| node2 | Spark Master | 512 MB |
| node2 | Spark Worker/Executor | 2 GB |
| node3 | HDFS NameNode | 512 MB |
| node3 | HDFS DataNode | 512 MB |
| node3 | HBase Master | 512 MB |
| node3 | HBase RegionServer | 1 GB |
| node3 | HBase Thrift | 256 MB |

Total per machine stays under 5 GB, leaving ~3 GB for OS and other processes.

