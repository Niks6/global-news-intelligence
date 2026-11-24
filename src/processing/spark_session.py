"""SparkSession factory for the Global News Intelligence Platform.

Creates a SparkSession configured for:
  - Kafka Structured Streaming (spark-sql-kafka connector)
  - HBase (via SHC or direct RDD)
  - Conservative memory settings suitable for 3×8 GB cluster nodes
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Default package coordinates — override via environment variables
_KAFKA_PKG = os.environ.get(
    "SPARK_KAFKA_PKG",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
)
_HBASE_PKG = os.environ.get(
    "SPARK_HBASE_PKG",
    "org.apache.hbase:hbase-client:2.5.7,"
    "org.apache.hbase:hbase-common:2.5.7,"
    "org.apache.hbase:hbase-mapreduce:2.5.7",
)
_HADOOP_AWS_PKG = os.environ.get(
    "SPARK_HADOOP_AWS_PKG",
    "",  # Unused on bare HDFS; can be set for S3-compatible storage
)


def create_spark_session(
    app_name: str = "GDELTProcessing",
    master: Optional[str] = None,
    driver_memory: str = "2g",
    executor_memory: str = "2g",
    executor_cores: int = 2,
    default_parallelism: int = 6,
    shuffle_partitions: int = 6,
    extra_packages: Optional[str] = None,
    extra_configs: Optional[dict] = None,
):
    """Create and return a configured SparkSession.

    Args:
        app_name: Spark application name shown in the Web UI.
        master: Spark master URL (defaults to SPARK_MASTER env var or "local[*]").
        driver_memory: Driver heap size (e.g. "2g").
        executor_memory: Executor heap size per executor.
        executor_cores: vCPUs per executor.
        default_parallelism: RDD default parallelism (≈ total vcores in cluster).
        shuffle_partitions: Number of shuffle partitions (keep low for small cluster).
        extra_packages: Additional Maven coordinates to append to --packages.
        extra_configs: Additional spark config key→value pairs.

    Returns:
        pyspark.sql.SparkSession
    """
    try:
        from pyspark.sql import SparkSession  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "PySpark not installed. Install it with: pip install pyspark==3.5.1"
        ) from exc

    spark_master = master or os.environ.get("SPARK_MASTER", "local[*]")
    hdfs_namenode = os.environ.get("HDFS_NAMENODE", os.environ.get("HDFS_NAMENODE_URL", "hdfs://namenode:9000"))
    hbase_zk_quorum = os.environ.get("HBASE_ZK_QUORUM", "localhost")

    # Build package list
    packages = [_KAFKA_PKG]
    if _HADOOP_AWS_PKG:
        packages.append(_HADOOP_AWS_PKG)
    if extra_packages:
        packages.append(extra_packages)
    packages_str = ",".join(p for p in packages if p)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        # --- Memory (conservative for 8 GB nodes) ---
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", str(executor_cores))
        .config("spark.default.parallelism", str(default_parallelism))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # --- Kafka connector ---
        .config("spark.jars.packages", packages_str)
        # --- Streaming micro-batch ---
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation", f"{hdfs_namenode}/gdelt/checkpoints")
        # --- Serialization ---
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # --- Off-heap / memory management ---
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.5")
        # --- Hadoop HDFS settings ---
        .config("spark.hadoop.fs.defaultFS", hdfs_namenode)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        # --- HBase settings passed through Hadoop conf ---
        .config("spark.hadoop.hbase.zookeeper.quorum", hbase_zk_quorum)
        .config("spark.hadoop.hbase.zookeeper.property.clientPort", "2181")
        # --- Adaptive query execution (Spark 3+) ---
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, str(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    logger.info(
        "SparkSession created: app=%s master=%s driver=%s executor=%s×%sc",
        app_name,
        spark_master,
        driver_memory,
        executor_memory,
        executor_cores,
    )
    return spark


def get_kafka_read_stream(
    spark,
    bootstrap_servers: Optional[str] = None,
    topic: str = "gdelt.events.raw",
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int = 10_000,
):
    """Return a streaming DataFrame reading from a Kafka topic.

    Args:
        spark: Active SparkSession.
        bootstrap_servers: Kafka bootstrap server list. Defaults to KAFKA_BOOTSTRAP_SERVERS env.
        topic: Kafka topic name.
        starting_offsets: "latest" or "earliest" or JSON offset spec.
        max_offsets_per_trigger: Throttle to avoid overwhelming small executors.
    """
    brokers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
        .option("kafka.group.id", f"spark-{topic}-consumer")
        .option("failOnDataLoss", "false")
        .load()
    )
