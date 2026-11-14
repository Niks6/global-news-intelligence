"""
Centralized configuration using pydantic-settings.
All settings read from environment variables or .env file.
"""
from __future__ import annotations
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------ Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "gdelt-consumers"
    kafka_topic_events_raw: str = "gdelt.events.raw"
    kafka_topic_mentions_raw: str = "gdelt.mentions.raw"
    kafka_topic_gkg_raw: str = "gdelt.gkg.raw"
    kafka_topic_events_normalized: str = "gdelt.events.normalized"
    kafka_topic_analytics: str = "gdelt.analytics"
    kafka_topic_dead_letter: str = "gdelt.dead-letter"

    # ------------------------------------------------------------------ HDFS
    hdfs_namenode_host: str = "node3"
    hdfs_namenode_port: int = 9000
    hdfs_namenode_url: str = "hdfs://node3:9000"
    hdfs_webhdfs_url: str = "http://node3:9870"
    hdfs_user: str = "hadoop"
    hdfs_base_path: str = "/gdelt"
    hdfs_replication_factor: int = 2

    # ------------------------------------------------------------------ HBase
    hbase_host: str = "node3"
    hbase_thrift_port: int = 9090
    hbase_zookeeper_quorum: str = "node3"
    hbase_zookeeper_port: int = 2181

    # ------------------------------------------------------------------ Spark
    spark_master: str = "spark://node2:7077"
    spark_driver_memory: str = "2g"
    spark_executor_memory: str = "2g"
    spark_executor_cores: int = 2
    spark_default_parallelism: int = 6
    spark_checkpoint_dir: str = "/gdelt/checkpoints"

    # ------------------------------------------------------------------ Ingestion
    poll_interval_minutes: int = 15
    gdelt_download_dir: str = "data/raw"
    gdelt_checkpoint_db: str = "data/checkpoints/ingestion.db"
    gdelt_dead_letter_dir: str = "data/dead_letter"
    gdelt_dry_run: bool = False
    max_download_retries: int = 5
    download_timeout_secs: int = 120
    doc_api_rate_limit_rpm: int = 1  # 1 request per minute for DOC API

    # ------------------------------------------------------------------ Backend / API
    backend_host: str = "0.0.0.0"
    backend_port: int = 8000
    frontend_url: str = "http://localhost:3000"
    api_max_page_size: int = 500
    api_default_page_size: int = 50
    api_max_time_range_days: int = 90
    api_default_time_range_days: int = 7

    # ------------------------------------------------------------------ Cluster nodes
    node1_host: str = "node1"  # Ingestion machine
    node2_host: str = "node2"  # Kafka + Spark
    node3_host: str = "node3"  # HDFS + HBase

    # ------------------------------------------------------------------ Geocoding
    geocode_cache_db: str = "data/cache/geocode_cache.db"
    nominatim_user_agent: str = "global-news-intelligence/1.0"
    nominatim_rate_limit_rps: float = 1.0

    # ------------------------------------------------------------------ Logging
    log_level: str = "INFO"
    log_format: str = "json"

    @property
    def hdfs_raw_events(self) -> str:
        return f"{self.hdfs_base_path}/raw/events"

    @property
    def hdfs_raw_mentions(self) -> str:
        return f"{self.hdfs_base_path}/raw/mentions"

    @property
    def hdfs_raw_gkg(self) -> str:
        return f"{self.hdfs_base_path}/raw/gkg"

    @property
    def hdfs_curated_events(self) -> str:
        return f"{self.hdfs_base_path}/curated/events"

    @property
    def hdfs_analytics(self) -> str:
        return f"{self.hdfs_base_path}/analytics"


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings

