"""Storage layer for the Global News Intelligence Platform.

Provides clients for HBase (happybase) and HDFS (pyarrow / hdfs).
"""

from .hbase_client import HBaseClient
from .hdfs_client import HDFSClient

__all__ = ["HBaseClient", "HDFSClient"]
