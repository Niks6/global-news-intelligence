"""HBase DDL helper — creates all tables for the Global News Intelligence Platform.

Usage (standalone):
    python -m src.storage.hbase_schema --host hbase-master --port 9090
    python -m src.storage.hbase_schema --host hbase-master --drop-recreate
"""

import argparse
import logging
import sys

from .hbase_client import HBaseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def create_all_tables(
    host: str = "localhost",
    port: int = 9090,
    drop_if_exists: bool = False,
) -> None:
    """Create (or recreate) all HBase tables.

    Args:
        host: HBase Thrift server hostname or IP.
        port: HBase Thrift port (default 9090).
        drop_if_exists: When True, disables and drops existing tables before
                        recreating them.  Use only in development environments.
    """
    client = HBaseClient(host=host, port=port, auto_connect=True)
    try:
        client.create_tables(drop_if_exists=drop_if_exists)
        logger.info("All HBase tables are ready.")
    finally:
        client.close()


def table_exists(client: HBaseClient, table_name: str) -> bool:
    """Return True if *table_name* already exists in HBase."""
    existing = {t.decode() for t in client._conn().tables()}
    return table_name in existing


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HBase DDL: create platform tables")
    parser.add_argument("--host", default="localhost", help="HBase Thrift host")
    parser.add_argument("--port", type=int, default=9090, help="HBase Thrift port")
    parser.add_argument(
        "--drop-recreate",
        action="store_true",
        default=False,
        help="Drop existing tables and recreate (development only)",
    )
    args = parser.parse_args()

    if args.drop_recreate:
        confirm = input(
            "WARNING: --drop-recreate will DELETE all data in existing tables. "
            "Type 'yes' to continue: "
        )
        if confirm.strip().lower() != "yes":
            print("Aborted.")
            sys.exit(0)

    create_all_tables(host=args.host, port=args.port, drop_if_exists=args.drop_recreate)
