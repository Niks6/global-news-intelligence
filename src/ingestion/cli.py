"""
GDELT Ingestion CLI.
Provides commands for one-shot ingestion, continuous polling, status, and replay.
"""
from __future__ import annotations
import logging
import sys

import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("gdelt.cli")


@click.group()
def cli():
    """Global News Intelligence — GDELT Ingestion CLI."""
    pass


@cli.command("once")
@click.option("--download-dir", default=None, help="Directory for downloaded files")
@click.option("--dry-run", is_flag=True, help="Parse but do not publish to Kafka")
def ingest_once(download_dir, dry_run):
    """One-shot: poll lastupdate.txt, download & process new files, then exit."""
    from src.ingestion.poller import GDELTPoller
    poller = GDELTPoller(download_dir=download_dir, dry_run=dry_run)
    results = poller.poll_once()
    for r in results:
        status = r.get("status", "?")
        url = r.get("url", "?")
        records = r.get("records", 0)
        logger.info("  %s — %s (%d records)", status.upper(), url, records)
    if not results:
        logger.info("No new files to process.")
    logger.info("Done.")


@cli.command("poll")
@click.option("--interval", type=int, default=None, help="Poll interval in minutes (default: 15)")
@click.option("--dry-run", is_flag=True, help="Parse but do not publish to Kafka")
def poll_continuously(interval, dry_run):
    """Continuous polling loop — runs until interrupted with Ctrl+C."""
    from src.ingestion.poller import GDELTPoller
    poller = GDELTPoller(poll_interval_minutes=interval, dry_run=dry_run)
    logger.info("Starting continuous polling (Ctrl+C to stop)...")
    try:
        poller.run_forever()
    except KeyboardInterrupt:
        logger.info("Stopped by user.")


@cli.command("status")
def show_status():
    """Show ingestion checkpoint database statistics."""
    from src.ingestion.checkpoint import CheckpointDB
    from src.ingestion.metrics import metrics
    import os
    db_path = os.environ.get("GDELT_CHECKPOINT_DB", "data/checkpoints/ingestion.db")
    db = CheckpointDB(db_path)
    stats = db.get_stats()
    click.echo("=== Ingestion Status ===")
    for key, val in stats.items():
        click.echo(f"  {key}: {val}")
    click.echo("")
    click.echo("=== Runtime Metrics ===")
    m = metrics.snapshot()
    for key, val in m.items():
        click.echo(f"  {key}: {val}")


@cli.command("replay")
@click.argument("filepath")
@click.option("--file-type", type=click.Choice(["events", "mentions", "gkg"]), default=None)
@click.option("--dry-run", is_flag=True)
def replay_file(filepath, file_type, dry_run):
    """Re-parse and re-publish a previously downloaded file."""
    from src.ingestion.poller import GDELTPoller
    poller = GDELTPoller(dry_run=dry_run)
    result = poller.replay_file(filepath, file_type=file_type)
    click.echo(f"Result: {result}")


if __name__ == "__main__":
    cli()

