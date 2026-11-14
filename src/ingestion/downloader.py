"""
HTTP downloader with retry/backoff for GDELT archive files.
Downloads to a temp file and atomically renames on success.
Verifies file size and MD5 hash from the GDELT lastupdate.txt manifest.
"""
from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
BACKOFF_BASE = 2.0          # seconds; actual wait = BACKOFF_BASE ** attempt
CONNECT_TIMEOUT = 10        # seconds
READ_TIMEOUT = 120          # seconds for large zip downloads
CHUNK_SIZE = 1024 * 256     # 256 KB read chunks


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=0,            # We handle retries ourselves for finer control
        backoff_factor=0,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "GNIP-GDELT-Poller/0.1 (academic research)"})
    return session


def _md5_of_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


class DownloadError(Exception):
    pass


class Downloader:
    """Downloads a URL to a local path with retry, verification, and atomic rename."""

    def __init__(self, download_dir: str | Path) -> None:
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._session = _build_session()

    def download(
        self,
        url: str,
        expected_size: Optional[int] = None,
        expected_md5: Optional[str] = None,
        dest_filename: Optional[str] = None,
    ) -> Path:
        """
        Download *url* to self.download_dir.

        Args:
            url:            Full HTTPS URL of the GDELT archive.
            expected_size:  Byte count from lastupdate.txt (or None to skip).
            expected_md5:   Hex MD5 from lastupdate.txt (or None to skip).
            dest_filename:  Override the destination filename (defaults to URL basename).

        Returns:
            Path of the successfully downloaded file.

        Raises:
            DownloadError on permanent failure.
        """
        filename = dest_filename or Path(url).name
        dest = self.download_dir / filename

        if dest.exists():
            # Quick integrity check if hashes provided
            if expected_md5 and _md5_of_file(dest) == expected_md5:
                logger.info("Already downloaded (hash matches): %s", dest)
                return dest
            logger.warning("Existing file %s failed hash check; re-downloading", dest)

        last_error: Exception = RuntimeError("No attempt made")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                dest_path = self._download_once(
                    url, dest, expected_size, expected_md5, attempt
                )
                return dest_path
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status == 429:
                    retry_after = int(
                        exc.response.headers.get("Retry-After", 60)  # type: ignore[union-attr]
                    )
                    logger.warning(
                        "HTTP 429 from server; sleeping %ds (attempt %d/%d)",
                        retry_after, attempt, MAX_RETRIES,
                    )
                    time.sleep(retry_after)
                elif 500 <= status < 600:
                    wait = BACKOFF_BASE ** attempt
                    logger.warning(
                        "HTTP %d (attempt %d/%d); retrying in %.1fs",
                        status, attempt, MAX_RETRIES, wait,
                    )
                    time.sleep(wait)
                else:
                    raise DownloadError(f"Non-retryable HTTP {status} for {url}") from exc
                last_error = exc
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                wait = BACKOFF_BASE ** attempt
                logger.warning(
                    "Network error (attempt %d/%d): %s; retrying in %.1fs",
                    attempt, MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)
                last_error = exc
            except DownloadError:
                raise
            except Exception as exc:
                last_error = exc
                wait = BACKOFF_BASE ** attempt
                logger.warning("Unexpected error attempt %d: %s", attempt, exc)
                time.sleep(wait)

        raise DownloadError(
            f"Failed to download {url} after {MAX_RETRIES} attempts: {last_error}"
        )

    def _download_once(
        self,
        url: str,
        dest: Path,
        expected_size: Optional[int],
        expected_md5: Optional[str],
        attempt: int,
    ) -> Path:
        """Single download attempt — streams to a temp file, then atomically renames."""
        tmp_fd, tmp_path_str = tempfile.mkstemp(
            dir=self.download_dir, prefix=".tmp_", suffix=".part"
        )
        tmp_path = Path(tmp_path_str)
        downloaded = 0

        try:
            with self._session.get(
                url,
                stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            ) as resp:
                resp.raise_for_status()

                content_length = resp.headers.get("Content-Length")
                if content_length:
                    logger.debug(
                        "Downloading %s (~%s bytes) attempt %d",
                        url, content_length, attempt,
                    )

                with os.fdopen(tmp_fd, "wb") as fh:
                    tmp_fd = -1  # fd is owned by fh now
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            fh.write(chunk)
                            downloaded += len(chunk)

            logger.info("Downloaded %d bytes for %s", downloaded, url)

            # Size verification
            if expected_size is not None and downloaded != expected_size:
                raise DownloadError(
                    f"Size mismatch for {url}: expected {expected_size}, got {downloaded}"
                )

            # MD5 verification
            if expected_md5:
                actual_md5 = _md5_of_file(tmp_path)
                if actual_md5 != expected_md5:
                    raise DownloadError(
                        f"MD5 mismatch for {url}: expected {expected_md5}, got {actual_md5}"
                    )
                logger.debug("MD5 verified: %s", actual_md5)

            # Atomic rename
            shutil.move(str(tmp_path), str(dest))
            logger.info("Saved to %s", dest)
            return dest

        except Exception:
            # Clean up temp file on any error
            try:
                if tmp_fd != -1:
                    os.close(tmp_fd)
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass
            raise
