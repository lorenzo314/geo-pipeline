"""
Ingestion helpers — download and decompress raw files.

Design principles applied here:
  • Idempotent  : skips work if output already exists and checksum matches.
  • Pure I/O    : functions only download/decompress, no transforms.
  • Retryable   : raises on error so Prefect can retry the task.
"""

import hashlib
import logging
import zipfile
from pathlib import Path

import httpx
from rich.progress import DownloadColumn, Progress, SpinnerColumn, TransferSpeedColumn

from config import RAW_DIR

log = logging.getLogger(__name__)


# ── Checksum helpers ──────────────────────────────────────────────────────────

def _md5(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _checksum_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".md5")


def _checksum_valid(path: Path) -> bool:
    cp = _checksum_path(path)
    if not cp.exists():
        return False
    return cp.read_text().strip() == _md5(path)


def _write_checksum(path: Path) -> None:
    _checksum_path(path).write_text(_md5(path))


# ── Download ──────────────────────────────────────────────────────────────────

def download(url: str, dest_dir: Path = RAW_DIR) -> Path:
    """
    Download *url* into *dest_dir*.  Skips if the file already exists and its
    MD5 checksum matches the stored value.

    Returns the local path of the downloaded file.
    """
    filename = url.split("/")[-1]
    dest = dest_dir / filename

    if dest.exists() and _checksum_valid(dest):
        log.info("download: cache hit → %s", dest)
        return dest

    log.info("download: fetching %s", url)
    with Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        DownloadColumn(),
        TransferSpeedColumn(),
    ) as progress:
        task = progress.add_task(f"Downloading {filename}", total=None)

        with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0)) or None
            progress.update(task, total=total)

            with open(dest, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=1 << 16):
                    f.write(chunk)
                    progress.advance(task, len(chunk))

    _write_checksum(dest)
    log.info("download: saved → %s", dest)
    return dest


# ── Decompress ────────────────────────────────────────────────────────────────

def decompress(archive: Path, dest_dir: Path = RAW_DIR) -> list[Path]:
    """
    Decompress a zip archive into *dest_dir*.
    Idempotent: skips files that already exist.

    Returns list of extracted file paths.
    """
    extracted: list[Path] = []

    if archive.suffix.lower() != ".zip":
        raise ValueError(f"Only .zip archives are supported, got: {archive.suffix}")

    with zipfile.ZipFile(archive) as zf:
        for member in zf.infolist():
            out = dest_dir / member.filename
            if out.exists():
                log.info("decompress: already exists → %s", out)
            else:
                log.info("decompress: extracting → %s", out)
                zf.extract(member, dest_dir)
            extracted.append(out)

    return extracted


# ── Convenience wrapper ───────────────────────────────────────────────────────

def fetch(url: str, dest_dir: Path = RAW_DIR) -> list[Path]:
    """Download + decompress in one call.  Returns list of usable file paths."""
    archive = download(url, dest_dir)
    if archive.suffix.lower() == ".zip":
        return decompress(archive, dest_dir)
    return [archive]
