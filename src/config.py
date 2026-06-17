"""
Central configuration — swap these paths/URLs to point at S3 in production.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DASK_WORKERS = int(os.getenv("DASK_WORKERS", 4))
DASK_MEM_LIMIT = os.getenv("DASK_MEM_LIMIT", "2GB")

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR  = ROOT / "data"
RAW_DIR   = DATA_DIR / "raw"
OUT_DIR   = DATA_DIR / "processed"

for _d in (RAW_DIR, OUT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── Sample datasets (public, no auth needed) ─────────────────────────────────
# SRTM 90m elevation tile covering central France (~25 MB zip)
SRTM_URL = (
    "https://srtm.csi.cgiar.org/wp-content/uploads/files/"
    "srtm_5x5/TIFF/srtm_38_04.zip"
)

# ── Dask local cluster settings ───────────────────────────────────────────────
DASK_WORKERS   = 4      # set to os.cpu_count() // 2 on larger machines
DASK_THREADS   = 2      # threads per worker
DASK_MEM_LIMIT = "2GB"  # per-worker memory cap

# ── Zarr chunk sizes (pixels) ─────────────────────────────────────────────────
CHUNK_X = 256
CHUNK_Y = 256

# ── Validation thresholds ─────────────────────────────────────────────────────
MAX_NULL_FRACTION = 0.10   # allow up to 10 % nodata
