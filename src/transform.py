"""
Transform stage — pure functions that convert raw rasters into
analysis-ready Zarr stores and summary Parquet files.

Every function is:
  • Pure (no global state mutated)
  • Idempotent (re-running produces the same output)
  • Testable (call with a tiny synthetic array in tests)
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import rioxarray  # noqa: F401  — registers .rio accessor on xarray
import xarray as xr

from config import CHUNK_X, CHUNK_Y, OUT_DIR

log = logging.getLogger(__name__)


# ── Reproject ─────────────────────────────────────────────────────────────────

def reproject(src: Path, target_crs: str = "EPSG:4326") -> xr.DataArray:
    """
    Open a GeoTIFF with Dask-backed chunks and reproject to *target_crs*.

    We use rioxarray so the CRS metadata travels with the array throughout
    the rest of the pipeline — no silent CRS loss.
    """
    log.info("reproject: opening %s", src)
    da = rioxarray.open_rasterio(
        src,
        chunks={"x": CHUNK_X, "y": CHUNK_Y},
        masked=True,   # nodata → NaN (float32), keeps semantics clean
        lock=False,    # allow concurrent reads from Dask workers
    )

    if da.rio.crs is None:
        raise ValueError(f"Source file has no CRS metadata: {src}")

    if str(da.rio.crs) != target_crs:
        log.info("reproject: %s → %s", da.rio.crs, target_crs)
        da = da.rio.reproject(target_crs)
    else:
        log.info("reproject: already in %s, skipping", target_crs)

    # Squeeze band dim when there's only one band (common for DEM / single-var rasters)
    if "band" in da.dims and da.sizes["band"] == 1:
        da = da.squeeze("band", drop=True)

    return da


# ── Rechunk ───────────────────────────────────────────────────────────────────

def rechunk(da: xr.DataArray) -> xr.DataArray:
    """
    Rechunk spatial dims to (CHUNK_X, CHUNK_Y).

    Explicit rechunking before writing ensures Zarr chunks align with
    how downstream readers (Dask, Xarray) will load the data — avoiding
    the "chunk mismatch" performance cliff.
    """
    chunks = {d: CHUNK_X if d in ("x", "lon") else CHUNK_Y if d in ("y", "lat") else -1
              for d in da.dims}
    return da.chunk(chunks)


# ── Write Zarr ────────────────────────────────────────────────────────────────

def write_zarr(da: xr.DataArray, name: str, out_dir: Path = OUT_DIR) -> Path:
    """
    Write DataArray to Zarr.  Idempotent via mode="w" (overwrite).

    In production swap *out_dir* for an s3:// URI — the rest stays identical.
    """
    out = out_dir / f"{name}.zarr"
    log.info("write_zarr: computing and writing → %s", out)

    ds = da.to_dataset(name=name)
    ds.to_zarr(str(out), mode="w", consolidated=True)

    log.info("write_zarr: done → %s", out)
    return out


# ── Summarise → Parquet ───────────────────────────────────────────────────────

def summarise_to_parquet(zarr_path: Path, out_dir: Path = OUT_DIR) -> Path:
    log.info("summarise: loading %s", zarr_path)
    ds = xr.open_zarr(str(zarr_path))
    var = next(v for v in ds.data_vars if v != "spatial_ref")
    da = ds[var]

    # detect whichever coordinate name is actually present
    log.info("summarise: computing array (triggering Dask graph)…")
    da = da.compute()
    flat = da.values.ravel()
    flat = flat[~np.isnan(flat)]

    df = pd.DataFrame({
        "mean":  [float(np.mean(flat))],
        "std":   [float(np.std(flat))],
        "min":   [float(np.min(flat))],
        "max":   [float(np.max(flat))],
        "count": [int(len(flat))],
    })

    out = out_dir / f"{zarr_path.stem}_stats.parquet"
    df.to_parquet(out, index=False)
    log.info("summarise: wrote %d rows → %s", len(df), out)
    return out

# ── Top-level convenience ─────────────────────────────────────────────────────

def process_geotiff(src: Path, name: str | None = None) -> tuple[Path, Path]:
    """
    Full transform for one GeoTIFF:
      1. Reproject to EPSG:4326
      2. Rechunk for Zarr
      3. Write Zarr
      4. Summarise to Parquet

    Returns (zarr_path, parquet_path).
    """
    name = name or src.stem
    da   = reproject(src)
    da   = rechunk(da)
    zp   = write_zarr(da, name)
    pp   = summarise_to_parquet(zp)
    return zp, pp
