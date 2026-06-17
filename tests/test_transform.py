"""
Unit tests for the transform module.

All tests use synthetic in-memory arrays — no network, no large files.
This is the key to making pipelines testable: pure functions that accept
xr.DataArray work identically on 4×4 synthetic tiles and 40000×40000 rasters.
"""

import sys
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def tiny_dem() -> xr.DataArray:
    """4×4 synthetic elevation array in EPSG:4326, mimicking a DEM tile."""
    data = np.array([
        [100.0, 200.0, 300.0, 400.0],
        [150.0, 250.0, 350.0, 450.0],
        [120.0, 220.0, 320.0, 420.0],
        [  0.0, 100.0, 200.0, 300.0],
    ], dtype=np.float32)
    da = xr.DataArray(
        data,
        dims=["y", "x"],
        coords={
            "y": [48.0, 47.5, 47.0, 46.5],  # lat (decreasing, as rasters are)
            "x": [2.0,  2.5,  3.0,  3.5],   # lon
        },
        attrs={"units": "m", "_CRS": "EPSG:4326"},
    )
    return da


@pytest.fixture
def tiny_dem_with_nans(tiny_dem) -> xr.DataArray:
    """Same tile but with 25 % nodata (1 pixel out of 4 per row)."""
    da = tiny_dem.copy(deep=True)
    da.values[0, 0] = np.nan
    da.values[2, 3] = np.nan
    da.values[3, 1] = np.nan
    da.values[1, 2] = np.nan
    return da


# ── rechunk ───────────────────────────────────────────────────────────────────

def test_rechunk_preserves_values(tiny_dem):
    from transform import rechunk
    rechunked = rechunk(tiny_dem)
    np.testing.assert_array_equal(rechunked.values, tiny_dem.values)


def test_rechunk_is_dask_backed(tiny_dem):
    import dask.array as da

    from transform import rechunk
    rechunked = rechunk(tiny_dem)
    assert isinstance(rechunked.data, da.Array), "rechunk should return a Dask-backed array"


def test_rechunk_idempotent(tiny_dem):
    from transform import rechunk
    once  = rechunk(tiny_dem)
    twice = rechunk(once)
    np.testing.assert_array_equal(once.values, twice.values)


# ── write_zarr / round-trip ───────────────────────────────────────────────────

def test_write_zarr_roundtrip(tmp_path, tiny_dem):
    from transform import rechunk, write_zarr
    da = rechunk(tiny_dem)
    zarr_path = write_zarr(da, name="test_dem", out_dir=tmp_path)

    assert zarr_path.exists(), "Zarr directory should be created"

    ds = xr.open_zarr(str(zarr_path))
    assert "test_dem" in ds.data_vars
    np.testing.assert_allclose(ds["test_dem"].values, tiny_dem.values, rtol=1e-5)


def test_write_zarr_idempotent(tmp_path, tiny_dem):
    from transform import rechunk, write_zarr
    da = rechunk(tiny_dem)
    write_zarr(da, name="test_dem", out_dir=tmp_path)
    write_zarr(da, name="test_dem", out_dir=tmp_path)  # second write must not raise

    ds = xr.open_zarr(str(tmp_path / "test_dem.zarr"))
    assert "test_dem" in ds.data_vars


# ── summarise_to_parquet ──────────────────────────────────────────────────────

def test_summarise_produces_parquet(tmp_path, tiny_dem):
    from transform import rechunk, summarise_to_parquet, write_zarr
    da = rechunk(tiny_dem)
    zarr_path = write_zarr(da, name="dem", out_dir=tmp_path)
    parquet_path = summarise_to_parquet(zarr_path, out_dir=tmp_path)

    assert parquet_path.exists()
    import pandas as pd
    df = pd.read_parquet(parquet_path)
    assert len(df) > 0
    assert "mean" in df.columns


# ── validate ──────────────────────────────────────────────────────────────────

def test_validate_passes_clean_data(tmp_path, tiny_dem):
    from transform import rechunk, write_zarr
    from validate import validate_zarr
    zarr_path = write_zarr(rechunk(tiny_dem), name="dem", out_dir=tmp_path)
    result = validate_zarr(zarr_path)
    assert result.checks["openable"]
    assert result.checks["has_variables"]
    assert result.checks["null_fraction_ok"]


def test_validate_fails_high_null_fraction(tmp_path, tiny_dem_with_nans):
    import config
    from transform import rechunk, write_zarr
    from validate import validate_zarr
    # Temporarily lower the threshold so 25 % nulls trigger a failure
    original = config.MAX_NULL_FRACTION
    config.MAX_NULL_FRACTION = 0.05
    try:
        zarr_path = write_zarr(rechunk(tiny_dem_with_nans), name="dem_nan", out_dir=tmp_path)
        result = validate_zarr(zarr_path)
        assert not result.checks["null_fraction_ok"]
        assert not result.passed
    finally:
        config.MAX_NULL_FRACTION = original


def test_validate_missing_file(tmp_path):
    from validate import validate_zarr
    result = validate_zarr(tmp_path / "nonexistent.zarr")
    assert not result.passed
    assert not result.checks["openable"]


# ── ingest helpers ────────────────────────────────────────────────────────────

def test_download_skip_if_exists(tmp_path, monkeypatch):
    """download() must return immediately if the file + checksum exist."""
    from ingest import _write_checksum, download

    dummy = tmp_path / "file.tif"
    dummy.write_bytes(b"fake raster data")
    _write_checksum(dummy)

    calls = []
    monkeypatch.setattr("httpx.stream", lambda *a, **k: calls.append(1))

    result = download("https://example.com/file.tif", dest_dir=tmp_path)
    assert result == dummy
    assert len(calls) == 0, "httpx.stream should not be called on cache hit"
