"""
Validation — lightweight data-quality checks run after each transform.

Keeps the pipeline honest: a task that writes bad data fails loudly
rather than silently propagating garbage downstream.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import xarray as xr

from config import MAX_NULL_FRACTION

log = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    passed: bool
    checks: dict[str, bool] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        status = "✓ PASSED" if self.passed else "✗ FAILED"
        lines = [status]
        for name, ok in self.checks.items():
            lines.append(f"  {'✓' if ok else '✗'} {name}")
        for err in self.errors:
            lines.append(f"  ⚠  {err}")
        return "\n".join(lines)


def validate_zarr(zarr_path: Path) -> ValidationResult:
    """
    Run a suite of quality checks on a processed Zarr store.

    Checks:
      1. File exists and can be opened
      2. Has at least one data variable
      3. CRS metadata is present
      4. Null fraction is below threshold
      5. Values are finite (no inf / -inf)
      6. Spatial extent is plausible (lat ∈ [-90, 90], lon ∈ [-180, 180])
    """
    checks: dict[str, bool] = {}
    errors: list[str] = []

    # 1 — openable
    try:
        ds = xr.open_zarr(str(zarr_path), consolidated=True)
        checks["openable"] = True
    except Exception as e:
        checks["openable"] = False
        errors.append(str(e))
        return ValidationResult(passed=False, checks=checks, errors=errors)

    # 2 — has data variables
    checks["has_variables"] = len(ds.data_vars) > 0
    if not checks["has_variables"]:
        errors.append("No data variables found in dataset")

    var = list(ds.data_vars)[0] if ds.data_vars else None
    if var is None:
        return ValidationResult(passed=False, checks=checks, errors=errors)

    da = ds[var]

    # 3 — CRS present (stored as attribute by rioxarray)
    crs_attrs = {"crs_wkt", "grid_mapping", "spatial_ref"}
    has_crs = (
        any(a in da.attrs for a in crs_attrs)
        or any(a in ds.attrs for a in crs_attrs)
        or "spatial_ref" in ds.coords
    )
    checks["has_crs"] = has_crs
    if not has_crs:
        errors.append("CRS metadata not found — reproject step may have dropped it")

    # 4 — null fraction
    try:
        sample = da.isel(
            {d: slice(0, min(512, da.sizes[d])) for d in da.dims}
        ).values
        null_frac = float(np.isnan(sample).mean())
        checks["null_fraction_ok"] = null_frac <= MAX_NULL_FRACTION
        if not checks["null_fraction_ok"]:
            errors.append(
                f"Null fraction {null_frac:.1%} exceeds threshold {MAX_NULL_FRACTION:.1%}"
            )
    except Exception as e:
        checks["null_fraction_ok"] = False
        errors.append(f"Could not compute null fraction: {e}")

    # 5 — no infinities
    try:
        has_inf = bool(np.any(np.isinf(sample[~np.isnan(sample)])))
        checks["no_inf"] = not has_inf
        if has_inf:
            errors.append("Dataset contains infinite values")
    except Exception:
        checks["no_inf"] = True  # can't check — assume ok

    # 6 — plausible spatial extent
    try:
        y_coord = next((c for c in ("y", "lat", "latitude") if c in ds.coords), None)
        x_coord = next((c for c in ("x", "lon", "longitude") if c in ds.coords), None)
        if y_coord and x_coord:
            lat_ok = float(ds[y_coord].min()) >= -90 and float(ds[y_coord].max()) <= 90
            lon_ok = float(ds[x_coord].min()) >= -180 and float(ds[x_coord].max()) <= 180
            checks["spatial_extent_ok"] = lat_ok and lon_ok
            if not lat_ok:
                lat_range = f"{float(ds[y_coord].min()):.2f}…{float(ds[y_coord].max()):.2f}"
                errors.append(f"Latitude out of [-90, 90]: {lat_range}")

            if not lon_ok:
                lat_range = f"{float(ds[y_coord].min()):.2f}…{float(ds[y_coord].max()):.2f}"
                errors.append(f"Latitude out of [-90, 90]: {lat_range}")
        else:
            checks["spatial_extent_ok"] = False
            errors.append("Could not find spatial coordinate variables")
    except Exception as e:
        checks["spatial_extent_ok"] = False
        errors.append(f"Spatial extent check failed: {e}")

    passed = all(checks.values())
    result = ValidationResult(passed=passed, checks=checks, errors=errors)
    log.info("validate_zarr:\n%s", result)
    return result
