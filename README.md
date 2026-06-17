# geo-pipeline 🌍

![CI](https://github.com/lorenzo314/geo-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Dask](https://img.shields.io/badge/dask-distributed-orange)
![Rasterio](https://img.shields.io/badge/rasterio-geospatial-green)
![DuckDB](https://img.shields.io/badge/DuckDB-analytics-yellow)
![Zarr](https://img.shields.io/badge/format-Zarr%20%7C%20GeoTIFF%20%7C%20Parquet-lightgrey)
![License](https://img.shields.io/badge/license-MIT-green)

A **laptop-scale** geospatial data pipeline that mirrors a production
architecture — same code, smaller data, zero cloud infra.

## Architecture

```
Sources (GeoTIFF / Zarr / APIs / DB)
        │
        ▼
   Data lake (local filesystem — swap for S3 in prod)
        │
        ▼
  Prefect flow  ──►  Dask LocalCluster
        │               (4 workers, same API as KubeCluster)
        ▼
  Transform tasks
   ├── reproject → EPSG:4326
   ├── rechunk   → 256×256 spatial tiles
   └── validate  → null fraction, CRS, extent
        │
        ▼
  Serving
   ├── Zarr store   (N-D array access)
   └── Parquet      (DuckDB analytics)
```

## Quickstart

```bash
# 1 — install
pip install -e ".[dev]"

# 2 — run tests (no network needed — synthetic data)
make test

# 3 — run the full pipeline (~25 MB SRTM tile download)
make run

# 4 — query results
make serve
```

## Project layout

```
src/
  config.py      — paths, chunk sizes, thresholds
  ingest.py      — download + decompress (idempotent)
  transform.py   — reproject, rechunk, Zarr + Parquet output
  validate.py    — data-quality checks
  serve.py       — DuckDB query helpers

flows/
  pipeline.py    — Prefect flow wiring all tasks

tests/
  test_transform.py   — unit tests (synthetic 4×4 tiles)
```

## Scaling to production

| Change | What to modify |
|---|---|
| Object store (S3/GCS) | `RAW_DIR` / `OUT_DIR` in `config.py` → `s3://…` |
| Distributed cluster | `LocalCluster()` → `KubeCluster()` in `pipeline.py` |
| Scheduling | `make prefect-start`, then `prefect deploy` |
| Larger datasets | Increase `CHUNK_X/Y` and `DASK_WORKERS` in `config.py` |
| CI/CD | Add `.github/workflows/ci.yml` calling `make lint test` |

## Data source

Default: **SRTM 90m elevation** tile (central France, ~25 MB zip, no auth).

To use your own GeoTIFF:
```bash
make run-url URL=https://example.com/your_file.zip
# or
PYTHONPATH=src python flows/pipeline.py https://example.com/your_file.zip
```

## Running with the Prefect UI

```bash
# Terminal 1
make prefect-start

# Terminal 2
prefect deploy flows/pipeline.py:geo_pipeline --name laptop
prefect worker start --pool default-agent-pool
```

Then open http://localhost:4200 to see runs, logs, and retries.
