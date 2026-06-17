# geo-pipeline 🌍

![CI](https://github.com/lorenzo314/geo-pipeline/actions/workflows/ci.yaml/badge.svg?branch=main)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Dask](https://img.shields.io/badge/dask-distributed-orange)
![Rasterio](https://img.shields.io/badge/rasterio-geospatial-green)
![DuckDB](https://img.shields.io/badge/DuckDB-analytics-yellow)
![Zarr](https://img.shields.io/badge/format-Zarr%20%7C%20GeoTIFF%20%7C%20Parquet-lightgrey)
![License](https://img.shields.io/badge/license-MIT-green)

A **laptop-scale** geospatial data pipeline that mirrors a production architecture — same code, smaller data, zero cloud infra.

Ingests satellite elevation data (SRTM GeoTIFF), reprojects and rechunks it into a Zarr store, validates it, and serves summary statistics via DuckDB — all orchestrated with a single command.

---

## Architecture

```
Sources
  ├── Object store (GeoTIFF, Zarr)
  ├── REST / OGC APIs (STAC, WFS)
  ├── Databases (PostGIS)
  └── Compressed archives (.zip, .tar.gz)
         │
         ▼
   Raw zone (local filesystem → S3 in production)
   Immutable files, MD5-checksummed, partitioned by source
         │
         ▼
   Dask LocalCluster (4 workers)
   ├── Ingest    — download, decompress, checksum (idempotent)
   ├── Transform — reproject → EPSG:4326, rechunk → Zarr
   └── Validate  — null fraction, CRS, spatial extent, infinities
         │
         ▼
   Serving
   ├── Zarr store   — chunked N-D array access (xarray / ML)
   └── Parquet      — columnar analytics via DuckDB
```

## Key design choices

| Concern | Choice | Why |
|---|---|---|
| Array compute | Dask | Native xarray/rioxarray integration; no JVM |
| Array format | Zarr | Chunk-level reads; Dask-native; N-D |
| Analytics | DuckDB | In-process SQL on Parquet; zero infra |
| Idempotency | MD5 checksums + `mode="w"` | Safe reruns, no silent duplicates |
| Testability | Pure functions on `xr.DataArray` | Synthetic 4×4 tiles in tests, no network |

---

## Quickstart

```bash
# 1 — clone
git clone git@github.com:lorenzo314/geo-pipeline.git
cd geo-pipeline

# 2 — create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

# 3 — install
pip install -e ".[dev]"

# 4 — activate pre-commit hooks
pre-commit install

# 5 — run tests (no network needed — synthetic data)
make test

# 6 — run the full pipeline (~25 MB SRTM download, central France)
make run

# 7 — query results
make serve
```

---

## Docker

```bash
# build and run the pipeline
docker compose up

# query results
docker compose --profile serve up serve
```

Results are printed to stdout — the same tables as `python run.py --serve-only`:

```
global_min      global_mean     global_max      avg_std

-15.0           719.021019      4017.0          627.083209
```

The `./data` directory is mounted as a volume, so processed files are accessible
on your host machine even after the container exits:

```bash
ls data/processed/       # Zarr store + Parquet files
python run.py --serve-only   # query without Docker
```

> **Note:** run the `pipeline` service before `serve` — the serve container
> reads the Parquet files written by the pipeline.

---

## Usage

```bash
python run.py                        # default SRTM tile (France)
python run.py --url https://...      # custom GeoTIFF zip
python run.py --workers 2            # fewer workers on low-RAM machines
python run.py --serve                # run pipeline then show DuckDB results
python run.py --serve-only           # query already-processed data
python run.py --help
```

---

## Project layout

```
geo-pipeline/
├── src/
│   ├── config.py      — paths, chunk sizes, thresholds
│   ├── ingest.py      — download + decompress (idempotent, checksummed)
│   ├── transform.py   — reproject, rechunk, Zarr + Parquet output
│   ├── validate.py    — 6 data-quality checks
│   └── serve.py       — DuckDB query helpers
├── tests/
│   └── test_transform.py   — unit tests (synthetic 4×4 tiles, no network)
├── .github/
│   └── workflows/
│       └── ci.yml     — lint + test on every push
├── run.py             — single entry point
├── Makefile           — common dev tasks
└── pyproject.toml     — dependencies + tooling config
```

---

## Scaling to production

| Change | What to modify |
|---|---|
| Object store (S3/GCS) | `RAW_DIR` / `OUT_DIR` in `config.py` → `s3://…` |
| Distributed cluster | `LocalCluster()` → `KubeCluster()` in `run.py` |
| Orchestration | Wrap stage functions in Prefect / Dagster tasks |
| Larger datasets | Increase `CHUNK_X/Y` and `DASK_WORKERS` in `config.py` |

---

## Data source

Default: **SRTM 90m elevation** tile covering central France (~25 MB zip, no authentication needed).

Sample output on a 12.5 M pixel tile:

| metric | value |
|---|---|
| min elevation | -15 m |
| mean elevation | 719 m |
| max elevation | 4017 m (Alps) |
| std | 627 m |

---

## Development

```bash
make test        # run test suite
make lint        # ruff check
make run         # full pipeline
make serve       # DuckDB query results
make clean       # remove processed data
```

Pre-commit hooks run `ruff` automatically on every commit.
