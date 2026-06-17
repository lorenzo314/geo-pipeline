"""
Serving layer — thin DuckDB wrapper for ad-hoc analytics on Parquet outputs.

In production this would be replaced by a proper query engine (Trino,
BigQuery, Redshift) or a GeoServer instance.  DuckDB gives us the same
SQL interface locally with zero infra.
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

from config import OUT_DIR

log = logging.getLogger(__name__)


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return an in-process DuckDB connection with spatial extension loaded."""
    con = duckdb.connect(database=":memory:", read_only=False)
    # Load spatial extension if available (optional — graceful fallback)
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
    except Exception:
        log.debug("DuckDB spatial extension not available — skipping")
    return con


def list_parquet_files(out_dir: Path = OUT_DIR) -> list[Path]:
    return sorted(out_dir.glob("*.parquet"))


def query(sql: str, out_dir: Path = OUT_DIR) -> pd.DataFrame:
    """
    Run an arbitrary SQL query against all Parquet files in *out_dir*.

    A glob view called `stats` is registered automatically so you can write:
        SELECT * FROM stats WHERE mean > 500
    """
    files = list_parquet_files(out_dir)
    if not files:
        raise FileNotFoundError(f"No Parquet files found in {out_dir}")

    con = get_connection()
    glob = str(out_dir / "*_stats.parquet")
    con.execute(f"CREATE VIEW stats AS SELECT * FROM read_parquet('{glob}')")

    log.info("query: %s", sql)
    return con.execute(sql).df()


# ── Canned queries ────────────────────────────────────────────────────────────

def summary_stats(out_dir: Path = OUT_DIR) -> pd.DataFrame:
    """Global min / mean / max / std across all processed Parquet files."""
    return query(
        "SELECT MIN(min) AS global_min, AVG(mean) AS global_mean, "
        "MAX(max) AS global_max, AVG(std) AS avg_std FROM stats",
        out_dir,
    )


def elevation_histogram(n_bins: int = 20, out_dir: Path = OUT_DIR) -> pd.DataFrame:
    """Bucket mean elevations into *n_bins* equal-width bins."""
    return query(
        f"""
        WITH bounds AS (SELECT MIN(mean) AS lo, MAX(mean) AS hi FROM stats),
        binned AS (
            SELECT
                floor((mean - lo) / ((hi - lo) / {n_bins})) AS bin,
                count(*) AS n,
                avg(mean) AS bin_mean
            FROM stats, bounds
            GROUP BY bin
        )
        SELECT bin, n, bin_mean FROM binned ORDER BY bin
        """,
        out_dir,
    )


def top_latitudes(n: int = 10, out_dir: Path = OUT_DIR) -> pd.DataFrame:
    """Return the *n* latitude bands with the highest mean value."""
    return query(
        f"SELECT * FROM stats ORDER BY mean DESC LIMIT {n}",
        out_dir,
    )


# ── CLI convenience ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    from rich import print as rprint
    from rich.table import Table

    files = list_parquet_files()
    if not files:
        print("No processed Parquet files found.  Run the pipeline first.")
        sys.exit(1)

    print("\n[bold]Global statistics[/bold]")
    rprint(summary_stats())

    print("\n[bold]Top 10 latitude bands by mean elevation[/bold]")
    df = top_latitudes(10)
    table = Table(*df.columns.tolist())
    for row in df.itertuples(index=False):
        table.add_row(*[str(round(v, 2)) if isinstance(v, float) else str(v) for v in row])
    rprint(table)
