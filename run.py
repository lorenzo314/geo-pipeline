import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from dask.distributed import Client, LocalCluster
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

import config
from ingest import fetch
from serve import list_parquet_files, summary_stats, top_latitudes
from transform import process_geotiff
from validate import validate_zarr

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger("geo-pipeline")
log.setLevel(logging.INFO)
console = Console()

def main():
    parser = argparse.ArgumentParser(description="Geospatial data pipeline")
    parser.add_argument("--url", default=config.SRTM_URL)
    parser.add_argument("--workers", type=int, default=config.DASK_WORKERS)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--serve-only", action="store_true")
    args = parser.parse_args()

    if args.serve_only:
        files = list_parquet_files()
        if not files:
            console.print("[yellow]No Parquet files found — run the pipeline first.[/yellow]")
            return
        console.print(summary_stats())
        console.print(top_latitudes(10))
        return

    console.print(Panel.fit(f"[bold]geo-pipeline[/bold] 🌍\nsource: {args.url}"))
    t0 = time.perf_counter()

    with LocalCluster(
        n_workers=args.workers,
        threads_per_worker=config.DASK_THREADS,
        memory_limit=config.DASK_MEM_LIMIT,
        silence_logs=logging.WARNING,
    ) as cluster:
        with Client(cluster) as client:
            console.print(f"[dim]Dask dashboard → {client.dashboard_link}[/dim]")

            with Progress(SpinnerColumn(), TextColumn("{task.description}"),
                          TimeElapsedColumn(), console=console) as progress:

                t = progress.add_task("Ingesting…", total=None)
                paths = fetch(args.url)
                tiffs = [p for p in paths if p.suffix.lower() in (".tif", ".tiff")]
                progress.update(t, description=f"[green]Ingested[/green] {len(tiffs)} file(s)")
                progress.stop_task(t)

                for tiff in tiffs:
                    t = progress.add_task(f"Transforming {tiff.name}…", total=None)
                    zarr_path, parquet_path = process_geotiff(tiff)
                    progress.update(t, description=f"[green]Transformed[/green] {tiff.name}")
                    progress.stop_task(t)

                    t = progress.add_task(f"Validating {tiff.name}…", total=None)
                    result = validate_zarr(zarr_path)
                    progress.update(t, description=f"[green]Validated[/green] {tiff.name}")
                    progress.stop_task(t)
                    console.print(str(result))

    console.print(Panel.fit(
        f"[green bold]Done[/green bold] in {time.perf_counter()-t0:.1f}s\n"
        f"[dim]output → {config.OUT_DIR}[/dim]\n\n"
        "Run [bold]python run.py --serve-only[/bold] to query results."
    ))

    if args.serve:
        console.print(summary_stats())

if __name__ == "__main__":
    main()
