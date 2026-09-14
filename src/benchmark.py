"""Benchmark comparativo: Pandas vs. Polars vs. DuckDB sobre la misma tarea analítica.

Mide tiempo de ejecución y delta de memoria RSS del proceso (vía `psutil`) para la
misma agregación (ingresos totales por categoría, solo eventos de compra) resuelta
con cada motor sobre el mismo archivo Parquet crudo.

Nota metodológica honesta: la medición de RAM es un delta de RSS del proceso *actual*
entre antes y después de cada corrida (con `gc.collect()` de por medio), no un pico
aislado por subproceso — más simple de implementar y suficiente para ver el orden de
magnitud de la diferencia entre motores, pero no un profiling de memoria riguroso.
"""
from __future__ import annotations

import gc
import os
import time
from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd
import polars as pl
import psutil

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = PROJECT_ROOT / "data" / "raw" / "clickstream_events.parquet"

_PROCESS = psutil.Process(os.getpid())


def _measure(fn: Callable[[], object]) -> tuple[object, float, float]:
    """Ejecuta `fn`, devuelve (resultado, segundos, delta_rss_mb)."""
    gc.collect()
    mem_before = _PROCESS.memory_info().rss
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    mem_after = _PROCESS.memory_info().rss
    gc.collect()
    delta_mb = max(mem_after - mem_before, 0) / (1024 ** 2)
    return result, elapsed, delta_mb


def run_pandas(path: Path = RAW_PATH) -> pd.DataFrame:
    df = pd.read_parquet(path)
    purchases = df[df["event_type"] == "purchase"]
    return (
        purchases.groupby("category", as_index=False)["revenue"]
        .agg(total_revenue="sum", purchase_count="count")
        .sort_values("total_revenue", ascending=False)
    )


def run_polars(path: Path = RAW_PATH) -> pl.DataFrame:
    df = pl.read_parquet(path)
    return (
        df.filter(pl.col("event_type") == "purchase")
        .group_by("category")
        .agg(
            total_revenue=pl.col("revenue").sum(),
            purchase_count=pl.col("revenue").count(),
        )
        .sort("total_revenue", descending=True)
    )


def run_duckdb(path: Path = RAW_PATH) -> pl.DataFrame:
    query = f"""
        SELECT category, SUM(revenue) AS total_revenue, COUNT(*) AS purchase_count
        FROM read_parquet('{path.as_posix()}')
        WHERE event_type = 'purchase'
        GROUP BY category
        ORDER BY total_revenue DESC
    """
    return duckdb.connect(database=":memory:").execute(query).pl()


ENGINES: dict[str, Callable[[Path], object]] = {
    "pandas": run_pandas,
    "polars": run_polars,
    "duckdb": run_duckdb,
}


def run_benchmark(path: Path = RAW_PATH) -> pd.DataFrame:
    """Corre los tres motores sobre la misma tarea y devuelve una tabla comparativa."""
    rows = []
    for name, fn in ENGINES.items():
        _, elapsed, delta_mb = _measure(lambda fn=fn: fn(path))
        rows.append({"engine": name, "seconds": round(elapsed, 4), "delta_rss_mb": round(delta_mb, 2)})
    return pd.DataFrame(rows).sort_values("seconds").reset_index(drop=True)


if __name__ == "__main__":
    results = run_benchmark()
    print(results.to_string(index=False))
