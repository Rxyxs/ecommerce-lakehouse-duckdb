"""Same query, four engines: pandas, Polars, DuckDB and PySpark over the Parquet lake.

The query is revenue, purchases and distinct buyers per top-level category, purchases
only. Each engine runs in a fresh subprocess so one cannot inherit another's caches or
heap; the parent samples the resident memory of that subprocess *and its children* every
50 ms, which is what counts the JVM that PySpark starts. Inside the subprocess the query
runs once cold and then ``WARM_RUNS`` times warm; Spark's session start-up is timed
separately so it is not mistaken for query time.

Each engine is written the way it is normally used: pandas reads the needed columns and
filters in memory, Polars scans lazily, DuckDB and Spark push the filter into the scan.
That difference is part of what is being measured, not a flaw to remove.

PySpark needs a 64-bit Java 17 in ``JAVA_HOME``; on Windows also ``HADOOP_HOME`` with
``winutils.exe``. Without them the Spark row is reported as skipped.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psutil

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAKE = PROJECT_ROOT / "data" / "lakehouse" / "events"
SCOPES = {"october": LAKE / "year=2019" / "month=10", "full": LAKE}
ENGINES = ["pandas", "polars", "duckdb", "pyspark"]
WARM_RUNS = 3
MEMORY_LIMIT_GB = 24.0
COLUMNS = ["event_type", "top_category", "price", "user_id"]


def _glob(path: Path) -> str:
    return f"{path.as_posix()}/**/*.parquet"


def query_pandas(path: Path):
    import pandas as pd
    df = pd.read_parquet(path, columns=COLUMNS)
    p = df[df["event_type"] == "purchase"]
    return (p.groupby("top_category")
             .agg(revenue=("price", "sum"), purchases=("price", "size"),
                  buyers=("user_id", "nunique"))
             .sort_values("revenue", ascending=False))


def query_polars(path: Path):
    import polars as pl
    return (pl.scan_parquet(_glob(path), hive_partitioning=False)
              .filter(pl.col("event_type") == "purchase")
              .group_by("top_category")
              .agg(revenue=pl.col("price").sum(), purchases=pl.len(),
                   buyers=pl.col("user_id").n_unique())
              .sort("revenue", descending=True)
              .collect())


def query_duckdb(path: Path, con=None):
    import duckdb
    con = con or duckdb.connect()
    return con.sql(f"""
        SELECT top_category, sum(price) AS revenue, count(*) AS purchases,
               count(DISTINCT user_id) AS buyers
        FROM read_parquet('{_glob(path)}', hive_partitioning=false)
        WHERE event_type = 'purchase' GROUP BY 1 ORDER BY revenue DESC
    """).df()


def query_pyspark(path: Path, spark=None):
    from pyspark.sql import functions as F
    return (spark.read.parquet(path.as_posix())
                 .filter(F.col("event_type") == "purchase")
                 .groupBy("top_category")
                 .agg(F.sum("price").alias("revenue"), F.count("*").alias("purchases"),
                      F.countDistinct("user_id").alias("buyers"))
                 .orderBy(F.desc("revenue"))
                 .toPandas())


def _child(engine: str, scope: str) -> dict:
    """Runs inside the subprocess: cold run, then warm runs; returns timings and the answer."""
    path = SCOPES[scope]
    startup = 0.0
    if engine == "pyspark":
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        t0 = time.perf_counter()
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.master("local[*]").appName("bench")
                 .config("spark.driver.memory", "12g")
                 .config("spark.sql.shuffle.partitions", "16")
                 .config("spark.ui.enabled", "false").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        startup = time.perf_counter() - t0
        run = lambda: query_pyspark(path, spark)  # noqa: E731
    elif engine == "duckdb":
        import duckdb
        con = duckdb.connect()
        run = lambda: query_duckdb(path, con)  # noqa: E731
    else:
        fn = {"pandas": query_pandas, "polars": query_polars}[engine]
        run = lambda: fn(path)  # noqa: E731
    times = []
    for _ in range(1 + WARM_RUNS):
        t0 = time.perf_counter()
        result = run()
        times.append(time.perf_counter() - t0)
    if engine == "pyspark":
        spark.stop()
    if hasattr(result, "to_pandas"):
        result = result.to_pandas()
    result = result.reset_index() if "top_category" not in result.columns else result
    top = result.iloc[0]
    return {"startup_s": startup, "cold_s": times[0], "warm_s": min(times[1:]),
            "top_category": str(top["top_category"]), "top_revenue": float(top["revenue"]),
            "top_buyers": int(top["buyers"])}


def _tree_rss(proc: psutil.Process) -> int:
    total = 0
    try:
        tree = [proc, *proc.children(recursive=True)]
    except psutil.Error:
        return 0
    for p in tree:
        try:
            total += p.memory_info().rss
        except psutil.Error:
            pass
    return total


def measure(engine: str, scope: str, env: dict | None = None) -> dict:
    """Launches one engine in its own process and records its peak memory."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "src.benchmark", "--child", engine, scope],
        cwd=PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, **(env or {})},
    )
    ps = psutil.Process(proc.pid)
    peak, killed = 0, False
    while proc.poll() is None:
        peak = max(peak, _tree_rss(ps))
        if peak > MEMORY_LIMIT_GB * 1024 ** 3:
            for p in [*ps.children(recursive=True), ps]:
                try:
                    p.kill()
                except psutil.Error:
                    pass
            killed = True
            break
        time.sleep(0.05)
    out, err = proc.communicate()
    row = {"engine": engine, "scope": scope, "peak_rss_gb": round(peak / 1024 ** 3, 2)}
    if killed:
        return {**row, "status": f"stopped above {MEMORY_LIMIT_GB:.0f} GB"}
    if proc.returncode != 0:
        return {**row, "status": "failed", "error": err.strip().splitlines()[-1] if err else ""}
    data = json.loads(next(l for l in out.splitlines() if l.startswith("RESULT "))[7:])
    return {**row, "status": "ok", **{k: round(v, 3) if isinstance(v, float) else v
                                      for k, v in data.items()}}


def spark_available() -> bool:
    return bool(os.environ.get("JAVA_HOME"))


def run_benchmark(scopes=("october", "full")) -> list[dict]:
    rows = []
    for scope in scopes:
        for engine in ENGINES:
            if engine == "pyspark" and not spark_available():
                rows.append({"engine": engine, "scope": scope, "status": "skipped: no JAVA_HOME"})
                continue
            rows.append(measure(engine, scope))
            print(rows[-1], flush=True)
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", nargs=2, metavar=("ENGINE", "SCOPE"))
    parser.add_argument("--out", default=str(PROJECT_ROOT / "reports" / "benchmark.json"))
    args = parser.parse_args()
    if args.child:
        print("RESULT " + json.dumps(_child(*args.child)), flush=True)
    else:
        rows = run_benchmark()
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(rows, indent=2), encoding="utf-8")
