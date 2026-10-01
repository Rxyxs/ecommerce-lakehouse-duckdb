[ 🇺🇸 English ] | [ 🇨🇱 [Leer en Español](README.es.md) ]

# Modern Lakehouse Analytics Pipeline

![Python](https://img.shields.io/badge/python-3.12-blue?logo=python&logoColor=white)
![Polars](https://img.shields.io/badge/polars-⚡_rust--backed-CD792C)
![DuckDB](https://img.shields.io/badge/duckdb-embedded_OLAP-FFF000?logo=duckdb&logoColor=black)
![PyArrow](https://img.shields.io/badge/pyarrow-columnar-2C5BB4)
![Pydantic](https://img.shields.io/badge/pydantic-v2-E92063?logo=pydantic&logoColor=white)
![Tests](https://github.com/Rxyxs/ecommerce-lakehouse-duckdb/actions/workflows/tests.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-green)

A modern data pipeline for ingesting and analyzing clickstream/e-commerce events at scale, built on a **lakehouse** of partitioned Parquet files: Polars for multithreaded ETL, PyArrow for columnar partitioning, and DuckDB as the embedded analytical SQL engine that queries the lake directly without loading it into RAM.

## Business use case

An e-commerce site generates clickstream events (page views, product views, carts, checkouts, purchases) at a volume that no longer fits comfortably in an in-memory `pandas.DataFrame`. This project simulates that scenario end to end:

1. **Generates** 100,000+ synthetic events with a realistic conversion funnel.
2. **Ingests** those events: cleans them, validates the schema contract, and persists them as a Parquet data lake partitioned by date.
3. **Analyzes** the lake with SQL run directly over Parquet (DuckDB) to answer business questions: where do people drop out of the funnel? which cohorts retain best? who are the highest-value customers?
4. **Compares** the approach against a traditional Pandas pipeline, measuring time and memory.

## Architecture

```
┌───────────────────────────┐
│ synthetic_clickstream.py  │   generates events (page_view -> ... -> purchase)
└─────────────┬──────────────┘
              │
              ▼
   data/raw/clickstream_events.parquet   (unpartitioned, ~100k+ rows)
              │
              ▼
┌───────────────────────────┐
│      ingestion.py          │   Polars: dedup, numeric sanity, partition columns
│  (Pydantic v2 contract)    │   PyArrow: Hive-style partitioned write
└─────────────┬──────────────┘
              │
              ▼
   data/lakehouse/events/
   ├── year=2025/month=01/day=01/part-0.parquet
   ├── year=2025/month=01/day=02/part-0.parquet
   └── ...                                  (1 Parquet file per day)
              │
              ▼
┌───────────────────────────┐
│       analytics.py         │   DuckDB: SQL straight over Parquet, nothing loaded into RAM
└─────────────┬──────────────┘
              │
   ┌──────────┼───────────┬───────────────┐
   ▼          ▼            ▼               ▼
Conversion  Cohorts /    Top-LTV         Revenue
 funnel     retention    customers      by category
```

## Project structure

```
ecommerce-lakehouse-duckdb/
├── data/
│   ├── raw/                          # clickstream_events.parquet (generated, untracked)
│   └── lakehouse/                    # events/year=/month=/day=/*.parquet (generated, untracked)
├── src/
│   ├── generators/
│   │   └── synthetic_clickstream.py  # Generates 100,000+ synthetic clickstream events
│   ├── lakehouse/
│   │   ├── ingestion.py              # Polars + PyArrow: cleaning, schema contract, partitioning
│   │   └── analytics.py              # DuckDB: funnel, cohorts, LTV, revenue by category
│   └── benchmark.py                  # Pandas vs. Polars vs. DuckDB: time and RAM
├── tests/
│   └── test_lakehouse.py             # Schema and metric tests (pytest)
├── .github/workflows/tests.yml       # CI: runs pytest on every push/PR
├── requirements.txt
└── README.md
```

## Synthetic dataset

`src/generators/synthetic_clickstream.py` simulates 8,000 users split across three engagement segments (`one_time`, `casual`, `loyal`, each with a different session count) over roughly 90 days. Every session walks a funnel with a realistic conversion drop at each step:

```
page_view -> product_view -> add_to_cart -> checkout_start -> purchase
  100%          ~60%             ~38%            ~55%            ~65%
                                                          (conditional on the previous step)
```

With the default seed this produces **~112,000 events** (above the 100,000 minimum asked for), with columns: `event_id`, `user_id`, `session_id`, `event_type`, `event_timestamp`, `product_id`, `category`, `price`, `quantity`, `revenue`, `device_type`, `country`, `referrer_source`.

## Modules

### `src/lakehouse/ingestion.py`
- **Cleaning (Polars)**: deduplicates on `event_id`, drops rows missing `user_id`/`event_timestamp`, and nulls out (rather than invents) negative `price`/`quantity`/`revenue`.
- **Schema contract (Pydantic v2)**: `ClickstreamEventContract` validates that the required columns exist, and type-validates a sample of rows rather than all of them — at this volume, row-by-row validation defeats the purpose of a high-throughput pipeline. A contract violation raises: it signals an ingestion bug, not an invalid business record to quarantine.
- **Partitioning (PyArrow)**: writes the result as Hive-partitioned Parquet (`year=/month=/day=`), with `existing_data_behavior="delete_matching"` so reprocessing is idempotent.

### `src/lakehouse/analytics.py`
Four business queries, all pure SQL executed by DuckDB directly against the Parquet files (`read_parquet(..., hive_partitioning=true)`):
- `conversion_funnel` — sessions reaching each funnel step.
- `cohort_retention` — active users per month, grouped by the month of their first event.
- `customer_ltv` — top customers by total purchase revenue.
- `revenue_by_category` — revenue and average ticket per category.

### `src/benchmark.py`
Runs the same aggregation (revenue by category, purchases only) with Pandas, Polars and DuckDB over the same Parquet file, measuring wall time (`time.perf_counter`) and the process's RSS memory delta (`psutil`).

## Benchmark results

A real run over ~112,000 events (see the methodology note below):

| Engine  | Time (s) | Δ RAM (MB) |
|---------|---------:|-----------:|
| DuckDB  |    ~0.04 |        ~6  |
| Polars  |    ~0.05 |       ~37  |
| Pandas  |    ~0.12 |       ~60  |

DuckDB wins on both dimensions because it never materializes the whole file as a Python object — it aggregates directly over the Parquet scan. Polars is ~2.5x faster than Pandas even while materializing the full DataFrame, thanks to its multithreaded Rust engine.

**Methodology and honest limitations**: the timing is a single run per engine (`time.perf_counter`); a rigorous benchmark would average several runs. The memory figure is an RSS delta of the *current* process between before and after each run (with a `gc.collect()` in between) — not an isolated peak per subprocess, so it's an order-of-magnitude approximation rather than strict memory profiling. I reproduced the numbers three times (`python -m src.benchmark`) before documenting them, and the relative ordering was stable across runs.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate      # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

```bash
# 1. Generate the synthetic dataset (data/raw/clickstream_events.parquet)
python -m src.generators.synthetic_clickstream

# 2. Ingest: clean, validate the contract, write the partitioned lake
python -m src.lakehouse.ingestion

# 3. Run the analytical queries over the lake
python -m src.lakehouse.analytics

# 4. Compare Pandas vs. Polars vs. DuckDB
python -m src.benchmark

# Unit tests
pytest tests/
```

## Tech stack

| Tool | Role |
|---|---|
| **Polars** | ETL — multithreaded, Rust-backed cleaning and transformation |
| **DuckDB** | Embedded analytical SQL engine, queries Parquet without loading it into RAM |
| **PyArrow** | Hive-style partitioned Parquet writes |
| **Pydantic v2** | Schema contract for the clickstream event |
| **pytest** | Unit tests for cleaning, schema contract and metrics |
| **pandas / psutil** | Only in `benchmark.py`, as the comparison baseline and for RAM measurement |
