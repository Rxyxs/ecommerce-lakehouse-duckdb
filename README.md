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
│   ├── benchmark.py                  # Pandas vs. Polars vs. DuckDB: time and RAM
│   └── make_figures.py               # README figures, from the same queries
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

![Conversion funnel and its step rates against the generator's design](outputs/figures/conversion_funnel.png)

#### The funnel measures something slightly different from what the generator configures

37,141 sessions enter and 3,037 purchase, an 8.2% end-to-end rate. Three of the four step rates land on the generator's parameters: 86.5% against 86.7% implied, 54.4% against 55%, 65.5% against 65%. The second does not — it reads **26.5% where the generator applies 38%**.

This is not a bug in either the query or the generator. `_simulate_session` emits 0–2 browsing `product_view` events *before* the funnel branch, independently of the funnel probability:

```python
emit("page_view")
for _ in range(rng.randint(0, 2)):        # browsing, outside the funnel
    emit("product_view", product=rng.choice(catalog))
if rng.random() < 0.60:                   # the funnel's own product_view
    emit("product_view", product=product)
    if rng.random() < 0.38:
        emit("add_to_cart", product=product)
```

So a session reaches `product_view` either by browsing or through the funnel: P = 1 − (1/3)(1 − 0.60) = **86.7%**, which is what the first step measures. But `add_to_cart` is reachable only through the funnel branch: P = 0.60 × 0.38 = **22.8% of all sessions**, and the lake gives 8,514/37,141 = 22.9%. Both numbers are exactly right. The 26.5% step rate is low because its *denominator* includes browsing-only sessions that were never in the funnel at all.

That is the ordinary failure mode of funnel analysis on real clickstream: an event type that fires both inside and outside the funnel corrupts the step rate that uses it as a denominator, while leaving every absolute count correct. The fix is to define the funnel over sessions with purchase intent rather than over all sessions — which the raw events support, and which this query deliberately does not do, so the distinction stays visible.

![Revenue and order volume by category](outputs/figures/revenue_by_category.png)

Revenue is concentrated: Electronics is 58.7% of the total on 549 purchases, the second-smallest order count. Purchases are nearly flat across the six categories (449–549), so the revenue ranking is almost entirely average order value — $1,255 for Electronics against $52 for Books, a 24x spread that comes straight from the generator's per-category price ranges.

![Cohort retention heatmap](outputs/figures/cohort_retention.png)

The cohort query works and the three cohorts sum to exactly the 8,000 users generated. The *result*, though, is a check on the data rather than a finding: the January cohort goes 100% → 49% → 50%, rising in month 2. Real retention decays. It does not here because the generator draws each user's sessions uniformly across the whole 90-day window, so a user is equally likely to be active in any month. There is no churn in this data to find, and the honest thing is to say so rather than present a flat curve as a retention insight.

### `src/benchmark.py`
Runs the same aggregation (revenue by category, purchases only) with Pandas, Polars and DuckDB over the same Parquet file, measuring wall time (`time.perf_counter`) and the process's RSS memory delta (`psutil`).

## Benchmark results

A real run over 112,608 events:

![Engine benchmark: wall time and memory](outputs/figures/engine_benchmark.png)

| Engine | Cold: time (s) | Cold: Δ RSS (MB) | Warm: median time (s) | Warm: median Δ RSS (MB) |
|--------|---------------:|-----------------:|----------------------:|------------------------:|
| DuckDB |         0.0182 |              8.7 |                0.0155 |                     0.4 |
| Polars |         0.0212 |             43.1 |                0.0147 |                    12.0 |
| Pandas |         0.1116 |             87.6 |                0.0776 |                    21.3 |

**The result that survives repetition is that both DuckDB and Polars are about 5x faster than pandas.** The gap between DuckDB and Polars is not one this benchmark can call: measured cold, DuckDB is ahead on both dimensions; measured warm over 7 runs, Polars has the lower median and the two ranges overlap. On a single aggregation over 112k rows the two are the same engine class, and claiming a winner between them would be reading noise.

Against pandas the mechanism is real and shows up either way: DuckDB aggregates over the Parquet scan without materialising the file as a Python object, and Polars materialises it but does the work in a multithreaded Rust engine.

**On the memory column — it is two different numbers, and the distinction matters.** The RSS delta is measured cold (one run in a freshly spawned interpreter, so the delta includes allocation) and warm (the median of repeats inside one process). They differ by about 10x for DuckDB, because after the first run the allocator already holds the pages and the delta of run 2 measures almost nothing. The warm figure is not a smaller measurement of the same thing; it is a measurement of something else, and **the cold column is the one to quote**.

The cold column comes from `src/make_figures.py`, which spawns one fresh interpreter per engine. `src/benchmark.py` instead runs all three sequentially in a single process, so only the first engine it measures is genuinely cold; in practice its numbers land close to the cold column (6.5 / 42.8 / 87.8 MB on the run checked here) because each engine allocates its own structures, but that is a property of this workload, not a guarantee.

Neither column is peak-memory profiling, so read both as orders of magnitude rather than precise costs.

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

# 5. Redraw the README figures (repeats the benchmark cold and warm)
python -m src.make_figures

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
| **matplotlib** | Only in `make_figures.py`, which draws the figures above from the same queries the analytics module runs |
