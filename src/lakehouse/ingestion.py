"""Ingests the REES46 clickstream into a Parquet lake partitioned by day.

Source: *eCommerce behavior data from multi category store* (Kaggle, ``mkechinov``, data
from the REES46 / Open CDP project), October and November 2019: 109,950,743 events in two
CSVs of 5.7 and 9.0 GB. The data is the authors' (license "copyright-authors"): it is
downloaded by each user and never committed; only aggregates are published.

DuckDB streams the CSVs into Parquet without holding them in memory, and the cleaning is
SQL so it runs at that scale:

- 130,739 rows are exact duplicates of another row: removed.
- 256,761 events carry a price of zero or less: the price is nulled, not the event (the
  view still happened).
- 12 events have no session: removed, since every session-level metric needs one.
- 32% of events have no ``category_code`` (the store only codes "meaningful" categories,
  per the dataset description): kept, labelled ``uncoded``, and reported as such.

The lake is Hive-partitioned ``year=/month=/day=`` and rewritten from scratch on every run.
"""
from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_FILES = ["2019-Oct.csv", "2019-Nov.csv"]
LAKE_PATH = PROJECT_ROOT / "data" / "lakehouse" / "events"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
PARTITION_COLUMNS = ["year", "month", "day"]


class ClickstreamEventContract(BaseModel):
    """Schema of a clean event in the lake. Validated on a sample of rows, not all of them:
    at 110 million rows a row-by-row check would defeat the point of the pipeline."""

    event_time: datetime
    event_type: str
    product_id: int
    category_id: int
    category_code: Optional[str] = None
    top_category: str
    brand: Optional[str] = None
    price: Optional[float] = None
    user_id: int
    user_session: str


def _source(files: list[Path]) -> str:
    paths = ", ".join(f"'{p.as_posix()}'" for p in files)
    return f"read_csv([{paths}], header=true, timestampformat='{TIMESTAMP_FORMAT}')"


def cleaning_sql(files: list[Path]) -> str:
    """The cleaned events as one SELECT, with partition columns."""
    return f"""
        SELECT
            event_time,
            event_type,
            product_id,
            category_id,
            category_code,
            COALESCE(split_part(category_code, '.', 1), 'uncoded') AS top_category,
            brand,
            CASE WHEN price > 0 THEN price END AS price,
            user_id,
            user_session,
            strftime(event_time, '%Y') AS year,
            strftime(event_time, '%m') AS month,
            strftime(event_time, '%d') AS day
        FROM (SELECT DISTINCT * FROM {_source(files)})
        WHERE user_session IS NOT NULL
    """


def quality_report(con: duckdb.DuckDBPyConnection, files: list[Path]) -> dict:
    """What the raw files contain before cleaning."""
    src = _source(files)
    row = con.execute(f"""
        SELECT count(*), sum((price <= 0)::int), sum((user_session IS NULL)::int),
               sum((category_code IS NULL)::int), sum((brand IS NULL)::int)
        FROM {src}
    """).fetchall()[0]
    distinct = con.execute(f"SELECT count(*) FROM (SELECT DISTINCT * FROM {src})").fetchall()[0][0]
    return {
        "raw_events": int(row[0]),
        "exact_duplicates": int(row[0] - distinct),
        "price_zero_or_negative": int(row[1]),
        "events_without_session": int(row[2]),
        "events_without_category": int(row[3]),
        "events_without_brand": int(row[4]),
    }


def validate_schema_contract(
    df: pl.DataFrame, contract: type[BaseModel] = ClickstreamEventContract, sample_size: int = 500
) -> None:
    """Fails loudly if a column is missing or a sampled row does not fit the contract: a
    schema break is an ingestion bug, not a record to quarantine."""
    missing = set(contract.model_fields) - set(df.columns)
    if missing:
        raise ValueError(f"columns required by the schema contract are missing: {sorted(missing)}")
    if df.height == 0:
        return
    sample = df.select(sorted(contract.model_fields)).sample(n=min(sample_size, df.height), seed=42)
    for record in sample.to_dicts():
        contract(**record)


def write_lake(files: list[Path], output_dir: Path = LAKE_PATH, threads: int | None = None) -> dict:
    """Cleans the CSVs, writes the partitioned lake, validates a sample against the contract
    and returns the quality report plus what was written."""
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    if threads:
        con.execute(f"SET threads={threads}").fetchall()
    report = quality_report(con, files)
    con.execute(
        f"COPY ({cleaning_sql(files)}) TO '{output_dir.as_posix()}' "
        f"(FORMAT PARQUET, PARTITION_BY ({', '.join(PARTITION_COLUMNS)}), COMPRESSION ZSTD)"
    ).fetchall()
    lake = f"read_parquet('{output_dir.as_posix()}/**/*.parquet', hive_partitioning=true)"
    report["lake_events"] = int(con.execute(f"SELECT count(*) FROM {lake}").fetchall()[0][0])
    report["lake_files"] = sum(1 for _ in output_dir.rglob("*.parquet"))
    report["lake_bytes"] = sum(p.stat().st_size for p in output_dir.rglob("*.parquet"))
    sample = con.sql(f"SELECT * FROM {lake} USING SAMPLE 2000 ROWS").pl()
    validate_schema_contract(sample)
    con.close()
    return report


def run_ingestion(raw_dir: Path = RAW_DIR, output_dir: Path = LAKE_PATH) -> dict:
    files = [raw_dir / name for name in RAW_FILES]
    missing = [f for f in files if not f.exists()]
    if missing:
        raise FileNotFoundError(f"{missing} not found: run `python -m src.sources` first")
    return write_lake(files, output_dir)


if __name__ == "__main__":
    import json
    report = run_ingestion()
    out = PROJECT_ROOT / "reports" / "ingestion.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(report)
