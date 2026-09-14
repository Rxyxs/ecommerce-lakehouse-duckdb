"""Ingesta y transformación de eventos de clickstream hacia un Data Lake Parquet.

Usa Polars para limpieza/transformación (multihilo, respaldado en Rust) y PyArrow
para escribir el resultado particionado estilo Hive (`year=/month=/day=`), que
`src/lakehouse/analytics.py` luego consulta directamente con DuckDB sin cargarlo
completo a RAM.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = PROJECT_ROOT / "data" / "raw" / "clickstream_events.parquet"
LAKE_PATH = PROJECT_ROOT / "data" / "lakehouse" / "events"

PARTITION_COLUMNS = ["year", "month", "day"]


class ClickstreamEventContract(BaseModel):
    """Contrato de esquema de un evento de clickstream ya limpio.

    Se usa para (a) validar que el DataFrame tiene las columnas esperadas antes de
    persistir, y (b) validar por tipo una muestra de filas — no todas, ya que a este
    volumen de datos una validación fila-por-fila completa contradice el propósito de
    un pipeline de alto rendimiento.
    """

    event_id: str
    user_id: str
    session_id: str
    event_type: str
    event_timestamp: datetime
    product_id: Optional[str] = None
    category: Optional[str] = None
    price: Optional[float] = None
    quantity: Optional[int] = None
    revenue: Optional[float] = None
    device_type: str
    country: str
    referrer_source: str


def validate_schema_contract(
    df: pl.DataFrame, contract: type[BaseModel] = ClickstreamEventContract, sample_size: int = 500
) -> None:
    """Valida `df` contra `contract`: columnas requeridas presentes + tipos en una muestra.

    Lanza `ValueError`/`pydantic.ValidationError` si el contrato se rompe — a diferencia
    de un pipeline de calidad de datos que aísla filas inválidas, aquí una violación de
    esquema indica un bug de ingestión que debe fallar ruidosamente, no ocultarse.
    """
    expected_columns = set(contract.model_fields.keys())
    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas por el contrato de esquema: {sorted(missing)}")

    if df.height == 0:
        return

    sample = df.select(sorted(expected_columns)).sample(n=min(sample_size, df.height), seed=42)
    for record in sample.to_dicts():
        contract(**record)


def clean_events(df: pl.DataFrame) -> pl.DataFrame:
    """Limpieza y transformación principal: dedup, sanidad numérica, columnas de partición."""
    df = df.unique(subset=["event_id"], keep="first")
    df = df.filter(pl.col("user_id").is_not_null() & pl.col("event_timestamp").is_not_null())

    df = df.with_columns([
        pl.col("event_timestamp").dt.strftime("%Y").alias("year"),
        pl.col("event_timestamp").dt.strftime("%m").alias("month"),
        pl.col("event_timestamp").dt.strftime("%d").alias("day"),
    ])

    # Ningún monto/cantidad puede ser negativo; se anula en vez de inventar un valor.
    for column in ("price", "quantity", "revenue"):
        df = df.with_columns(
            pl.when(pl.col(column) < 0).then(None).otherwise(pl.col(column)).alias(column)
        )

    return df


def write_partitioned_lakehouse(df: pl.DataFrame, output_dir: Path = LAKE_PATH) -> Path:
    """Escribe `df` como Parquet particionado Hive-style (year=/month=/day=) vía PyArrow.

    `existing_data_behavior="delete_matching"` hace la escritura idempotente: al
    reprocesar, los archivos previos de las particiones tocadas se reemplazan en vez
    de acumularse.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    table = df.to_arrow()
    partitioning = ds.partitioning(
        pa.schema([(col, pa.string()) for col in PARTITION_COLUMNS]), flavor="hive"
    )
    ds.write_dataset(
        table,
        base_dir=str(output_dir),
        format="parquet",
        partitioning=partitioning,
        existing_data_behavior="delete_matching",
    )
    return output_dir


def run_ingestion(raw_path: Path = RAW_PATH, output_dir: Path = LAKE_PATH) -> Path:
    """Pipeline completo: leer crudo -> limpiar -> validar contrato -> escribir particionado."""
    df = pl.read_parquet(raw_path)
    df = clean_events(df)
    validate_schema_contract(df)
    return write_partitioned_lakehouse(df, output_dir)


if __name__ == "__main__":
    output_dir = run_ingestion()
    n_files = sum(1 for _ in output_dir.rglob("*.parquet"))
    print(f"Lakehouse particionado escrito en {output_dir}")
    print(f"Archivos Parquet: {n_files}")
