"""Pruebas de esquema y métricas para el pipeline de lakehouse."""
from datetime import datetime

import polars as pl
import pytest
from pydantic import ValidationError

from src.lakehouse import analytics
from src.lakehouse.ingestion import clean_events, validate_schema_contract, write_partitioned_lakehouse


def _sample_events() -> pl.DataFrame:
    """4 eventos controlados: un duplicado exacto, una cantidad negativa, dos sesiones."""
    rows = [
        {
            "event_id": "e1", "user_id": "u1", "session_id": "s1", "event_type": "page_view",
            "event_timestamp": datetime(2025, 1, 5, 10, 0), "product_id": None, "category": None,
            "price": None, "quantity": None, "revenue": None, "device_type": "mobile",
            "country": "US", "referrer_source": "organic_search",
        },
        {
            "event_id": "e2", "user_id": "u1", "session_id": "s1", "event_type": "purchase",
            "event_timestamp": datetime(2025, 1, 5, 10, 5), "product_id": "P-01", "category": "Books",
            "price": 20.0, "quantity": 2, "revenue": 40.0, "device_type": "mobile",
            "country": "US", "referrer_source": "organic_search",
        },
        {
            "event_id": "e2", "user_id": "u1", "session_id": "s1", "event_type": "purchase",  # duplicado exacto
            "event_timestamp": datetime(2025, 1, 5, 10, 5), "product_id": "P-01", "category": "Books",
            "price": 20.0, "quantity": 2, "revenue": 40.0, "device_type": "mobile",
            "country": "US", "referrer_source": "organic_search",
        },
        {
            "event_id": "e3", "user_id": "u2", "session_id": "s2", "event_type": "purchase",
            "event_timestamp": datetime(2025, 2, 10, 9, 0), "product_id": "P-02", "category": "Electronics",
            "price": 100.0, "quantity": -1, "revenue": -100.0, "device_type": "desktop",  # cantidad invalida
            "country": "MX", "referrer_source": "paid_search",
        },
    ]
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# ingestion.clean_events
# ---------------------------------------------------------------------------

def test_clean_events_deduplicates_by_event_id():
    df = clean_events(_sample_events())
    assert df.filter(pl.col("event_id") == "e2").height == 1


def test_clean_events_derives_zero_padded_partition_columns():
    df = clean_events(_sample_events())
    row = df.filter(pl.col("event_id") == "e1").to_dicts()[0]
    assert (row["year"], row["month"], row["day"]) == ("2025", "01", "05")


def test_clean_events_nulls_out_negative_quantity_and_revenue():
    df = clean_events(_sample_events())
    row = df.filter(pl.col("event_id") == "e3").to_dicts()[0]
    assert row["quantity"] is None
    assert row["revenue"] is None
    assert row["price"] == 100.0  # el precio en si no era negativo


# ---------------------------------------------------------------------------
# ingestion.validate_schema_contract
# ---------------------------------------------------------------------------

def test_validate_schema_contract_raises_on_missing_column():
    df = clean_events(_sample_events()).drop("device_type")
    with pytest.raises(ValueError):
        validate_schema_contract(df)


def test_validate_schema_contract_passes_for_clean_data():
    df = clean_events(_sample_events())
    validate_schema_contract(df)  # no debe lanzar


def test_validate_schema_contract_raises_on_type_mismatch():
    df = clean_events(_sample_events())
    df = df.with_columns(pl.lit("not-a-number").alias("price"))
    with pytest.raises(ValidationError):
        validate_schema_contract(df, sample_size=df.height)


# ---------------------------------------------------------------------------
# analytics (sobre un lakehouse de prueba escrito con la misma función de ingestión)
# ---------------------------------------------------------------------------

@pytest.fixture
def lake_dir(tmp_path):
    df = clean_events(_sample_events())
    output_dir = tmp_path / "events"
    write_partitioned_lakehouse(df, output_dir)
    return output_dir


def test_revenue_by_category_aggregates_only_purchases(lake_dir):
    con = analytics.connect(lake_dir)
    result = analytics.revenue_by_category(con)
    books = result.filter(pl.col("category") == "Books").to_dicts()[0]
    assert books["total_revenue"] == 40.0
    assert books["purchase_count"] == 1

    # El purchase de Electronics sigue contando como compra; su revenue quedo nulo
    # en la limpieza (cantidad invalida), asi que no debe inventarsele un monto.
    electronics = result.filter(pl.col("category") == "Electronics").to_dicts()[0]
    assert electronics["purchase_count"] == 1
    assert electronics["total_revenue"] is None


def test_customer_ltv_sums_revenue_per_user(lake_dir):
    con = analytics.connect(lake_dir)
    result = analytics.customer_ltv(con)
    u1 = result.filter(pl.col("user_id") == "u1").to_dicts()[0]
    assert u1["lifetime_value"] == 40.0
    assert u1["purchase_count"] == 1


def test_conversion_funnel_counts_sessions_per_step(lake_dir):
    con = analytics.connect(lake_dir)
    result = analytics.conversion_funnel(con).to_dicts()[0]
    assert result["page_view_sessions"] == 1  # solo s1 tiene un page_view explicito
    assert result["purchase_sessions"] == 2  # s1 y s2 llegaron a purchase


def test_cohort_retention_assigns_cohort_by_first_event_month(lake_dir):
    con = analytics.connect(lake_dir)
    result = analytics.cohort_retention(con)
    u1_rows = result.filter(pl.col("months_since_signup") == 0)
    assert u1_rows.height >= 1
