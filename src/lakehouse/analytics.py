"""Consultas analíticas de negocio (embudo, cohortes, LTV, ingresos) sobre el Data Lake.

Todas las consultas corren con DuckDB directamente sobre los archivos Parquet
particionados vía `read_parquet(..., hive_partitioning=true)` — el dataset completo
nunca se materializa en un DataFrame de Python antes de agregarse.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LAKE_PATH = PROJECT_ROOT / "data" / "lakehouse" / "events"


def connect(lake_dir: Path = LAKE_PATH) -> duckdb.DuckDBPyConnection:
    """Abre una conexión DuckDB en memoria con una vista `events` sobre el lake particionado."""
    glob_pattern = str(Path(lake_dir) / "**" / "*.parquet")
    con = duckdb.connect(database=":memory:")
    con.execute(
        f"CREATE OR REPLACE VIEW events AS "
        f"SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true)"
    )
    return con


def conversion_funnel(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Cuenta cuántas sesiones alcanzaron cada paso del embudo de conversión."""
    query = """
        WITH funnel AS (
            SELECT
                session_id,
                MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)      AS reached_page_view,
                MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END)   AS reached_product_view,
                MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)    AS reached_add_to_cart,
                MAX(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS reached_checkout,
                MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)       AS reached_purchase
            FROM events
            GROUP BY session_id
        )
        SELECT
            SUM(reached_page_view)     AS page_view_sessions,
            SUM(reached_product_view)  AS product_view_sessions,
            SUM(reached_add_to_cart)   AS add_to_cart_sessions,
            SUM(reached_checkout)      AS checkout_sessions,
            SUM(reached_purchase)      AS purchase_sessions
        FROM funnel
    """
    return con.execute(query).pl()


def cohort_retention(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Usuarios activos por mes, agrupados por el mes de su primer evento (cohorte)."""
    query = """
        WITH first_seen AS (
            SELECT user_id, date_trunc('month', MIN(event_timestamp)) AS cohort_month
            FROM events
            GROUP BY user_id
        ),
        activity AS (
            SELECT
                e.user_id,
                f.cohort_month,
                date_trunc('month', e.event_timestamp) AS activity_month
            FROM events e
            JOIN first_seen f USING (user_id)
        )
        SELECT
            cohort_month,
            activity_month,
            DATEDIFF('month', cohort_month, activity_month) AS months_since_signup,
            COUNT(DISTINCT user_id) AS active_users
        FROM activity
        GROUP BY cohort_month, activity_month
        ORDER BY cohort_month, activity_month
    """
    return con.execute(query).pl()


def customer_ltv(con: duckdb.DuckDBPyConnection, top_n: int = 20) -> pl.DataFrame:
    """Top `top_n` clientes por valor de vida (ingresos totales de compras)."""
    query = f"""
        SELECT
            user_id,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_count,
            COALESCE(SUM(revenue), 0) AS lifetime_value
        FROM events
        GROUP BY user_id
        ORDER BY lifetime_value DESC
        LIMIT {top_n}
    """
    return con.execute(query).pl()


def revenue_by_category(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Ingresos totales y ticket promedio por categoría de producto (solo compras)."""
    query = """
        SELECT
            category,
            COUNT(*) AS purchase_count,
            SUM(revenue) AS total_revenue,
            AVG(revenue) AS avg_order_value
        FROM events
        WHERE event_type = 'purchase'
        GROUP BY category
        ORDER BY total_revenue DESC
    """
    return con.execute(query).pl()


if __name__ == "__main__":
    # La consola de Windows (cp1252 por defecto) no puede codificar los caracteres
    # Unicode que polars usa al imprimir (bordes de tabla, "μs" en dtypes, etc.).
    sys.stdout.reconfigure(encoding="utf-8")
    pl.Config.set_ascii_tables(True)

    con = connect()

    print("=== Embudo de conversión (sesiones que alcanzan cada paso) ===")
    print(conversion_funnel(con))

    print("\n=== Ingresos por categoría ===")
    print(revenue_by_category(con))

    print("\n=== Top 20 clientes por LTV ===")
    print(customer_ltv(con))

    print("\n=== Retención por cohorte (usuarios activos por mes) ===")
    print(cohort_retention(con))
