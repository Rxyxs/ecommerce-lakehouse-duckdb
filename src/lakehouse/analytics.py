"""Business and data-quality queries over the REES46 lake, all in DuckDB SQL.

Two things in the data change what the business numbers mean, and both are measured here
before any funnel or revenue figure is reported:

- **Purchase capture gaps.** Some hours carry thousands of events and not one purchase.
  An hour of a store this size with >= ``GAP_MIN_EVENTS`` events and zero purchases is not
  a quiet hour, it is a logging gap; days that contain one are flagged and left out of the
  daily revenue series and the funnels.
- **Cart tracking regimes.** The number of cart events per purchase moves in steps, from
  one day to the next, while purchases stay flat. Customers do not change habits
  overnight; the tracking does. The steps are located with binary segmentation on the
  daily log ratio and each funnel is reported per regime, never pooled across them.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from src.lakehouse.ingestion import LAKE_PATH

GAP_MIN_EVENTS = 5_000
MAX_REGIME_BREAKS = 10
MIN_SEGMENT_DAYS = 3
MIN_STEP_RATIO = 1.3


def connect(lake: Path = LAKE_PATH, threads: int | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if threads:
        con.execute(f"SET threads={threads}")
    con.execute(
        "CREATE VIEW events AS SELECT * FROM "
        f"read_parquet('{lake.as_posix()}/**/*.parquet', hive_partitioning=true)"
    )
    return con


def overview(con) -> dict:
    row = con.execute("""
        SELECT count(*), count(DISTINCT user_id), count(DISTINCT user_session),
               count(DISTINCT product_id), min(event_time)::DATE, max(event_time)::DATE
        FROM events
    """).fetchone()
    return {"events": row[0], "users": row[1], "sessions": row[2], "products": row[3],
            "first_day": str(row[4]), "last_day": str(row[5])}


def daily(con, gap_min_events: int = GAP_MIN_EVENTS) -> pd.DataFrame:
    """One row per day: event counts, revenue and how many busy hours had no purchase."""
    return con.sql(f"""
        WITH hourly AS (
            SELECT event_time::DATE AS day, hour(event_time) AS hour, count(*) AS n,
                   count(*) FILTER (WHERE event_type = 'purchase') AS purchases
            FROM events GROUP BY 1, 2
        ), gaps AS (
            SELECT day, sum((n >= {gap_min_events} AND purchases = 0)::INT) AS gap_hours
            FROM hourly GROUP BY 1
        ), days AS (
            SELECT event_time::DATE AS day,
                   count(*) FILTER (WHERE event_type = 'view') AS views,
                   count(*) FILTER (WHERE event_type = 'cart') AS carts,
                   count(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                   coalesce(sum(price) FILTER (WHERE event_type = 'purchase'), 0) AS revenue,
                   count(DISTINCT user_id) AS users
            FROM events GROUP BY 1
        )
        SELECT days.*, gaps.gap_hours, gaps.gap_hours > 0 AS capture_gap
        FROM days JOIN gaps USING (day) ORDER BY day
    """).df()


def _best_split(y: np.ndarray, min_size: int) -> tuple[int, float]:
    """Index that most reduces the squared error of a two-mean fit, and that reduction."""
    best, gain = -1, 0.0
    total = ((y - y.mean()) ** 2).sum()
    for i in range(min_size, len(y) - min_size + 1):
        left, right = y[:i], y[i:]
        g = total - ((left - left.mean()) ** 2).sum() - ((right - right.mean()) ** 2).sum()
        if g > gain:
            best, gain = i, g
    return best, gain


def segment(y: np.ndarray, max_breaks: int = MAX_REGIME_BREAKS, min_size: int = MIN_SEGMENT_DAYS,
            min_step: float = np.log(MIN_STEP_RATIO)) -> list[int]:
    """Binary segmentation: keep splitting the segment whose best split gains most, while
    the two sides of that split differ by at least ``min_step``."""
    breaks: list[int] = []
    for _ in range(max_breaks):
        edges = [0, *sorted(breaks), len(y)]
        candidates = []
        for a, b in zip(edges[:-1], edges[1:]):
            if b - a >= 2 * min_size:
                i, g = _best_split(y[a:b], min_size)
                if i > 0 and abs(y[a:a + i].mean() - y[a + i:b].mean()) >= min_step:
                    candidates.append((g, a + i))
        if not candidates:
            break
        breaks.append(max(candidates)[1])
    return sorted(breaks)


def cart_regimes(day_table: pd.DataFrame) -> list[dict]:
    """Date ranges with a stable cart/purchase ratio, located on the days without gaps."""
    clean = day_table[~day_table["capture_gap"]].reset_index(drop=True)
    y = np.log(clean["carts"] / clean["purchases"]).to_numpy()
    edges = [0, *segment(y), len(clean)]
    regimes = []
    for a, b in zip(edges[:-1], edges[1:]):
        part = clean.iloc[a:b]
        regimes.append({
            "start": str(part["day"].iloc[0].date()),
            "end": str(part["day"].iloc[-1].date()),
            "days": int(b - a),
            "carts_per_purchase": round(float(part["carts"].sum() / part["purchases"].sum()), 2),
        })
    return regimes


def _clean_days_sql(day_table: pd.DataFrame) -> str:
    bad = day_table.loc[day_table["capture_gap"], "day"].dt.strftime("'%Y-%m-%d'").tolist()
    return f"event_time::DATE NOT IN ({', '.join(bad)})" if bad else "TRUE"


def session_funnel(con, start: str, end: str, day_table: pd.DataFrame) -> dict:
    """View -> cart -> purchase at session level inside one tracking regime."""
    row = con.execute(f"""
        WITH s AS (
            SELECT user_session,
                   bool_or(event_type = 'view') AS v, bool_or(event_type = 'cart') AS c,
                   bool_or(event_type = 'purchase') AS p
            FROM events
            WHERE event_time::DATE BETWEEN '{start}' AND '{end}' AND {_clean_days_sql(day_table)}
            GROUP BY 1
        )
        SELECT count(*), sum(v::INT), sum(c::INT), sum(p::INT), sum((p AND NOT c)::INT) FROM s
    """).fetchone()
    sessions, views, carts, purchases, no_cart = (int(x) for x in row)
    return {
        "start": start, "end": end, "sessions": sessions,
        "view_sessions": views, "cart_sessions": carts, "purchase_sessions": purchases,
        "cart_rate": round(carts / views, 4),
        "purchase_rate": round(purchases / views, 4),
        "purchase_sessions_without_cart": round(no_cart / purchases, 4),
    }


def revenue_by_category(con, day_table: pd.DataFrame) -> pd.DataFrame:
    return con.sql(f"""
        SELECT top_category, sum(price) AS revenue, count(*) AS purchases,
               sum(price) / sum(sum(price)) OVER () AS share
        FROM events
        WHERE event_type = 'purchase' AND price IS NOT NULL AND {_clean_days_sql(day_table)}
        GROUP BY 1 ORDER BY revenue DESC
    """).df()


def buyer_concentration(con, day_table: pd.DataFrame) -> dict:
    """Revenue share of the top buyers, the Gini of revenue per buyer and the Lorenz curve."""
    spend = con.sql(f"""
        SELECT sum(price) AS revenue FROM events
        WHERE event_type = 'purchase' AND price IS NOT NULL AND {_clean_days_sql(day_table)}
        GROUP BY user_id ORDER BY revenue DESC
    """).df()["revenue"].to_numpy()
    total = spend.sum()
    n = len(spend)
    ascending = np.sort(spend)
    gini = float((2 * np.arange(1, n + 1) - n - 1) @ ascending / (n * total))
    cum = np.cumsum(spend) / total
    grid = np.linspace(0, 1, 101)
    lorenz = np.interp(grid, np.arange(1, n + 1) / n, cum, left=0.0)
    return {
        "buyers": int(n),
        "top_1pct_share": round(float(cum[max(int(n * 0.01), 1) - 1]), 4),
        "top_10pct_share": round(float(cum[max(int(n * 0.10), 1) - 1]), 4),
        "gini": round(gini, 4),
        "lorenz_top_share": [round(float(x), 4) for x in lorenz],
    }


def retention(con) -> dict:
    """October users and buyers seen again in November. Activity retention uses every event;
    buyer retention would be biased low by the November purchase gaps, so it is reported
    with that caveat rather than corrected."""
    row = con.execute("""
        WITH m AS (
            SELECT user_id, month,
                   bool_or(event_type = 'purchase') AS bought
            FROM events GROUP BY 1, 2
        ), o AS (SELECT * FROM m WHERE month = '10'), n AS (SELECT * FROM m WHERE month = '11')
        SELECT count(*), count(n.user_id),
               sum(o.bought::INT), sum((o.bought AND coalesce(n.bought, FALSE))::INT)
        FROM o LEFT JOIN n USING (user_id)
    """).fetchone()
    users, back, buyers, buyers_back = (int(x) for x in row)
    return {
        "october_users": users, "active_again_in_november": round(back / users, 4),
        "october_buyers": buyers, "bought_again_in_november": round(buyers_back / buyers, 4),
    }


def repeat_buyers(con, day_table: pd.DataFrame) -> dict:
    row = con.execute(f"""
        WITH b AS (
            SELECT user_id, count(DISTINCT user_session) AS sessions FROM events
            WHERE event_type = 'purchase' AND {_clean_days_sql(day_table)} GROUP BY 1
        )
        SELECT count(*), sum((sessions > 1)::INT) FROM b
    """).fetchone()
    return {"buyers": int(row[0]), "multi_session_share": round(row[1] / row[0], 4)}


def run_all(con) -> tuple[dict, dict]:
    """Every metric for ``results.json`` plus the tables the figures need."""
    day_table = daily(con)
    regimes = cart_regimes(day_table)
    gaps = day_table[day_table["capture_gap"]]
    clean = day_table[~day_table["capture_gap"]]
    categories = revenue_by_category(con, day_table)
    results = {
        "overview": overview(con),
        "capture_gaps": {
            "rule": f"an hour with >= {GAP_MIN_EVENTS} events and zero purchases",
            "days": [str(d.date()) for d in gaps["day"]],
            "gap_hours": int(gaps["gap_hours"].sum()),
            "views_on_gap_days_vs_clean_median": round(
                float(gaps["views"].mean() / clean["views"].median()), 2),
        },
        "daily_revenue_median": round(float(clean["revenue"].median()), 0),
        "cart_regimes": regimes,
        "funnels": [session_funnel(con, r["start"], r["end"], day_table) for r in regimes],
        "categories": [
            {"top_category": r.top_category, "share": round(float(r.share), 4)}
            for r in categories.head(8).itertuples()
        ],
        "concentration": buyer_concentration(con, day_table),
        "retention": retention(con),
        "repeat": repeat_buyers(con, day_table),
    }
    return results, {"daily": day_table, "categories": categories}
