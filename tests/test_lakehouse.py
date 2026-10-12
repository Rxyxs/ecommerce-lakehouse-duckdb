"""Tests on a hand-written fixture in the REES46 CSV format, small enough to check by eye."""
from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from src import benchmark
from src.lakehouse import analytics
from src.lakehouse.ingestion import validate_schema_contract, write_lake

HEADER = ["event_time", "event_type", "product_id", "category_id", "category_code",
          "brand", "price", "user_id", "user_session"]


def _row(ts, kind, product, code, price, user, session):
    return [f"2019-10-{ts} UTC", kind, product, 1000 + product, code, "acme", price, user, session]


ROWS = [
    # day 01: session a views, carts and buys; session b views and buys without a cart
    _row("01 10:00:00", "view", 1, "electronics.smartphone", 100.0, 1, "a"),
    _row("01 10:01:00", "cart", 1, "electronics.smartphone", 100.0, 1, "a"),
    _row("01 10:02:00", "purchase", 1, "electronics.smartphone", 100.0, 1, "a"),
    _row("01 10:02:00", "purchase", 1, "electronics.smartphone", 100.0, 1, "a"),  # exact duplicate
    _row("01 11:00:00", "view", 2, "", 30.0, 2, "b"),
    _row("01 11:05:00", "purchase", 2, "", 30.0, 2, "b"),
    # day 02: a zero price, an event without session, and a busy hour with no purchase
    _row("02 09:00:00", "view", 3, "appliances.kitchen", 0.0, 3, "c"),
    _row("02 09:01:00", "view", 3, "appliances.kitchen", 50.0, 3, ""),
    _row("02 12:00:00", "view", 1, "electronics.smartphone", 100.0, 4, "d"),
    _row("02 12:01:00", "view", 2, "", 30.0, 4, "d"),
    _row("02 12:02:00", "cart", 2, "", 30.0, 4, "d"),
    _row("02 13:00:00", "purchase", 3, "appliances.kitchen", 50.0, 1, "e"),
]


@pytest.fixture(scope="module")
def lake(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("rees46")
    raw = root / "2019-Oct.csv"
    with raw.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)
        writer.writerows(ROWS)
    out = root / "lake"
    report = write_lake([raw], out, threads=1)
    return out, report


@pytest.fixture(scope="module")
def con(lake):
    return analytics.connect(lake[0], threads=1)


def test_quality_report_counts_raw_problems(lake):
    _, report = lake
    assert report["raw_events"] == 12
    assert report["exact_duplicates"] == 1
    assert report["price_zero_or_negative"] == 1
    assert report["events_without_session"] == 1
    assert report["lake_events"] == 10


def test_cleaning_nulls_bad_prices_and_labels_uncoded(con):
    rows = con.execute("SELECT top_category, price FROM events WHERE user_id = 3 ORDER BY event_time").fetchall()
    assert rows == [("appliances", None)]
    assert con.execute("SELECT count(*) FROM events WHERE top_category = 'uncoded'").fetchone()[0] == 4


def test_lake_is_partitioned_by_day(lake):
    days = sorted(p.name for p in lake[0].rglob("day=*"))
    assert days == ["day=01", "day=02"]


def test_contract_rejects_missing_column():
    with pytest.raises(ValueError, match="user_session"):
        validate_schema_contract(pl.DataFrame({"event_type": ["view"]}))


def test_capture_gap_flags_busy_hour_without_purchases(con):
    day = analytics.daily(con, gap_min_events=2)
    assert day["capture_gap"].tolist() == [False, True]
    assert day["revenue"].tolist() == [130.0, 50.0]


def test_segment_recovers_known_steps():
    rng = np.random.default_rng(0)
    y = np.log(np.r_[np.full(10, 0.8), np.full(12, 1.5), np.full(9, 2.9)]) + rng.normal(0, 0.03, 31)
    assert analytics.segment(y) == [10, 22]


def test_segment_ignores_noise_without_steps():
    y = np.log(1.2) + np.random.default_rng(1).normal(0, 0.05, 40)
    assert analytics.segment(y) == []


def test_session_funnel_counts_purchases_without_cart(con):
    day = analytics.daily(con, gap_min_events=1_000)
    f = analytics.session_funnel(con, "2019-10-01", "2019-10-02", day)
    assert f["purchase_sessions"] == 3
    assert f["purchase_sessions_without_cart"] == round(2 / 3, 4)


def test_concentration_of_known_spend(con):
    day = analytics.daily(con, gap_min_events=1_000)
    c = analytics.buyer_concentration(con, day)
    assert c["buyers"] == 2
    assert c["top_1pct_share"] == round(150 / 180, 4)


def test_engines_agree(lake):
    path = lake[0]
    duck = benchmark.query_duckdb(path)
    pol = benchmark.query_polars(path).to_pandas()
    pan = benchmark.query_pandas(path).reset_index()
    for frame in (pol, pan):
        assert frame["top_category"].tolist() == duck["top_category"].tolist()
        assert np.allclose(frame["revenue"], duck["revenue"])
        assert frame["buyers"].tolist() == duck["buyers"].tolist()
