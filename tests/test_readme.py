"""The figures quoted in both READMEs must be the ones in ``reports/*.json``."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
RESULTS = json.loads((ROOT / "reports" / "results.json").read_text(encoding="utf-8"))
INGESTION = json.loads((ROOT / "reports" / "ingestion.json").read_text(encoding="utf-8"))
BENCH = json.loads((ROOT / "reports" / "benchmark.json").read_text(encoding="utf-8"))


def _pct(x: float, es: bool) -> str:
    s = f"{x * 100:.1f}%"
    return s.replace(".", ",") if es else s


def _int(n: int, es: bool) -> str:
    s = f"{n:,}"
    return s.replace(",", ".") if es else s


def _dec(x: float, digits: int, es: bool) -> str:
    s = f"{x:.{digits}f}"
    return s.replace(".", ",") if es else s


def expected(es: bool) -> list[str]:
    r, ing = RESULTS, INGESTION
    cats = {c["top_category"]: c["share"] for c in r["categories"]}
    funnels = r["funnels"]
    out = [
        _int(ing["raw_events"], es), _int(ing["exact_duplicates"], es),
        _int(ing["price_zero_or_negative"], es), _int(ing["events_without_session"], es),
        _int(ing["events_without_category"], es), _int(ing["lake_events"], es),
        f"{r['capture_gaps']['gap_hours']} ",
        _dec(r["capture_gaps"]["views_on_gap_days_vs_clean_median"], 1, es),
        _pct(cats["electronics"], es), _pct(cats["uncoded"], es),
        _pct(cats["appliances"], es), _pct(cats["computers"], es),
        _int(r["concentration"]["buyers"], es),
        _pct(r["concentration"]["top_1pct_share"], es),
        _pct(r["concentration"]["top_10pct_share"], es),
        _dec(r["concentration"]["gini"], 2, es),
        _pct(r["retention"]["active_again_in_november"], es),
        _pct(r["retention"]["bought_again_in_november"], es),
        _pct(r["repeat"]["multi_session_share"], es),
        _dec(r["daily_revenue_median"] / 1e6, 2, es),
        _pct(funnels[-1]["purchase_sessions_without_cart"], es),
    ]
    early = [f["purchase_sessions_without_cart"] for f in funnels[:-1]]
    out += [f"{min(early) * 100:.0f}%", f"{max(early) * 100:.0f}%"]
    out += [_dec(g["carts_per_purchase"], 2, es) for g in r["cart_regimes"]]
    for row in BENCH:
        if row.get("status") == "ok":
            out += [_dec(row["warm_s"], 2, es), _dec(row["peak_rss_gb"], 2, es)]
    return out


@pytest.mark.parametrize("name,es", [("README.md", False), ("README.es.md", True)])
def test_readme_numbers_match_results(name, es):
    text = (ROOT / name).read_text(encoding="utf-8")
    missing = [s for s in expected(es) if s not in text]
    assert not missing, f"{name} does not quote: {missing}"
