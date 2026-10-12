"""Figures for the README, drawn only from ``reports/results.json`` and
``reports/benchmark.json`` so every chart shows the numbers the README quotes."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIG_DIR = PROJECT_ROOT / "outputs" / "figures"
REPORTS = PROJECT_ROOT / "reports"

INK, GRID, MAIN, ALT, GAP = "#2B2B2B", "#D9D9D9", "#2C5BB4", "#C05B3C", "#F2D7D0"
ENGINE_COLORS = {"pandas": "#6E8CA0", "polars": "#C05B3C", "duckdb": "#B58900", "pyspark": "#5B8C5A"}


def _style(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.grid(axis="y", color=GRID, lw=0.6)
    ax.set_axisbelow(True)


def _shade_gaps(ax, day: pd.DataFrame):
    for d in day.loc[day["capture_gap"], "day"]:
        ax.axvspan(d - pd.Timedelta(hours=12), d + pd.Timedelta(hours=12), color=GAP, lw=0)


def daily_capture(results: dict, path: Path):
    day = pd.DataFrame(results["daily"]).assign(day=lambda d: pd.to_datetime(d["day"]))
    fig, (a, b) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    for ax in (a, b):
        _shade_gaps(ax, day)
        _style(ax)
    a.plot(day["day"], day["views"] / 1e6, color=MAIN, lw=1.6)
    a.set_ylabel("views (M)")
    a.set_title("Traffic triples in mid-November while purchases go missing (shaded: capture gaps)",
                loc="left", fontsize=11, color=INK)
    b.bar(day["day"], day["purchases"] / 1e3, color=ALT, width=0.8)
    b.set_ylabel("purchases (k)")
    b.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def cart_regimes(results: dict, path: Path):
    day = pd.DataFrame(results["daily"]).assign(day=lambda d: pd.to_datetime(d["day"]))
    clean = day[~day["capture_gap"]]
    fig, ax = plt.subplots(figsize=(10, 4))
    _shade_gaps(ax, day)
    ax.plot(clean["day"], clean["carts"] / clean["purchases"], "o", ms=3.5, color=MAIN,
            label="daily carts per purchase")
    for r in results["cart_regimes"]:
        s, e = pd.Timestamp(r["start"]), pd.Timestamp(r["end"])
        ax.hlines(r["carts_per_purchase"], s, e, color=ALT, lw=2.5)
    ax.plot([], [], color=ALT, lw=2.5, label="detected regime level")
    ax.set_ylabel("carts / purchase")
    ax.set_title("Cart events per purchase move in steps: a tracking change, not behaviour",
                 loc="left", fontsize=11, color=INK)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.legend(frameon=False, loc="upper left")
    _style(ax)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def funnel_by_regime(results: dict, path: Path):
    f = pd.DataFrame(results["funnels"])
    labels = [f"{pd.Timestamp(s):%b %d}-{pd.Timestamp(e):%b %d}" for s, e in zip(f["start"], f["end"])]
    x = np.arange(len(f))
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(x - 0.2, f["cart_rate"] * 100, 0.4, color=MAIN, label="sessions with a cart (% of viewing)")
    ax.bar(x + 0.2, f["purchase_sessions_without_cart"] * 100, 0.4, color=ALT,
           label="purchase sessions with no cart event (%)")
    ax.set_xticks(x, labels, fontsize=8)
    ax.set_title("Before Nov 8 most purchases have no cart event; the funnel cannot be pooled",
                 loc="left", fontsize=11, color=INK)
    ax.legend(frameon=False)
    _style(ax)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def revenue_concentration(results: dict, path: Path):
    cats = pd.DataFrame(results["categories"]).iloc[::-1]
    lorenz = results["concentration"]["lorenz_top_share"]
    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4))
    a.barh(cats["top_category"], cats["share"] * 100, color=MAIN)
    a.set_xlabel("% of revenue")
    a.set_title("Revenue by top-level category", loc="left", fontsize=11, color=INK)
    grid = np.linspace(0, 100, len(lorenz))
    b.plot(grid, np.array(lorenz) * 100, color=ALT, lw=2)
    b.plot([0, 100], [0, 100], color=GRID, ls="--")
    c = results["concentration"]
    b.set_xlabel("% of buyers, highest spenders first")
    b.set_ylabel("% of revenue")
    b.set_title(f"Top 10% of buyers: {c['top_10pct_share']:.0%} of revenue (Gini {c['gini']:.2f})",
                loc="left", fontsize=11, color=INK)
    for ax in (a, b):
        _style(ax)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def engine_benchmark(rows: list[dict], path: Path):
    ok = pd.DataFrame([r for r in rows if r.get("status") == "ok"])
    scopes = list(dict.fromkeys(ok["scope"]))
    engines = list(dict.fromkeys(ok["engine"]))
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    width = 0.8 / len(engines)
    for i, eng in enumerate(engines):
        part = ok[ok["engine"] == eng].set_index("scope").reindex(scopes)
        x = np.arange(len(scopes)) + (i - (len(engines) - 1) / 2) * width
        axes[0].bar(x, part["warm_s"], width, color=ENGINE_COLORS[eng], label=eng)
        axes[1].bar(x, part["peak_rss_gb"], width, color=ENGINE_COLORS[eng])
    for ax, label in zip(axes, ["warm query time (s)", "peak memory incl. child processes (GB)"]):
        ax.set_xticks(np.arange(len(scopes)), scopes)
        ax.set_ylabel(label)
        _style(ax)
    axes[0].legend(frameon=False)
    axes[0].set_title("Same query, four engines", loc="left", fontsize=11, color=INK)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def make_all(results: dict | None = None):
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    for old in FIG_DIR.glob("*.png"):
        old.unlink()
    results = results or json.loads((REPORTS / "results.json").read_text(encoding="utf-8"))
    daily_capture(results, FIG_DIR / "daily_capture.png")
    cart_regimes(results, FIG_DIR / "cart_regimes.png")
    funnel_by_regime(results, FIG_DIR / "funnel_by_regime.png")
    revenue_concentration(results, FIG_DIR / "revenue_concentration.png")
    bench = REPORTS / "benchmark.json"
    if bench.exists():
        engine_benchmark(json.loads(bench.read_text(encoding="utf-8")), FIG_DIR / "engine_benchmark.png")


if __name__ == "__main__":
    make_all()
