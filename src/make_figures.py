"""Figures for the README: run with `python -m src.make_figures`.

Reads the same lakehouse the analytics module queries and calls the same
functions, so the numbers on the charts are the numbers those modules return.
The engine benchmark is repeated here (the standalone `src/benchmark.py`
reports a single run) so the chart can show a median and a spread rather than
one draw.
"""
from __future__ import annotations

import gc
import json
import subprocess
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from src.benchmark import run_duckdb, run_pandas, run_polars, _measure
from src.lakehouse.analytics import (
    cohort_retention,
    connect,
    conversion_funnel,
    customer_ltv,
    revenue_by_category,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FIG_DIR = PROJECT_ROOT / "outputs" / "figures"

INK = "#2B2B2B"
GRID = "#D9D9D9"
DUCK = "#B58900"
POLAR = "#C05B3C"
PANDA = "#6E8CA0"
ACCENT = "#4C7A3E"

# The generator's own funnel parameters (src/generators/synthetic_clickstream.py).
DESIGNED_STEP_PROB = {"product_view": 0.60, "add_to_cart": 0.38,
                      "checkout_start": 0.55, "purchase": 0.65}


def _style(ax, title=None, xlabel=None, ylabel=None, grid_axis="y"):
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(GRID)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.6, alpha=0.7)
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK, labelsize=9)
    if title:
        ax.set_title(title, fontsize=11.5, color=INK, pad=12)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=10, color=INK)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=10, color=INK)
    return ax


def _save(fig, name):
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    path = FIG_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  wrote outputs/figures/{name}")


# --------------------------------------------------------------------------
# 1. The funnel, and the gap between what it measures and what was designed
# --------------------------------------------------------------------------
def figure_funnel(con):
    print("1/4 conversion_funnel ...")
    row = conversion_funnel(con).to_dicts()[0]
    # The funnel query names the checkout column `checkout_sessions`, not
    # `checkout_start_sessions`, though the event type is `checkout_start`.
    steps = ["page_view", "product_view", "add_to_cart", "checkout_start", "purchase"]
    columns = ["page_view_sessions", "product_view_sessions", "add_to_cart_sessions",
               "checkout_sessions", "purchase_sessions"]
    counts = [int(row[c]) for c in columns]

    measured = [counts[i] / counts[i - 1] for i in range(1, len(counts))]
    # What the generator actually implies for each session-level step. Only the
    # first differs from the per-step parameter, and the derivation is in the
    # README: browsing product-views happen outside the funnel branch.
    p_browse_none = 1 / 3
    implied_product_view = 1 - p_browse_none * (1 - DESIGNED_STEP_PROB["product_view"])
    implied = [implied_product_view, None, DESIGNED_STEP_PROB["checkout_start"],
               DESIGNED_STEP_PROB["purchase"]]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.2, 5.2),
                                   gridspec_kw={"width_ratios": [1.15, 1]})

    bars = ax1.barh(range(len(steps)), counts, color=ACCENT, height=0.6,
                    edgecolor="white", linewidth=1.2)
    bars[0].set_color("#8FA8B8")
    ax1.set_yticks(range(len(steps)))
    ax1.set_yticklabels(steps)
    ax1.invert_yaxis()
    for i, (b, c) in enumerate(zip(bars, counts)):
        ax1.text(c + counts[0] * 0.012, b.get_y() + b.get_height() / 2,
                 f"{c:,}" + (f"   ({c / counts[0]:.1%} of all sessions)" if i else ""),
                 va="center", fontsize=9, color=INK)
    _style(ax1, xlabel="Sessions reaching the step", grid_axis="x")
    ax1.set_title(f"{counts[0]:,} sessions enter; {counts[-1]:,} purchase "
                  f"({counts[-1] / counts[0]:.1%})", fontsize=11.5, color=INK, pad=12)
    ax1.set_xlim(0, counts[0] * 1.42)

    labels = [f"{steps[i]}\n->{steps[i + 1]}" for i in range(len(steps) - 1)]
    x = np.arange(len(labels))
    ax2.bar(x - 0.19, measured, width=0.38, color=POLAR, label="Measured from the lake",
            edgecolor="white", linewidth=1)
    impl_x = [i for i, v in enumerate(implied) if v is not None]
    impl_v = [v for v in implied if v is not None]
    ax2.bar(np.array(impl_x) + 0.19, impl_v, width=0.38, color="#9A9A9A",
            label="Implied by the generator", edgecolor="white", linewidth=1)

    for i, v in enumerate(measured):
        ax2.text(i - 0.19, v + 0.018, f"{v:.1%}", ha="center", fontsize=8.6, color=POLAR)
    for i, v in zip(impl_x, impl_v):
        ax2.text(i + 0.19, v + 0.018, f"{v:.1%}", ha="center", fontsize=8.6, color="#777777")

    ax2.annotate("no clean\ncomparison:\nsee README",
                 xy=(1.19, 0.06), fontsize=8.2, color="#777777", ha="center")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, fontsize=8.6)
    ax2.set_ylim(0, 1.06)
    ax2.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(xmax=1, decimals=0))
    _style(ax2, ylabel="Step-to-step conversion")
    ax2.set_title("Step rates agree with the design — except the second,\n"
                  "where browsing views inflate the denominator",
                  fontsize=11.5, color=INK, pad=12)
    ax2.legend(frameon=False, fontsize=8.8, loc="upper right")

    fig.text(0.5, -0.045,
             "Sessions are counted once per step. A session can reach product_view by browsing "
             "(0-2 views emitted before the funnel branch)\nor through the funnel itself, so the "
             "product_view -> add_to_cart rate reads lower than the 38% the generator applies "
             "inside the funnel.",
             ha="center", fontsize=8.5, color="#666666")
    _save(fig, "conversion_funnel.png")
    return counts, measured


# --------------------------------------------------------------------------
# 2. Where the revenue actually is
# --------------------------------------------------------------------------
def figure_revenue(con):
    print("2/4 revenue_by_category ...")
    df = revenue_by_category(con).sort("total_revenue", descending=True)
    cats = df["category"].to_list()
    rev = np.array(df["total_revenue"].to_list())
    cnt = np.array(df["purchase_count"].to_list())
    aov = np.array(df["avg_order_value"].to_list())
    y = np.arange(len(cats))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.6, 4.8), sharey=True)

    ax1.barh(y, rev, color=DUCK, height=0.62, edgecolor="white", linewidth=1.2)
    for i, v in enumerate(rev):
        ax1.text(v + rev.max() * 0.015, i, f"${v:,.0f}  ({v / rev.sum():.0%})",
                 va="center", fontsize=8.8, color=INK)
    ax1.set_yticks(y)
    ax1.set_yticklabels(cats)
    ax1.invert_yaxis()
    ax1.set_xlim(0, rev.max() * 1.42)
    _style(ax1, xlabel="Total revenue", grid_axis="x")
    ax1.set_title(f"Electronics is {rev[0] / rev.sum():.0%} of revenue", fontsize=11, color=INK, pad=10)

    ax2.barh(y, cnt, color="#8FA8B8", height=0.62, edgecolor="white", linewidth=1.2)
    for i, (c, a) in enumerate(zip(cnt, aov)):
        ax2.text(c + cnt.max() * 0.015, i, f"{c:,} orders   AOV ${a:,.0f}",
                 va="center", fontsize=8.8, color=INK)
    ax2.set_xlim(0, cnt.max() * 1.55)
    _style(ax2, xlabel="Purchases", grid_axis="x")
    ax2.set_title("...on the second-fewest orders", fontsize=11, color=INK, pad=10)

    fig.text(0.5, -0.05,
             f"Order counts are nearly flat across categories ({cnt.min():,}-{cnt.max():,}), so the "
             f"revenue ranking is almost entirely average order value:\n${aov.max():,.0f} for "
             f"Electronics against ${aov.min():,.0f} for Books, a {aov.max() / aov.min():.0f}x spread "
             "that comes straight from the generator's per-category price ranges.",
             ha="center", fontsize=8.5, color="#666666")
    _save(fig, "revenue_by_category.png")
    return cats, rev, cnt, aov


# --------------------------------------------------------------------------
# 3. Engine benchmark, repeated
# --------------------------------------------------------------------------
def _cold_measure(engine: str) -> tuple[float, float]:
    """One run of one engine in a fresh interpreter.

    Measuring repeated runs inside a single process understates memory badly:
    after the first run the allocator already holds the pages, so the RSS delta
    of run 2 is near zero regardless of how much the engine actually needs. A
    subprocess per engine is the only way to get a delta that includes
    allocation, and it also stops the engines from contaminating each other's
    measurement through whatever the previous one left resident.
    """
    code = (
        "import json;from src.benchmark import _measure, run_%s as fn;"
        "_, s, m = _measure(fn);print(json.dumps([s, m]))" % engine
    )
    out = subprocess.run([sys.executable, "-c", code], cwd=PROJECT_ROOT,
                         capture_output=True, text=True, check=True)
    secs, delta = json.loads(out.stdout.strip().splitlines()[-1])
    return secs, delta


def figure_benchmark(n_runs=7):
    print(f"3/4 engine_benchmark ({n_runs} warm runs + 1 cold run each) ...")
    engines = [("duckdb", run_duckdb, DUCK), ("polars", run_polars, POLAR),
               ("pandas", run_pandas, PANDA)]
    times, mems, cold = {}, {}, {}
    for name, fn, _ in engines:
        cold[name] = _cold_measure(name)
        t, m = [], []
        for _ in range(n_runs):
            gc.collect()
            _, secs, delta = _measure(fn)
            t.append(secs), m.append(delta)
        times[name], mems[name] = np.array(t), np.array(m)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.4, 4.8))
    names = [e[0] for e in engines]
    colors = [e[2] for e in engines]
    x = np.arange(len(names))

    med_t = [np.median(times[n]) for n in names]
    ax1.bar(x, med_t, color=colors, width=0.55, edgecolor="white", linewidth=1.2)
    ax1.errorbar(x, med_t,
                 yerr=[[med_t[i] - times[n].min() for i, n in enumerate(names)],
                       [times[n].max() - med_t[i] for i, n in enumerate(names)]],
                 fmt="none", ecolor=INK, elinewidth=1.3, capsize=5)
    for i, n in enumerate(names):
        ax1.text(i, times[n].max(), f"  {med_t[i]:.4f}s", ha="center", va="bottom",
                 fontsize=9, color=INK)
    ax1.set_xticks(x)
    ax1.set_xticklabels(names)
    ax1.set_yscale("log")
    _style(ax1, ylabel="Seconds (log scale)")
    ax1.set_title(f"Wall time — {n_runs} warm runs", fontsize=11, color=INK, pad=10)

    warm_med = [np.median(mems[n]) for n in names]
    cold_mb = [cold[n][1] for n in names]
    ax2.bar(x - 0.19, cold_mb, width=0.38, color=colors, edgecolor="white", linewidth=1.2,
            label="Cold: one run in a fresh process")
    ax2.bar(x + 0.19, warm_med, width=0.38, color=colors, alpha=0.4, edgecolor="white",
            linewidth=1.2, label="Warm: median of the repeats")
    for i in range(len(names)):
        ax2.text(i - 0.19, cold_mb[i], f"{cold_mb[i]:.0f}", ha="center", va="bottom",
                 fontsize=8.8, color=INK)
        ax2.text(i + 0.19, warm_med[i], f"{warm_med[i]:.0f}", ha="center", va="bottom",
                 fontsize=8.8, color="#777777")
    ax2.set_xticks(x)
    ax2.set_xticklabels(names)
    _style(ax2, ylabel="Δ process RSS (MB)")
    ax2.set_title("Memory — the metric depends on how you ask", fontsize=11, color=INK, pad=10)
    ax2.legend(frameon=False, fontsize=8.5)

    slow = np.median(times["pandas"]) / np.median(times["duckdb"])
    ratio = cold_mb[2] / max(cold_mb[0], 0.1)
    overlap = (times["duckdb"].min() <= times["polars"].max()
               and times["polars"].min() <= times["duckdb"].max())
    verdict = ("DuckDB and Polars are not separable here — their ranges overlap"
               if overlap else "DuckDB and Polars are separable on this run")
    fig.text(0.5, -0.085,
             f"Same aggregation (revenue by category, purchases only) over the same Parquet file. "
             f"Left: {n_runs} warm runs, bars are medians and whiskers the full range.\n"
             f"{verdict}; both are about {slow:.0f}x faster than pandas, which is the result that "
             "survives repetition.\n"
             "Right: the RSS delta is not a stable quantity. Measured cold, in a fresh process, it "
             f"includes allocation and pandas costs {ratio:.0f}x DuckDB.\n"
             "Measured warm, the allocator already holds the pages and every engine looks cheap. "
             "Neither is peak-memory profiling; the cold number is the one to quote.",
             ha="center", fontsize=8.5, color="#666666")
    _save(fig, "engine_benchmark.png")
    return times, mems, cold


# --------------------------------------------------------------------------
# 4. Cohort retention — and why it does not decay
# --------------------------------------------------------------------------
def figure_cohorts(con):
    print("4/4 cohort_retention ...")
    df = cohort_retention(con)
    pdf = df.to_pandas()
    pdf["cohort"] = pdf["cohort_month"].dt.strftime("%Y-%m")
    pivot = pdf.pivot(index="cohort", columns="months_since_signup", values="active_users")
    base = pivot[0]
    pct = pivot.div(base, axis=0)

    fig, ax = plt.subplots(figsize=(8.4, 4.4))
    im = ax.imshow(pct.values, cmap="YlGnBu", vmin=0, vmax=1, aspect="auto")
    for i in range(pct.shape[0]):
        for j in range(pct.shape[1]):
            v = pct.values[i, j]
            if np.isnan(v):
                continue
            ax.text(j, i, f"{v:.0%}\n{int(pivot.values[i, j]):,}", ha="center", va="center",
                    fontsize=9, color="white" if v > 0.55 else INK)
    ax.set_xticks(range(pct.shape[1]))
    ax.set_xticklabels([f"month {c}" for c in pct.columns])
    ax.set_yticks(range(pct.shape[0]))
    ax.set_yticklabels(pct.index)
    ax.set_title("Cohort retention — share of each cohort active in later months",
                 fontsize=11.5, color=INK, pad=12)
    ax.tick_params(colors=INK, labelsize=9)
    for s in ax.spines.values():
        s.set_visible(False)
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)

    jan = pct.loc[pct.index[0]]
    fig.text(0.5, -0.09,
             f"Read this one as a check on the generator, not as a retention finding: the January "
             f"cohort goes {jan[0]:.0%} -> {jan[1]:.0%} -> {jan[2]:.0%}, rising again in month 2.\n"
             "Real retention decays. It does not here because the generator draws each user's "
             "sessions uniformly across the whole 90-day window,\nso a user is equally likely to be "
             "active in any month. The query is correct; the data has no churn in it to find.",
             ha="center", fontsize=8.5, color="#666666")
    _save(fig, "cohort_retention.png")
    return pct


if __name__ == "__main__":
    print(f"Writing figures to {FIG_DIR}\n")
    con = connect()
    counts, measured = figure_funnel(con)
    cats, rev, cnt, aov = figure_revenue(con)
    times, mems, cold = figure_benchmark()
    pct = figure_cohorts(con)
    top = customer_ltv(con, top_n=20)

    print("\nNumbers annotated on the figures:")
    print(f"  funnel sessions   : {' -> '.join(f'{c:,}' for c in counts)}")
    print(f"  step rates        : {', '.join(f'{m:.1%}' for m in measured)}")
    print(f"  revenue top       : {cats[0]} ${rev[0]:,.0f} ({rev[0] / rev.sum():.1%}), "
          f"AOV ${aov[0]:,.0f}")
    print("  benchmark warm    : " + ", ".join(
        f"{n} {np.median(times[n]):.4f}s/{np.median(mems[n]):.1f}MB" for n in times))
    print("  benchmark cold    : " + ", ".join(
        f"{n} {cold[n][0]:.4f}s/{cold[n][1]:.1f}MB" for n in cold))
    print(f"  top LTV customer  : {top['user_id'][0]} ${top['lifetime_value'][0]:,.2f}")
