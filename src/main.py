"""End to end: lake -> every metric -> ``reports/results.json`` -> figures.

Run ``python -m src.sources`` and ``python -m src.lakehouse.ingestion`` first (download
and lake build); ``python -m src.benchmark`` is separate because it takes minutes per engine.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.lakehouse.analytics import connect, run_all

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS = PROJECT_ROOT / "reports" / "results.json"


def main() -> dict:
    results, tables = run_all(connect())
    day = tables["daily"]
    results["daily"] = [
        {"day": str(r.day.date()), "views": int(r.views), "carts": int(r.carts),
         "purchases": int(r.purchases), "revenue": round(float(r.revenue), 0),
         "capture_gap": bool(r.capture_gap)}
        for r in day.itertuples()
    ]
    ingestion = PROJECT_ROOT / "reports" / "ingestion.json"
    if ingestion.exists():
        results["ingestion"] = json.loads(ingestion.read_text(encoding="utf-8"))
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(json.dumps(results, indent=2), encoding="utf-8")
    from src.make_figures import make_all
    make_all(results)
    return results


if __name__ == "__main__":
    main()
