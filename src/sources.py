"""Downloads the REES46 clickstream from Kaggle into ``data/raw``.

Dataset: *eCommerce behavior data from multi category store*
(https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store),
license "copyright-authors". It needs a Kaggle API token in ``~/.kaggle``; the files are
never committed and never redistributed, only aggregates computed from them.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

DATASET = "mkechinov/ecommerce-behavior-data-from-multi-category-store"
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
FILES = ["2019-Oct.csv", "2019-Nov.csv"]


def download(raw_dir: Path = RAW_DIR) -> list[Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    targets = [raw_dir / name for name in FILES]
    if all(t.exists() for t in targets):
        return targets
    subprocess.run(
        [sys.executable, "-m", "kaggle.cli", "datasets", "download", "-d", DATASET,
         "-p", str(raw_dir), "--unzip"],
        check=True,
    )
    missing = [t for t in targets if not t.exists()]
    if missing:
        raise FileNotFoundError(f"download finished but {missing} are missing")
    return targets


if __name__ == "__main__":
    for path in download():
        print(path, f"{path.stat().st_size / 1e9:.2f} GB")
