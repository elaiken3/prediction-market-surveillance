"""Seed an empty resolutions parquet so the calibration model builds offline.

Idempotent: if any resolutions parquet already exists (e.g. a real one pulled by
fetch_resolutions on the live feed), this does nothing. Only the offline/first-run
case gets an empty file with the right schema so dbt's fct_calibration compiles.

    python -m ingestion.seed_resolutions --lake data/lake
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="data/lake")
    args = ap.parse_args()

    out_dir = Path(args.lake) / "resolutions"
    out_dir.mkdir(parents=True, exist_ok=True)
    if list(out_dir.glob("*.parquet")):
        print("resolutions already present; leaving as-is")
        return

    empty = pd.DataFrame({
        "market_id": pd.Series([], dtype="object"),
        "asset_id": pd.Series([], dtype="object"),
        "outcome": pd.Series([], dtype="object"),
        "won": pd.Series([], dtype="int64"),
    })
    out_path = out_dir / "_seed_empty.parquet"
    empty.to_parquet(out_path, index=False)
    print(f"seeded empty resolutions at {out_path}")


if __name__ == "__main__":
    main()
