"""Seed empty placeholder parquet for every lake zone.

DuckDB's read_parquet() throws when a glob matches zero files, so an empty zone
(e.g. a dead-letter folder on a clean feed that hasn't rejected anything) breaks
the dbt build. This writes a 0-row placeholder with the right columns into any
zone that has no parquet yet. 0 rows means it never affects counts; it only
guarantees the glob always matches something.

Idempotent: a zone that already has parquet is left untouched.

    python -m ingestion.seed_lake --lake data/lake
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

# Columns each zone's downstream models reference. Must cover what the dbt models
# select, because in the empty case the placeholder is the only schema present.
ZONES: dict[str, dict[str, str]] = {
    "valid": {
        "source": "object", "event_type": "object", "event_id": "object",
        "event_ts": "datetime64[ns]", "market_id": "object", "asset_id": "object",
        "outcome": "object", "price": "float64", "size": "float64", "side": "object",
    },
    "dlq": {
        "raw_json": "object", "reject_reason": "object", "rejected_at": "datetime64[ns]",
    },
    "flagged": {
        "asset_id": "object", "market_id": "object",
        "window_start": "datetime64[ns]", "window_end": "datetime64[ns]",
        "price_range": "float64", "volume": "float64", "tick_count": "int64", "rule": "object",
    },
    "resolutions": {
        "market_id": "object", "asset_id": "object", "outcome": "object", "won": "int64",
    },
}


def seed(lake: str) -> None:
    lake_path = Path(lake)
    for zone, schema in ZONES.items():
        zone_dir = lake_path / zone
        zone_dir.mkdir(parents=True, exist_ok=True)
        if list(zone_dir.rglob("*.parquet")):
            continue  # real (or previously-seeded) data already here
        empty = pd.DataFrame({c: pd.Series([], dtype=t) for c, t in schema.items()})
        empty.to_parquet(zone_dir / "_seed_empty.parquet", index=False)
        print(f"seeded empty {zone}/")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="data/lake")
    args = ap.parse_args()
    seed(args.lake)


if __name__ == "__main__":
    main()