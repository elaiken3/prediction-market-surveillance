"""Pull resolved markets from the public Gamma API into the lake as parquet.

Feeds fct_calibration. Run periodically (the Airflow DAG calls it):

    python -m ingestion.fetch_resolutions --lake data/lake --limit 200

Field names follow current Gamma responses; confirm against
https://docs.polymarket.com and adjust the parsing below if they change.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import requests

GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"


def fetch(limit: int) -> pd.DataFrame:
    resp = requests.get(GAMMA_MARKETS, params={"closed": "true", "limit": limit}, timeout=30)
    resp.raise_for_status()
    rows = []
    for m in resp.json():
        token_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or "[]"
        token_ids = json.loads(token_ids) if isinstance(token_ids, str) else token_ids
        outcomes = m.get("outcomes") or "[]"
        outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
        prices = m.get("outcomePrices") or "[]"
        prices = json.loads(prices) if isinstance(prices, str) else prices
        for i, asset_id in enumerate(token_ids):
            # A resolved binary market has a winning outcome priced at ~1.0.
            won = 0
            try:
                won = 1 if float(prices[i]) >= 0.99 else 0
            except (IndexError, ValueError, TypeError):
                pass
            rows.append({
                "market_id": m.get("conditionId") or m.get("condition_id"),
                "asset_id": str(asset_id),
                "outcome": outcomes[i] if i < len(outcomes) else None,
                "won": won,
            })
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--limit", type=int, default=200)
    args = ap.parse_args()

    df = fetch(args.limit)
    out_dir = Path(args.lake) / "resolutions"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "resolutions.parquet"
    df.to_parquet(out_path, index=False)
    print(f"wrote {len(df)} resolved outcomes to {out_path}")


if __name__ == "__main__":
    main()
