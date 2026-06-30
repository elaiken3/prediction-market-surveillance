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


def _rows_from_market(m: dict) -> list[dict]:
    token_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or "[]"
    token_ids = json.loads(token_ids) if isinstance(token_ids, str) else token_ids
    outcomes = m.get("outcomes") or "[]"
    outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
    prices = m.get("outcomePrices") or "[]"
    prices = json.loads(prices) if isinstance(prices, str) else prices
    rows = []
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
    return rows


def fetch(limit: int, pages: int = 1) -> pd.DataFrame:
    """Pull closed markets from Gamma, paginating to widen the window.

    Gamma returns the most-recently-closed markets first, so a single page can
    miss a market that resolved while the collector was down and has since been
    pushed past `limit`. Paginating scans a wider window so our tracked markets
    are still captured after an outage.
    """
    rows: list[dict] = []
    for page in range(pages):
        resp = requests.get(
            GAMMA_MARKETS,
            params={"closed": "true", "limit": limit, "offset": page * limit},
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for m in batch:
            rows.extend(_rows_from_market(m))
    return pd.DataFrame(rows)


def tracked_market_ids(lake: str) -> set[str]:
    """The market_ids we actually stream, read from the valid lake zone.

    Calibration only cares about resolutions for markets we tracked, so we keep
    only those. Best-effort: an unreadable/empty lake returns an empty set and
    we fall back to keeping every resolution.
    """
    try:
        import duckdb

        con = duckdb.connect()
        rows = con.execute(
            f"select distinct market_id from read_parquet('{lake}/valid/**/*.parquet') "
            "where market_id is not null"
        ).fetchall()
        return {r[0] for r in rows}
    except Exception as e:  # noqa: BLE001
        print(f"WARN: could not read tracked market_ids from lake ({e}); keeping all resolutions")
        return set()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--pages", type=int, default=4, help="pages of closed markets to scan")
    args = ap.parse_args()

    df = fetch(args.limit, args.pages)

    # Keep only resolutions for markets we actually track, if we can determine them.
    tracked = tracked_market_ids(args.lake)
    if tracked and not df.empty:
        before = len(df)
        df = df[df["market_id"].isin(tracked)].reset_index(drop=True)
        print(f"filtered {before} -> {len(df)} resolutions for {len(tracked)} tracked markets")

    out_dir = Path(args.lake) / "resolutions"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "resolutions.parquet"
    df.to_parquet(out_path, index=False)
    print(f"wrote {len(df)} resolved outcomes to {out_path}")


if __name__ == "__main__":
    main()
