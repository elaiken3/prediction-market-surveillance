"""Market surveillance detectors.

Framework-free so the *definitions* of "suspicious" are deterministically tested.
The Spark job applies the same concepts via windowed aggregations; the rules live
here, in one tested place.

Three detectors, each grounded in real market-abuse / data-quality concerns:

  1. price_range   - an outcome's price swinging hard inside a short window
                     (pump-and-dump, news-driven dislocation, or fat-finger).
  2. volume_zscore - a window whose traded size is a large outlier vs the
                     account/asset's recent volume (wash-trading footprint).
  3. coherence     - the outcomes of one market should price to ~1.0; a
                     persistent deviation is arbitrage or manipulation, AND a
                     data-quality signal on the feed itself.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Mapping


@dataclass(frozen=True)
class Tick:
    asset_id: str
    event_ts: datetime
    price: float
    size: float = 0.0
    event_type: str = "quote"


@dataclass(frozen=True)
class Flag:
    rule: str
    score: float
    detail: str


def price_range_flag(
    window: Iterable[Tick],
    *,
    max_range: float = 0.15,
    window_seconds: int = 60,
    now: datetime | None = None,
) -> Flag | None:
    """Flag if max-min price in the trailing window exceeds `max_range`
    (prices are probabilities in [0,1], so 0.15 is a 15-point swing)."""
    ticks = sorted(window, key=lambda t: t.event_ts)
    if len(ticks) < 2:
        return None
    ref = now or ticks[-1].event_ts
    cutoff = ref - timedelta(seconds=window_seconds)
    recent = [t.price for t in ticks if t.event_ts >= cutoff]
    if len(recent) < 2:
        return None
    rng = max(recent) - min(recent)
    if rng >= max_range:
        return Flag("price_range", round(rng, 4), f"price moved {rng:.3f} in {window_seconds}s")
    return None


def volume_zscore_flag(
    window_volumes: Iterable[float],
    *,
    z_threshold: float = 3.0,
    min_history: int = 5,
) -> Flag | None:
    """Given a series of per-bucket traded volumes, flag the latest bucket if it
    is a high positive z-score outlier against the prior buckets."""
    vols = list(window_volumes)
    if len(vols) <= min_history:
        return None
    latest, baseline = vols[-1], vols[:-1]
    mu = statistics.fmean(baseline)
    sigma = statistics.pstdev(baseline)
    if sigma == 0:
        return None
    z = (latest - mu) / sigma
    if z >= z_threshold:
        return Flag("volume_zscore", round(z, 2), f"volume {latest:.0f} is {z:.1f}\u03c3 above mean {mu:.0f}")
    return None


def coherence_flag(
    latest_prices: Mapping[str, float],
    *,
    tolerance: float = 0.03,
) -> Flag | None:
    """Given the latest price per outcome token in ONE market, flag if they do
    not sum to ~1.0. Works for binary and multi-outcome markets."""
    prices = [p for p in latest_prices.values() if p is not None]
    if len(prices) < 2:
        return None
    total = sum(prices)
    deviation = total - 1.0
    if abs(deviation) > tolerance:
        return Flag("coherence", round(deviation, 4),
                    f"outcomes sum to {total:.3f} (off by {deviation:+.3f})")
    return None
