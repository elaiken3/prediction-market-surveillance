"""Tests for streaming.detectors."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from streaming.detectors import (
    Tick, coherence_flag, price_range_flag, volume_zscore_flag,
)

T0 = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)


def _ticks(prices, step=5):
    return [Tick("0xtok", T0 + timedelta(seconds=i * step), p) for i, p in enumerate(prices)]


def test_price_range_fires_on_swing():
    flag = price_range_flag(_ticks([0.50, 0.55, 0.72], step=10), max_range=0.15, window_seconds=60)
    assert flag and flag.rule == "price_range"


def test_price_range_quiet_when_stable():
    assert price_range_flag(_ticks([0.50, 0.51, 0.49], step=10), max_range=0.15) is None


def test_price_range_needs_two_points():
    assert price_range_flag(_ticks([0.5]), max_range=0.15) is None


def test_volume_zscore_fires():
    vols = [100, 120, 90, 110, 105, 95, 1000]
    flag = volume_zscore_flag(vols, z_threshold=3.0)
    assert flag and flag.score >= 3.0


def test_volume_zscore_quiet_on_normal():
    assert volume_zscore_flag([100, 120, 90, 110, 105, 95, 108], z_threshold=3.0) is None


def test_volume_zscore_needs_history():
    assert volume_zscore_flag([100, 1000], min_history=5) is None


def test_coherence_fires_when_off():
    # YES 0.60 + NO 0.60 = 1.20 -> incoherent
    assert coherence_flag({"yes": 0.60, "no": 0.60}, tolerance=0.03) is not None


def test_coherence_ok_when_sums_to_one():
    assert coherence_flag({"yes": 0.62, "no": 0.38}, tolerance=0.03) is None


def test_coherence_multi_outcome():
    flag = coherence_flag({"a": 0.4, "b": 0.4, "c": 0.4}, tolerance=0.03)  # sums to 1.2
    assert flag and flag.rule == "coherence"


def test_coherence_needs_two_outcomes():
    assert coherence_flag({"yes": 0.62}, tolerance=0.03) is None
