"""Tests for ingestion.normalize.

Sample payloads are illustrative of the documented market-channel shape; if the
live format differs, update both these fixtures and ingestion/normalize.py
together so the test keeps guarding the mapping.
"""
from __future__ import annotations

from ingestion.normalize import normalize
from streaming.validate import validate_record


def test_last_trade_price_maps_to_trade():
    msg = {
        "event_type": "last_trade_price",
        "asset_id": "0xtokenyes",
        "market": "0xcondition",
        "price": "0.63",
        "size": "150",
        "side": "BUY",
        "timestamp": "1750000000000",
        "outcome": "Yes",
    }
    out = normalize(msg)
    assert len(out) == 1
    ev = out[0]
    assert ev["event_type"] == "trade" and ev["price"] == 0.63 and ev["side"] == "buy"
    assert validate_record(ev).valid


def test_price_change_emits_one_quote_per_change():
    msg = {
        "event_type": "price_change",
        "market": "0xcondition",
        "timestamp": "1750000000000",
        "changes": [
            {"asset_id": "0xyes", "price": "0.61"},
            {"asset_id": "0xno", "price": "0.39"},
        ],
    }
    out = normalize(msg)
    assert len(out) == 2
    assert {o["asset_id"] for o in out} == {"0xyes", "0xno"}
    assert all(validate_record(o).valid for o in out)


def test_unknown_event_type_returns_empty():
    assert normalize({"event_type": "heartbeat"}) == []


def test_non_dict_returns_empty():
    assert normalize("not-a-dict") == []  # type: ignore[arg-type]
