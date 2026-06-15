"""Tests for streaming.validate (market event contract)."""
from __future__ import annotations

from streaming.validate import EVENT_TYPES, REQUIRED_FIELDS, load_contract, validate_record


def _good() -> dict:
    return {
        "source": "polymarket",
        "event_type": "trade",
        "event_id": "abc123def456",
        "event_ts": "2026-06-15T12:00:00+00:00",
        "market_id": "0xcondition",
        "asset_id": "0xtokenyes",
        "outcome": "Yes",
        "price": 0.62,
        "size": 120.0,
        "side": "buy",
    }


def test_valid_passes():
    assert validate_record(_good()).valid


def test_missing_required_fails():
    rec = _good(); del rec["price"]
    r = validate_record(rec)
    assert not r.valid and any("price" in e for e in r.errors)


def test_price_out_of_range_fails():
    assert not validate_record({**_good(), "price": 1.4}).valid
    assert not validate_record({**_good(), "price": -0.1}).valid


def test_negative_size_fails():
    assert not validate_record({**_good(), "size": -5}).valid


def test_null_size_ok():
    assert validate_record({**_good(), "size": None}).valid


def test_bad_event_type_fails():
    assert not validate_record({**_good(), "event_type": "explosion"}).valid


def test_bad_side_fails():
    assert not validate_record({**_good(), "side": "sideways"}).valid


def test_zulu_ts_ok():
    assert validate_record({**_good(), "event_ts": "2026-06-15T12:00:00Z"}).valid


def test_validator_in_sync_with_contract():
    contract = load_contract()
    assert set(contract["required"]) == set(REQUIRED_FIELDS)
    assert set(contract["properties"]["event_type"]["enum"]) == EVENT_TYPES
