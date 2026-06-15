"""Normalise raw Polymarket market-channel messages into canonical MarketEvents.

Pure and unit-tested so the brittle part -- mapping a third-party wire format to
our contract -- is pinned down by tests rather than discovered in production.

IMPORTANT: Polymarket's exact field names evolve. The mappings below follow the
documented market channel (each message carries an `event_type`; see
https://docs.polymarket.com/developers/CLOB/websocket/market-channel). Confirm
field paths against a live message before relying on them, then update the
EXTRACTORS table here -- that is the single place this knowledge lives.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

SOURCE = "polymarket"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts(raw_ts: Any) -> str:
    """Polymarket timestamps are usually epoch millis as a string. Fall back to
    ingest time if absent or unparseable -- never drop an event for lack of a ts."""
    if raw_ts in (None, ""):
        return _now_iso()
    try:
        millis = int(raw_ts)
        return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return str(raw_ts)


def _event_id(*parts: Any) -> str:
    digest = hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()
    return digest[:16]


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _from_last_trade_price(msg: dict) -> list[dict]:
    return [{
        "source": SOURCE,
        "event_type": "trade",
        "event_id": _event_id(msg.get("asset_id"), msg.get("timestamp"), msg.get("price"), msg.get("size")),
        "event_ts": _ts(msg.get("timestamp")),
        "market_id": msg.get("market") or msg.get("condition_id"),
        "asset_id": msg.get("asset_id"),
        "outcome": msg.get("outcome"),
        "price": _to_float(msg.get("price")),
        "size": _to_float(msg.get("size")),
        "side": (msg.get("side") or "").lower() or None,
    }]


def _from_price_change(msg: dict) -> list[dict]:
    """A price_change message can carry several changes; emit one quote each."""
    out: list[dict] = []
    market = msg.get("market") or msg.get("condition_id")
    ts = _ts(msg.get("timestamp"))
    for ch in msg.get("price_changes", msg.get("changes", [])) or []:
        out.append({
            "source": SOURCE,
            "event_type": "quote",
            "event_id": _event_id(ch.get("asset_id"), msg.get("timestamp"), ch.get("price")),
            "event_ts": ts,
            "market_id": market,
            "asset_id": ch.get("asset_id"),
            "outcome": ch.get("outcome"),
            "price": _to_float(ch.get("price")),
            "size": _to_float(ch.get("size")),
            "side": (ch.get("side") or "").lower() or None,
        })
    return out


def _from_book(msg: dict) -> list[dict]:
    """Top-of-book snapshot -> a single 'book' event at the best price we can find."""
    bids = msg.get("bids") or msg.get("buys") or []
    best_bid = _to_float(bids[0].get("price")) if bids else None
    return [{
        "source": SOURCE,
        "event_type": "book",
        "event_id": _event_id(msg.get("asset_id"), msg.get("timestamp"), "book"),
        "event_ts": _ts(msg.get("timestamp")),
        "market_id": msg.get("market") or msg.get("condition_id"),
        "asset_id": msg.get("asset_id"),
        "outcome": msg.get("outcome"),
        "price": best_bid,
        "size": None,
        "side": None,
    }]


EXTRACTORS = {
    "last_trade_price": _from_last_trade_price,
    "price_change": _from_price_change,
    "book": _from_book,
}


def normalize(msg: dict) -> list[dict]:
    """Map one raw Polymarket message to zero or more canonical MarketEvents.

    Unknown event types return [] -- the websocket client routes those to the
    dead-letter topic so we never silently swallow an unrecognised message.
    """
    if not isinstance(msg, dict):
        return []
    handler = EXTRACTORS.get(msg.get("event_type"))
    return handler(msg) if handler else []