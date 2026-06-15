"""Contract validation for canonical market events.

Pure functions, no Kafka/Spark. Single source of truth is
contracts/market_event.schema.json; test_validate.py asserts these constants
stay in sync with that file so the two can never drift.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

CONTRACT_PATH = Path(__file__).resolve().parents[1] / "contracts" / "market_event.schema.json"

REQUIRED_FIELDS = ("source", "event_type", "event_id", "event_ts", "market_id", "asset_id", "price")
EVENT_TYPES = {"trade", "quote", "book"}
SIDES = {"buy", "sell"}


@dataclass
class ValidationResult:
    valid: bool
    errors: list[str] = field(default_factory=list)

    @property
    def reason(self) -> str:
        return "; ".join(self.errors)


def _is_iso_datetime(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def validate_record(record: Any) -> ValidationResult:
    """Validate one canonical market event. Returns a result, never raises:
    a bad record in a stream is data to route to a dead-letter topic, not a crash."""
    errors: list[str] = []
    if not isinstance(record, dict):
        return ValidationResult(False, ["record is not an object"])

    for f in REQUIRED_FIELDS:
        if record.get(f) is None:
            errors.append(f"missing required field: {f}")
    if errors:
        return ValidationResult(False, errors)

    if not str(record["source"]).strip():
        errors.append("source is blank")
    if record["event_type"] not in EVENT_TYPES:
        errors.append(f"event_type not in {sorted(EVENT_TYPES)}")
    if len(str(record["event_id"])) < 6:
        errors.append("event_id too short")
    if not _is_iso_datetime(record["event_ts"]):
        errors.append("event_ts is not a valid ISO-8601 datetime")
    if not str(record["market_id"]).strip():
        errors.append("market_id is blank")
    if not str(record["asset_id"]).strip():
        errors.append("asset_id is blank")

    price = record["price"]
    if not isinstance(price, (int, float)) or isinstance(price, bool):
        errors.append("price is not numeric")
    elif not (0.0 <= price <= 1.0):
        errors.append("price outside [0, 1]")

    size = record.get("size")
    if size is not None:
        if not isinstance(size, (int, float)) or isinstance(size, bool):
            errors.append("size is not numeric")
        elif size < 0:
            errors.append("size is negative")

    side = record.get("side")
    if side is not None and side not in SIDES:
        errors.append(f"side not in {sorted(SIDES)}")

    return ValidationResult(not errors, errors)


def load_contract() -> dict[str, Any]:
    with CONTRACT_PATH.open() as fh:
        return json.load(fh)
