"""Offline synthetic market-event generator.

Lets you build and test the whole pipeline with NO network and NO Polymarket
dependency -- useful for CI, demos, and reproducing manipulation scenarios on
demand. Emits the same canonical contract as the live ingestion, and injects:

  * MANIPULATION: price-spike bursts and volume bursts on a single outcome.
  * INCOHERENCE: a market whose YES + NO temporarily stops summing to ~1.
  * DIRTY: contract violations (price > 1, bad event_type, missing fields).

    python -m ingestion.synthetic --dry-run --count 20 --seed 1
    python -m ingestion.synthetic --bootstrap localhost:19092 --topic markets.events --count 5000
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

# A few fake binary markets, each with a YES and NO token and a "true" prob.
MARKETS = {
    f"0xmkt{n:03d}": {
        "yes": f"0xyes{n:03d}",
        "no": f"0xno{n:03d}",
        "p": random.uniform(0.2, 0.8),
    }
    for n in range(1, 9)
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _event(market_id, asset_id, outcome, price, *, etype="quote", size=None, side=None) -> dict:
    return {
        "source": "synthetic",
        "event_type": etype,
        "event_id": uuid.uuid4().hex[:16],
        "event_ts": _now_iso(),
        "market_id": market_id,
        "asset_id": asset_id,
        "outcome": outcome,
        "price": round(price, 3),
        "size": size,
        "side": side,
    }


def _quote_pair(market_id: str) -> list[dict]:
    """A coherent YES/NO quote pair around the market's true probability."""
    m = MARKETS[market_id]
    p = min(max(m["p"] + random.gauss(0, 0.02), 0.02), 0.98)
    return [
        _event(market_id, m["yes"], "Yes", p),
        _event(market_id, m["no"], "No", 1 - p),
    ]


def _trade(market_id: str) -> dict:
    m = MARKETS[market_id]
    side = random.choice(["buy", "sell"])
    return _event(market_id, m["yes"], "Yes", min(max(m["p"] + random.gauss(0, 0.02), 0.02), 0.98),
                  etype="trade", size=round(random.uniform(10, 300), 1), side=side)


def _manip_price_spike(market_id: str) -> list[dict]:
    m = MARKETS[market_id]
    base = m["p"]
    return [_event(market_id, m["yes"], "Yes", min(0.98, base + 0.25 * i), etype="trade",
                   size=round(random.uniform(50, 200), 1), side="buy") for i in range(1, 5)]


def _manip_volume_burst(market_id: str) -> list[dict]:
    m = MARKETS[market_id]
    return [_event(market_id, m["yes"], "Yes", m["p"], etype="trade",
                   size=round(random.uniform(800, 1500), 1), side=random.choice(["buy", "sell"]))
            for _ in range(6)]


def _incoherent(market_id: str) -> list[dict]:
    """Emit YES and NO that do NOT sum to 1 (arbitrage / bad feed)."""
    m = MARKETS[market_id]
    p = m["p"]
    return [_event(market_id, m["yes"], "Yes", min(0.98, p + 0.15)),
            _event(market_id, m["no"], "No", min(0.98, (1 - p) + 0.15))]


_DIRTY = [
    lambda e: {**e, "price": 1.7},
    lambda e: {**e, "event_type": "garbage"},
    lambda e: {k: v for k, v in e.items() if k != "asset_id"},
    lambda e: {**e, "size": -10, "event_type": "trade"},
]


def _dirty(market_id: str) -> dict:
    base = _quote_pair(market_id)[0]
    return random.choice(_DIRTY)(base)


def stream(count: int, manip_rate: float, dirty_rate: float):
    emitted = 0
    while emitted < count:
        mkt = random.choice(list(MARKETS))
        roll = random.random()
        if roll < dirty_rate:
            batch = [_dirty(mkt)]
        elif roll < dirty_rate + manip_rate:
            batch = random.choice([_manip_price_spike, _manip_volume_burst, _incoherent])(mkt)
        elif random.random() < 0.5:
            batch = _quote_pair(mkt)
        else:
            batch = [_trade(mkt)]
        for ev in batch:
            if emitted >= count:
                break
            yield ev
            emitted += 1


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--topic", default="markets.events")
    ap.add_argument("--count", type=int, default=2000)
    ap.add_argument("--rate", type=float, default=20.0)
    ap.add_argument("--manip-rate", type=float, default=0.05)
    ap.add_argument("--dirty-rate", type=float, default=0.05)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: (k or "").encode("utf-8"),
            acks="all", linger_ms=20,
        )

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    sent = 0
    count = args.count if args.count > 0 else 10**18  # 0 (or negative) => run forever
    try:
        for ev in stream(count, args.manip_rate, args.dirty_rate):
            if args.dry_run:
                print(json.dumps(ev))
            else:
                producer.send(args.topic, key=ev.get("asset_id"), value=ev)
            sent += 1
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        if producer is not None:
            producer.flush()
    if not args.dry_run:
        print(f"produced {sent} events to {args.topic}")


if __name__ == "__main__":
    main()
