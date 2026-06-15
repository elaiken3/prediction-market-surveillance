"""Live Polymarket market-channel ingestion.

Connects to the PUBLIC market-data websocket (no auth needed for market data),
subscribes to a set of outcome token ids, normalises each message to the
canonical contract, and produces to Kafka. Anything that normalises to nothing
(unknown event type) is sent to a dead-letter topic -- we never silently drop.

Token ids are discovered from the public Gamma metadata API. Run:

    python -m ingestion.polymarket_ws --markets 8 --topic markets.events

Notes / things to confirm against current docs (https://docs.polymarket.com):
  * market channel URL and the subscribe payload shape can change;
  * respect ~5 concurrent ws connections per IP and back off on 429s.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time

import requests
import websockets

from ingestion.normalize import normalize

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"


def discover_token_ids(limit: int) -> list[str]:
    """Pull active markets from Gamma and return their outcome token ids."""
    resp = requests.get(GAMMA_MARKETS, params={"active": "true", "closed": "false", "limit": limit}, timeout=15)
    resp.raise_for_status()
    token_ids: list[str] = []
    for m in resp.json():
        raw = m.get("clobTokenIds") or m.get("clob_token_ids") or "[]"
        ids = json.loads(raw) if isinstance(raw, str) else raw
        token_ids.extend(str(t) for t in ids)
    return token_ids


def _make_producer(bootstrap: str):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k or "").encode("utf-8"),
        acks="all",
        linger_ms=20,
    )


async def run(args) -> None:
    token_ids = discover_token_ids(args.markets)
    if not token_ids:
        raise SystemExit("no token ids discovered from Gamma; check connectivity/params")
    print(f"subscribing to {len(token_ids)} outcome tokens")
    producer = None if args.dry_run else _make_producer(args.bootstrap)

    backoff = 1.0
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, max_size=2**22) as ws:
                await ws.send(json.dumps({"type": "market", "assets_ids": token_ids}))
                backoff = 1.0  # reset on a clean connect
                async for raw in ws:
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        _emit_dlq(producer, args, raw, "non-json frame")
                        continue
                    messages = payload if isinstance(payload, list) else [payload]
                    for msg in messages:
                        events = normalize(msg)
                        if not events:
                            _emit_dlq(producer, args, json.dumps(msg), f"unknown event_type={msg.get('event_type')}")
                            continue
                        for ev in events:
                            if args.dry_run:
                                print(json.dumps(ev))
                            else:
                                producer.send(args.topic, key=ev.get("asset_id"), value=ev)
        except (websockets.ConnectionClosed, OSError) as exc:
            print(f"ws disconnected ({exc}); reconnecting in {backoff:.0f}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)  # exponential backoff, capped
        finally:
            if producer is not None:
                producer.flush()


def _emit_dlq(producer, args, raw_text: str, reason: str) -> None:
    record = {"raw": raw_text, "reject_reason": reason}
    if producer is None:
        print(json.dumps({"_dlq": record}))
    else:
        producer.send(args.dlq_topic, value=record)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--topic", default="markets.events")
    ap.add_argument("--dlq-topic", default="markets.dlq")
    ap.add_argument("--markets", type=int, default=8, help="number of active markets to subscribe to")
    ap.add_argument("--dry-run", action="store_true", help="print events instead of producing")
    args = ap.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
