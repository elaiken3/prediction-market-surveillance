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
from datetime import datetime, timezone

import requests
import websockets

from ingestion.normalize import normalize

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"


def _token_ids(m: dict) -> list[str]:
    raw = m.get("clobTokenIds") or m.get("clob_token_ids") or "[]"
    ids = json.loads(raw) if isinstance(raw, str) else raw
    return [str(t) for t in ids]


def _volume(m: dict) -> float:
    for k in ("volumeNum", "volume", "volume24hr"):
        try:
            return float(m.get(k))
        except (TypeError, ValueError):
            continue
    return 0.0


def _future_end(m: dict):
    """Parsed endDate if it is in the future, else None."""
    ed = m.get("endDate") or m.get("end_date")
    if not ed:
        return None
    try:
        dt = datetime.fromisoformat(str(ed).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt > datetime.now(timezone.utc) else None


def _cid(m: dict) -> str:
    return m.get("conditionId") or m.get("condition_id") or str(id(m))


def discover_token_ids(markets: int, resolve_soon: int = 0) -> list[str]:
    """Outcome token ids for a mix of high-volume and soon-resolving markets.

    `resolve_soon` of the `markets` slots go to the markets whose endDate is
    nearest in the future, so calibration (fct_calibration) gets ground truth
    without waiting on long-dated markets; the rest go to the highest-volume
    active markets, the interesting surveillance targets. resolve_soon=0
    reproduces plain top-by-volume selection.
    """
    resp = requests.get(
        GAMMA_MARKETS,
        params={"active": "true", "closed": "false", "limit": 100},
        timeout=15,
    )
    resp.raise_for_status()
    pool = [m for m in resp.json() if _token_ids(m)]
    if not pool:
        return []

    resolve_soon = max(0, min(resolve_soon, markets))
    chosen: list[dict] = []
    seen: set[str] = set()

    # Reserve slots for the soonest-resolving markets (future endDate, ascending).
    soon = sorted(((d, m) for m in pool if (d := _future_end(m))), key=lambda x: x[0])
    for _, m in soon[:resolve_soon]:
        if _cid(m) not in seen:
            seen.add(_cid(m))
            chosen.append(m)
    soon_count = len(chosen)

    # Fill the remaining slots with the highest-volume markets.
    for m in sorted(pool, key=_volume, reverse=True):
        if len(chosen) >= markets:
            break
        if _cid(m) not in seen:
            seen.add(_cid(m))
            chosen.append(m)

    token_ids: list[str] = []
    for m in chosen:
        token_ids.extend(_token_ids(m))
    print(
        f"discovered {len(chosen)} markets "
        f"({soon_count} soon-resolving + {len(chosen) - soon_count} by volume) "
        f"-> {len(token_ids)} tokens"
    )
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
    producer = None if args.dry_run else _make_producer(args.bootstrap)

    backoff = 1.0
    while True:
        try:
            # Re-discover token ids on every (re)connect. Markets close and go
            # quiet, so reusing the startup set can reconnect to channels that
            # emit nothing; a fresh set keeps us on active markets.
            token_ids = discover_token_ids(args.markets, args.resolve_soon)
            if not token_ids:
                raise RuntimeError("no active token ids discovered from Gamma")
            print(f"subscribing to {len(token_ids)} outcome tokens")

            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=20, max_size=2**22
            ) as ws:
                await ws.send(json.dumps({"type": "market", "assets_ids": token_ids}))
                backoff = 1.0  # reset on a clean connect
                while True:
                    # Liveness watchdog: the socket can stay open (pings answered)
                    # while the server streams zero market data. Without a timeout
                    # the receive blocks forever -- the silent 4-day stall that
                    # froze the lake while every container still showed "Up". A
                    # timeout forces a fresh reconnect + resubscribe instead.
                    raw = await asyncio.wait_for(ws.recv(), timeout=args.idle_timeout)
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
        except asyncio.TimeoutError:
            print(f"no market data for {args.idle_timeout}s; forcing reconnect")
        except (websockets.ConnectionClosed, OSError, requests.RequestException, RuntimeError) as exc:
            print(f"ws disconnected ({exc}); reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
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
    ap.add_argument("--resolve-soon", type=int, default=0,
                    help="reserve this many of --markets slots for the soonest-resolving markets (feeds calibration)")
    ap.add_argument("--idle-timeout", type=float, default=120.0,
                    help="seconds of zero market data before forcing a reconnect (liveness watchdog)")
    ap.add_argument("--dry-run", action="store_true", help="print events instead of producing")
    args = ap.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
