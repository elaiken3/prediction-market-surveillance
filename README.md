# Prediction-Market Surveillance

A real-time surveillance pipeline for a live prediction market (Polymarket),
built around one question:

> **Can you trust a live market feed enough to act on it?**

It treats a prediction market like a market a bank would have to supervise:
enforce a data contract on every message, never silently drop a bad one, detect
dislocations and manipulation footprints in real time, and *prove* the stream
told the truth by reconciling it against batch — and against how markets
actually resolved.

It's transaction-fraud surveillance, pointed at a live order book instead of a
card network.

```
  Polymarket market WS ─┐
 (public, no auth)      │ normalize → canonical MarketEvent
  synthetic generator ──┘            │
 (offline fallback)                  ▼
                          ┌──────────────┐  markets.events
                          │  Redpanda    │  markets.dlq
                          └──────┬───────┘
                                 ▼
                       ┌────────────────────────┐
                       │ PySpark Structured      │
                       │ Streaming               │
                       │ parse → validate        │
                       │   valid / reject(reason)│
                       │ windowed price-range    │
                       │ + volume                │
                       └───┬────┬────┬───────────┘
                           ▼    ▼    ▼
                       valid/ flagged/ dlq/        (parquet lake)
                           │    │    │
                           ▼    ▼    ▼
                  ┌──────────────────────────┐       ┌──────────────┐
                  │ dbt (DuckDB / Snowflake)  │ ◀──── │   Airflow    │
                  │ price history             │ /15m  │ fetch resolns│
                  │ coherence (Σ outcomes≈1)  │       │ + build + rec│
                  │ volume anomalies (z)      │       └──────────────┘
                  │ RECONCILE  +  CALIBRATION │
                  └────────────┬──────────────┘
                               ▼
                    Streamlit data-health dashboard
```

## The three ideas that make this more than a Kafka demo

1. **Coherence as contract + signal.** A market's outcomes should price to ~1.0.
   A persistent deviation is an arbitrage or a manipulation footprint — *and* a
   data-quality check on the feed. (`fct_coherence`)
2. **Reconciliation.** Real-time price-range flags are recomputed in batch and
   labelled `BOTH` / `STREAM_ONLY` / `BATCH_ONLY`, so stream/batch disagreement
   (late data, watermark artefacts) is a metric, not a surprise. (`rec_stream_vs_batch`)
3. **Resolution as ground truth.** Markets resolve, so you get the real answer.
   `fct_calibration` scores each market's final price against the outcome with a
   Brier score — turning "is this trustworthy?" into a number.

## What runs where (the architectural bet)

| Concern | Where | Why |
|---|---|---|
| Price dislocation, volume burst | **Stream** (Spark windows) | must fire in seconds |
| Cross-outcome coherence | **Batch** (dbt) | needs latest price across *all* outcomes |
| Volume z-score baseline | **Batch** (dbt) | needs per-asset history |
| Calibration vs reality | **Batch** (dbt) | needs resolution outcomes |
| "Did the stream lie?" | **Batch** (dbt reconcile) | compares the two paths |

## Reliability: the dashboard outlives the collector

The public dashboard reads **static Parquet from S3**, not a live database. The
collector can crash, fill its disk, or be stopped for cost and the dashboard keeps
serving the last good data instead of erroring. The batch job rebuilds the marts and
syncs them to S3 every 15 minutes.

The flip side of a decoupled serving store is that "the build passed" does not mean
"the dashboard is fresh" — a publish can die while every build stays green. So
freshness is monitored on two layers: an **on-box** timer that alarms when a publish
breaks while the collector is up, and an **off-box** GitHub Actions cron that checks
the S3 `Last-Modified` and so still fires when the whole box is down. See
`deploy/README.md` for the operational detail.

## Quickstart

```bash
uv venv && source .venv/bin/activate && uv pip install -e ".[dev]"
make up && make topics

# EITHER live Polymarket data (public market channel, no auth):
make live
# OR fully offline synthetic stream (no network):
make synth

make stream          # PySpark Structured Streaming  (separate shell)
make resolutions     # pull resolved markets for calibration
make dbt             # build marts + tests (local DuckDB)
make dash            # live dashboard
make test            # unit tests for the pure logic (no infra)
```

Snowflake instead of DuckDB: `dbt build --target snowflake` with `SNOWFLAKE_*` set.

## Tested core vs run-on-your-machine

The brittle, logic-heavy parts are pure and unit-tested: the **contract
validator**, the **detectors** (price-range, volume z-score, coherence), and the
**Polymarket message normaliser**. The websocket client, Spark job, dbt models,
and Airflow DAG are written to run but need your machine + network.

> Polymarket's wire format evolves. `ingestion/normalize.py` is the single place
> that knows it; confirm field paths against https://docs.polymarket.com and the
> tests in `tests/test_normalize.py` will keep the mapping honest.

## Layout

| Path | What |
|---|---|
| `contracts/market_event.schema.json` | the canonical data contract |
| `ingestion/polymarket_ws.py` | live market-channel websocket → Kafka |
| `ingestion/synthetic.py` | offline generator (manipulation + dirty records) |
| `ingestion/normalize.py` | **pure, tested** raw→canonical mapping |
| `ingestion/fetch_resolutions.py` | Gamma API resolved markets → parquet |
| `streaming/validate.py`, `detectors.py` | **pure, tested** rules |
| `streaming/spark_job.py` | Structured Streaming job |
| `dbt/market_surveillance/` | price history, coherence, volume, reconcile, calibration |
| `dashboard/app.py` | live data-health view |

---

## Write-up outline (Medium series)

1. **Surveilling a live market like a bank would** — the trust thesis; why a feed
   you'd act on looks different from a demo.
2. **From dbt to Spark Structured Streaming** — windows, watermarks, and a
   dead-letter contract, for people coming from the warehouse world.
3. **When YES + NO ≠ \$1** — coherence as a manipulation *and* data-quality signal.
4. **Did the stream lie? And was the market right?** — stream/batch reconciliation
   plus calibration against real resolutions (Brier scores).
```
