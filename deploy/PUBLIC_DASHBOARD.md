# Public dashboard: MotherDuck + Streamlit Community Cloud

Goal: a public, shareable dashboard URL for your resume/LinkedIn, with **no raw
data and no open ports** on the collector VM. The VM's dbt run pushes only the
aggregated marts up to MotherDuck; the hosted app reads those.

```
  VM (private)                         Cloud (public)
  ┌───────────────────────┐
  │ Spark → parquet lake   │           ┌────────────────────────┐
  │ dbt build              │  marts    │  MotherDuck             │
  │   --target motherduck  │ ────────▶ │  market_surveillance.*  │
  └───────────────────────┘           └───────────┬────────────┘
   raw events never leave                          │ read-only token
                                       ┌───────────▼────────────┐
                                       │ Streamlit Community     │
                                       │ Cloud  (public URL)     │
                                       └─────────────────────────┘
```

## One-time setup

1. **MotherDuck account** (free tier is fine). Create two tokens in Settings:
   - a **write** token for the VM,
   - a **read-only / read-scaling** token for the public dashboard.
2. **On the VM**, run the batch against MotherDuck:
   ```bash
   export MOTHERDUCK_TOKEN=<write-token>
   DBT_TARGET=motherduck MOTHERDUCK_TOKEN=$MOTHERDUCK_TOKEN \
     docker compose -f deploy/docker-compose.vm.yml run --rm batch
   ```
   To make the scheduled batch publish every 15 min, add those two env lines to
   `dbt-batch.service` (`Environment=DBT_TARGET=motherduck` and
   `Environment=MOTHERDUCK_TOKEN=...`), then `systemctl daemon-reload`.
   This reads the **local** lake and writes the marts into MotherDuck — only the
   small aggregated tables leave the box.
3. **Deploy the dashboard** on https://share.streamlit.io:
   - Repo: your GitHub repo. Main file path: `dashboard/cloud_app.py`.
   - Dependencies: `dashboard/requirements.txt` (auto-detected).
   - In the app's **Secrets**, paste:
     ```toml
     motherduck_token = "<read-only-token>"
     md_database = "market_surveillance"
     ```
4. Done — share the resulting `*.streamlit.app` URL.

## What the public dashboard shows

Ingest health + contract reject rate, dead-letter reasons, market coherence
violations, the stream-vs-batch reconciliation split, and calibration (Brier
score) against real resolutions — i.e. the whole trust story, none of the raw feed.

## Notes

- The marts land in the `main` schema of the `market_surveillance` database by
  default; `cloud_app.py` queries them unqualified. If you set a custom dbt
  schema, qualify the table names accordingly.
- Keep the write token on the VM only. The dashboard uses the read-only token so
  a public viewer can never mutate anything.
- MotherDuck preloads common extensions (parquet/httpfs) but not custom
  extensions/UDFs — the models here stay within that.
- `make dash` still runs the **local** dashboard (`dashboard/app.py`) straight off
  the parquet lake; `cloud_app.py` is the MotherDuck-backed hosted variant.
