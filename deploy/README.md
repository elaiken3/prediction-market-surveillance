# Running on a VM

This folder turns the project into an unattended, reboot-surviving deployment.
Develop locally; run the long-lived collector here so your laptop stays free.

## What runs

| Service | Always on? | Notes |
|---|---|---|
| `redpanda` | yes | broker, persistent volume, topics created with retention |
| `ingestion` | yes | `INGEST_MODE=live` (Polymarket) or `synthetic` (offline) |
| `spark-stream` | yes | Structured Streaming → partitioned Parquet, 60s trigger |
| `dashboard` | yes | Streamlit on `127.0.0.1:8501` |
| `batch` | on a timer | resolutions + dbt build + reconciliation, every 15 min |

Always-on services use `restart: unless-stopped`; `surveillance.service` brings
the whole stack up on boot.

## Quick deploy

```bash
git clone <repo> /opt/prediction-market-surveillance
cd /opt/prediction-market-surveillance
WITH_TAILSCALE=1 sudo bash deploy/bootstrap.sh        # installs Docker, Tailscale, systemd units
sudo systemctl start surveillance.service
```

Smoke-test offline first (no network needed) with `INGEST_MODE=synthetic`, then
switch to `live` once data is flowing end to end.

## Sizing

8GB RAM / 2 vCPU / 80GB disk is comfortable (Hetzner CX32 or an Oracle Always
Free Ampere instance). 4GB works if you don't run the dashboard and Spark heavy
at once. Spark `local[*]` will use all cores — cap it in the compose `command`
(`--master local[2]`) on a small box.

## Access (don't expose ports publicly)

Both `19092` and `8501` bind to `127.0.0.1` only. Reach the dashboard by either:
- **Tailscale** (recommended): `sudo tailscale up`, then `http://<tailscale-ip>:8501`.
- **SSH tunnel**: `ssh -L 8501:127.0.0.1:8501 user@vm` and open `localhost:8501`.

## Disk hygiene (matters for multi-day runs)

- Spark writes **date-partitioned** Parquet with a 60s trigger, so files stay
  large and old days are easy to prune: `find data/lake -type d -name 'event_date=*' -mtime +14 -exec rm -rf {} +`.
- Kafka topics carry retention (events 3d, dlq 7d) so the broker disk is bounded.
- Watch `data/checkpoints` and `du -sh data/*`; add a block volume if you collect
  for weeks.

## Going further

- **Public dashboard:** point dbt at MotherDuck (swap the `local` profile target)
  and host the Streamlit app on Streamlit Community Cloud reading from it — gives
  you a shareable URL without exposing the VM.
- **Snowflake sink:** set `SNOWFLAKE_*` and run the batch with `--target snowflake`.

## Troubleshooting

- **Spark image arch:** on ARM (Oracle Ampere), confirm `apache/spark:3.5.1-python3`
  resolves for aarch64, or pin a matching tag in `Dockerfile.spark`.
- **First Spark start is slow:** it downloads the Kafka connector into the `ivy`
  volume once; subsequent restarts are fast and offline.
- **No data in dashboard:** check `docker compose ... logs -f ingestion spark-stream`;
  the dashboard reads the shared `data/lake` volume the Spark service writes.
