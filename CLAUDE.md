# CLAUDE.md: Prediction-Market Surveillance

Context and operating guide for working on this repository in Claude Code. Read this first.
Repo: https://github.com/elaiken3/prediction-market-surveillance (public)

---

## 1. What this project is

A real-time surveillance pipeline over a live Polymarket feed. It treats a prediction
market the way a bank treats a transaction stream it must supervise: enforce a data
contract on every message, never silently drop a bad one, detect dislocations and
manipulation footprints in real time, and prove the stream told the truth by reconciling
it against a batch recomputation and against how markets actually resolved.

Two separate jobs, kept deliberately distinct:
1. **Data integrity**: is every message well-formed and trustworthy?
2. **Surveillance**: given trustworthy data, is the market behaving coherently?

---

## 2. Architecture (data flow)

```
Polymarket WebSocket (or synthetic generator fallback)
  → normalize to canonical MarketEvent
  → Redpanda (Kafka-compatible)   topics: markets.events, markets.dlq
  → PySpark Structured Streaming  (validate vs JSON contract; dead-letter rejects;
                                   windowed price-range / volume detections)
  → date-partitioned Parquet lake (zones: valid / dlq / flagged / resolutions)
  → dbt marts on DuckDB           (local target builds warehouse.duckdb)
  → publish_marts.py exports marts → Parquet
  → S3 (public-read)              s3://pms-marts-elaiken3/marts/*.parquet
  → Streamlit dashboard           reads Parquet from S3 over HTTPS
```

The serving layer (dashboard) depends on **nothing live**. It reads static Parquet from
S3. The collector can be dead and the dashboard still serves the last good data. This is
intentional and hard-won (see §9).

---

## 3. Repo layout

```
contracts/market_event.schema.json   The data contract (JSON schema) every event is validated against
ingestion/
  polymarket_ws.py                   WebSocket client → normalized events → Redpanda
  normalize.py                       Raw update → canonical MarketEvent (contract shape)
  synthetic.py                       Offline synthetic event generator (fallback / tests)
  fetch_resolutions.py               Pulls resolved-market outcomes (for calibration)
  seed_lake.py                       Seeds empty lake zones so dbt never fails on missing paths
  seed_resolutions.py                Seeds resolution data
  publish_marts.py                   Exports dbt marts from warehouse.duckdb → data/marts/*.parquet
streaming/
  spark_job.py                       PySpark Structured Streaming job (the heart of ingest)
  validate.py                        Contract validation + dead-letter routing
  detectors.py                       Windowed price-range / volume anomaly detectors
dbt/market_surveillance/
  models/staging/stg_market_events.sql
  models/marts/                      fct_ingest_summary, fct_dead_letter_reasons, fct_coherence,
                                     fct_calibration, fct_price_minutely, fct_volume_anomalies,
                                     fct_flagged_recent
  models/reconciliation/rec_stream_vs_batch.sql
  profiles.example.yml               dbt profile template
dashboard/
  app.py                             Local dashboard (reads local DuckDB)
  cloud_app.py                       LEGACY: MotherDuck-backed dashboard (no longer used)
  s3_app.py                          CURRENT public dashboard (reads Parquet from S3), finance-terminal theme
  requirements.txt                   Streamlit Cloud deps
deploy/
  Dockerfile                         Batch/ingestion image (pins duckdb==1.5.3)
  Dockerfile.spark                   Spark image
  docker-compose.vm.yml              Production compose (redpanda, ingestion, spark-stream, dashboard, batch, topic-init)
  bootstrap.sh                       One-shot VM provisioner (Docker, Tailscale, systemd units)
  surveillance.service               systemd: runs the streaming stack
  dbt-batch.service / .timer         systemd: builds marts (ExecStart) + syncs to S3 (ExecStartPost) every 15 min
  publish_to_s3.sh                   Host step (dbt-batch ExecStartPost): aws s3 sync data/marts → S3
  check_marts_fresh.sh               Freshness probe: non-zero exit if newest S3 mart older than threshold
  marts-freshness.service / .timer   systemd: runs check_marts_fresh.sh every 15 min (on-box staleness alarm)
  lake-prune.service / .timer        systemd: daily prune of lake partitions older than 5 days (disk-full guard)
  setup_autoreboot_alarm.sh          Creates a CloudWatch alarm to force-reboot the box on instance status-check fail
  profiles.yml                       dbt profile used on the VM
.github/workflows/marts-freshness.yml  Off-box monitor: GitHub Actions cron curls S3 Last-Modified, fails the job if stale
.streamlit/config.toml               Dark finance theme for the dashboard
tests/                               pytest: test_validate, test_normalize, test_detectors (24 tests)
airflow/dags/surveillance_dag.py     Airflow DAG variant (not the primary deploy path)
Makefile                             Local dev shortcuts
```

---

## 4. Tech stack

Python, PySpark 3.5.1, Redpanda (Kafka API), DuckDB, dbt-duckdb, Streamlit, Parquet,
Docker / docker-compose, AWS EC2 + S3 + IAM, Tailscale. Dev tooling: pytest, ruff,
uv/pyenv. MotherDuck was used earlier and has been removed from the serving path.

---

## 5. Local development

```bash
make up           # start Redpanda + local stack via docker-compose.yml
make topics       # create Kafka topics
make synth        # run synthetic event generator (offline, no network needed)
make live         # run live Polymarket WS ingestion
make stream       # run the Spark streaming job
make resolutions  # fetch resolved-market outcomes
make dbt          # build dbt marts (local DuckDB target)
make test         # run pytest (24 tests)
make dash         # run the local Streamlit dashboard
make down         # stop the stack
make clean        # tear down + clear local data
```

Convention: `DBT_TARGET=local` builds `warehouse.duckdb` locally. There is no MotherDuck
target in the active path anymore.

---

## 6. Live deployment state (as of last session)

**EC2 collector (the "VM"):**
- Instance: `pms-prod-2`, id `i-05de8e049933efea0`, Ubuntu 24.04 x86_64, t3.large, 40 GB gp3
- Region: **us-east-1** (AZ us-east-1d). Confirm from the box via IMDS:
  `TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60"); curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region`
- Public IPv4 **changes on stop/start** (no Elastic IP). Prefer the stable Tailscale IP.
- Tailscale IP: `100.76.72.41` (stable). Dashboard over Tailscale: `http://100.76.72.41:8501`
- SSH key on Mac: `~/.ssh/pms-prod-2.pem` (chmod 400). User: `ubuntu`
- Code path on box: `/opt/prediction-market-surveillance`
- IAM role attached: `pms-s3-writer` (grants S3 put + list on the marts bucket)
- systemd: `surveillance.service` (stack, INGEST_MODE=live, DASH_BIND=0.0.0.0),
  `dbt-batch.timer` (every 15 min → build marts, then ExecStartPost syncs to S3),
  `marts-freshness.timer` (every 15 min → on-box staleness alarm; goes to `failed` if S3 marts age out),
  `lake-prune.timer` (daily → delete partitions older than 5 days; prevents disk-full wedge)
- Env file `/opt/prediction-market-surveillance/.env.motherduck` (name is legacy) contains:
  `DBT_TARGET=local` and `MARTS_S3_BUCKET=pms-marts-elaiken3`. Optional knobs the
  freshness units read: `MARTS_MAX_AGE_SECONDS` (default 1800) and `FRESHNESS_ALERT_WEBHOOK`.

**Monitoring (two layers, because each misses what the other catches):**
- **On-box:** `marts-freshness.timer` runs `check_marts_fresh.sh`, which heads the newest mart on
  S3 and exits non-zero if it is older than `MARTS_MAX_AGE_SECONDS`. A stale publish drives the unit
  to `failed` (visible in `systemctl --failed`). Catches "box up but publish broken." Cannot catch a
  dead box, because it runs on that box.
- **Off-box:** `.github/workflows/marts-freshness.yml` runs every 30 min on GitHub's infra, curls the
  S3 `Last-Modified`, and fails the job (GitHub emails the owner) if older than `MAX_AGE_MINUTES`
  (default 60). Catches "whole box is down/stopped." Optional `MARTS_ALERT_SLACK_WEBHOOK` repo secret
  posts to Slack. Note: it cannot tell "stopped for cost" from "broken," so it will fire during
  deliberate stop windows — raise the threshold or disable the workflow for long stops.

**S3 (the serving store):**
- Bucket: `pms-marts-elaiken3`, region **us-east-1** (same region as the VM)
- Block Public Access: OFF. Bucket policy grants public `s3:GetObject` on `marts/*`
- Bucket uses **owner-enforced** object ownership (ACLs disabled) → do NOT use `--acl` flags
- Public mart URL pattern:
  `https://pms-marts-elaiken3.s3.us-east-1.amazonaws.com/marts/<mart>.parquet`

**Public dashboard (Streamlit Community Cloud):**
- Stable URL: **https://prediction-market-surveillance.streamlit.app/** (custom subdomain, survives app recreation)
- Main file path: `dashboard/s3_app.py`
- Secret (Streamlit Cloud → app settings → Secrets):
  `marts_base_url = "https://pms-marts-elaiken3.s3.us-east-1.amazonaws.com/marts"`
- Auto-redeploys on push to `main`.

---

## 7. Common runbooks

**SSH to the VM:**
```bash
ssh -i ~/.ssh/pms-prod-2.pem ubuntu@100.76.72.41        # stable Tailscale IP
# or use the current Public IPv4 from the EC2 console if Tailscale is down
```

**Manually refresh marts + publish to S3:**
```bash
cd /opt/prediction-market-surveillance
sudo systemctl start dbt-batch.service
journalctl -u dbt-batch.service --no-pager | tail -15   # want PASS=18 ERROR=0, then "synced marts to s3://..."
aws s3 ls s3://pms-marts-elaiken3/marts/
```

**Rebuild the batch image after a code change to ingestion/dbt:**
```bash
docker compose -f deploy/docker-compose.vm.yml build batch
```
Force a clean rebuild (when a pinned dep isn't taking): add `--no-cache`.

**Bring the whole stack up:**
```bash
sudo systemctl start surveillance.service
docker compose -f deploy/docker-compose.vm.yml ps        # want all services Up, redpanda healthy
```

**Verify the public serving path:**
```bash
curl -sI "https://pms-marts-elaiken3.s3.us-east-1.amazonaws.com/marts/fct_ingest_summary.parquet" | head -1
# want: HTTP/1.1 200 OK
curl -sI "https://pms-marts-elaiken3.s3.us-east-1.amazonaws.com/marts/fct_ingest_summary.parquet" | grep -i last-modified
# Last-Modified should be within the last ~15 min if the box is up and publishing
```

**Run the freshness probe by hand (and prove it fires):**
```bash
cd /opt/prediction-market-surveillance
sudo systemctl start marts-freshness.service                 # happy path: "marts fresh: ... Ns old"
systemctl --failed                                           # a stale publish lands the unit here
# force the stale branch (needs MARTS_S3_BUCKET; the env file is not auto-sourced for a manual run):
sudo MARTS_S3_BUCKET=pms-marts-elaiken3 MARTS_MAX_AGE_SECONDS=1 bash deploy/check_marts_fresh.sh; echo "EXIT=$?"
```

**Deploy a code change (always from the Mac, never the VM):**
```bash
# on Mac
git add -A && git commit -m "..." && git push
# on VM
cd /opt/prediction-market-surveillance && git fetch origin && git reset --hard origin/main
# (reset --hard because the VM is a deploy target, not a dev checkout; .env.motherduck is gitignored and survives)
```

> **If the change touched a systemd unit file** (`deploy/*.service` or `*.timer`), `reset --hard`
> alone is NOT enough: the installed copies live in `/etc/systemd/system/`, which git does not touch.
> Reinstall and reload:
> ```bash
> sudo install -m 0644 deploy/<unit> /etc/systemd/system/<unit>
> sudo systemctl daemon-reload
> ```
> (This bit us twice: the publish wiring and the freshness units only took effect after a reinstall.)

---

## 8. Working conventions (important)

- **Everything lives in version control.** Infra, systemd units, migrations, pipeline logic.
  Anything outside source control is bad practice, not a preference.
- **Git pushes only from the Mac.** The VM has no GitHub credentials and should not get any
  (it's a throwaway box). Pull/reset on the VM, never push from it.
- **Surgical fixes over rewrites.** Change only what's necessary; verify the change is isolated.
- **Pin versions across boundaries you don't control.** `duckdb==1.5.3` is pinned in the
  Dockerfile and dashboard requirements for exactly this reason.
- **Writing style** (commits, PRs, docs, articles): no em dashes, US spelling, concise and
  high-signal, change-focused commit messages that explain the *reason* not just the *what*.
- Architecture docs are framed as north-star vision, not mandates, to ease stakeholder buy-in.

---

## 9. Gotchas and war stories (read before debugging deploy issues)

- **A full disk silently kills sshd.** When `/` fills, sshd fails with no clear error and you
  lose all access (SSH, Instance Connect, serial). The `lake-prune.timer` exists to prevent
  this. If a box becomes unreachable on every path, suspect a full disk first.
- **`sudo` strips environment variables.** `WITH_TAILSCALE=1 sudo bash bootstrap.sh` does NOT
  pass the var through (sudo strips it). The bootstrap now defaults `WITH_TAILSCALE=1` so
  Tailscale installs by default. Pattern lesson: bootstrap scripts must default their env vars.
- **DuckDB version pin.** MotherDuck only supported duckdb ≤ 1.5.3; an unpinned duckdb floated
  to 1.5.4 and broke everything. MotherDuck is now out of the path, but the pin stays as
  defensive practice. Pinned in `deploy/Dockerfile` AND `dashboard/requirements.txt`.
- **S3 owner-enforced ACLs.** The bucket has ACLs disabled, so `aws s3 cp/sync --acl public-read`
  fails with `AccessControlListNotSupported`. Public read is granted via a **bucket policy**, not
  per-object ACLs. Never reintroduce `--acl` flags. (This regressed once: `publish_to_s3.sh` shipped
  with `--acl public-read` and every sync failed silently. The flag is now gone; public read still
  works because the bucket policy grants it.)
- **The marts froze for days behind a green build.** The most important lesson of the whole project.
  `dbt-batch` built marts and exported them locally, but nothing synced them to S3, so the dashboard
  served stale Parquet while every build reported `PASS=18`. A green build is not a healthy pipeline;
  only a fresh `Last-Modified` on S3 proves the serving store is current. This is why the two
  freshness checks (§6 Monitoring) exist. Same family as "zero reject rate is ambiguous": success
  metrics can mask a dead downstream.
- **`aws` is a snap install; systemd has a minimal PATH.** `aws` lives at `/snap/bin/aws`. Interactive
  shells get `/snap/bin` on PATH automatically, but systemd units do not, so an `ExecStartPost` calling
  `aws` died with exit 127 "command not found" even though it ran fine by hand. `publish_to_s3.sh` and
  `check_marts_fresh.sh` both `export PATH="$PATH:/snap/bin"` to fix this. Watch for it in any new unit.
- **One flaky external call can abort the whole batch.** The batch command is a single `&&` chain
  (`seed_lake && fetch_resolutions && dbt build && publish_marts`). A transient DNS failure reaching
  the Polymarket Gamma API in `fetch_resolutions` (common right after a reboot) killed the entire chain,
  so nothing built or published. `fetch_resolutions` only feeds calibration, so it is now wrapped to be
  non-fatal (`{ ... || echo WARN; }`); `seed_lake` already seeds the resolutions zone so the build is
  safe without a fresh fetch. Lesson: best-effort enrichment must not gate the core pipeline.
- **Unit files are not deployed by `git reset --hard`.** Installed units live in `/etc/systemd/system/`;
  the repo only holds templates. Editing `deploy/*.service` and pulling does nothing until you
  `install` the file and `systemctl daemon-reload` (see §7). Forgetting this makes a "deployed" fix
  silently inert.
- **On-box monitoring can't see a dead box.** `marts-freshness.timer` only runs while the instance is
  up, so a stopped/wedged box produces no alarm from it. The off-box GitHub Actions monitor covers that
  blind spot. Conversely it cannot distinguish a deliberate cost-saving stop from a real outage.
- **The box loses its network but stays powered (recurring).** Twice the publish froze while the
  instance was still running: SSH dead, instance status check red, but `journald` kept logging locally.
  The trap is that this looks like every other failure. Diagnosis ruled them out one by one from the
  *previous* boot's logs (`journalctl --list-boots`, then `-b -2`): no OOM/`killed process` (not memory),
  no `nvme timeout`/`hung task` (not disk), no `ena`/`ens5` reset (NIC fine at the kernel level), and the
  only link events were `veth*` pairs cycling every 15 min (just the dbt-batch *containers*, a red
  herring). The real signature was tailscaled `Rebind; defIf="", ips=[]` plus `connect: network is
  unreachable` (ENETUNREACH) to real IPv4s, corroborated by SSM DNS failing and `check_marts_fresh.sh`
  exiting 253: **the host lost its default route while the ENI stayed attached.** ENETUNREACH is a
  routing verdict, not a throttle, so it is NOT a t3 CPU/network-credit problem (credits cause timeouts,
  never "no route") and changing instance type would not fix it. There is no resource cause to fix, so
  the durable answer is automated recovery: `deploy/setup_autoreboot_alarm.sh` sets a CloudWatch alarm
  that force-reboots on `StatusCheckFailed_Instance` (reboot re-runs DHCP and restores the route; AWS's
  `ec2:recover` action does NOT apply to *instance* check failures, only *system* ones). To pin an
  AWS-side ENI/VPC blip vs. a local cause, check the instance Status-checks history and the AWS Health
  Dashboard for the failure window. Lesson: when a box is "down" but its disk/journal survive, read the
  prior boot's logs and trust the ENETUNREACH signature over the noisiest logger (tailscale).
- **Use the bucket's real region in the dashboard URL.** Both the bucket and the VM are in
  **us-east-1**, but the dashboard URL still must name the bucket's region explicitly or S3 returns
  `301 Moved Permanently`. Confirm with `aws s3api get-bucket-location --bucket pms-marts-elaiken3`
  (null = us-east-1). (An earlier version of this file claimed the VM was in us-east-2; IMDS says
  us-east-1. Trust IMDS / the SSM endpoint in the logs over the doc.)
- **EC2 public IP changes on stop/start.** Use the Tailscale IP (`100.76.72.41`) for stable
  access, or pull the current Public IPv4 from the console each time.
- **Streamlit can't change the main file path** on an existing app, you must delete and
  recreate, which changes the URL. A **custom subdomain** (already set:
  `prediction-market-surveillance`) makes the URL survive recreation. Don't lose it.
- **Zero reject rate is ambiguous.** A clean dead-letter path can mean the contract is healthy
  OR that it has quietly stopped firing. Keep dead-letter reasons visible; periodically feed a
  known-bad event to confirm the path still fires.

---

## 10. Open / pending work

- [ ] **Commit `matplotlib>=3.7` to `dashboard/requirements.txt`.** The redesigned `s3_app.py`
      uses `Styler.background_gradient`, which needs matplotlib; without it the dashboard throws
      an ImportError on the coherence/calibration tables. (Highest priority: dashboard is
      currently erroring on those tables until this lands.)
- [ ] Confirm `dashboard/requirements.txt` has the duckdb pin committed (was not in an earlier commit).
- [ ] Publish the Medium deep-dive series (5 parts drafted) and swap the placeholder inter-part
      links for real Medium URLs.
- [ ] Calibration mart needs multi-day uptime to populate as markets resolve.
- [ ] Future: weight the coherence signal by book depth/liquidity to separate "thin and stale"
      from "actively pushed."
- [ ] Future: real alerting path for anomaly *detections* (still passive dashboard only). Note: mart
      *freshness* alerting now exists (on-box `marts-freshness.timer` + off-box GitHub Actions); the
      `FRESHNESS_ALERT_WEBHOOK` / `MARTS_ALERT_SLACK_WEBHOOK` hooks are the natural place to extend it.
- [ ] Cost hygiene: stop the EC2 instance when not actively collecting (only EBS ~$3-4/mo when stopped).
      Remember the off-box freshness monitor will fire while it is stopped; raise `MAX_AGE_MINUTES` or
      disable the workflow for long stop windows.
- [ ] Apply the auto-reboot alarm (`deploy/setup_autoreboot_alarm.sh`) for the recurring "lost network
      but powered" failure (§9). Run it from the Mac (the VM's `pms-s3-writer` role has no CloudWatch
      perms) and in `us-east-1` (the alarm must match the instance region). If reboots do not clear it,
      open an AWS support/Health case for an ENI/VPC-side cause.

**Resolved this session (was: dashboard serving stale data):** wired the S3 sync into `dbt-batch`
(`ExecStartPost`), fixed `aws`-not-on-systemd-PATH, removed the `--acl` flag, made `fetch_resolutions`
non-fatal to the batch, added on-box + off-box freshness monitoring, and committed the previously
live-only `lake-prune` units. See §9 for the war stories behind each.

---

## 11. Key marts (what each answers)

| Mart | Question it answers |
|------|---------------------|
| `fct_ingest_summary` | Can I trust the feed? (valid count, dlq count, reject rate) |
| `fct_dead_letter_reasons` | When I can't, why not? |
| `fct_coherence` | Is the market internally consistent? (Σ outcomes vs 1) |
| `rec_stream_vs_batch` | Did the real-time path agree with batch recomputation? |
| `fct_calibration` | Was the market actually right, once resolved? (Brier score) |
| `fct_price_minutely` | Minutely price series per market/outcome |
| `fct_volume_anomalies` | Volume spikes vs normal activity |
| `fct_flagged_recent` | Recently flagged anomalies |

The dashboard (`s3_app.py`) reads five of these:
fct_ingest_summary, fct_dead_letter_reasons, fct_coherence, rec_stream_vs_batch, fct_calibration.
If you add a mart to the dashboard, also add it to the `MARTS` list in `ingestion/publish_marts.py`
so it gets exported to S3.
