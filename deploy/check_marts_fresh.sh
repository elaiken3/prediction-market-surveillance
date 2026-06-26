#!/usr/bin/env bash
# Alert if the marts on S3 (the dashboard's serving store) have gone stale.
# The batch timer republishes every 15 min; if the newest mart is older than
# MARTS_MAX_AGE_SECONDS the publish path has silently died (see CLAUDE.md s9 --
# a green build masking a dead publish froze the dashboard for days). This exits
# non-zero on staleness so systemd marks the unit failed and it shows up in
# `systemctl --failed` and the journal. Set FRESHNESS_ALERT_WEBHOOK to also POST
# the message to an external notifier.
#
# Requires the AWS CLI (snap install -> /snap/bin) and an instance role with
# s3:GetObject (or the bucket's public read policy), plus MARTS_S3_BUCKET set in
# the batch env file.
set -euo pipefail
# aws is a snap install; systemd's minimal PATH excludes /snap/bin (see
# publish_to_s3.sh for the same fix).
export PATH="$PATH:/snap/bin"
: "${MARTS_S3_BUCKET:?set MARTS_S3_BUCKET in the batch env file}"

MAX_AGE="${MARTS_MAX_AGE_SECONDS:-1800}"   # 30 min = two missed 15-min cycles
KEY="marts/fct_ingest_summary.parquet"     # always built, always exported

last_modified="$(aws s3api head-object \
  --bucket "$MARTS_S3_BUCKET" --key "$KEY" \
  --query LastModified --output text)"
last_epoch="$(date -d "$last_modified" +%s)"
age=$(( $(date +%s) - last_epoch ))

if [ "$age" -gt "$MAX_AGE" ]; then
  msg="STALE marts: s3://${MARTS_S3_BUCKET}/${KEY} is ${age}s old (max ${MAX_AGE}s). The publish path may be dead -- check dbt-batch.service."
  echo "$msg" >&2
  if [ -n "${FRESHNESS_ALERT_WEBHOOK:-}" ]; then
    curl -fsS -X POST -H 'Content-Type: application/json' \
      -d "{\"text\": \"${msg}\"}" "$FRESHNESS_ALERT_WEBHOOK" || true
  fi
  exit 1
fi

echo "marts fresh: ${KEY} is ${age}s old (max ${MAX_AGE}s)"
