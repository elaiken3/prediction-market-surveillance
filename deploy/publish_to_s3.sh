#!/usr/bin/env bash
# Sync the exported marts to S3 (public-read). Run as a host post-step after the
# batch builds them. Requires the AWS CLI and an instance role (or creds) with
# s3:PutObject on the bucket, and MARTS_S3_BUCKET set in the batch env file.
set -euo pipefail
: "${MARTS_S3_BUCKET:?set MARTS_S3_BUCKET in the batch env file}"
DIR="/opt/prediction-market-surveillance/data/marts"
if [ -d "$DIR" ] && [ -n "$(ls -A "$DIR" 2>/dev/null)" ]; then
  aws s3 sync "$DIR" "s3://${MARTS_S3_BUCKET}/marts/" --acl public-read --no-progress
  echo "synced marts to s3://${MARTS_S3_BUCKET}/marts/"
else
  echo "no marts to sync (empty $DIR)"
fi
