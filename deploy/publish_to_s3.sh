#!/usr/bin/env bash
# Sync the exported marts to S3 (public-read). Run as a host post-step after the
# batch builds them. Requires the AWS CLI and an instance role (or creds) with
# s3:PutObject on the bucket, and MARTS_S3_BUCKET set in the batch env file.
set -euo pipefail
# The AWS CLI on this VM is a snap install (/snap/bin/aws). systemd units run
# with a minimal PATH that excludes /snap/bin, so append it explicitly or
# ExecStartPost fails with "aws: command not found" even though it works fine
# from an interactive shell.
export PATH="$PATH:/snap/bin"
: "${MARTS_S3_BUCKET:?set MARTS_S3_BUCKET in the batch env file}"
DIR="/opt/prediction-market-surveillance/data/marts"
if [ -d "$DIR" ] && [ -n "$(ls -A "$DIR" 2>/dev/null)" ]; then
  # No --acl flag: the bucket is owner-enforced (ACLs disabled), so --acl
  # public-read fails with AccessControlListNotSupported. Public read is
  # granted by the bucket policy on marts/*, not per-object ACLs.
  aws s3 sync "$DIR" "s3://${MARTS_S3_BUCKET}/marts/" --no-progress
  echo "synced marts to s3://${MARTS_S3_BUCKET}/marts/"
else
  echo "no marts to sync (empty $DIR)"
fi
