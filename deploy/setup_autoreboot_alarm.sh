#!/usr/bin/env bash
# Create a CloudWatch alarm that auto-reboots the collector when its EC2
# *instance* status check fails.
#
# Why this exists: the box has twice lost its default route / outbound network
# while staying powered on (interface up at the kernel level, but no route ->
# ENETUNREACH; publish stops, SSH dies, instance status check goes red). See
# CLAUDE.md s9 "The box loses its network but stays powered." There is no
# resource (CPU/mem/disk) cause, so the durable fix is automated recovery: a
# forced reboot re-runs DHCP and restores the default route, cutting downtime
# from "hours until someone notices" to ~3-5 min. The off-box GitHub freshness
# monitor still emails on the staleness, so the bounce is visible.
#
# Action is ec2:reboot, NOT ec2:recover: these are *instance* status-check
# failures, and AWS's recover action only fires on *system* status-check
# failures. Reboot is what applies here.
#
# treat-missing-data is notBreaching on purpose: a deliberately stopped instance
# reports no metric (missing), and we do not want the alarm firing during a
# cost-saving stop. A wedged-but-running box reports the failure as real data
# (value 1), so genuine failures still trigger.
#
# Run from the Mac (has AWS creds). Idempotent: re-running updates the alarm.
set -euo pipefail
export PATH="$PATH:/snap/bin"   # in case aws is a snap install

INSTANCE_ID="${INSTANCE_ID:-i-05de8e049933efea0}"   # pms-prod-2
REGION="${REGION:-us-east-2}"
ALARM_NAME="${ALARM_NAME:-pms-prod-2-autoreboot}"

aws cloudwatch put-metric-alarm \
  --alarm-name "$ALARM_NAME" \
  --alarm-description "Auto-reboot $INSTANCE_ID when its instance status check fails (lost-network recovery)" \
  --namespace AWS/EC2 \
  --metric-name StatusCheckFailed_Instance \
  --dimensions "Name=InstanceId,Value=${INSTANCE_ID}" \
  --statistic Maximum \
  --period 60 \
  --evaluation-periods 3 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "arn:aws:automate:${REGION}:ec2:reboot" \
  --region "$REGION"

echo "alarm '${ALARM_NAME}' set: reboot ${INSTANCE_ID} after 3x60s of StatusCheckFailed_Instance"
echo "verify:  aws cloudwatch describe-alarms --alarm-names '${ALARM_NAME}' --region ${REGION}"
