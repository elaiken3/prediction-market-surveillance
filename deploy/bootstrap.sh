#!/usr/bin/env bash
# Provision a fresh Ubuntu 22.04/24.04 VM to run the surveillance stack.
# Usage (as a sudo-capable user):
#   git clone <repo> /opt/prediction-market-surveillance
#   cd /opt/prediction-market-surveillance && sudo bash deploy/bootstrap.sh
#
# Optional Tailscale (recommended for reaching the dashboard privately):
#   WITH_TAILSCALE=1 sudo bash deploy/bootstrap.sh
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/prediction-market-surveillance}"
WITH_TAILSCALE="${WITH_TAILSCALE:-0}"

echo "==> Installing Docker (if absent)"
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
fi
# Let the invoking (non-root) user run docker without sudo.
if [ -n "${SUDO_USER:-}" ]; then
  usermod -aG docker "$SUDO_USER" || true
fi
systemctl enable --now docker

echo "==> Creating data directory"
mkdir -p "$APP_DIR/data"

if [ "$WITH_TAILSCALE" = "1" ]; then
  echo "==> Installing Tailscale"
  curl -fsSL https://tailscale.com/install.sh | sh
  echo "    Run 'sudo tailscale up' to authenticate, then reach the dashboard at"
  echo "    http://<tailscale-ip>:8501 (no public port is exposed)."
fi

echo "==> Installing systemd units"
install -m 0644 "$APP_DIR/deploy/surveillance.service" /etc/systemd/system/surveillance.service
install -m 0644 "$APP_DIR/deploy/dbt-batch.service"   /etc/systemd/system/dbt-batch.service
install -m 0644 "$APP_DIR/deploy/dbt-batch.timer"     /etc/systemd/system/dbt-batch.timer
systemctl daemon-reload
systemctl enable surveillance.service
systemctl enable --now dbt-batch.timer

cat <<'NEXT'

==> Done. Next steps:
  1. Choose ingestion mode (edit /etc/systemd/system/surveillance.service):
       Environment=INGEST_MODE=live       # real Polymarket feed
       Environment=INGEST_MODE=synthetic  # offline, no network (good first smoke test)
     then: sudo systemctl daemon-reload
  2. Start the stack:
       sudo systemctl start surveillance.service
  3. Watch it:
       docker compose -f deploy/docker-compose.vm.yml ps
       docker compose -f deploy/docker-compose.vm.yml logs -f spark-stream
  4. Dashboard: http://127.0.0.1:8501 (tunnel via SSH -L 8501:127.0.0.1:8501, or use Tailscale).

  The batch (resolutions + dbt) runs automatically every 15 min via dbt-batch.timer.
NEXT
