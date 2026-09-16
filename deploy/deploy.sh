#!/usr/bin/env bash
# MacroHarvey — server-side deploy script.
# Pulls the latest code, syncs dependencies, and restarts the service.
# Run manually on the server (`bash deploy/deploy.sh`) or via GitHub Actions
# (.github/workflows/deploy.yml) after each push to main.

set -euo pipefail

APP_DIR="${APP_DIR:-/opt/macroharvey}"
VENV_DIR="${VENV_DIR:-$APP_DIR/.venv}"
SERVICE="${SERVICE:-macroharvey}"
BRANCH="${BRANCH:-main}"

echo "==> Deploying MacroHarvey to $APP_DIR (branch: $BRANCH)"
cd "$APP_DIR"

echo "==> Fetching latest code"
git fetch --all --prune
git reset --hard "origin/$BRANCH"

echo "==> Ensuring virtualenv"
if [ ! -d "$VENV_DIR" ]; then
	python3 -m venv "$VENV_DIR"
fi

echo "==> Installing dependencies"
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install -r requirements.txt

echo "==> Restarting service: $SERVICE"
sudo systemctl restart "$SERVICE"
sleep 2
sudo systemctl --no-pager --lines=10 status "$SERVICE" || true

echo "==> Done."
