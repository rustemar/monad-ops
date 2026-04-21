#!/usr/bin/env bash
# Install / reinstall the monad-ops systemd unit on this host.
# Idempotent: safe to re-run after config or code changes.
set -euo pipefail

UNIT_SRC="$(dirname "$(readlink -f "$0")")/../systemd/monad-ops.service"
UNIT_DST="/etc/systemd/system/monad-ops.service"

if [[ ! -f "$UNIT_SRC" ]]; then
    echo "unit source not found: $UNIT_SRC" >&2
    exit 1
fi

echo "Installing $UNIT_DST ..."
sudo install -m 0644 "$UNIT_SRC" "$UNIT_DST"

echo "Reloading systemd ..."
sudo systemctl daemon-reload

echo "Enabling monad-ops.service ..."
sudo systemctl enable monad-ops.service

echo
echo "To start now:         sudo systemctl start monad-ops"
echo "To follow the logs:   journalctl -u monad-ops -f"
echo "To stop:              sudo systemctl stop monad-ops"
