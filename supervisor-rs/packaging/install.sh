#!/usr/bin/env bash
# Build and install supervisor-rs as a system service.
# Linux   -> systemd unit (packaging/supervisord.service)
# macOS   -> LaunchDaemon (/Library/LaunchDaemons)
#
# Usage:
#   sudo ./packaging/install.sh              # build + install + enable + start
#   sudo ./packaging/install.sh --uninstall  # stop + disable + remove the service
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PREFIX=/usr/local/bin
CONF_DIR=/etc/supervisor
LOG_DIR=/var/log/supervisor
CONF_FILE="$CONF_DIR/supervisord.conf"
LABEL=com.supervisor-rs.supervisord
PLIST=/Library/LaunchDaemons/$LABEL.plist
UNIT=/etc/systemd/system/supervisord.service

if [[ $EUID -ne 0 ]]; then
  echo "Run this with sudo (it installs binaries and a system service)." >&2
  exit 1
fi

OS="$(uname -s)"

uninstall() {
  case "$OS" in
    Linux)
      systemctl disable --now supervisord 2>/dev/null || true
      rm -f "$UNIT"
      systemctl daemon-reload
      ;;
    Darwin)
      launchctl bootout system/"$LABEL" 2>/dev/null || true
      rm -f "$PLIST"
      ;;
    *)
      echo "Unsupported OS: $OS" >&2; exit 1 ;;
  esac
  echo "Service removed. Binaries and $CONF_DIR were left in place."
}

if [[ "${1:-}" == "--uninstall" ]]; then
  uninstall
  exit 0
fi

command -v cargo >/dev/null || { echo "cargo not found; install Rust first." >&2; exit 1; }

echo "==> Building release binaries"
( cd "$REPO_ROOT" && cargo build --release )

echo "==> Installing binaries to $PREFIX"
install -m 0755 "$REPO_ROOT/target/release/supervisord"  "$PREFIX/supervisord"
install -m 0755 "$REPO_ROOT/target/release/supervisorctl" "$PREFIX/supervisorctl"

echo "==> Preparing $CONF_DIR and $LOG_DIR"
mkdir -p "$CONF_DIR" "$LOG_DIR"

if [[ ! -f "$CONF_FILE" ]]; then
  echo "==> Writing default config to $CONF_FILE"
  sed \
    -e 's#/tmp/supervisor\.sock#/var/run/supervisor.sock#' \
    -e 's#/tmp/supervisord\.log#/var/log/supervisor/supervisord.log#' \
    -e 's#/tmp/supervisord\.pid#/var/run/supervisord.pid#' \
    -e 's#childlogdir=/tmp#childlogdir=/var/log/supervisor#' \
    "$REPO_ROOT/examples/supervisord.conf" > "$CONF_FILE"
else
  echo "==> $CONF_FILE already exists, leaving it untouched"
fi

case "$OS" in
  Linux)
    echo "==> Installing systemd unit"
    cp "$REPO_ROOT/packaging/supervisord.service" "$UNIT"
    systemctl daemon-reload
    systemctl enable --now supervisord
    echo "==> Done. Check status with: systemctl status supervisord"
    ;;
  Darwin)
    echo "==> Installing LaunchDaemon"
    cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PREFIX/supervisord</string>
    <string>-n</string>
    <string>-c</string>
    <string>$CONF_FILE</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key><false/>
  </dict>
  <key>ThrottleInterval</key><integer>5</integer>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>StandardOutPath</key><string>$LOG_DIR/supervisord.stdout.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/supervisord.stderr.log</string>
</dict>
</plist>
EOF
    chown root:wheel "$PLIST"
    chmod 644 "$PLIST"
    launchctl bootout system/"$LABEL" 2>/dev/null || true
    launchctl bootstrap system "$PLIST"
    launchctl enable system/"$LABEL"
    echo "==> Done. Check status with: launchctl print system/$LABEL"
    ;;
  *)
    echo "Unsupported OS: $OS" >&2; exit 1 ;;
esac

echo "==> Manage programs with: supervisorctl -c $CONF_FILE status"
