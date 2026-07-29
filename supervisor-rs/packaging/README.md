# Running supervisor-rs as a system service (Linux / macOS)

`supervisord` can run in the foreground (`-n`), which is exactly what a
service manager wants: it supervises the process directly and restarts it if
it dies. On Linux that's systemd; on macOS it's launchd.

## Quick install

```sh
sudo ./packaging/install.sh
```

This builds the release binaries, installs them to `/usr/local/bin`, writes
a default config to `/etc/supervisor/supervisord.conf` (if one doesn't
already exist), and registers + starts the service — a systemd unit on
Linux, a LaunchDaemon on macOS.

To remove the service (binaries and config are left in place):

```sh
sudo ./packaging/install.sh --uninstall
```

The rest of this document explains what the script does, for anyone who
wants to do it by hand or customize a step.

## 1. Build and install the binaries

```sh
cd supervisor-rs
cargo build --release
sudo install -m 0755 target/release/supervisord  /usr/local/bin/supervisord
sudo install -m 0755 target/release/supervisorctl /usr/local/bin/supervisorctl
```

## 2. Place a config and log directory

```sh
sudo mkdir -p /etc/supervisor /var/log/supervisor
sudo cp examples/supervisord.conf /etc/supervisor/supervisord.conf
# Then edit /etc/supervisor/supervisord.conf — for a real service, point
# logfile/childlogdir at /var/log/supervisor and add your [program:x] sections.
```

## 3a. Linux: install the systemd unit

Either copy the bundled file:

```sh
sudo cp packaging/supervisord.service /etc/systemd/system/supervisord.service
```

…or generate it in one command:

```sh
sudo tee /etc/systemd/system/supervisord.service >/dev/null <<'EOF'
[Unit]
Description=Supervisor process control system (Rust)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/supervisord -n -c /etc/supervisor/supervisord.conf
ExecReload=/usr/local/bin/supervisorctl -c /etc/supervisor/supervisord.conf reload
KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=30
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

## 3b. Linux: enable and start it

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now supervisord
```

## 3c. macOS: install the LaunchDaemon

```sh
sudo tee /Library/LaunchDaemons/com.supervisor-rs.supervisord.plist >/dev/null <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.supervisor-rs.supervisord</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/supervisord</string>
    <string>-n</string>
    <string>-c</string>
    <string>/etc/supervisor/supervisord.conf</string>
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
  <key>StandardOutPath</key><string>/var/log/supervisor/supervisord.stdout.log</string>
  <key>StandardErrorPath</key><string>/var/log/supervisor/supervisord.stderr.log</string>
</dict>
</plist>
EOF
sudo launchctl bootstrap system /Library/LaunchDaemons/com.supervisor-rs.supervisord.plist
sudo launchctl enable system/com.supervisor-rs.supervisord
```

`KeepAlive.SuccessfulExit=false` is launchd's equivalent of systemd's
`Restart=on-failure`: it restarts the daemon whenever it exits non-cleanly,
not on every exit.

## Managing it

Linux:

```sh
systemctl status supervisord          # service state
journalctl -u supervisord -f          # follow supervisord's own log
sudo systemctl restart supervisord    # full restart
sudo systemctl reload supervisord     # re-read config (supervisorctl reload)
```

macOS:

```sh
sudo launchctl print system/com.supervisor-rs.supervisord   # service state
tail -f /var/log/supervisor/supervisord.log                 # supervisord's own log
sudo launchctl kickstart -k system/com.supervisor-rs.supervisord  # full restart
```

Both platforms — manage individual programs:

```sh
sudo supervisorctl -c /etc/supervisor/supervisord.conf status
sudo supervisorctl -c /etc/supervisor/supervisord.conf restart <name>
```

## Stopping it

Stop a single program, leaving supervisord and everything else running:

```sh
sudo supervisorctl stop <name>
```

Stop supervisord itself, right now (it comes back on the next reboot, since
the unit/plist stays installed with autostart enabled):

```sh
# Linux
sudo systemctl stop supervisord

# macOS
sudo launchctl bootout system/com.supervisor-rs.supervisord
```

Remove the service for good (binaries and `/etc/supervisor` config are left
in place — see [Quick install](#quick-install)):

```sh
sudo ./packaging/install.sh --uninstall
```

## Notes / caveats

- **Foreground, not forking.** Both the systemd unit and the launchd plist
  run `supervisord -n`. Do not rely on `nodaemon=false` semantics here; let
  the service manager own the process.
- **Reload ≠ SIGHUP.** This build does not handle `SIGHUP`. On Linux,
  `ExecReload` calls `supervisorctl reload`, which makes supervisord re-exec
  itself (same PID, so systemd keeps tracking it); run the same
  `supervisorctl reload` by hand on macOS.
- **Graceful child shutdown.** On Linux, `KillMode=mixed` sends `SIGTERM` to
  supervisord only, so it can stop children using their configured
  `stopsignal` / `stopwaitsecs`; systemd force-kills stragglers after
  `TimeoutStopSec`. launchd's default stop behavior (`launchctl bootout`)
  also sends `SIGTERM` to just the daemon process, which is the same
  contract.
- **Run as root** if your programs use `user=` to drop privileges; otherwise
  add `User=` / `Group=` to the systemd `[Service]` section, or a
  `UserName` key to the launchd plist, to run unprivileged.
- **launchd's default `PATH` is minimal** (`/usr/bin:/bin:/usr/sbin:/sbin` —
  no `/usr/local/bin`, no Homebrew). The plist above widens it, but the
  robust fix is to always use an **absolute path** in each `[program:x]`
  `command=`, regardless of platform — `stat`/`file` the path yourself if a
  program goes straight to `FATAL`/`BACKOFF` with "spawn failed: No such
  file or directory".
