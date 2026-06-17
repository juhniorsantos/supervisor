# Running supervisor-rs as a Linux service (systemd)

`supervisord` can run in the foreground (`-n`), which is exactly what systemd
wants: it supervises the process directly and restarts it if it dies.

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

## 3. Install the systemd unit

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

## 4. Enable and start it

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now supervisord
```

## Managing it

```sh
systemctl status supervisord          # service state
journalctl -u supervisord -f          # follow supervisord's own log
sudo systemctl restart supervisord    # full restart
sudo systemctl reload supervisord     # re-read config (supervisorctl reload)

# Manage individual programs:
sudo supervisorctl -c /etc/supervisor/supervisord.conf status
sudo supervisorctl -c /etc/supervisor/supervisord.conf restart <name>
```

## Notes / caveats

- **Foreground, not forking.** The unit uses `-n` + `Type=simple`. Do not use
  `nodaemon=false` semantics here; let systemd own the process.
- **Reload ≠ SIGHUP.** This build does not handle `SIGHUP`; `ExecReload` calls
  `supervisorctl reload`, which makes supervisord re-exec itself (same PID, so
  systemd keeps tracking it).
- **Graceful child shutdown.** `KillMode=mixed` sends `SIGTERM` to supervisord
  only, so it can stop children using their configured `stopsignal` /
  `stopwaitsecs`; systemd force-kills stragglers after `TimeoutStopSec`.
- **Run as root** if your programs use `user=` to drop privileges; otherwise
  add `User=` / `Group=` to the `[Service]` section to run unprivileged.
