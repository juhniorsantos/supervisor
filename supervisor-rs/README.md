# supervisor-rs

A Rust reimplementation of the **core** of [Supervisor](http://supervisord.org/)
— the client/server process control system for UNIX — living alongside the
original Python implementation in this repository.

This is the **functional MVP**: a real, working `supervisord` daemon and
`supervisorctl` client that you can configure with a familiar
`supervisord.conf`, start processes, supervise them with autorestart and
backoff, capture their output to rotating log files, and control them at
runtime over a unix socket. It is faithful to the original's process state
machine (`STARTING → RUNNING → BACKOFF → FATAL`, etc.) so `status` output
behaves the way existing users expect.

It is **not** a complete 1:1 port — see [Scope](#scope) below.

## Building

```sh
cd supervisor-rs
cargo build --release
```

This produces two binaries in `target/release/`:

* `supervisord`   — the supervisor daemon
* `supervisorctl` — the control client

## Quick start

```sh
# Start the daemon in the foreground using the bundled example config:
./target/release/supervisord -n -c examples/supervisord.conf

# In another terminal, talk to it:
./target/release/supervisorctl -c examples/supervisord.conf status
./target/release/supervisorctl -c examples/supervisord.conf start web
./target/release/supervisorctl -c examples/supervisord.conf restart clock
./target/release/supervisorctl -c examples/supervisord.conf stop all
./target/release/supervisorctl -c examples/supervisord.conf shutdown

# Or drop into the interactive shell:
./target/release/supervisorctl -c examples/supervisord.conf
supervisor> status
supervisor> stop clock
supervisor> quit
```

## Architecture

The crate is split into focused modules (`src/`):

| Module        | Responsibility |
|---------------|----------------|
| `states.rs`   | Process / supervisor state enums (mirrors `supervisor/states.py`). |
| `config.rs`   | INI parser for `supervisord.conf` (a useful subset). |
| `logger.rs`   | Size-based rotating log files. |
| `process.rs`  | One supervised process and its full state machine. |
| `daemon.rs`   | The event loop, control socket server and command dispatch. |
| `control.rs`  | The line-based client/server control protocol. |
| `bin/supervisord.rs`   | Daemon entry point (arg parsing, daemonize, pidfile). |
| `bin/supervisorctl.rs` | Client entry point (one-shot + interactive REPL). |

The daemon runs a single-threaded tick loop (~100 ms): it reaps exited
children with `waitpid(WNOHANG)`, drains their stdout/stderr pipes into the
rotating loggers, advances every process's state machine, and services any
pending control connections.

## Supported configuration

`[supervisord]`: `logfile`, `logfile_maxbytes`, `logfile_backups`,
`loglevel`, `pidfile`, `nodaemon`, `silent`, `childlogdir`, `directory`,
`identifier`, `umask`, `environment`.

`[unix_http_server]`: `file` (the control socket path).

`[program:x]`: `command`, `process_name`, `numprocs`, `directory`,
`autostart`, `autorestart` (`true`/`false`/`unexpected`), `startsecs`,
`startretries`, `exitcodes`, `stopsignal`, `stopwaitsecs`, `environment`,
`user`, `umask`, `priority`, `redirect_stderr`, `stdout_logfile`(+`_maxbytes`,
`_backups`), `stderr_logfile`(+`_maxbytes`, `_backups`).

## Control commands

```
status [name|all]    start <name|all>    stop <name|all>
restart <name|all>   pid [name]          version
shutdown             help
```

Unlike the original (which speaks XML-RPC over HTTP), this core uses a small
line-based protocol over the same unix socket. The wire format is documented
in `src/control.rs`, leaving room to add an XML-RPC compatibility layer later.

## Scope

Implemented (the MVP you asked for):

- [x] `supervisord.conf` parsing (the options listed above)
- [x] Spawning & supervising processes, in priority order
- [x] Full process state machine: STARTING/RUNNING/BACKOFF/STOPPING/EXITED/FATAL
- [x] `autostart`, `autorestart` (incl. `unexpected` + `exitcodes`)
- [x] Start backoff with `startretries` → FATAL
- [x] Graceful stop with `stopsignal`, escalating to SIGKILL after `stopwaitsecs`
- [x] Process-group signalling (children are placed in their own session)
- [x] Captured stdout/stderr with size-based log rotation (`redirect_stderr` too)
- [x] `setuid`/`umask`/`directory`/`environment` per program
- [x] Unix control socket + `supervisorctl` (one-shot and interactive)
- [x] Daemonization, pidfile, clean shutdown (socket/pidfile removal)

Not yet ported (intentionally out of MVP scope):

- [ ] XML-RPC API and HTTP server
- [ ] Web management UI
- [ ] Event listeners / the event notification protocol
- [ ] `[group:x]` sections and `[inet_http_server]`
- [ ] `supervisorctl tail`/`fg`, log capture mode, `reread`/`update`
- [ ] Syslog output, config `[include]` files

These are natural next phases that can be layered on top of this core.

## Tests

```sh
cargo test
```

Unit tests cover config parsing (byte sizes, comments, `process_name`
expansion, environment), command-line splitting, and `waitpid` status
decoding. The state machine has been exercised end-to-end against a live
daemon (autostart, backoff→FATAL, stop/start/restart, autorestart, and clean
shutdown with no leaked children).

## Relationship to the original

This Rust core is inspired by, and aims to be behaviourally compatible with,
the original Supervisor (BSD-licensed). It is an independent implementation;
no original source code is copied.
