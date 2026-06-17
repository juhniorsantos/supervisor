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
| `daemon.rs`   | The event loop, HTTP control server and operations. |
| `http.rs`     | A tiny HTTP/1.1 request/response core (+ Basic auth). |
| `xmlrpc.rs`   | A dependency-free XML-RPC parser/serializer. |
| `rpc.rs`      | The `supervisor.*` XML-RPC method set (API 3.0). |
| `events.rs`   | Event names, payloads and listener-subscription matching. |
| `syslog.rs`   | Minimal line-buffered syslog client (`/dev/log`). |
| `util.rs`     | Small shared helpers (e.g. subslice search). |
| `web.rs`      | The web management UI served at `GET /`. |
| `control.rs`  | XML-RPC-over-HTTP client used by `supervisorctl`. |
| `bin/supervisord.rs`   | Daemon entry point (arg parsing, daemonize, pidfile). |
| `bin/supervisorctl.rs` | Client entry point (one-shot + interactive REPL). |

The daemon runs a single-threaded tick loop (~100 ms): it reaps exited
children with `waitpid(WNOHANG)`, drains their stdout/stderr pipes into the
rotating loggers, advances every process's state machine, and services any
pending control connections. Control I/O is fully non-blocking — requests are
parsed and responses are written incrementally across ticks — so a slow or
stalled client can never block process supervision. HTTP Basic credentials
are checked in constant time.

### Control over HTTP / XML-RPC

Just like the original Supervisor, the daemon speaks **XML-RPC over HTTP** at
`POST /RPC2` and serves a **web UI** at `GET /`, on both the unix socket and
the optional `[inet_http_server]` TCP port. The `supervisor.*` method set
(API version `3.0`) is compatible enough that **the upstream Python
`supervisorctl` can control this Rust daemon unchanged**:

```sh
# Point the original client at the Rust daemon's socket:
python -m supervisor.supervisorctl -c supervisord.conf status
```

Implemented RPC methods: `getAPIVersion`/`getVersion`,
`getSupervisorVersion`, `getIdentification`, `getState`, `getPID`,
`getAllProcessInfo`, `getProcessInfo`, `startProcess`, `stopProcess`,
`startProcessGroup`, `stopProcessGroup`, `startAllProcesses`,
`stopAllProcesses`, `readProcessStdoutLog`/`readProcessStderrLog`,
`tailProcessStdoutLog`/`tailProcessStderrLog`, `signalProcess`/`Group`/`All`,
`clearProcessLogs`/`clearAllProcessLogs`, `clearLog`, `readLog`,
`sendProcessStdin`, `sendRemoteCommEvent`, `reloadConfig`,
`addProcessGroup`/`removeProcessGroup`, `shutdown`, `restart`. HTTP Basic auth
(`username=`/`password=`) is enforced when configured.

## Supported configuration

`[supervisord]`: `logfile` (a path, or `syslog`), `logfile_maxbytes`, `logfile_backups`,
`loglevel` (`critical`/`error`/`warn`/`info`/`debug`/`trace`/`blather`, filters
the main log), `pidfile`, `nodaemon`, `silent`, `childlogdir`, `directory`,
`identifier`, `umask`, `environment`.

`[unix_http_server]`: `file` (the control socket path), `username`,
`password`.

`[inet_http_server]`: `port` (`ip:port`, or `*:port` for all interfaces),
`username`, `password`.

`[group:x]`: `programs` (comma-separated program names), `priority`.

`[eventlistener:x]`: like `[program:x]`, plus `events` (subscribed event
types) and `buffer_size`.

`[fcgi-program:x]`: like `[program:x]`, plus `socket` (`unix://path` or
`tcp://host:port`), `socket_owner`, `socket_mode`; the group shares one
listening socket passed to each child as fd 0.

`[include]`: `files` (whitespace-separated paths/globs, relative to the main
config's directory).

`[program:x]`: `command`, `process_name`, `numprocs`, `directory`,
`autostart`, `autorestart` (`true`/`false`/`unexpected`), `startsecs`,
`startretries`, `exitcodes`, `stopsignal`, `stopwaitsecs`, `stopasgroup`,
`killasgroup` (both default true — stop a program's whole process tree),
`environment`, `user`, `umask`, `priority`, `redirect_stderr`,
`stdout_logfile`(+`_maxbytes`,
`_backups`, `_capture_maxbytes`, `_syslog`), `stderr_logfile`(+`_maxbytes`,
`_backups`, `_capture_maxbytes`, `_syslog`).

## Control commands

```
status [name|all]    start <name|all>    stop <name|all>
restart <name|all>   tail <name> [stderr]
reread               update [group|all]  add <group>   remove <group>
pid [name]           version             reload (restart supervisord)
shutdown             help
```

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
- [x] Daemonization, pidfile, clean shutdown (socket/pidfile removal)
- [x] **XML-RPC API over HTTP** (`POST /RPC2`) on unix + inet sockets
- [x] **Web management UI** (`GET /`) with start/stop/restart actions
- [x] `[inet_http_server]` TCP control + HTTP Basic auth
- [x] Compatibility with the upstream Python `supervisorctl`
- [x] `supervisorctl tail`, `reload` (daemon `restart` via re-exec)
- [x] **`[group:x]` sections** with `programs=`/`priority`, plus `group:name` namespecs
- [x] **Event listeners** (`[eventlistener:x]`): the full READY/RESULT protocol,
      `PROCESS_STATE_*`, `TICK_5/60/3600` and `SUPERVISOR_STATE_CHANGE_*`
      events with upstream-compatible envelopes (works with real listener
      scripts), `events=` subscriptions and `buffer_size`
- [x] **Dynamic config**: `[include] files=` globs, and
      `reread`/`update`/`add`/`remove` (`reloadConfig`, `addProcessGroup`,
      `removeProcessGroup` RPC) to add/remove/restart groups without a full
      restart
- [x] **Signals & logs**: `signalProcess`/`Group`/`All`, `clearProcessLogs`/
      `clearAllProcessLogs`, `clearLog`, `readLog`, `sendProcessStdin`,
      `sendRemoteCommEvent`; `supervisorctl signal`/`clear`/`maintail`/`fg`
- [x] **Log capture mode**: `stdout/stderr_capture_maxbytes` →
      `PROCESS_COMMUNICATION_*` events
- [x] **Syslog output**: `stdout_syslog`/`stderr_syslog` forward lines to
      `/dev/log`, plus `logfile=syslog` for the main log
- [x] **FastCGI** (`[fcgi-program:x]`): one shared listening socket
      (`unix://`/`tcp://`, `socket_owner`/`socket_mode`) handed to every child
      as fd 0
- [x] **Group wildcards** (`group:*`) for `start`/`stop`/`signalProcess`, and
      faithful `tailProcess*Log` offset/overflow semantics

Not yet ported (smaller remaining options):

- [ ] `numprocs_start`, per-program `serverurl`, `*_events_enabled`
- [ ] `minfds`/`minprocs`, supervisord `user`/`nocleanup`/`strip_ansi`
- [ ] `%(here)s`/`%(ENV_x)s` expansions in arbitrary option values
- [ ] `getAllConfigInfo` / `system.*` RPC, `[rpcinterface:x]` plugins

## Tests

```sh
cargo test
```

Unit tests cover config parsing (byte sizes, comments, `process_name`
expansion, environment), command-line splitting, `waitpid` status decoding,
XML-RPC parsing/serialisation, and HTTP request parsing + Basic auth
decoding. The system has additionally been exercised end-to-end against a
live daemon: autostart, backoff→FATAL, stop/start/restart, autorestart, clean
shutdown with no leaked children, the web UI over the inet port, and — as a
compatibility check — the **upstream Python `supervisorctl` driving the Rust
daemon** (status/start/stop).

## Relationship to the original

This Rust core is inspired by, and aims to be behaviourally compatible with,
the original Supervisor (BSD-licensed). It is an independent implementation;
no original source code is copied.
