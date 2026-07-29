//! supervisor-rs: a Rust reimplementation of the core of Supervisor
//! (the UNIX process control system, supervisord.org).
//!
//! This crate provides the building blocks shared by the `supervisord`
//! daemon and the `supervisorctl` client:
//!
//! * [`states`]   — the process state machine values (faithful to the
//!   original `supervisor/states.py`).
//! * [`config`]   — an INI configuration parser compatible with a useful
//!   subset of `supervisord.conf`.
//! * [`logger`]   — size-based rotating log files.
//! * [`process`]  — a single supervised process and its state transitions.
//! * [`daemon`]   — the supervisor event loop and control protocol server.
//! * [`control`]  — the line-based control protocol shared with the client.

pub mod config;
pub mod control;
pub mod daemon;
pub mod events;
pub mod http;
pub mod logger;
pub mod process;
pub mod rpc;
pub mod states;
pub mod syslog;
pub mod util;
pub mod web;
pub mod xmlrpc;
