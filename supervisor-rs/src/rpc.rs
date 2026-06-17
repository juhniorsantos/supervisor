//! The `supervisor.*` XML-RPC method set, dispatched against a live
//! [`Supervisor`]. Method names, parameters, return shapes and fault codes
//! follow the original `supervisor/rpcinterface.py` (API version 3.0) so the
//! upstream `supervisorctl` can talk to this daemon unchanged.

use std::time::Instant;

use crate::daemon::Supervisor;
use crate::process::ProcessInfo;
use crate::xmlrpc::Value;

/// The XML-RPC API version this daemon implements.
pub const API_VERSION: &str = "3.0";

/// Fault codes, matching `supervisor/xmlrpc.py` `Faults`.
pub mod faults {
    pub const UNKNOWN_METHOD: i32 = 1;
    pub const INCORRECT_PARAMETERS: i32 = 2;
    pub const BAD_NAME: i32 = 10;
    pub const NO_FILE: i32 = 20;
    pub const FAILED: i32 = 30;
    pub const SPAWN_ERROR: i32 = 50;
    pub const ALREADY_STARTED: i32 = 60;
    pub const NOT_RUNNING: i32 = 70;
    pub const SUCCESS: i32 = 80;
}

/// Dispatch a single XML-RPC call. Returns the response value, or a
/// `(fault_code, fault_string)` pair.
pub fn dispatch(
    sup: &mut Supervisor,
    method: &str,
    params: &[Value],
    now: Instant,
) -> Result<Value, (i32, String)> {
    // Methods are namespaced as `supervisor.<name>`; accept the bare name too.
    let name = method.strip_prefix("supervisor.").unwrap_or(method);

    match name {
        "getAPIVersion" | "getVersion" => Ok(Value::Str(API_VERSION.to_string())),
        "getSupervisorVersion" => Ok(Value::Str(env!("CARGO_PKG_VERSION").to_string())),
        "getIdentification" => Ok(Value::Str(sup.identifier().to_string())),
        "getState" => {
            let state = sup.supervisor_state();
            Ok(Value::Struct(vec![
                ("statecode".into(), Value::Int(state as i64)),
                ("statename".into(), Value::Str(state.description().to_string())),
            ]))
        }
        "getPID" => Ok(Value::Int(sup.supervisor_pid() as i64)),

        "getAllProcessInfo" => {
            let infos = sup.all_process_info();
            Ok(Value::Array(infos.iter().map(info_to_value).collect()))
        }
        "getProcessInfo" => {
            let name = str_param(params, 0)?;
            match sup.process_info(&name) {
                Some(info) => Ok(info_to_value(&info)),
                None => Err((faults::BAD_NAME, name)),
            }
        }

        "startProcess" => {
            let name = str_param(params, 0)?;
            sup.op_start(&name, now)?;
            Ok(Value::Bool(true))
        }
        "stopProcess" => {
            let name = str_param(params, 0)?;
            sup.op_stop(&name, now)?;
            Ok(Value::Bool(true))
        }
        "startProcessGroup" => {
            // Each program is its own group here, so this targets one process.
            let name = str_param(params, 0)?;
            let (code, desc) = match sup.op_start(&name, now) {
                Ok(()) => (faults::SUCCESS, "started".to_string()),
                Err((c, _)) => (c, "error".to_string()),
            };
            Ok(Value::Array(vec![result_struct(&name, &name, code, &desc)]))
        }
        "stopProcessGroup" => {
            let name = str_param(params, 0)?;
            let (code, desc) = match sup.op_stop(&name, now) {
                Ok(()) => (faults::SUCCESS, "stopped".to_string()),
                Err((c, _)) => (c, "error".to_string()),
            };
            Ok(Value::Array(vec![result_struct(&name, &name, code, &desc)]))
        }
        "startAllProcesses" => {
            let results = sup.op_start_all(now);
            Ok(Value::Array(
                results
                    .iter()
                    .map(|(n, g, c, d)| result_struct(n, g, *c, d))
                    .collect(),
            ))
        }
        "stopAllProcesses" => {
            let results = sup.op_stop_all(now);
            Ok(Value::Array(
                results
                    .iter()
                    .map(|(n, g, c, d)| result_struct(n, g, *c, d))
                    .collect(),
            ))
        }

        "readProcessStdoutLog" | "readProcessLog" => read_log(sup, params, "stdout"),
        "readProcessStderrLog" => read_log(sup, params, "stderr"),
        "tailProcessStdoutLog" => tail_log(sup, params, "stdout"),
        "tailProcessStderrLog" => tail_log(sup, params, "stderr"),

        "shutdown" => {
            sup.request_shutdown();
            Ok(Value::Bool(true))
        }
        "restart" => {
            sup.request_restart();
            Ok(Value::Bool(true))
        }

        other => Err((faults::UNKNOWN_METHOD, format!("supervisor.{other}"))),
    }
}

fn read_log(
    sup: &Supervisor,
    params: &[Value],
    channel: &str,
) -> Result<Value, (i32, String)> {
    let name = str_param(params, 0)?;
    let offset = int_param(params, 1).unwrap_or(0);
    let length = int_param(params, 2).unwrap_or(0);
    let text = sup.read_log(&name, channel, offset, length)?;
    Ok(Value::Str(text))
}

/// `tailProcess*Log` returns `[bytes, offset, overflow]`. We read the tail of
/// the file and report the new offset; overflow is reported when the file is
/// larger than `length`.
fn tail_log(
    sup: &Supervisor,
    params: &[Value],
    channel: &str,
) -> Result<Value, (i32, String)> {
    let name = str_param(params, 0)?;
    let offset = int_param(params, 1).unwrap_or(0);
    let length = int_param(params, 2).unwrap_or(0);
    // tail is lenient when the log is missing.
    let text = sup.read_log(&name, channel, offset, length).unwrap_or_default();
    let new_offset = offset + text.len() as i64;
    Ok(Value::Array(vec![
        Value::Str(text),
        Value::Int(new_offset),
        Value::Bool(false),
    ]))
}

fn info_to_value(info: &ProcessInfo) -> Value {
    Value::Struct(vec![
        ("name".into(), Value::Str(info.name.clone())),
        ("group".into(), Value::Str(info.group.clone())),
        ("start".into(), Value::Int(info.start)),
        ("stop".into(), Value::Int(info.stop)),
        ("now".into(), Value::Int(info.now)),
        ("state".into(), Value::Int(info.state)),
        ("statename".into(), Value::Str(info.statename.clone())),
        ("spawnerr".into(), Value::Str(info.spawnerr.clone())),
        ("exitstatus".into(), Value::Int(info.exitstatus as i64)),
        ("logfile".into(), Value::Str(info.stdout_logfile.clone())),
        ("stdout_logfile".into(), Value::Str(info.stdout_logfile.clone())),
        ("stderr_logfile".into(), Value::Str(info.stderr_logfile.clone())),
        ("pid".into(), Value::Int(info.pid as i64)),
        ("description".into(), Value::Str(info.description.clone())),
    ])
}

fn result_struct(name: &str, group: &str, status: i32, description: &str) -> Value {
    Value::Struct(vec![
        ("name".into(), Value::Str(name.to_string())),
        ("group".into(), Value::Str(group.to_string())),
        ("status".into(), Value::Int(status as i64)),
        ("description".into(), Value::Str(description.to_string())),
    ])
}

fn str_param(params: &[Value], i: usize) -> Result<String, (i32, String)> {
    params
        .get(i)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or((faults::INCORRECT_PARAMETERS, format!("expected string param {i}")))
}

fn int_param(params: &[Value], i: usize) -> Option<i64> {
    match params.get(i) {
        Some(Value::Int(n)) => Some(*n),
        _ => None,
    }
}
