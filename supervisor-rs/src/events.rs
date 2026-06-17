//! The event model: event names, payload formatting and subscription
//! matching, faithful to `supervisor/events.py` so that real event listener
//! scripts (e.g. ones built on `supervisor.childutils`) work against this
//! daemon.
//!
//! Supervisor names events hierarchically: a concrete event like
//! `PROCESS_STATE_RUNNING` is also delivered to listeners subscribed to its
//! abstract parents `PROCESS_STATE` and `EVENT`. We model that with
//! [`ancestors`].

use std::collections::HashSet;

use crate::states::ProcessState;

/// The concrete event name for a process entering `state`.
pub fn process_state_event_name(state: ProcessState) -> &'static str {
    match state {
        ProcessState::Stopped => "PROCESS_STATE_STOPPED",
        ProcessState::Starting => "PROCESS_STATE_STARTING",
        ProcessState::Running => "PROCESS_STATE_RUNNING",
        ProcessState::Backoff => "PROCESS_STATE_BACKOFF",
        ProcessState::Stopping => "PROCESS_STATE_STOPPING",
        ProcessState::Exited => "PROCESS_STATE_EXITED",
        ProcessState::Fatal => "PROCESS_STATE_FATAL",
        ProcessState::Unknown => "PROCESS_STATE_UNKNOWN",
    }
}

/// Build a `PROCESS_STATE_*` event payload, matching the field order and
/// `name:value` formatting of the original.
pub fn process_state_payload(
    processname: &str,
    groupname: &str,
    from_state: ProcessState,
    new_state: ProcessState,
    pid: i32,
    backoff: u32,
    expected: bool,
) -> String {
    let mut s = format!(
        "processname:{processname} groupname:{groupname} from_state:{}",
        from_state.description()
    );
    match new_state {
        ProcessState::Starting | ProcessState::Backoff => {
            s.push_str(&format!(" tries:{backoff}"));
        }
        ProcessState::Running | ProcessState::Stopping | ProcessState::Stopped => {
            s.push_str(&format!(" pid:{pid}"));
        }
        ProcessState::Exited => {
            s.push_str(&format!(" expected:{} pid:{pid}", expected as i32));
        }
        _ => {}
    }
    s
}

/// The ancestors of an event name, including the name itself, used for
/// subscription matching.
pub fn ancestors(event_name: &str) -> Vec<&'static str> {
    if event_name.starts_with("PROCESS_STATE") {
        vec![leak(event_name), "PROCESS_STATE", "EVENT"]
    } else if event_name.starts_with("TICK") {
        vec![leak(event_name), "TICK", "EVENT"]
    } else if event_name.starts_with("SUPERVISOR_STATE_CHANGE") {
        vec![leak(event_name), "SUPERVISOR_STATE_CHANGE", "EVENT"]
    } else if event_name.starts_with("PROCESS_GROUP") {
        vec![leak(event_name), "PROCESS_GROUP", "EVENT"]
    } else {
        vec![leak(event_name), "EVENT"]
    }
}

/// A listener subscribed to `subscribed` should receive `event_name` if any
/// of the event's ancestors is in its subscription set.
pub fn subscription_matches(subscribed: &HashSet<String>, event_name: &str) -> bool {
    ancestors(event_name).iter().any(|a| subscribed.contains(*a))
}

/// Return one of a small fixed set of static names matching `s`, so callers
/// can work with `&'static str`. Concrete names we emit are always known, so
/// this is exhaustive for our own events; unknown names fall back to "EVENT".
fn leak(s: &str) -> &'static str {
    const KNOWN: &[&str] = &[
        "PROCESS_STATE_STOPPED",
        "PROCESS_STATE_STARTING",
        "PROCESS_STATE_RUNNING",
        "PROCESS_STATE_BACKOFF",
        "PROCESS_STATE_STOPPING",
        "PROCESS_STATE_EXITED",
        "PROCESS_STATE_FATAL",
        "PROCESS_STATE_UNKNOWN",
        "TICK_5",
        "TICK_60",
        "TICK_3600",
        "SUPERVISOR_STATE_CHANGE_RUNNING",
        "SUPERVISOR_STATE_CHANGE_STOPPING",
    ];
    KNOWN.iter().copied().find(|k| *k == s).unwrap_or("EVENT")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_concrete_and_abstract_subscriptions() {
        let mut subs = HashSet::new();
        subs.insert("PROCESS_STATE".to_string());
        assert!(subscription_matches(&subs, "PROCESS_STATE_RUNNING"));
        assert!(!subscription_matches(&subs, "TICK_60"));

        let mut subs2 = HashSet::new();
        subs2.insert("TICK_60".to_string());
        assert!(subscription_matches(&subs2, "TICK_60"));
        assert!(!subscription_matches(&subs2, "TICK_5"));
    }

    #[test]
    fn formats_running_payload() {
        let p = process_state_payload(
            "web",
            "web",
            ProcessState::Starting,
            ProcessState::Running,
            42,
            0,
            true,
        );
        assert_eq!(p, "processname:web groupname:web from_state:STARTING pid:42");
    }

    #[test]
    fn formats_exited_payload() {
        let p = process_state_payload(
            "job",
            "grp",
            ProcessState::Running,
            ProcessState::Exited,
            7,
            0,
            false,
        );
        assert_eq!(
            p,
            "processname:job groupname:grp from_state:RUNNING expected:0 pid:7"
        );
    }
}
