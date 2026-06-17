//! Process and supervisor states.
//!
//! These mirror `supervisor/states.py` from the original project so the
//! observable behaviour (the values surfaced by `supervisorctl status`)
//! matches what existing users expect.

/// The lifecycle state of a single supervised process.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProcessState {
    /// The process has been stopped (or was never started).
    Stopped = 0,
    /// The process has been spawned and is waiting out `startsecs`.
    Starting = 10,
    /// The process stayed up long enough to be considered running.
    Running = 20,
    /// The process exited too quickly while starting; waiting to retry.
    Backoff = 30,
    /// A stop signal was sent; waiting for the process to exit.
    Stopping = 40,
    /// The process exited after having run successfully.
    Exited = 100,
    /// The process could not be started after `startretries` attempts.
    Fatal = 200,
    /// The process is in an indeterminate state.
    Unknown = 1000,
}

impl ProcessState {
    /// The human-readable name shown by `supervisorctl status`.
    pub fn description(self) -> &'static str {
        match self {
            ProcessState::Stopped => "STOPPED",
            ProcessState::Starting => "STARTING",
            ProcessState::Running => "RUNNING",
            ProcessState::Backoff => "BACKOFF",
            ProcessState::Stopping => "STOPPING",
            ProcessState::Exited => "EXITED",
            ProcessState::Fatal => "FATAL",
            ProcessState::Unknown => "UNKNOWN",
        }
    }

    /// States in which the process is considered "up" (has, or is trying to
    /// get, a live PID).
    pub fn is_running(self) -> bool {
        matches!(
            self,
            ProcessState::Starting | ProcessState::Running | ProcessState::Backoff
        )
    }

    /// States in which the process is considered "down".
    pub fn is_stopped(self) -> bool {
        matches!(
            self,
            ProcessState::Stopped
                | ProcessState::Exited
                | ProcessState::Fatal
                | ProcessState::Unknown
        )
    }
}

/// The lifecycle state of the supervisor daemon itself.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SupervisorState {
    Fatal = 2,
    Running = 1,
    Restarting = 0,
    Shutdown = -1,
}

impl SupervisorState {
    pub fn description(self) -> &'static str {
        match self {
            SupervisorState::Fatal => "FATAL",
            SupervisorState::Running => "RUNNING",
            SupervisorState::Restarting => "RESTARTING",
            SupervisorState::Shutdown => "SHUTDOWN",
        }
    }
}
