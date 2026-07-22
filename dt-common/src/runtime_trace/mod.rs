use std::{fmt, str::FromStr};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum TaskSummaryMode {
    Task = 0,
    #[default]
    Marker = 1,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum TraceOutputFormat {
    #[default]
    Plain = 0,
    Json = 1,
}

impl fmt::Display for TraceOutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain => f.write_str("plain"),
            Self::Json => f.write_str("json"),
        }
    }
}

impl FromStr for TraceOutputFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "plain" => Ok(Self::Plain),
            "json" => Ok(Self::Json),
            _ => Err(format!(
                "invalid trace output format: {value}, expected plain or json"
            )),
        }
    }
}

impl fmt::Display for TaskSummaryMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task => f.write_str("task"),
            Self::Marker => f.write_str("marker"),
        }
    }
}

impl FromStr for TaskSummaryMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "task" => Ok(Self::Task),
            "marker" => Ok(Self::Marker),
            _ => Err(format!(
                "invalid task summary mode: {value}, expected task or marker"
            )),
        }
    }
}

/// Structured, machine-readable view of the aggregated runtime trace state.
///
/// Unlike [`dump_global_summary`], which renders a human-oriented report
/// honoring the configured summary mode and output format, the snapshot API
/// always aggregates by task marker and is meant for periodic metric exports
/// (e.g. Prometheus). All counters are cumulative since process start.
#[derive(Clone, Debug, Default)]
pub struct GlobalTraceSnapshot {
    pub generated_at: String,
    pub total_waker_calls: u64,
    pub markers: Vec<MarkerTraceSnapshot>,
}

#[derive(Clone, Debug, Default)]
pub struct MarkerTraceSnapshot {
    /// `name@file:line` of the traced task marker.
    pub marker: String,
    pub task_count: u64,
    pub poll_count: u64,
    pub scheduled_count: u64,
    pub busy_ms: f64,
    pub waker_calls: u64,
}

// The whole implementation is selected at module level: `traced` compiles the
// real instrumentation, `noop` compiles zero-cost passthroughs. Callers stay
// free of `#[cfg]` noise; each implementation method is defined exactly once
// per module instead of interleaving feature/not(feature) variants.
#[cfg(not(feature = "tracing"))]
mod noop;
#[cfg(feature = "tracing")]
mod traced;

#[cfg(feature = "tracing")]
pub use traced::{
    dump_global_summary, enable, init_tracing, instrument_wait, set_output_format,
    set_task_summary_mode, snapshot_global, trace_task_future,
};

#[cfg(not(feature = "tracing"))]
pub use noop::{
    dump_global_summary, enable, init_tracing, instrument_wait, set_output_format,
    set_task_summary_mode, snapshot_global, trace_task_future,
};
