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

/// Marker-aggregated runtime trace snapshot for metric export.
/// Values are cumulative since process start and omit source locations.
#[derive(Clone, Debug, Default)]
pub struct RuntimeTraceMetricsSnapshot {
    pub markers: Vec<MarkerMetricsSnapshot>,
}

#[derive(Clone, Debug, Default)]
pub struct MarkerMetricsSnapshot {
    pub marker: String,
    pub tasks_created: u64,
    pub poll_count: u64,
    pub busy_seconds: f64,
    pub attributed_waker_calls: u64,
    pub wait_points: Vec<WaitPointMetricsSnapshot>,
}

#[derive(Clone, Debug, Default)]
pub struct WaitPointMetricsSnapshot {
    pub wait_point: String,
    pub waker_calls: u64,
}

// Select one implementation here to keep feature branches out of callers.
#[cfg(not(feature = "tracing"))]
mod noop;
#[cfg(feature = "tracing")]
mod traced;

#[cfg(not(feature = "tracing"))]
pub(crate) use noop::snapshot_global;
#[cfg(not(feature = "tracing"))]
pub use noop::{
    dump_global_summary, enable, init_tracing, instrument_wait, set_output_format,
    set_task_summary_mode, trace_task_future,
};
#[cfg(feature = "tracing")]
pub(crate) use traced::snapshot_global;
#[cfg(feature = "tracing")]
pub use traced::{
    dump_global_summary, enable, init_tracing, instrument_wait, set_output_format,
    set_task_summary_mode, trace_task_future,
};
