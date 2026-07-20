use crate::runtime_trace::{TaskSummaryMode, TraceOutputFormat};

#[derive(Clone, Debug, Default)]
pub struct TracingConfig {
    pub task_summary_mode: TaskSummaryMode,
    pub output_format: TraceOutputFormat,
}
