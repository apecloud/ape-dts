//! Zero-cost passthroughs, compiled only without `feature = "tracing"`.

use super::{GlobalTraceSnapshot, TaskSummaryMode, TraceOutputFormat};

#[inline(always)]
pub fn enable() {}

#[inline(always)]
pub fn init_tracing() {}

#[inline(always)]
pub fn dump_global_summary() -> Option<String> {
    None
}

#[inline(always)]
pub fn snapshot_global() -> Option<GlobalTraceSnapshot> {
    None
}

#[inline(always)]
pub fn set_task_summary_mode(_mode: TaskSummaryMode) {}

#[inline(always)]
pub fn set_output_format(_format: TraceOutputFormat) {}

#[inline(always)]
pub fn instrument_wait<Fut>(_name: &'static str, future: Fut) -> Fut
where
    Fut: std::future::Future,
{
    future
}

#[inline(always)]
pub fn trace_task_future<Fut>(_name: &'static str, future: Fut) -> Fut
where
    Fut: std::future::Future,
{
    future
}
