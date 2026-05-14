use async_trait::async_trait;

pub mod counter;
pub mod counter_type;
pub mod group_monitor;
pub mod task_metrics;
pub mod task_monitor;
pub mod task_monitor_handle;

#[allow(clippy::module_inception)]
pub mod monitor;
pub mod time_window_counter;

#[cfg(feature = "metrics")]
pub mod prometheus_metrics;

#[async_trait]
pub trait FlushableMonitor {
    async fn flush(&self);
}
