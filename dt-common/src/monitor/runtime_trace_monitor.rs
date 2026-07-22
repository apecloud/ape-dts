use async_trait::async_trait;

#[cfg(all(feature = "metrics", feature = "tracing"))]
use std::sync::Arc;

use super::FlushableMonitor;
#[cfg(all(feature = "metrics", feature = "tracing"))]
use crate::monitor::prometheus_metrics::PrometheusMetrics;
use crate::{log_runtime_trace, runtime_trace};

/// Periodically dumps the tokio runtime trace summary so that long-running
/// tasks (e.g. CDC, which normally never reaches the finish-time dump) get
/// continuous runtime diagnostics. With both `metrics` and `tracing` features
/// enabled, the structured snapshot is also exported via Prometheus.
///
/// Without the `tracing` feature this monitor is a no-op.
pub struct RuntimeTraceMonitor {
    #[cfg(all(feature = "metrics", feature = "tracing"))]
    prometheus_metrics: Arc<PrometheusMetrics>,
}

impl RuntimeTraceMonitor {
    #[cfg(not(all(feature = "metrics", feature = "tracing")))]
    pub fn new() -> Self {
        Self {}
    }

    #[cfg(all(feature = "metrics", feature = "tracing"))]
    pub fn new(prometheus_metrics: Arc<PrometheusMetrics>) -> Self {
        Self { prometheus_metrics }
    }
}

#[cfg(not(all(feature = "metrics", feature = "tracing")))]
impl Default for RuntimeTraceMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FlushableMonitor for RuntimeTraceMonitor {
    async fn flush(&self) {
        let Some(summary) = runtime_trace::dump_global_summary() else {
            return;
        };
        log_runtime_trace!("{}", summary.trim_end());

        #[cfg(all(feature = "metrics", feature = "tracing"))]
        if let Some(snapshot) = runtime_trace::snapshot_global() {
            self.prometheus_metrics.set_runtime_trace_metrics(&snapshot);
        }
    }
}
