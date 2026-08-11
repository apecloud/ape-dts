use async_trait::async_trait;

#[cfg(all(feature = "metrics", feature = "tracing"))]
use std::sync::{Arc, Mutex};

#[cfg(all(feature = "metrics", feature = "tracing"))]
use anyhow::Context;
#[cfg(all(feature = "metrics", feature = "tracing"))]
use prometheus::{core::Collector, CounterVec, IntCounterVec, Opts, Registry};

use super::FlushableMonitor;
#[cfg(all(feature = "metrics", feature = "tracing"))]
use crate::config::metrics_config::MetricsConfig;
#[cfg(all(feature = "metrics", feature = "tracing"))]
use crate::error::DtError;
#[cfg(all(feature = "metrics", feature = "tracing"))]
use crate::monitor::prometheus_metrics::PrometheusMetrics;
#[cfg(all(feature = "metrics", feature = "tracing"))]
use crate::runtime_trace::RuntimeTraceMetricsSnapshot;
use crate::{log_runtime_trace, runtime_trace};

#[cfg(all(feature = "metrics", feature = "tracing"))]
pub(super) struct RuntimeTraceMetrics {
    tasks_created: IntCounterVec,
    task_polls: IntCounterVec,
    task_busy_seconds: CounterVec,
    task_attributed_waker_calls: IntCounterVec,
    wait_point_waker_calls: IntCounterVec,
    previous_snapshot: Mutex<RuntimeTraceMetricsSnapshot>,
}

#[cfg(all(feature = "metrics", feature = "tracing"))]
impl RuntimeTraceMetrics {
    const MARKER_LABEL: &'static str = "marker";
    const WAIT_POINT_LABEL: &'static str = "wait_point";

    pub(super) fn initialization(
        config: &MetricsConfig,
        registry: &Registry,
    ) -> anyhow::Result<Self> {
        for label in [Self::MARKER_LABEL, Self::WAIT_POINT_LABEL] {
            if config.metrics_labels.contains_key(label) {
                return Err(DtError::MetricsInitializationFailed(format!(
                    "metrics label [{label}] is reserved by runtime trace"
                ))
                .into());
            }
        }

        let int_counter_vec = |name: &str, desc: &str, labels: &[&str]| -> anyhow::Result<_> {
            IntCounterVec::new(
                Opts::new(name, desc).const_labels(config.metrics_labels.to_owned()),
                labels,
            )
            .context(DtError::MetricsInitializationFailed(format!(
                "Failed to initialize metric [{name}]"
            )))
        };

        let metrics = Self {
            tasks_created: int_counter_vec(
                "runtime_trace_tasks_created_total",
                "traced tokio tasks created per marker",
                &[Self::MARKER_LABEL],
            )?,
            task_polls: int_counter_vec(
                "runtime_trace_task_polls_total",
                "tokio task polls per marker",
                &[Self::MARKER_LABEL],
            )?,
            task_busy_seconds: CounterVec::new(
                Opts::new(
                    "runtime_trace_task_busy_seconds_total",
                    "tokio task busy seconds per marker",
                )
                .const_labels(config.metrics_labels.to_owned()),
                &[Self::MARKER_LABEL],
            )
            .context(DtError::MetricsInitializationFailed(
                "Failed to initialize metric [runtime_trace_task_busy_seconds_total]".to_owned(),
            ))?,
            task_attributed_waker_calls: int_counter_vec(
                "runtime_trace_task_attributed_waker_calls_total",
                "attributed waker calls per marker",
                &[Self::MARKER_LABEL],
            )?,
            wait_point_waker_calls: int_counter_vec(
                "runtime_trace_wait_point_waker_calls_total",
                "attributed waker calls per wait point across all markers",
                &[Self::WAIT_POINT_LABEL],
            )?,
            previous_snapshot: Mutex::new(RuntimeTraceMetricsSnapshot::default()),
        };
        metrics.register(registry)?;
        Ok(metrics)
    }

    fn register(&self, registry: &Registry) -> anyhow::Result<()> {
        let register_metric = |name: &str, collector: Box<dyn Collector>| -> anyhow::Result<()> {
            registry
                .register(collector)
                .context(DtError::MetricsInitializationFailed(format!(
                    "Failed to initialize metric [{name}]"
                )))?;
            Ok(())
        };

        register_metric(
            "runtime_trace_tasks_created_total",
            Box::new(self.tasks_created.clone()),
        )?;
        register_metric(
            "runtime_trace_task_polls_total",
            Box::new(self.task_polls.clone()),
        )?;
        register_metric(
            "runtime_trace_task_busy_seconds_total",
            Box::new(self.task_busy_seconds.clone()),
        )?;
        register_metric(
            "runtime_trace_task_attributed_waker_calls_total",
            Box::new(self.task_attributed_waker_calls.clone()),
        )?;
        register_metric(
            "runtime_trace_wait_point_waker_calls_total",
            Box::new(self.wait_point_waker_calls.clone()),
        )?;
        Ok(())
    }

    fn update_snapshot(&self, snapshot: &RuntimeTraceMetricsSnapshot) {
        let mut previous_snapshot = self
            .previous_snapshot
            .lock()
            .unwrap_or_else(|posisoned| posisoned.into_inner());
        for marker in &snapshot.markers {
            let previous_marker = previous_snapshot
                .markers
                .iter()
                .find(|previous| previous.marker == marker.marker);
            let marker_labels = &[marker.marker.as_str()];

            self.tasks_created.with_label_values(marker_labels).inc_by(
                marker
                    .tasks_created
                    .saturating_sub(previous_marker.map_or(0, |previous| previous.tasks_created)),
            );
            self.task_polls.with_label_values(marker_labels).inc_by(
                marker
                    .poll_count
                    .saturating_sub(previous_marker.map_or(0, |previous| previous.poll_count)),
            );
            self.task_busy_seconds
                .with_label_values(marker_labels)
                .inc_by(
                    (marker.busy_seconds
                        - previous_marker.map_or(0.0, |previous| previous.busy_seconds))
                    .max(0.0),
                );
            self.task_attributed_waker_calls
                .with_label_values(marker_labels)
                .inc_by(marker.attributed_waker_calls.saturating_sub(
                    previous_marker.map_or(0, |previous| previous.attributed_waker_calls),
                ));

            for wait_point in &marker.wait_points {
                let previous_waker_calls = previous_marker
                    .and_then(|previous| {
                        previous
                            .wait_points
                            .iter()
                            .find(|previous| previous.wait_point == wait_point.wait_point)
                    })
                    .map_or(0, |previous| previous.waker_calls);
                self.wait_point_waker_calls
                    .with_label_values(&[wait_point.wait_point.as_str()])
                    .inc_by(wait_point.waker_calls.saturating_sub(previous_waker_calls));
            }
        }
        *previous_snapshot = snapshot.clone();
    }
}

/// Flushes runtime trace summaries periodically and on shutdown.
/// With metrics enabled, it also exports a Prometheus snapshot.
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
        let Some((summary, _snapshot)) = runtime_trace::snapshot_global() else {
            return;
        };
        log_runtime_trace!("{}", summary.trim_end());

        #[cfg(all(feature = "metrics", feature = "tracing"))]
        self.prometheus_metrics
            .runtime_trace_metrics()
            .map(|trace_metrics| trace_metrics.update_snapshot(&_snapshot));
    }
}

#[cfg(all(test, feature = "metrics", feature = "tracing"))]
mod tests {
    use std::collections::HashMap;

    use prometheus::TextEncoder;

    use super::*;
    use crate::runtime_trace::{
        MarkerMetricsSnapshot, RuntimeTraceMetricsSnapshot, WaitPointMetricsSnapshot,
    };

    #[test]
    fn exports_runtime_trace_metrics_by_marker_and_global_wait_point() -> anyhow::Result<()> {
        let config = MetricsConfig {
            http_host: "127.0.0.1".to_owned(),
            http_port: 0,
            workers: 1,
            metrics_labels: HashMap::new(),
        };
        let registry = Registry::new();
        let metrics = RuntimeTraceMetrics::initialization(&config, &registry)?;
        let first_snapshot = RuntimeTraceMetricsSnapshot {
            markers: vec![
                MarkerMetricsSnapshot {
                    marker: "task.extractor_worker".to_owned(),
                    tasks_created: 1,
                    poll_count: 1084,
                    busy_seconds: 0.125,
                    attributed_waker_calls: 7,
                    wait_points: vec![WaitPointMetricsSnapshot {
                        wait_point: "dtqueue.not_empty.wait".to_owned(),
                        waker_calls: 7,
                    }],
                },
                MarkerMetricsSnapshot {
                    marker: "task.sinker_worker".to_owned(),
                    attributed_waker_calls: 3,
                    wait_points: vec![WaitPointMetricsSnapshot {
                        wait_point: "dtqueue.not_empty.wait".to_owned(),
                        waker_calls: 3,
                    }],
                    ..Default::default()
                },
            ],
        };
        metrics.update_snapshot(&first_snapshot);
        metrics.update_snapshot(&first_snapshot);
        metrics.update_snapshot(&RuntimeTraceMetricsSnapshot {
            markers: vec![
                MarkerMetricsSnapshot {
                    marker: "task.extractor_worker".to_owned(),
                    tasks_created: 2,
                    poll_count: 1090,
                    busy_seconds: 0.25,
                    attributed_waker_calls: 9,
                    wait_points: vec![WaitPointMetricsSnapshot {
                        wait_point: "dtqueue.not_empty.wait".to_owned(),
                        waker_calls: 9,
                    }],
                },
                MarkerMetricsSnapshot {
                    marker: "task.sinker_worker".to_owned(),
                    attributed_waker_calls: 4,
                    wait_points: vec![WaitPointMetricsSnapshot {
                        wait_point: "dtqueue.not_empty.wait".to_owned(),
                        waker_calls: 4,
                    }],
                    ..Default::default()
                },
            ],
        });

        let mut output = String::new();
        TextEncoder::new()
            .encode_utf8(&registry.gather(), &mut output)
            .unwrap();

        assert!(output
            .contains("runtime_trace_tasks_created_total{marker=\"task.extractor_worker\"} 2"));
        assert!(output
            .contains("runtime_trace_task_polls_total{marker=\"task.extractor_worker\"} 1090"));
        assert!(!output.contains("runtime_trace_task_schedules_total"));
        assert!(output.contains(
            "runtime_trace_task_busy_seconds_total{marker=\"task.extractor_worker\"} 0.25"
        ));
        assert!(output.contains(
            "runtime_trace_task_attributed_waker_calls_total{marker=\"task.extractor_worker\"} 9"
        ));
        assert!(output.contains(
            "runtime_trace_wait_point_waker_calls_total{wait_point=\"dtqueue.not_empty.wait\"} 13"
        ));
        assert!(!output.contains("runtime_trace_wait_point_waker_calls_total{marker="));
        assert!(!output.contains("task_name="));
        assert!(!output.contains("task.extractor_worker@"));
        Ok(())
    }
}
