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

impl TraceOutputFormat {
    #[cfg(feature = "tracing")]
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Json,
            _ => Self::Plain,
        }
    }
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

impl TaskSummaryMode {
    #[cfg(feature = "tracing")]
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Marker,
            _ => Self::Task,
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

#[cfg(feature = "tracing")]
use std::panic::Location;

#[cfg(feature = "tracing")]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WaitPoint {
    name: &'static str,
    file: &'static str,
    line: u32,
}

#[cfg(feature = "tracing")]
impl WaitPoint {
    fn new(name: &'static str, location: &'static Location<'static>) -> Self {
        Self {
            name,
            file: location.file(),
            line: location.line(),
        }
    }

    fn display(&self) -> String {
        format!("{}@{}:{}", self.name, self.file, self.line)
    }
}

#[cfg(feature = "tracing")]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TaskMarker {
    name: &'static str,
    file: &'static str,
    line: u32,
}

#[cfg(feature = "tracing")]
impl TaskMarker {
    fn new(name: &'static str, location: &'static Location<'static>) -> Self {
        Self {
            name,
            file: location.file(),
            line: location.line(),
        }
    }

    fn display(&self) -> String {
        format!("{}@{}:{}", self.name, self.file, self.line)
    }
}

#[cfg(feature = "tracing")]
mod imp {
    use std::{
        collections::HashMap,
        fmt::Write,
        future::Future,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, OnceLock,
        },
        task::{Context, Wake, Waker},
    };

    use chrono::{SecondsFormat, Utc};
    use dashmap::DashMap;
    use futures::future::poll_fn;
    use serde_json::{json, Value};
    use tokio_metrics::TaskMonitor;
    use tracing_subscriber::filter::Targets;

    use super::{TaskMarker, TaskSummaryMode, TraceOutputFormat, WaitPoint};

    static ENABLED: AtomicBool = AtomicBool::new(false);
    static TASK_SUMMARY_MODE: AtomicU64 = AtomicU64::new(TaskSummaryMode::Marker as u64);
    static TRACE_OUTPUT_FORMAT: AtomicU64 = AtomicU64::new(TraceOutputFormat::Plain as u64);
    static INIT_TRACING: OnceLock<()> = OnceLock::new();
    static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);
    static TASKS: OnceLock<DashMap<u64, Arc<TaskTrace>>> = OnceLock::new();

    tokio::task_local! {
        static TRACE_TASK: Arc<TaskTrace>;
    }

    struct TaskTrace {
        id: u64,
        marker: TaskMarker,
        monitor: TaskMonitor,
        wait_point_waker_calls: DashMap<WaitPoint, AtomicU64>,
    }

    struct TaskSnapshot {
        id: u64,
        marker: TaskMarker,
        poll_count: u64,
        scheduled_count: u64,
        busy_ns: u64,
        // Raw attributed Waker calls are not equivalent to TaskMonitor scheduling cycles.
        wait_point_waker_calls: Vec<(WaitPoint, u64)>,
    }

    struct MarkerSnapshot {
        marker: TaskMarker,
        task_count: u64,
        poll_count: u64,
        scheduled_count: u64,
        busy_ns: u64,
        wait_point_waker_calls: Vec<(WaitPoint, u64)>,
    }

    struct CachedAttributedWaker {
        task: Arc<TaskTrace>,
        original: Waker,
        attributed: Waker,
    }

    struct AttributedWaker {
        task: Arc<TaskTrace>,
        wait_point: WaitPoint,
        inner: Waker,
    }

    pub fn enable() {
        ENABLED.store(true, Ordering::Release);
    }

    pub fn init_tracing() {
        INIT_TRACING.get_or_init(|| {
            enable();

            let fmt_filter = std::env::var("RUST_LOG")
                .ok()
                .and_then(|log_filter| match log_filter.parse::<Targets>() {
                    Ok(targets) => Some(targets),
                    Err(err) => {
                        eprintln!("failed to parse RUST_LOG={log_filter:?}: {err}");
                        None
                    }
                })
                .unwrap_or_else(|| "error".parse().expect("error filter should parse"));

            use tracing_subscriber::prelude::*;

            let console_layer = console_subscriber::ConsoleLayer::builder().spawn();
            let _ = tracing_subscriber::registry()
                .with(console_layer)
                .with(tracing_subscriber::fmt::layer().with_filter(fmt_filter))
                .try_init();
        });
    }

    pub fn set_task_summary_mode(mode: TaskSummaryMode) {
        TASK_SUMMARY_MODE.store(mode as u64, Ordering::Release);
    }

    pub fn set_output_format(format: TraceOutputFormat) {
        TRACE_OUTPUT_FORMAT.store(format as u64, Ordering::Release);
    }

    pub fn dump_global_summary() -> Option<String> {
        if !is_enabled() {
            return None;
        }

        let generated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
        if trace_output_format() == TraceOutputFormat::Json {
            return Some(dump_json_summary(&generated_at));
        }

        let mut summary = String::new();
        let wait_point_counts = collect_global_wait_point_counts();
        let attributed_call_count = wait_point_total(&wait_point_counts);

        let _ = writeln!(
            summary,
            "{generated_at} | ape-dts Tokio task runtime summary"
        );
        if attributed_call_count == 0 {
            let _ = writeln!(summary, "attributed waker calls: none");
        } else {
            let _ = writeln!(
                summary,
                "attributed waker calls: total={attributed_call_count}"
            );
            for (wait_point, count) in wait_point_counts {
                let _ = writeln!(
                    summary,
                    "  count={count} percent_of_all_attributed_calls={:.2}% wait_point={}",
                    percent(count, attributed_call_count),
                    wait_point.display()
                );
            }
        }

        match task_summary_mode() {
            TaskSummaryMode::Task => dump_task_summary(&mut summary),
            TaskSummaryMode::Marker => dump_marker_summary(&mut summary),
        }
        Some(summary)
    }

    pub async fn instrument_wait<Fut>(wait_point: WaitPoint, future: Fut) -> Fut::Output
    where
        Fut: Future,
    {
        let mut cached_waker = None;
        futures::pin_mut!(future);

        poll_fn(|cx| {
            let Ok(task) = TRACE_TASK.try_with(Arc::clone) else {
                return future.as_mut().poll(cx);
            };

            let rebuild_waker =
                cached_waker
                    .as_ref()
                    .is_none_or(|cached: &CachedAttributedWaker| {
                        !Arc::ptr_eq(&cached.task, &task) || !cached.original.will_wake(cx.waker())
                    });
            if rebuild_waker {
                let original = cx.waker().clone();
                let attributed = Waker::from(Arc::new(AttributedWaker {
                    task: Arc::clone(&task),
                    wait_point,
                    inner: original.clone(),
                }));
                cached_waker = Some(CachedAttributedWaker {
                    task,
                    original,
                    attributed,
                });
            }

            let attributed = &cached_waker
                .as_ref()
                .expect("attributed waker should be initialized")
                .attributed;
            let mut attributed_cx = Context::from_waker(attributed);
            future.as_mut().poll(&mut attributed_cx)
        })
        .await
    }

    pub async fn trace_task_future<Fut>(marker: TaskMarker, future: Fut) -> Fut::Output
    where
        Fut: Future,
    {
        let task = Arc::new(TaskTrace::new(
            NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed),
            marker,
        ));
        tasks().insert(task.id, Arc::clone(&task));

        let instrumented = task.monitor.instrument(future);
        TRACE_TASK.scope(task, instrumented).await
    }

    impl Wake for AttributedWaker {
        fn wake(self: Arc<Self>) {
            self.task.record_wait_point_wake(self.wait_point);
            self.inner.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.task.record_wait_point_wake(self.wait_point);
            self.inner.wake_by_ref();
        }
    }

    impl TaskTrace {
        fn new(id: u64, marker: TaskMarker) -> Self {
            Self {
                id,
                marker,
                monitor: TaskMonitor::new(),
                wait_point_waker_calls: DashMap::new(),
            }
        }

        fn record_wait_point_wake(&self, wait_point: WaitPoint) {
            increment_wait_point(&self.wait_point_waker_calls, wait_point, 1);
        }

        fn snapshot(&self) -> TaskSnapshot {
            let metrics = self.monitor.cumulative();
            let mut wait_point_waker_calls = self
                .wait_point_waker_calls
                .iter()
                .map(|entry| (*entry.key(), entry.value().load(Ordering::Acquire)))
                .collect::<Vec<_>>();
            sort_wait_point_counts(&mut wait_point_waker_calls);

            TaskSnapshot {
                id: self.id,
                marker: self.marker,
                poll_count: metrics.total_poll_count,
                scheduled_count: metrics.total_scheduled_count,
                busy_ns: duration_ns(metrics.total_poll_duration),
                wait_point_waker_calls,
            }
        }
    }

    fn dump_json_summary(generated_at: &str) -> String {
        let wait_point_counts = collect_global_wait_point_counts();
        let attributed_call_count = wait_point_total(&wait_point_counts);
        let mode = task_summary_mode();
        let mut summary = json!({
            "generated_at": generated_at,
            "title": "ape-dts Tokio task runtime summary",
            "attributed_waker_calls": {
                "total": attributed_call_count,
                "attributions": wait_point_counts
                    .into_iter()
                    .map(|(wait_point, count)| {
                        wait_point_count_json(wait_point, count, attributed_call_count)
                    })
                    .collect::<Vec<_>>(),
            },
            "task_summary_mode": mode.to_string(),
        });

        match mode {
            TaskSummaryMode::Task => {
                summary["tasks"] = Value::Array(
                    collect_sorted_task_snapshots()
                        .into_iter()
                        .map(task_snapshot_json)
                        .collect(),
                );
            }
            TaskSummaryMode::Marker => {
                summary["markers"] = Value::Array(
                    collect_sorted_marker_snapshots()
                        .into_iter()
                        .map(marker_snapshot_json)
                        .collect(),
                );
            }
        }

        serde_json::to_string(&summary).unwrap_or_else(|err| {
            json!({
                "generated_at": generated_at,
                "error": format!("failed to serialize runtime trace summary: {err}")
            })
            .to_string()
        })
    }

    fn dump_task_summary(summary: &mut String) {
        let task_snapshots = collect_sorted_task_snapshots();
        if task_snapshots.is_empty() {
            let _ = writeln!(summary, "traced tokio tasks: none");
            return;
        }

        let _ = writeln!(
            summary,
            "traced tokio tasks: total={}",
            task_snapshots.len()
        );
        for task in task_snapshots {
            let attributed_call_count = wait_point_total(&task.wait_point_waker_calls);
            let _ = writeln!(
                summary,
                "  task_id={} marker={} poll_count={} scheduled_count={} busy_ms={:.3} attributed_waker_calls={}",
                task.id,
                task.marker.display(),
                task.poll_count,
                task.scheduled_count,
                task.busy_ns as f64 / 1_000_000.0,
                attributed_call_count
            );
            write_wait_point_counts(
                summary,
                task.wait_point_waker_calls,
                attributed_call_count,
                "percent_of_task_attributed_calls",
            );
        }
    }

    fn dump_marker_summary(summary: &mut String) {
        let marker_snapshots = collect_sorted_marker_snapshots();
        if marker_snapshots.is_empty() {
            let _ = writeln!(summary, "traced tokio task markers: none");
            return;
        }

        let _ = writeln!(
            summary,
            "traced tokio task markers: total={}",
            marker_snapshots.len()
        );
        for marker in marker_snapshots {
            let attributed_call_count = wait_point_total(&marker.wait_point_waker_calls);
            let _ = writeln!(
                summary,
                "  marker={} task_count={} poll_count={} scheduled_count={} busy_ms={:.3} attributed_waker_calls={}",
                marker.marker.display(),
                marker.task_count,
                marker.poll_count,
                marker.scheduled_count,
                marker.busy_ns as f64 / 1_000_000.0,
                attributed_call_count
            );
            write_wait_point_counts(
                summary,
                marker.wait_point_waker_calls,
                attributed_call_count,
                "percent_of_marker_attributed_calls",
            );
        }
    }

    fn write_wait_point_counts(
        summary: &mut String,
        wait_point_counts: Vec<(WaitPoint, u64)>,
        attributed_call_count: u64,
        percent_field: &str,
    ) {
        for (wait_point, count) in wait_point_counts {
            let _ = writeln!(
                summary,
                "    count={count} {percent_field}={:.2}% wait_point={}",
                percent(count, attributed_call_count),
                wait_point.display()
            );
        }
    }

    fn is_enabled() -> bool {
        ENABLED.load(Ordering::Acquire)
    }

    fn task_summary_mode() -> TaskSummaryMode {
        TaskSummaryMode::from_u8(TASK_SUMMARY_MODE.load(Ordering::Acquire) as u8)
    }

    fn trace_output_format() -> TraceOutputFormat {
        TraceOutputFormat::from_u8(TRACE_OUTPUT_FORMAT.load(Ordering::Acquire) as u8)
    }

    fn tasks() -> &'static DashMap<u64, Arc<TaskTrace>> {
        TASKS.get_or_init(DashMap::new)
    }

    fn increment_wait_point(
        counts: &DashMap<WaitPoint, AtomicU64>,
        wait_point: WaitPoint,
        count: u64,
    ) {
        counts
            .entry(wait_point)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(count, Ordering::Release);
    }

    fn collect_global_wait_point_counts() -> Vec<(WaitPoint, u64)> {
        let mut counts = HashMap::<WaitPoint, u64>::new();
        for task in tasks().iter() {
            for wait_point in task.wait_point_waker_calls.iter() {
                let count = wait_point.value().load(Ordering::Acquire);
                counts
                    .entry(*wait_point.key())
                    .and_modify(|total| *total = total.saturating_add(count))
                    .or_insert(count);
            }
        }

        let mut wait_point_counts = counts.into_iter().collect::<Vec<_>>();
        sort_wait_point_counts(&mut wait_point_counts);
        wait_point_counts
    }

    fn collect_task_snapshots() -> Vec<TaskSnapshot> {
        tasks()
            .iter()
            .map(|entry| entry.value().snapshot())
            .collect()
    }

    fn collect_marker_snapshots() -> Vec<MarkerSnapshot> {
        let mut marker_snapshots = HashMap::<TaskMarker, MarkerSnapshot>::new();
        for task in collect_task_snapshots() {
            let marker_snapshot =
                marker_snapshots
                    .entry(task.marker)
                    .or_insert_with(|| MarkerSnapshot {
                        marker: task.marker,
                        task_count: 0,
                        poll_count: 0,
                        scheduled_count: 0,
                        busy_ns: 0,
                        wait_point_waker_calls: Vec::new(),
                    });
            marker_snapshot.task_count = marker_snapshot.task_count.saturating_add(1);
            marker_snapshot.poll_count = marker_snapshot.poll_count.saturating_add(task.poll_count);
            marker_snapshot.scheduled_count = marker_snapshot
                .scheduled_count
                .saturating_add(task.scheduled_count);
            marker_snapshot.busy_ns = marker_snapshot.busy_ns.saturating_add(task.busy_ns);

            for (wait_point, count) in task.wait_point_waker_calls {
                if let Some((_, existing_count)) = marker_snapshot
                    .wait_point_waker_calls
                    .iter_mut()
                    .find(|(existing_wait_point, _)| *existing_wait_point == wait_point)
                {
                    *existing_count = existing_count.saturating_add(count);
                } else {
                    marker_snapshot
                        .wait_point_waker_calls
                        .push((wait_point, count));
                }
            }
        }

        let mut snapshots = marker_snapshots.into_values().collect::<Vec<_>>();
        for snapshot in &mut snapshots {
            sort_wait_point_counts(&mut snapshot.wait_point_waker_calls);
        }
        snapshots
    }

    fn collect_sorted_task_snapshots() -> Vec<TaskSnapshot> {
        let mut task_snapshots = collect_task_snapshots();
        task_snapshots.sort_by(|a, b| {
            b.poll_count
                .cmp(&a.poll_count)
                .then_with(|| b.scheduled_count.cmp(&a.scheduled_count))
                .then_with(|| a.id.cmp(&b.id))
        });
        task_snapshots
    }

    fn collect_sorted_marker_snapshots() -> Vec<MarkerSnapshot> {
        let mut marker_snapshots = collect_marker_snapshots();
        marker_snapshots.sort_by(|a, b| {
            b.poll_count
                .cmp(&a.poll_count)
                .then_with(|| b.scheduled_count.cmp(&a.scheduled_count))
                .then_with(|| a.marker.name.cmp(b.marker.name))
                .then_with(|| a.marker.file.cmp(b.marker.file))
                .then_with(|| a.marker.line.cmp(&b.marker.line))
        });
        marker_snapshots
    }

    fn wait_point_json(wait_point: WaitPoint) -> Value {
        json!({
            "name": wait_point.name,
            "file": wait_point.file,
            "line": wait_point.line,
            "display": wait_point.display(),
        })
    }

    fn marker_json(marker: TaskMarker) -> Value {
        json!({
            "name": marker.name,
            "file": marker.file,
            "line": marker.line,
            "display": marker.display(),
        })
    }

    fn wait_point_count_json(wait_point: WaitPoint, count: u64, total: u64) -> Value {
        json!({
            "wait_point": wait_point_json(wait_point),
            "count": count,
            "percent_of_attributed_calls": percent(count, total),
        })
    }

    fn task_snapshot_json(task: TaskSnapshot) -> Value {
        let attributed_call_count = wait_point_total(&task.wait_point_waker_calls);
        json!({
            "task_id": task.id,
            "marker": marker_json(task.marker),
            "poll_count": task.poll_count,
            "scheduled_count": task.scheduled_count,
            "busy_ms": task.busy_ns as f64 / 1_000_000.0,
            "attributed_waker_calls": {
                "total": attributed_call_count,
                "attributions": task.wait_point_waker_calls
                    .into_iter()
                    .map(|(wait_point, count)| {
                        wait_point_count_json(wait_point, count, attributed_call_count)
                    })
                    .collect::<Vec<_>>(),
            },
        })
    }

    fn marker_snapshot_json(marker: MarkerSnapshot) -> Value {
        let attributed_call_count = wait_point_total(&marker.wait_point_waker_calls);
        json!({
            "marker": marker_json(marker.marker),
            "task_count": marker.task_count,
            "poll_count": marker.poll_count,
            "scheduled_count": marker.scheduled_count,
            "busy_ms": marker.busy_ns as f64 / 1_000_000.0,
            "attributed_waker_calls": {
                "total": attributed_call_count,
                "attributions": marker.wait_point_waker_calls
                    .into_iter()
                    .map(|(wait_point, count)| {
                        wait_point_count_json(wait_point, count, attributed_call_count)
                    })
                    .collect::<Vec<_>>(),
            },
        })
    }

    fn wait_point_total(wait_point_counts: &[(WaitPoint, u64)]) -> u64 {
        wait_point_counts
            .iter()
            .fold(0, |total, (_, count)| total.saturating_add(*count))
    }

    fn sort_wait_point_counts(wait_point_counts: &mut [(WaitPoint, u64)]) {
        wait_point_counts.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.name.cmp(b.0.name))
                .then_with(|| a.0.file.cmp(b.0.file))
                .then_with(|| a.0.line.cmp(&b.0.line))
        });
    }

    fn duration_ns(duration: std::time::Duration) -> u64 {
        duration.as_nanos().min(u64::MAX as u128) as u64
    }

    fn percent(count: u64, total: u64) -> f64 {
        if total == 0 {
            0.0
        } else {
            count as f64 * 100.0 / total as f64
        }
    }

    #[cfg(test)]
    mod tests {
        use std::{
            future::Future,
            pin::Pin,
            sync::{
                atomic::{AtomicU64, Ordering},
                Arc, Mutex,
            },
            task::{Context, Poll, Wake, Waker},
        };

        use futures::task::noop_waker;

        use super::*;

        struct CapturePendingWaker {
            captured: Arc<Mutex<Option<Waker>>>,
        }

        impl Future for CapturePendingWaker {
            type Output = ();

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                *self.captured.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            }
        }

        struct YieldOnce(bool);

        impl Future for YieldOnce {
            type Output = ();

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                if self.0 {
                    Poll::Ready(())
                } else {
                    self.0 = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }

        #[derive(Default)]
        struct WakeRecorder {
            wakes: AtomicU64,
        }

        impl Wake for WakeRecorder {
            fn wake(self: Arc<Self>) {
                self.wakes.fetch_add(1, Ordering::Release);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.wakes.fetch_add(1, Ordering::Release);
            }
        }

        #[track_caller]
        fn test_wait_point(name: &'static str) -> WaitPoint {
            WaitPoint::new(name, std::panic::Location::caller())
        }

        #[track_caller]
        fn test_marker(name: &'static str) -> TaskMarker {
            TaskMarker::new(name, std::panic::Location::caller())
        }

        fn test_task(id: u64) -> Arc<TaskTrace> {
            Arc::new(TaskTrace::new(id, test_marker("test.task")))
        }

        fn wait_point_count(task: &TaskTrace, wait_point: WaitPoint) -> u64 {
            task.wait_point_waker_calls
                .get(&wait_point)
                .map(|count| count.load(Ordering::Acquire))
                .unwrap_or(0)
        }

        #[test]
        fn trace_task_future_registers_marker_and_task_metrics() {
            let marker = test_marker("test.monitored_task");
            let mut future = Box::pin(trace_task_future(marker, YieldOnce(false)));
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);

            assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(()));

            let task = tasks()
                .iter()
                .find(|task| task.marker == marker)
                .expect("traced task should be registered");
            let snapshot = task.snapshot();
            assert_eq!(snapshot.marker, marker);
            assert_eq!(snapshot.poll_count, 2);
            assert_eq!(snapshot.scheduled_count, 1);
        }

        #[test]
        fn instrument_wait_records_only_when_woken() {
            let wait_point = test_wait_point("wait.pending");
            let task = test_task(1);
            let captured = Arc::new(Mutex::new(None));
            let mut future = Box::pin(TRACE_TASK.scope(
                Arc::clone(&task),
                instrument_wait(wait_point, CapturePendingWaker { captured }),
            ));
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);

            assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(wait_point_count(&task, wait_point), 0);
        }

        #[test]
        fn instrument_wait_attributes_stored_waker_to_wait_point() {
            let wait_point = test_wait_point("wait.stored_waker");
            let task = test_task(2);
            let captured = Arc::new(Mutex::new(None));
            let recorder = Arc::new(WakeRecorder::default());
            let waker = Waker::from(Arc::clone(&recorder));
            let mut future = Box::pin(TRACE_TASK.scope(
                Arc::clone(&task),
                instrument_wait(
                    wait_point,
                    CapturePendingWaker {
                        captured: Arc::clone(&captured),
                    },
                ),
            ));
            let mut cx = Context::from_waker(&waker);

            assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
            let captured_waker = captured.lock().unwrap().take().unwrap();
            std::thread::spawn(move || captured_waker.wake_by_ref())
                .join()
                .unwrap();

            assert_eq!(wait_point_count(&task, wait_point), 1);
            assert_eq!(recorder.wakes.load(Ordering::Acquire), 1);
        }

        #[test]
        fn instrument_wait_keeps_target_tasks_isolated() {
            let wait_point = test_wait_point("wait.isolated");
            let first_task = test_task(3);
            let second_task = test_task(4);
            let first_captured = Arc::new(Mutex::new(None));
            let second_captured = Arc::new(Mutex::new(None));
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);

            let mut first_future = Box::pin(TRACE_TASK.scope(
                Arc::clone(&first_task),
                instrument_wait(
                    wait_point,
                    CapturePendingWaker {
                        captured: Arc::clone(&first_captured),
                    },
                ),
            ));
            let mut second_future = Box::pin(TRACE_TASK.scope(
                Arc::clone(&second_task),
                instrument_wait(
                    wait_point,
                    CapturePendingWaker {
                        captured: Arc::clone(&second_captured),
                    },
                ),
            ));

            assert_eq!(first_future.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(second_future.as_mut().poll(&mut cx), Poll::Pending);
            first_captured.lock().unwrap().take().unwrap().wake_by_ref();

            assert_eq!(wait_point_count(&first_task, wait_point), 1);
            assert_eq!(wait_point_count(&second_task, wait_point), 0);
        }
    }
}

#[cfg(not(feature = "tracing"))]
mod imp {
    #[inline(always)]
    pub fn enable() {}

    #[inline(always)]
    pub fn init_tracing() {}

    #[inline(always)]
    pub fn dump_global_summary() -> Option<String> {
        None
    }

    #[inline(always)]
    pub fn set_task_summary_mode(_mode: super::TaskSummaryMode) {}

    #[inline(always)]
    pub fn set_output_format(_format: super::TraceOutputFormat) {}
}

pub use imp::{
    dump_global_summary, enable, init_tracing, set_output_format, set_task_summary_mode,
};

#[cfg(feature = "tracing")]
#[track_caller]
pub fn instrument_wait<Fut>(
    name: &'static str,
    future: Fut,
) -> impl std::future::Future<Output = Fut::Output>
where
    Fut: std::future::Future,
{
    let wait_point = WaitPoint::new(name, Location::caller());
    imp::instrument_wait(wait_point, future)
}

#[cfg(not(feature = "tracing"))]
#[inline(always)]
pub fn instrument_wait<Fut>(_name: &'static str, future: Fut) -> Fut
where
    Fut: std::future::Future,
{
    future
}

#[cfg(feature = "tracing")]
#[track_caller]
pub fn trace_task_future<Fut>(
    name: &'static str,
    future: Fut,
) -> impl std::future::Future<Output = Fut::Output>
where
    Fut: std::future::Future,
{
    let marker = TaskMarker::new(name, Location::caller());
    imp::trace_task_future(marker, future)
}

#[cfg(not(feature = "tracing"))]
#[inline(always)]
pub fn trace_task_future<Fut>(_name: &'static str, future: Fut) -> Fut
where
    Fut: std::future::Future,
{
    future
}
