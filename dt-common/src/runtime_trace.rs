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
pub struct WakeSource {
    name: &'static str,
    file: &'static str,
    line: u32,
}

#[cfg(feature = "tracing")]
impl WakeSource {
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
        cell::RefCell,
        collections::HashMap,
        fmt::Write,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex, OnceLock,
        },
        time::Instant,
    };

    use dashmap::DashMap;
    use tracing::{
        field::{Field, Visit},
        span::{Attributes, Id},
        subscriber::Interest,
        Event, Metadata, Subscriber,
    };
    use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

    use serde_json::{json, Value};

    use super::{TaskMarker, TaskSummaryMode, TraceOutputFormat, WakeSource};

    static ENABLED: AtomicBool = AtomicBool::new(false);
    static TASK_SUMMARY_MODE: AtomicU64 = AtomicU64::new(TaskSummaryMode::Task as u64);
    static TRACE_OUTPUT_FORMAT: AtomicU64 = AtomicU64::new(TraceOutputFormat::Plain as u64);
    static TASKS: OnceLock<DashMap<u64, Arc<TaskStats>>> = OnceLock::new();
    static GLOBAL_WAKE_SOURCES: OnceLock<DashMap<WakeSource, AtomicU64>> = OnceLock::new();

    thread_local! {
        static WAKE_SOURCE_STACK: RefCell<Vec<WakeSource>> = const { RefCell::new(Vec::new()) };
        static CURRENT_TASK_STACK: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
    }

    pub struct TaskStatsLayer;

    struct TaskStats {
        id: u64,
        name: Mutex<String>,
        location: Mutex<Option<String>>,
        polls: AtomicU64,
        wakes: AtomicU64,
        self_wakes: AtomicU64,
        busy_ns: AtomicU64,
        current_poll_started: Mutex<Option<Instant>>,
        wake_sources: DashMap<WakeSource, AtomicU64>,
        marker: Mutex<Option<TaskMarker>>,
    }

    #[derive(Default)]
    struct TaskFields {
        name: Option<String>,
        file: Option<String>,
        line: Option<u64>,
        column: Option<u64>,
    }

    #[derive(Default)]
    struct WakerFields {
        task_id: Option<u64>,
        is_wake: bool,
    }

    struct TaskSnapshot {
        id: u64,
        name: String,
        location: Option<String>,
        polls: u64,
        wakes: u64,
        self_wakes: u64,
        busy_ns: u64,
        wake_sources: Vec<(WakeSource, u64)>,
        marker: Option<TaskMarker>,
    }

    struct MarkerSnapshot {
        marker: TaskMarker,
        tasks: u64,
        polls: u64,
        wakes: u64,
        self_wakes: u64,
        busy_ns: u64,
        wake_sources: Vec<(WakeSource, u64)>,
    }

    pub fn enable() {
        ENABLED.store(true, Ordering::Release);
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

        if trace_output_format() == TraceOutputFormat::Json {
            return Some(dump_json_summary());
        }

        let mut summary = String::new();
        let mut source_counts = collect_global_source_counts();
        let total_known_sources = source_counts.iter().map(|(_, count)| *count).sum::<u64>();

        let _ = writeln!(summary, "=== ape-dts tokio wake trace summary ===");
        if total_known_sources == 0 {
            let _ = writeln!(summary, "known wake sources: none");
        } else {
            let _ = writeln!(summary, "known wake sources: total={}", total_known_sources);
            for (source, count) in source_counts.drain(..) {
                let _ = writeln!(
                    summary,
                    "  {:>8} {:>6.2}% {}",
                    count,
                    percent(count, total_known_sources),
                    source.display()
                );
            }
        }

        if task_summary_mode() == TaskSummaryMode::Marker {
            dump_marker_summary(&mut summary);
            return Some(summary);
        }

        dump_task_summary(&mut summary);
        Some(summary)
    }

    fn dump_json_summary() -> String {
        let source_counts = collect_global_source_counts();
        let total_known_sources = source_counts.iter().map(|(_, count)| *count).sum::<u64>();
        let mode = task_summary_mode();
        let mut summary = json!({
            "known_wake_sources": {
                "total": total_known_sources,
                "sources": source_counts
                    .into_iter()
                    .map(|(source, count)| source_count_json(source, count, total_known_sources))
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
                "error": format!("failed to serialize runtime trace summary: {err}")
            })
            .to_string()
        })
    }

    fn dump_task_summary(summary: &mut String) {
        let tasks = collect_sorted_task_snapshots();
        if tasks.is_empty() {
            let _ = writeln!(summary, "traced tokio tasks: none");
            return;
        }

        let _ = writeln!(summary, "traced tokio tasks: total={}", tasks.len());
        for task in tasks {
            let known_task_sources = task
                .wake_sources
                .iter()
                .map(|(_, count)| *count)
                .sum::<u64>();
            let location = task.location.as_deref().unwrap_or("-");
            let marker = task
                .marker
                .map(|marker| marker.display())
                .unwrap_or_else(|| "-".into());
            let _ = writeln!(
                summary,
                "  task={} marker={} name={} polls={} wakes={} self_wakes={} busy_ms={:.3} spawn={}",
                task.id,
                marker,
                task.name,
                task.polls,
                task.wakes,
                task.self_wakes,
                task.busy_ns as f64 / 1_000_000.0,
                location
            );

            if known_task_sources > 0 {
                for (source, count) in task.wake_sources {
                    let _ = writeln!(
                        summary,
                        "    source {:>8} {:>6.2}% known {:>6.2}% wakes {}",
                        count,
                        percent(count, known_task_sources),
                        percent(count, task.wakes),
                        source.display()
                    );
                }
            }
        }
    }

    fn dump_marker_summary(summary: &mut String) {
        let markers = collect_sorted_marker_snapshots();
        if markers.is_empty() {
            let _ = writeln!(summary, "traced tokio task markers: none");
            return;
        }

        let _ = writeln!(
            summary,
            "traced tokio task markers: total={}",
            markers.len()
        );
        for marker in markers {
            let known_marker_sources = marker
                .wake_sources
                .iter()
                .map(|(_, count)| *count)
                .sum::<u64>();
            let _ = writeln!(
                summary,
                "  marker={} tasks={} polls={} wakes={} self_wakes={} busy_ms={:.3}",
                marker.marker.display(),
                marker.tasks,
                marker.polls,
                marker.wakes,
                marker.self_wakes,
                marker.busy_ns as f64 / 1_000_000.0
            );

            if known_marker_sources > 0 {
                for (source, count) in marker.wake_sources {
                    let _ = writeln!(
                        summary,
                        "    source {:>8} {:>6.2}% known {:>6.2}% wakes {}",
                        count,
                        percent(count, known_marker_sources),
                        percent(count, marker.wakes),
                        source.display()
                    );
                }
            }
        }
    }

    pub fn with_wake_source<R>(source: WakeSource, f: impl FnOnce() -> R) -> R {
        let _guard = WakeSourceGuard::new(source);
        f()
    }

    pub async fn with_wake_source_future<Fut>(source: WakeSource, future: Fut) -> Fut::Output
    where
        Fut: std::future::Future,
    {
        let _guard = WakeSourceGuard::new(source);
        future.await
    }

    pub async fn trace_task_future<Fut>(marker: TaskMarker, future: Fut) -> Fut::Output
    where
        Fut: std::future::Future,
    {
        mark_current_task(marker);
        future.await
    }

    impl TaskStatsLayer {
        pub fn new() -> Self {
            Self
        }
    }

    impl Default for TaskStatsLayer {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<S> Layer<S> for TaskStatsLayer
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
            if is_enabled() && (is_task_metadata(metadata) || is_waker_metadata(metadata)) {
                Interest::always()
            } else {
                Interest::never()
            }
        }

        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: Context<'_, S>) {
            if !is_enabled() || !is_task_metadata(attrs.metadata()) {
                return;
            }

            let mut fields = TaskFields::default();
            attrs.record(&mut fields);

            tasks().entry(id.into_u64()).or_insert_with(|| {
                Arc::new(TaskStats::new(
                    id.into_u64(),
                    fields.name(),
                    fields.location(),
                ))
            });
        }

        fn on_enter(&self, id: &Id, _ctx: Context<'_, S>) {
            if !is_enabled() {
                return;
            }

            let task_id = id.into_u64();
            if let Some(task) = tasks().get(&task_id) {
                task.start_poll();
                CURRENT_TASK_STACK.with(|stack| stack.borrow_mut().push(task_id));
            }
        }

        fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
            if !is_enabled() {
                return;
            }

            let task_id = id.into_u64();
            if let Some(task) = tasks().get(&task_id) {
                task.end_poll();
                CURRENT_TASK_STACK.with(|stack| {
                    let mut stack = stack.borrow_mut();
                    if let Some(pos) = stack.iter().rposition(|id| *id == task_id) {
                        stack.remove(pos);
                    }
                });
            }
        }

        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if !is_enabled() || !is_waker_metadata(event.metadata()) {
                return;
            }

            let mut fields = WakerFields::default();
            event.record(&mut fields);
            if !fields.is_wake {
                return;
            }

            let Some(task_id) = fields.task_id else {
                return;
            };

            let task = tasks()
                .entry(task_id)
                .or_insert_with(|| Arc::new(TaskStats::new(task_id, "unknown".into(), None)))
                .clone();
            let self_wake = CURRENT_TASK_STACK.with(|stack| stack.borrow().contains(&task_id));
            let source = current_wake_source();
            task.record_wake(self_wake, source);

            if let Some(source) = source {
                increment_source(global_wake_sources(), source);
            }
        }
    }

    impl TaskStats {
        fn new(id: u64, name: String, location: Option<String>) -> Self {
            Self {
                id,
                name: Mutex::new(name),
                location: Mutex::new(location),
                polls: AtomicU64::new(0),
                wakes: AtomicU64::new(0),
                self_wakes: AtomicU64::new(0),
                busy_ns: AtomicU64::new(0),
                current_poll_started: Mutex::new(None),
                wake_sources: DashMap::new(),
                marker: Mutex::new(None),
            }
        }

        fn set_marker(&self, marker: TaskMarker) {
            *self.marker.lock().unwrap() = Some(marker);
        }

        fn start_poll(&self) {
            let mut started = self.current_poll_started.lock().unwrap();
            if started.is_none() {
                *started = Some(Instant::now());
                self.polls.fetch_add(1, Ordering::Release);
            }
        }

        fn end_poll(&self) {
            let Some(started) = self.current_poll_started.lock().unwrap().take() else {
                return;
            };

            let elapsed = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            self.busy_ns.fetch_add(elapsed, Ordering::Release);
        }

        fn record_wake(&self, self_wake: bool, source: Option<WakeSource>) {
            self.wakes.fetch_add(1, Ordering::Release);
            if self_wake {
                self.self_wakes.fetch_add(1, Ordering::Release);
            }

            if let Some(source) = source {
                increment_source(&self.wake_sources, source);
            }
        }

        fn snapshot(&self) -> TaskSnapshot {
            let mut wake_sources = self
                .wake_sources
                .iter()
                .map(|entry| (*entry.key(), entry.value().load(Ordering::Acquire)))
                .collect::<Vec<_>>();
            sort_source_counts(&mut wake_sources);

            TaskSnapshot {
                id: self.id,
                name: self.name.lock().unwrap().clone(),
                location: self.location.lock().unwrap().clone(),
                polls: self.polls.load(Ordering::Acquire),
                wakes: self.wakes.load(Ordering::Acquire),
                self_wakes: self.self_wakes.load(Ordering::Acquire),
                busy_ns: self.busy_ns.load(Ordering::Acquire),
                wake_sources,
                marker: *self.marker.lock().unwrap(),
            }
        }
    }

    impl TaskFields {
        fn name(&self) -> String {
            self.name.clone().unwrap_or_else(|| "unnamed".into())
        }

        fn location(&self) -> Option<String> {
            match (&self.file, self.line, self.column) {
                (Some(file), Some(line), Some(column)) => Some(format!("{file}:{line}:{column}")),
                (Some(file), Some(line), None) => Some(format!("{file}:{line}")),
                _ => None,
            }
        }
    }

    impl Visit for TaskFields {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "task.name" {
                self.name = Some(format!("{value:?}"));
            }
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            match field.name() {
                "task.name" => self.name = Some(value.into()),
                "loc.file" => self.file = Some(value.into()),
                _ => {}
            }
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            match field.name() {
                "loc.line" => self.line = Some(value),
                "loc.col" => self.column = Some(value),
                _ => {}
            }
        }
    }

    impl Visit for WakerFields {
        fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}

        fn record_u64(&mut self, field: &Field, value: u64) {
            if field.name() == "task.id" {
                self.task_id = Some(value);
            }
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "op" {
                self.is_wake = matches!(value, "waker.wake" | "waker.wake_by_ref");
            }
        }
    }

    struct WakeSourceGuard {
        active: bool,
    }

    impl WakeSourceGuard {
        fn new(source: WakeSource) -> Self {
            if !is_enabled() {
                return Self { active: false };
            }

            WAKE_SOURCE_STACK.with(|stack| stack.borrow_mut().push(source));
            Self { active: true }
        }
    }

    impl Drop for WakeSourceGuard {
        fn drop(&mut self) {
            if self.active {
                WAKE_SOURCE_STACK.with(|stack| {
                    stack.borrow_mut().pop();
                });
            }
        }
    }

    fn is_enabled() -> bool {
        ENABLED.load(Ordering::Acquire)
    }

    fn is_task_metadata(metadata: &Metadata<'_>) -> bool {
        matches!(
            (metadata.name(), metadata.target()),
            ("runtime.spawn", _) | ("task", "tokio::task")
        )
    }

    fn is_waker_metadata(metadata: &Metadata<'_>) -> bool {
        matches!(metadata.target(), "runtime::waker" | "tokio::task::waker")
    }

    fn current_wake_source() -> Option<WakeSource> {
        WAKE_SOURCE_STACK.with(|stack| stack.borrow().last().copied())
    }

    fn task_summary_mode() -> TaskSummaryMode {
        TaskSummaryMode::from_u8(TASK_SUMMARY_MODE.load(Ordering::Acquire) as u8)
    }

    fn trace_output_format() -> TraceOutputFormat {
        TraceOutputFormat::from_u8(TRACE_OUTPUT_FORMAT.load(Ordering::Acquire) as u8)
    }

    fn mark_current_task(marker: TaskMarker) {
        let task_id = CURRENT_TASK_STACK.with(|stack| stack.borrow().last().copied());
        let Some(task_id) = task_id else {
            return;
        };

        if let Some(task) = tasks().get(&task_id) {
            task.set_marker(marker);
        }
    }

    fn tasks() -> &'static DashMap<u64, Arc<TaskStats>> {
        TASKS.get_or_init(DashMap::new)
    }

    fn global_wake_sources() -> &'static DashMap<WakeSource, AtomicU64> {
        GLOBAL_WAKE_SOURCES.get_or_init(DashMap::new)
    }

    fn increment_source(sources: &DashMap<WakeSource, AtomicU64>, source: WakeSource) {
        sources
            .entry(source)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Release);
    }

    fn collect_global_source_counts() -> Vec<(WakeSource, u64)> {
        let mut source_counts = global_wake_sources()
            .iter()
            .map(|entry| (*entry.key(), entry.value().load(Ordering::Acquire)))
            .collect::<Vec<_>>();
        sort_source_counts(&mut source_counts);
        source_counts
    }

    fn collect_task_snapshots() -> Vec<TaskSnapshot> {
        tasks()
            .iter()
            .filter_map(|entry| {
                let snapshot = entry.value().snapshot();
                snapshot.marker.is_some().then_some(snapshot)
            })
            .collect()
    }

    fn collect_marker_snapshots() -> Vec<MarkerSnapshot> {
        let mut marker_snapshots = HashMap::<TaskMarker, MarkerSnapshot>::new();
        for task in collect_task_snapshots() {
            let Some(marker) = task.marker else {
                continue;
            };

            let marker_snapshot =
                marker_snapshots
                    .entry(marker)
                    .or_insert_with(|| MarkerSnapshot {
                        marker,
                        tasks: 0,
                        polls: 0,
                        wakes: 0,
                        self_wakes: 0,
                        busy_ns: 0,
                        wake_sources: Vec::new(),
                    });
            marker_snapshot.tasks += 1;
            marker_snapshot.polls += task.polls;
            marker_snapshot.wakes += task.wakes;
            marker_snapshot.self_wakes += task.self_wakes;
            marker_snapshot.busy_ns += task.busy_ns;

            for (source, count) in task.wake_sources {
                if let Some((_, existing_count)) = marker_snapshot
                    .wake_sources
                    .iter_mut()
                    .find(|(existing_source, _)| *existing_source == source)
                {
                    *existing_count += count;
                } else {
                    marker_snapshot.wake_sources.push((source, count));
                }
            }
        }

        let mut snapshots = marker_snapshots.into_values().collect::<Vec<_>>();
        for snapshot in &mut snapshots {
            sort_source_counts(&mut snapshot.wake_sources);
        }
        snapshots
    }

    fn collect_sorted_task_snapshots() -> Vec<TaskSnapshot> {
        let mut tasks = collect_task_snapshots();
        tasks.sort_by(|a, b| {
            b.polls
                .cmp(&a.polls)
                .then_with(|| b.wakes.cmp(&a.wakes))
                .then_with(|| a.id.cmp(&b.id))
        });
        tasks
    }

    fn collect_sorted_marker_snapshots() -> Vec<MarkerSnapshot> {
        let mut markers = collect_marker_snapshots();
        markers.sort_by(|a, b| {
            b.polls
                .cmp(&a.polls)
                .then_with(|| b.wakes.cmp(&a.wakes))
                .then_with(|| a.marker.name.cmp(b.marker.name))
                .then_with(|| a.marker.file.cmp(b.marker.file))
                .then_with(|| a.marker.line.cmp(&b.marker.line))
        });
        markers
    }

    fn source_json(source: WakeSource) -> Value {
        json!({
            "name": source.name,
            "file": source.file,
            "line": source.line,
            "display": source.display(),
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

    fn source_count_json(source: WakeSource, count: u64, total: u64) -> Value {
        json!({
            "source": source_json(source),
            "count": count,
            "percent": percent(count, total),
        })
    }

    fn task_source_count_json(
        source: WakeSource,
        count: u64,
        known_total: u64,
        wakes: u64,
    ) -> Value {
        json!({
            "source": source_json(source),
            "count": count,
            "percent_of_known": percent(count, known_total),
            "percent_of_wakes": percent(count, wakes),
        })
    }

    fn task_snapshot_json(task: TaskSnapshot) -> Value {
        let known_sources = task
            .wake_sources
            .iter()
            .map(|(_, count)| *count)
            .sum::<u64>();
        json!({
            "id": task.id,
            "name": task.name,
            "spawn": task.location,
            "polls": task.polls,
            "wakes": task.wakes,
            "self_wakes": task.self_wakes,
            "busy_ms": task.busy_ns as f64 / 1_000_000.0,
            "marker": task.marker.map(marker_json),
            "wake_sources": task.wake_sources
                .into_iter()
                .map(|(source, count)| {
                    task_source_count_json(source, count, known_sources, task.wakes)
                })
                .collect::<Vec<_>>(),
        })
    }

    fn marker_snapshot_json(marker: MarkerSnapshot) -> Value {
        let known_sources = marker
            .wake_sources
            .iter()
            .map(|(_, count)| *count)
            .sum::<u64>();
        json!({
            "marker": marker_json(marker.marker),
            "tasks": marker.tasks,
            "polls": marker.polls,
            "wakes": marker.wakes,
            "self_wakes": marker.self_wakes,
            "busy_ms": marker.busy_ns as f64 / 1_000_000.0,
            "wake_sources": marker.wake_sources
                .into_iter()
                .map(|(source, count)| {
                    task_source_count_json(source, count, known_sources, marker.wakes)
                })
                .collect::<Vec<_>>(),
        })
    }

    fn sort_source_counts(source_counts: &mut [(WakeSource, u64)]) {
        source_counts.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.name.cmp(b.0.name))
                .then_with(|| a.0.file.cmp(b.0.file))
                .then_with(|| a.0.line.cmp(&b.0.line))
        });
    }

    fn percent(count: u64, total: u64) -> f64 {
        if total == 0 {
            0.0
        } else {
            count as f64 * 100.0 / total as f64
        }
    }
}

#[cfg(not(feature = "tracing"))]
mod imp {
    #[inline(always)]
    pub fn enable() {}

    #[inline(always)]
    pub fn dump_global_summary() -> Option<String> {
        None
    }

    #[inline(always)]
    pub fn set_task_summary_mode(_mode: super::TaskSummaryMode) {}

    #[inline(always)]
    pub fn set_output_format(_format: super::TraceOutputFormat) {}
}

pub use imp::{dump_global_summary, enable, set_output_format, set_task_summary_mode};

#[cfg(feature = "tracing")]
pub use imp::TaskStatsLayer;

#[cfg(feature = "tracing")]
#[track_caller]
pub fn with_wake_source<R>(name: &'static str, f: impl FnOnce() -> R) -> R {
    imp::with_wake_source(WakeSource::new(name, Location::caller()), f)
}

#[cfg(not(feature = "tracing"))]
#[inline(always)]
pub fn with_wake_source<R>(_name: &'static str, f: impl FnOnce() -> R) -> R {
    f()
}

#[cfg(feature = "tracing")]
#[track_caller]
pub fn with_wake_source_future<Fut>(
    name: &'static str,
    future: Fut,
) -> impl std::future::Future<Output = Fut::Output>
where
    Fut: std::future::Future,
{
    let source = WakeSource::new(name, Location::caller());
    imp::with_wake_source_future(source, future)
}

#[cfg(not(feature = "tracing"))]
#[inline(always)]
pub fn with_wake_source_future<Fut>(_name: &'static str, future: Fut) -> Fut
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
