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
mod imp {
    use std::{
        cell::RefCell,
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

    use super::WakeSource;

    static ENABLED: AtomicBool = AtomicBool::new(false);
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
    }

    pub fn enable() {
        ENABLED.store(true, Ordering::Release);
    }

    pub fn dump_global_summary() -> Option<String> {
        if !is_enabled() {
            return None;
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

        let mut tasks = collect_task_snapshots();
        if tasks.is_empty() {
            let _ = writeln!(summary, "tokio tasks: none");
            return Some(summary);
        }

        tasks.sort_by(|a, b| {
            b.polls
                .cmp(&a.polls)
                .then_with(|| b.wakes.cmp(&a.wakes))
                .then_with(|| a.id.cmp(&b.id))
        });

        let _ = writeln!(summary, "tokio tasks: total={}", tasks.len());
        for task in tasks {
            let known_task_sources = task
                .wake_sources
                .iter()
                .map(|(_, count)| *count)
                .sum::<u64>();
            let location = task.location.as_deref().unwrap_or("-");
            let _ = writeln!(
                summary,
                "  task={} name={} polls={} wakes={} self_wakes={} busy_ms={:.3} spawn={}",
                task.id,
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

        Some(summary)
    }

    pub fn with_wake_source<R>(source: WakeSource, f: impl FnOnce() -> R) -> R {
        let _guard = WakeSourceGuard::new(source);
        f()
    }

    pub fn with_wake_source_future<Fut>(
        source: WakeSource,
        future: Fut,
    ) -> impl std::future::Future<Output = Fut::Output>
    where
        Fut: std::future::Future,
    {
        async move {
            let _guard = WakeSourceGuard::new(source);
            future.await
        }
    }

    impl TaskStatsLayer {
        pub fn new() -> Self {
            Self
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
            }
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
            .map(|entry| entry.value().snapshot())
            .collect()
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
}

pub use imp::{dump_global_summary, enable};

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
