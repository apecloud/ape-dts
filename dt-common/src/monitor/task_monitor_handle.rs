use std::sync::Arc;

use crate::{
    config::config_enums::{TaskKind, TaskType},
    meta::{
        ddl_meta::ddl_data::DdlData, dt_data::DtData, row_data::RowData,
        struct_meta::struct_data::StructData,
    },
    monitor::{
        counter_type::CounterType,
        monitor::Monitor,
        task_metrics::TaskMetricsType,
        task_monitor::{MonitorType, TaskMonitor},
    },
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct TaskMonitorHandle {
    task_monitor: Option<Arc<TaskMonitor>>,
    monitor_type: MonitorType,
    // fallback task id used when the caller does not provide a more specific task id.
    default_task_id: String,
    time_window_secs: u64,
    max_sub_count: u64,
    count_window: u64,
}

impl TaskMonitorHandle {
    pub fn new(
        task_monitor: Arc<TaskMonitor>,
        monitor_type: MonitorType,
        default_task_id: String,
        time_window_secs: u64,
        max_sub_count: u64,
        count_window: u64,
    ) -> Self {
        Self {
            task_monitor: Some(task_monitor),
            monitor_type,
            default_task_id,
            time_window_secs,
            max_sub_count,
            count_window,
        }
    }

    pub fn noop(monitor_type: MonitorType) -> Self {
        Self {
            task_monitor: None,
            monitor_type,
            default_task_id: String::new(),
            time_window_secs: 0,
            max_sub_count: 0,
            count_window: 0,
        }
    }

    pub fn with_type(&self, monitor_type: MonitorType) -> Self {
        Self {
            task_monitor: self.task_monitor.clone(),
            monitor_type,
            default_task_id: self.default_task_id.clone(),
            time_window_secs: self.time_window_secs,
            max_sub_count: self.max_sub_count,
            count_window: self.count_window,
        }
    }

    pub fn task_type(&self) -> Option<TaskType> {
        self.task_monitor
            .as_ref()
            .and_then(|task_monitor| task_monitor.get_task_type().copied())
    }

    pub fn is_snapshot_task(&self) -> bool {
        self.task_type()
            .is_some_and(|task_type| task_type.kind == TaskKind::Snapshot)
    }

    pub fn task_id_from_schema_tb(schema: &str, tb: &str) -> String {
        if schema.is_empty() || tb.is_empty() {
            String::new()
        } else {
            format!("{}.{}", schema, tb)
        }
    }

    pub fn task_id_from_row_data(row_data: &RowData) -> String {
        Self::task_id_from_schema_tb(&row_data.schema, &row_data.tb)
    }

    pub fn task_id_from_ddl_data(ddl_data: &DdlData) -> String {
        let (schema, tb) = ddl_data.get_schema_tb();
        Self::task_id_from_schema_tb(&schema, &tb)
    }

    pub fn task_id_from_struct_data(struct_data: &StructData) -> String {
        struct_data.schema.clone()
    }

    pub fn task_id_from_dt_data(dt_data: &DtData) -> String {
        match dt_data {
            DtData::Dml { row_data } => Self::task_id_from_row_data(row_data),
            DtData::Ddl { ddl_data } => Self::task_id_from_ddl_data(ddl_data),
            DtData::Struct { struct_data } => Self::task_id_from_struct_data(struct_data),
            _ => String::new(),
        }
    }

    pub fn task_id_for_schema_tb(&self, schema: &str, tb: &str) -> String {
        if self.is_snapshot_task() {
            let task_id = Self::task_id_from_schema_tb(schema, tb);
            if !task_id.is_empty() {
                return task_id;
            }
        }
        self.default_task_id.clone()
    }

    pub fn task_id_for_rows(&self, rows: &[RowData]) -> String {
        if !self.is_snapshot_task() {
            return self.default_task_id.clone();
        }

        // FIXME: pipeline promises to keep rows of the same table together for now.
        let Some(first) = rows.first() else {
            return self.default_task_id.clone();
        };

        self.task_id_for_schema_tb(&first.schema, &first.tb)
    }

    pub fn ensure_snapshot_monitor(&self, task_id: &str) {
        if self.is_snapshot_task() && !task_id.is_empty() && task_id != self.default_task_id {
            self.ensure_monitor(task_id);
        }
    }

    pub fn default_task_id(&self) -> &str {
        &self.default_task_id
    }

    pub async fn add_counter(&self, task_id: &str, counter_type: CounterType, value: u64) -> &Self {
        if let Some(task_monitor) = &self.task_monitor {
            task_monitor
                .add_counter(task_id, self.monitor_type.clone(), counter_type, value)
                .await;
        }
        self
    }

    pub fn set_counter(&self, task_id: &str, counter_type: CounterType, value: u64) -> &Self {
        if let Some(task_monitor) = &self.task_monitor {
            task_monitor.set_counter(task_id, self.monitor_type.clone(), counter_type, value);
        }
        self
    }

    pub async fn add_batch_counter(
        &self,
        task_id: &str,
        counter_type: CounterType,
        value: u64,
        count: u64,
    ) -> &Self {
        if let Some(task_monitor) = &self.task_monitor {
            task_monitor
                .add_batch_counter(
                    task_id,
                    self.monitor_type.clone(),
                    counter_type,
                    value,
                    count,
                )
                .await;
        }
        self
    }

    pub async fn add_multi_counter(
        &self,
        task_id: &str,
        counter_type: CounterType,
        entry: &LimitedQueue<(u64, u64)>,
    ) -> &Self {
        if let Some(task_monitor) = &self.task_monitor {
            task_monitor
                .add_multi_counter(task_id, self.monitor_type.clone(), counter_type, entry)
                .await;
        }
        self
    }

    pub fn add_no_window_metrics(&self, metrics_type: TaskMetricsType, value: u64) {
        if let Some(task_monitor) = &self.task_monitor {
            task_monitor.add_no_window_metrics(metrics_type, value);
        }
    }

    pub fn build_monitor(&self, name: &str, task_id: &str) -> Arc<Monitor> {
        Arc::new(Monitor::new(
            name,
            task_id,
            self.time_window_secs,
            self.max_sub_count,
            self.count_window,
        ))
    }

    pub fn ensure_monitor(&self, task_id: &str) {
        if let Some(task_monitor) = &self.task_monitor {
            task_monitor.ensure_monitor(
                task_id,
                self.monitor_type.clone(),
                self.time_window_secs,
                self.max_sub_count,
                self.count_window,
            );
        }
    }

    pub fn register_monitor(&self, task_id: &str, monitor: Arc<Monitor>) {
        if let Some(task_monitor) = &self.task_monitor {
            task_monitor.register(task_id, vec![(self.monitor_type.clone(), monitor)]);
        }
    }

    pub fn unregister_monitor(&self, task_id: &str) {
        if let Some(task_monitor) = &self.task_monitor {
            task_monitor.unregister(task_id, vec![self.monitor_type.clone()]);
        }
    }

    pub fn time_window_secs(&self) -> u64 {
        self.time_window_secs
    }

    pub fn count_window(&self) -> u64 {
        self.count_window
    }
}

impl Default for TaskMonitorHandle {
    fn default() -> Self {
        Self::noop(MonitorType::Pipeline)
    }
}
