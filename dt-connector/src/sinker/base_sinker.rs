use dt_common::{
    config::config_enums::TaskKind,
    meta::row_data::RowData,
    monitor::{counter_type::CounterType, task_monitor::TaskMonitorHandle},
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone, Default)]
pub struct BaseSinker {
    pub monitor: TaskMonitorHandle,
    pub default_task_id: String,
    pub monitor_interval: u64,
}

impl BaseSinker {
    pub fn new(monitor: TaskMonitorHandle, default_task_id: String, monitor_interval: u64) -> Self {
        Self {
            monitor,
            default_task_id,
            monitor_interval,
        }
    }

    pub fn monitor_interval_secs(&self) -> u64 {
        if self.monitor_interval > 0 {
            self.monitor_interval
        } else {
            10
        }
    }

    pub fn is_snapshot_task(&self) -> bool {
        self.monitor
            .task_type()
            .is_some_and(|task_type| task_type.kind == TaskKind::Snapshot)
    }

    pub fn task_id_for_schema_tb(&self, schema: &str, tb: &str) -> String {
        if self.is_snapshot_task() && !schema.is_empty() && !tb.is_empty() {
            return format!("{}.{}", schema, tb);
        }
        self.default_task_id.clone()
    }

    pub fn task_id_for_rows(&self, rows: &[RowData]) -> String {
        if !self.is_snapshot_task() {
            return self.default_task_id.clone();
        }

        let Some(first) = rows.first() else {
            return self.default_task_id.clone();
        };

        // FIXME: pipline promise that rows are from the same table for now.
        self.task_id_for_schema_tb(&first.schema, &first.tb)
    }

    pub fn ensure_monitor_for(&self, task_id: &str) {
        if self.is_snapshot_task() && !task_id.is_empty() && task_id != self.default_task_id {
            self.monitor.ensure_monitor(task_id);
        }
    }

    pub async fn update_batch_monitor(
        &self,
        batch_size: u64,
        data_size: u64,
    ) -> anyhow::Result<()> {
        self.update_batch_monitor_for(&self.default_task_id, batch_size, data_size)
            .await
    }

    pub async fn update_batch_monitor_for(
        &self,
        task_id: &str,
        batch_size: u64,
        data_size: u64,
    ) -> anyhow::Result<()> {
        self.monitor
            .add_counter(task_id, CounterType::RecordsPerQuery, batch_size)
            .await
            .add_counter(task_id, CounterType::RecordCount, batch_size)
            .await
            .add_counter(task_id, CounterType::DataBytes, data_size)
            .await;
        Ok(())
    }

    pub async fn update_serial_monitor(
        &self,
        record_count: u64,
        data_size: u64,
    ) -> anyhow::Result<()> {
        self.update_serial_monitor_for(&self.default_task_id, record_count, data_size)
            .await
    }

    pub async fn update_serial_monitor_for(
        &self,
        task_id: &str,
        record_count: u64,
        data_size: u64,
    ) -> anyhow::Result<()> {
        self.monitor
            .add_batch_counter(
                task_id,
                CounterType::RecordsPerQuery,
                record_count,
                record_count,
            )
            .await
            .add_counter(task_id, CounterType::RecordCount, record_count)
            .await
            .add_counter(task_id, CounterType::SerialWrites, record_count)
            .await
            .add_batch_counter(task_id, CounterType::DataBytes, data_size, record_count)
            .await;
        Ok(())
    }

    pub async fn update_monitor_rt(&self, rts: &LimitedQueue<(u64, u64)>) -> anyhow::Result<()> {
        self.update_monitor_rt_for(&self.default_task_id, rts).await
    }

    pub async fn update_monitor_rt_for(
        &self,
        task_id: &str,
        rts: &LimitedQueue<(u64, u64)>,
    ) -> anyhow::Result<()> {
        self.monitor
            .add_multi_counter(task_id, CounterType::RtPerQuery, rts)
            .await;
        Ok(())
    }
}

#[macro_export(local_inner_macros)]
macro_rules! call_batch_fn {
    ($self:ident, $data:ident, $batch_fn:expr) => {
        let all_count = $data.len();
        let mut sinked_count = 0;

        loop {
            let mut batch_size = $self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            if batch_size == 0 {
                break;
            }

            $batch_fn($self, &mut $data[..], sinked_count, batch_size).await?;
            sinked_count += batch_size;
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! sync_call_batch_fn {
    ($self:ident, $data:ident, $batch_fn:expr) => {
        let all_count = $data.len();
        let mut sinked_count = 0;

        loop {
            let mut batch_size = $self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            if batch_size == 0 {
                break;
            }

            $batch_fn($self, &mut $data[..], sinked_count, batch_size)?;
            sinked_count += batch_size;
        }
    };
}
