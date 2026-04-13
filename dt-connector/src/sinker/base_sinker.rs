use std::sync::Arc;

use dt_common::{
    monitor::{counter_type::CounterType, monitor::Monitor},
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct BaseSinker {
    pub monitor: Arc<Monitor>,
    pub monitor_interval: u64,
}

impl BaseSinker {
    pub fn new(monitor: Arc<Monitor>, monitor_interval: u64) -> Self {
        Self { monitor, monitor_interval }
    }

    pub fn monitor_interval_secs(&self) -> u64 {
        if self.monitor_interval > 0 {
            self.monitor_interval
        } else {
            10
        }
    }

    pub async fn update_batch_monitor(
        &self,
        batch_size: u64,
        data_size: u64,
    ) -> anyhow::Result<()> {
        self.monitor
            .add_counter(CounterType::RecordsPerQuery, batch_size)
            .await
            .add_counter(CounterType::RecordCount, batch_size)
            .await
            .add_counter(CounterType::DataBytes, data_size)
            .await;
        Ok(())
    }

    pub async fn update_serial_monitor(
        &self,
        record_count: u64,
        data_size: u64,
    ) -> anyhow::Result<()> {
        self.monitor
            .add_batch_counter(CounterType::RecordsPerQuery, record_count, record_count)
            .await
            .add_counter(CounterType::RecordCount, record_count)
            .await
            .add_counter(CounterType::SerialWrites, record_count)
            .await
            .add_batch_counter(CounterType::DataBytes, data_size, record_count)
            .await;
        Ok(())
    }

    pub async fn update_monitor_rt(
        &self,
        rts: &LimitedQueue<(u64, u64)>,
    ) -> anyhow::Result<()> {
        self.monitor
            .add_multi_counter(CounterType::RtPerQuery, rts)
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

            $batch_fn($self, &mut $data, sinked_count, batch_size).await?;
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

            $batch_fn($self, &mut $data, sinked_count, batch_size)?;
            sinked_count += batch_size;
        }
    };
}
