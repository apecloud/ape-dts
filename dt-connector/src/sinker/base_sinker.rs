use std::sync::Arc;

use dt_common::{
    monitor::{counter_type::CounterType, monitor::Monitor},
    utils::limit_queue::LimitedQueue,
};

pub struct BaseSinker {}

impl BaseSinker {
    pub async fn update_batch_monitor(
        monitor: &Arc<Monitor>,
        batch_size: u64,
        data_size: u64,
    ) -> anyhow::Result<()> {
        monitor
            .add_counter(CounterType::RecordsPerQuery, batch_size)
            .await
            .add_counter(CounterType::RecordCount, batch_size)
            .await
            .add_counter(CounterType::DataBytes, data_size)
            .await;
        Ok(())
    }

    pub async fn update_serial_monitor(
        monitor: &Arc<Monitor>,
        record_count: u64,
        data_size: u64,
    ) -> anyhow::Result<()> {
        monitor
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
        monitor: &Arc<Monitor>,
        rts: &LimitedQueue<(u64, u64)>,
    ) -> anyhow::Result<()> {
        monitor
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
