use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::bail;
use ratelimit::Ratelimiter;

use dt_common::{
    error::Error,
    meta::{
        dcl_meta::dcl_data::DclData, ddl_meta::ddl_data::DdlData, dt_data::DtItem,
        dt_queue::DtQueue, row_data::RowData,
    },
    monitor::{counter::Counter, counter_type::CounterType, monitor::Monitor},
};
use dt_connector::Sinker;

#[derive(Default)]
pub struct BaseParallelizer {
    pub popped_data: VecDeque<DtItem>,
    pub monitor: Arc<Monitor>,
    pub rps_limiter: Option<Ratelimiter>,
}

impl BaseParallelizer {
    pub async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        let mut data = Vec::new();
        while let Some(item) = self.popped_data.pop_front() {
            data.push(item);
        }

        let mut record_size_counter = Counter::new(0, 0);
        // ddls and dmls should be drained separately
        while let Ok(item) = self.pop(buffer, &mut record_size_counter).await {
            if data.is_empty()
                || (data[0].get_row_sql_type() == item.get_row_sql_type()
                    && data[0].data_origin_node == item.data_origin_node)
            {
                // merge when sql type is the same
                data.push(item);
            } else {
                self.popped_data.push_back(item);
                break;
            }
        }

        self.update_monitor(&record_size_counter).await;
        Ok(data)
    }

    pub async fn drain_by_count(
        &mut self,
        buffer: &DtQueue,
        max_count: usize,
    ) -> anyhow::Result<Vec<DtItem>> {
        let mut data = Vec::new();
        let mut record_size_counter = Counter::new(0, 0);
        while let Ok(item) = self.pop(buffer, &mut record_size_counter).await {
            data.push(item);
            if data.len() >= max_count {
                break;
            }
        }
        self.update_monitor(&record_size_counter).await;
        Ok(data)
    }

    pub async fn pop(
        &self,
        buffer: &DtQueue,
        record_size_counter: &mut Counter,
    ) -> anyhow::Result<DtItem> {
        // rps limit
        if let Some(rps_limiter) = &self.rps_limiter {
            // refer: https://docs.rs/ratelimit/0.10.0/ratelimit/
            if let Err(_sleep) = rps_limiter.try_wait() {
                bail! {Error::PipelineError(format!(
                    "reach rps limit: {}",
                    rps_limiter.max_tokens(),
                ))};
            }
        }

        match buffer.pop() {
            Ok(item) => {
                // counter
                record_size_counter.add(
                    item.dt_data.get_data_size(),
                    item.dt_data.get_data_count() as u64,
                );
                Ok(item)
            }
            Err(error) => bail! {Error::PipelineError(format!("buffer pop error: {}", error))},
        }
    }

    pub async fn update_monitor(&self, record_size_counter: &Counter) {
        if record_size_counter.value > 0 {
            self.monitor
                .add_batch_counter(
                    CounterType::RecordSize,
                    record_size_counter.value,
                    record_size_counter.count,
                )
                .await;
        }
    }

    pub async fn sink_dml(
        &self,
        mut sub_data_items: Vec<Vec<Arc<RowData>>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..sub_data_items.len() {
            let data = sub_data_items.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            join_set.spawn(async move { sinker.lock().await.sink_dml(data, batch).await });
        }
        while let Some(result) = join_set.join_next().await {
            result??;
        }
        Ok(())
    }

    pub async fn sink_ddl(
        &self,
        mut sub_data_items: Vec<Vec<DdlData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..sub_data_items.len() {
            let data = sub_data_items.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            join_set.spawn(async move { sinker.lock().await.sink_ddl(data, batch).await });
        }
        while let Some(result) = join_set.join_next().await {
            result??;
        }
        Ok(())
    }

    pub async fn sink_dcl(
        &self,
        mut sub_data_items: Vec<Vec<DclData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let mut futures = Vec::new();
        for i in 0..sub_data_items.len() {
            let data = sub_data_items.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            let future =
                tokio::spawn(
                    async move { sinker.lock().await.sink_dcl(data, batch).await.unwrap() },
                );
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }

    pub async fn sink_raw(
        &self,
        mut sub_data_items: Vec<Vec<DtItem>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..sub_data_items.len() {
            let data = sub_data_items.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            join_set.spawn(async move { sinker.lock().await.sink_raw(data, batch).await });
        }
        while let Some(result) = join_set.join_next().await {
            result??;
        }
        Ok(())
    }
}
