use std::{collections::VecDeque, future::Future, sync::Arc};

use anyhow::bail;

use dt_common::{
    error::Error,
    meta::{
        dcl_meta::dcl_data::DclData, ddl_meta::ddl_data::DdlData, dt_data::DtItem,
        dt_queue::DtQueue, row_data::RowData,
    },
    monitor::{
        counter::Counter, counter_type::CounterType, task_monitor_handle::TaskMonitorHandle,
    },
};
use dt_connector::Sinker;
use tokio::task::JoinSet;

type SharedSinker = Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>;

#[derive(Default)]
pub struct BaseParallelizer {
    pub popped_data: VecDeque<DtItem>,
    pub monitor: TaskMonitorHandle,
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
        match buffer.pop().await {
            Ok(item) => {
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
                    self.monitor.default_task_id(),
                    CounterType::RecordSize,
                    record_size_counter.value,
                    record_size_counter.count,
                )
                .await;
        }
    }

    pub async fn sink_dml(
        &self,
        sub_data_items: Vec<Vec<RowData>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        self.sink_by_available_sinker(
            sub_data_items,
            sinkers,
            parallel_size,
            move |sinker, data| async move { sinker.lock().await.sink_dml(data, batch).await },
        )
        .await
    }

    pub async fn sink_ddl(
        &self,
        sub_data_items: Vec<Vec<DdlData>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        self.sink_by_available_sinker(
            sub_data_items,
            sinkers,
            parallel_size,
            move |sinker, data| async move { sinker.lock().await.sink_ddl(data, batch).await },
        )
        .await
    }

    pub async fn sink_dcl(
        &self,
        sub_data_items: Vec<Vec<DclData>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        self.sink_by_available_sinker(
            sub_data_items,
            sinkers,
            parallel_size,
            move |sinker, data| async move { sinker.lock().await.sink_dcl(data, batch).await },
        )
        .await
    }

    pub async fn sink_raw(
        &self,
        sub_data_items: Vec<Vec<DtItem>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        self.sink_by_available_sinker(
            sub_data_items,
            sinkers,
            parallel_size,
            move |sinker, data| async move { sinker.lock().await.sink_raw(data, batch).await },
        )
        .await
    }

    async fn sink_by_available_sinker<T, Run, Fut>(
        &self,
        sub_data_items: Vec<Vec<T>>,
        sinkers: &[SharedSinker],
        parallel_size: usize,
        run: Run,
    ) -> anyhow::Result<()>
    where
        T: Send + 'static,
        Run: Fn(SharedSinker, Vec<T>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        if sub_data_items.is_empty() {
            return Ok(());
        }
        if parallel_size < 1 {
            bail!("parallel_size must be greater than 0");
        }
        if sinkers.is_empty() {
            bail!("sinkers must not be empty");
        }

        let mut pending = sub_data_items.into_iter();
        let active_sinkers = parallel_size.min(sinkers.len());
        let mut join_set = JoinSet::new();
        let spawn_sink_task = |join_set: &mut JoinSet<anyhow::Result<usize>>,
                               sinker_index: usize,
                               sinker: SharedSinker,
                               data: Vec<T>,
                               run: Run| {
            join_set.spawn(async move {
                run(sinker, data).await?;
                Ok(sinker_index)
            });
        };

        for (sinker_index, sinker) in sinkers.iter().enumerate().take(active_sinkers) {
            let Some(data) = pending.next() else {
                break;
            };
            spawn_sink_task(
                &mut join_set,
                sinker_index,
                sinker.clone(),
                data,
                run.clone(),
            );
        }

        while let Some(result) = join_set.join_next().await {
            let sinker_index = result??;
            if let Some(data) = pending.next() {
                spawn_sink_task(
                    &mut join_set,
                    sinker_index,
                    sinkers[sinker_index].clone(),
                    data,
                    run.clone(),
                );
            }
        }

        Ok(())
    }
}
