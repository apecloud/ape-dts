use std::{collections::VecDeque, future::Future, sync::Arc};

use anyhow::{anyhow, bail};
use tokio::task::JoinSet;

use dt_common::{
    config::config_enums::RdbParallelType,
    monitor::{monitor_task_id, task_monitor::TaskMonitorHandle},
};

use super::{
    base_extractor::ExtractState,
    extractor_monitor::{ExtractorCounters, ExtractorMonitor},
};

pub struct TableMonitorGuard {
    handle: TaskMonitorHandle,
    task_id: String,
}

impl Drop for TableMonitorGuard {
    fn drop(&mut self) {
        self.handle.unregister_monitor(&self.task_id);
    }
}

pub struct SnapshotDispatcher;

impl SnapshotDispatcher {
    pub async fn dispatch_work_source<
        State,
        Work,
        WorkResult,
        NextWork,
        NextWorkFut,
        Run,
        RunFut,
        OnDone,
        OnDoneFut,
    >(
        mut state: State,
        parallel_size: usize,
        worker_name: &'static str,
        mut next_work: NextWork,
        run: Run,
        mut on_done: OnDone,
    ) -> anyhow::Result<State>
    where
        Work: Send + 'static,
        WorkResult: Send + 'static,
        NextWork: FnMut(State) -> NextWorkFut,
        NextWorkFut: Future<Output = anyhow::Result<(State, Option<Work>)>>,
        Run: Fn(Work) -> RunFut + Send + Sync + 'static,
        RunFut: Future<Output = anyhow::Result<WorkResult>> + Send + 'static,
        OnDone: FnMut(State, WorkResult) -> OnDoneFut,
        OnDoneFut: Future<Output = anyhow::Result<State>>,
    {
        if parallel_size < 1 {
            bail!("parallel_size must be greater than 0");
        }
        let run = Arc::new(run);
        let mut join_set = JoinSet::new();

        while join_set.len() < parallel_size {
            let (next_state, work) = next_work(state).await?;
            state = next_state;
            let Some(work) = work else {
                break;
            };
            let run_worker = Arc::clone(&run);
            join_set.spawn(async move { run_worker(work).await });
        }

        while let Some(result) = join_set.join_next().await {
            let result = result.map_err(|e| anyhow!("{} join error: {}", worker_name, e))??;
            state = on_done(state, result).await?;

            while join_set.len() < parallel_size {
                let (next_state, work) = next_work(state).await?;
                state = next_state;
                let Some(work) = work else {
                    break;
                };
                let run_worker = Arc::clone(&run);
                join_set.spawn(async move { run_worker(work).await });
            }
        }

        Ok(state)
    }

    pub async fn dispatch_tables<TableId, Run, Fut>(
        tables: Vec<TableId>,
        parallel_type: RdbParallelType,
        parallel_size: usize,
        worker_name: &'static str,
        run: Run,
    ) -> anyhow::Result<()>
    where
        TableId: Send + 'static,
        Run: Fn(TableId) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        if parallel_size < 1 {
            bail!("parallel_size must be greater than 0");
        }
        let run = Arc::new(run);

        match parallel_type {
            RdbParallelType::Table => {
                Self::dispatch_work_source(
                    tables.into_iter().collect::<VecDeque<_>>(),
                    parallel_size,
                    worker_name,
                    |mut tables: VecDeque<TableId>| async move {
                        let work = tables.pop_front();
                        Ok((tables, work))
                    },
                    {
                        let run = Arc::clone(&run);
                        move |table_id| {
                            let run = Arc::clone(&run);
                            async move { run(table_id).await }
                        }
                    },
                    |tables, _| async move { Ok(tables) },
                )
                .await?;
            }

            RdbParallelType::Chunk => {
                for table_id in tables {
                    Arc::clone(&run)(table_id).await?;
                }
            }
        }

        Ok(())
    }

    pub fn clone_extract_state(extract_state: &ExtractState) -> ExtractState {
        ExtractState {
            monitor: ExtractorMonitor {
                monitor: extract_state.monitor.monitor.clone(),
                default_task_id: extract_state.monitor.default_task_id.clone(),
                count_window: extract_state.monitor.count_window,
                time_window_secs: extract_state.monitor.time_window_secs,
                last_flush_time: tokio::time::Instant::now(),
                flushed_counters: ExtractorCounters::default(),
                counters: ExtractorCounters::default(),
            },
            data_marker: extract_state.data_marker.clone(),
            time_filter: extract_state.time_filter.clone(),
        }
    }

    pub async fn derive_table_extract_state(
        extract_state: &ExtractState,
        schema: &str,
        tb: &str,
    ) -> (ExtractState, TableMonitorGuard) {
        let task_id = monitor_task_id::from_schema_tb(schema, tb);
        let monitor_handle = extract_state.monitor.monitor.clone();
        let monitor = monitor_handle.build_monitor("extractor", &task_id);
        monitor_handle.register_monitor(&task_id, monitor);
        let guard = TableMonitorGuard {
            handle: monitor_handle.clone(),
            task_id: task_id.clone(),
        };
        let extractor_monitor = ExtractorMonitor::new(monitor_handle, task_id).await;
        let data_marker = extract_state.data_marker.clone();
        let table_state = extract_state
            .derive_for_table(extractor_monitor, data_marker)
            .await;

        (table_state, guard)
    }
}
