use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use dt_common::{
    config::sinker_config::SinkerConfig,
    error::{DtResultExt, Stage},
    log_error, log_finished, log_info, log_position, log_warn,
    meta::{
        dcl_meta::dcl_data::DclData,
        ddl_meta::ddl_data::DdlData,
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
        position::{Position, Progress},
        row_data::RowData,
    },
    monitor::{
        counter_type::CounterType, task_metrics::TaskMetricsType, task_monitor::MonitorType,
        task_monitor_handle::TaskMonitorHandle,
    },
    runtime_trace,
};
use dt_connector::{
    data_marker::DataMarker,
    extractor::resumer::{recorder::Recorder, utils::ResumerUtil},
    Sinker,
};
use dt_parallelizer::{DataSize, Parallelizer};
use tokio::{
    sync::{Mutex, RwLock},
    time::{Duration, Instant},
};

use crate::{lua_processor::LuaProcessor, Pipeline};

pub struct BasePipeline {
    pub buffer: Arc<DtQueue>,
    pub parallelizer: Box<dyn Parallelizer + Send + Sync>,
    pub sinker_config: SinkerConfig,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: Arc<AtomicBool>,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub progress: Arc<Mutex<Progress>>,
    pub monitor: TaskMonitorHandle,
    pub pending_snapshot_finished: HashMap<String, Position>,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
    pub lua_processor: Option<LuaProcessor>,
    pub recorder: Option<Arc<dyn Recorder + Send + Sync>>,
    pub propagate_checkpoint_to_sinker: bool,
}

enum SinkMethod {
    Raw,
    Ddl,
    Dcl,
    Dml,
    Struct,
}

#[async_trait]
impl Pipeline for BasePipeline {
    async fn stop(&mut self) -> anyhow::Result<()> {
        let final_position = {
            let progress = self.progress.lock().await;
            Self::final_commit_position(&progress)
        };
        for sinker in self.sinkers.iter_mut() {
            sinker
                .lock()
                .await
                .close_with_position(final_position.as_ref())
                .await?;
        }
        self.parallelizer.close().await.stage(Stage::Parallelizer)
    }

    async fn start(&mut self) -> anyhow::Result<()> {
        log_info!(
            "{} starts, parallel_size: {}, checkpoint_interval_secs: {}",
            self.parallelizer.get_name(),
            self.sinkers.len(),
            self.checkpoint_interval_secs
        );

        let mut last_sink_time = Instant::now();
        let mut last_checkpoint_time = Instant::now();
        let mut record_time = Instant::now();

        loop {
            let shutting_down = self.shut_down.load(Ordering::Acquire);
            let buffer_empty = self.buffer.is_empty();
            let pending_finish_empty = self.pending_snapshot_finished.is_empty();
            let has_pending_work = !buffer_empty || !pending_finish_empty;

            if shutting_down && !has_pending_work {
                break;
            }

            if !has_pending_work {
                self.buffer.wait_for_data(Duration::from_secs(1)).await;
            }

            // to avoid too many sub counters, only add counter when buffer is not empty
            if !self.buffer.is_empty() {
                self.monitor
                    .add_counter(
                        self.monitor.default_task_id(),
                        CounterType::BufferSize,
                        self.buffer.len() as u64,
                    )
                    .await;
            }
            if record_time.elapsed().as_secs() > 1 {
                let len = self.buffer.len() as u64;
                let size = self.buffer.get_curr_size();
                self.monitor.set_counter(
                    self.monitor.default_task_id(),
                    CounterType::QueuedRecordCurrent,
                    len,
                );
                self.monitor.set_counter(
                    self.monitor.default_task_id(),
                    CounterType::QueuedByteCurrent,
                    size,
                );
                record_time = Instant::now();
            }

            // some sinkers need to accumulate data to a big batch and sink
            let data = if last_sink_time.elapsed().as_secs() < self.batch_sink_interval_secs
                && !self.buffer.is_full()
            {
                Vec::new()
            } else {
                last_sink_time = Instant::now();
                self.parallelizer
                    .drain(self.buffer.as_ref())
                    .await
                    .stage(Stage::Parallelizer)?
            };

            if let Some(data_marker) = &mut self.data_marker {
                if !data.is_empty() {
                    data_marker.write().await.data_origin_node = data[0].data_origin_node.clone();
                }
            }

            // process all row_data_items in buffer at a time
            let (data_size, last_received, last_commits) = match self.get_sink_method(&data) {
                SinkMethod::Ddl => self.sink_ddl(data).await?,
                SinkMethod::Dcl => self.sink_dcl(data).await?,
                SinkMethod::Dml => self.sink_dml(data).await?,
                SinkMethod::Raw => self.sink_raw(data).await?,
                SinkMethod::Struct => self.sink_struct(data).await?,
            };

            {
                let mut progress = self.progress.lock().await;
                progress.update(
                    last_received,
                    last_commits
                        .into_iter()
                        .map(|position| (ResumerUtil::get_key_from_position(&position), position)),
                );
            }

            last_checkpoint_time = self.record_checkpoint(Some(last_checkpoint_time)).await?;

            self.monitor
                .add_counter(
                    self.monitor.default_task_id(),
                    CounterType::SinkedRecordTotal,
                    data_size.count,
                )
                .await
                .add_counter(
                    self.monitor.default_task_id(),
                    CounterType::SinkedByteTotal,
                    data_size.bytes,
                )
                .await;

            self.try_finish_snapshot_tasks().await?;

            runtime_trace::instrument_wait("yield_now.pipeline.run", tokio::task::yield_now())
                .await;
        }

        self.record_checkpoint(None).await?;
        self.try_finish_snapshot_tasks().await?;
        Ok(())
    }
}

impl BasePipeline {
    fn final_commit_position(progress: &Progress) -> Option<Position> {
        (!matches!(progress.committed_position, Position::None))
            .then_some(progress.committed_position.clone())
    }

    async fn sink_raw(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let (data_count, last_received_position, commit_positions) =
            Self::fetch_raw(&all_data, &mut self.pending_snapshot_finished);
        if data_count > 0 {
            let data_size = self
                .parallelizer
                .sink_raw(all_data, &self.sinkers)
                .await
                .stage(Stage::Parallelizer)?;
            Ok((data_size, last_received_position, commit_positions))
        } else {
            Ok((
                DataSize::default(),
                last_received_position,
                commit_positions,
            ))
        }
    }

    async fn sink_struct(
        &mut self,
        mut all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let mut data = Vec::new();
        for i in all_data.drain(..) {
            if let DtData::Struct { struct_data } = i.dt_data {
                data.push(struct_data);
            }
        }
        if data.is_empty() {
            return Ok((DataSize::default(), None, Vec::new()));
        }

        let data_size = self
            .parallelizer
            .sink_struct(data, &self.sinkers)
            .await
            .stage(Stage::Parallelizer)?;

        Ok((data_size, None, Vec::new()))
    }

    async fn sink_dml(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let (mut data, last_received_position, last_commit_position) =
            Self::fetch_dml(all_data, &mut self.pending_snapshot_finished);
        let commit_positions = last_commit_position.into_iter().collect();
        if data.is_empty() {
            return Ok((
                DataSize::default(),
                last_received_position,
                commit_positions,
            ));
        }

        // execute lua processor
        if let Some(lua_processor) = &self.lua_processor {
            data = lua_processor.process(data)?;
        }

        let data_size = self
            .parallelizer
            .sink_dml(data, &self.sinkers)
            .await
            .stage(Stage::Parallelizer)?;
        Ok((data_size, last_received_position, commit_positions))
    }

    async fn sink_ddl(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let (data, last_received_position, last_commit_position) =
            Self::fetch_ddl(all_data, &mut self.pending_snapshot_finished);
        let commit_positions: Vec<_> = last_commit_position.clone().into_iter().collect();
        if !data.is_empty() {
            let data_size = self
                .parallelizer
                .sink_ddl(data.clone(), &self.sinkers)
                .await
                .stage(Stage::Parallelizer)?;
            // only part of sinkers will execute sink_ddl, but all sinkers should refresh metadata
            for sinker in self.sinkers.iter_mut() {
                sinker.lock().await.refresh_meta(data.clone()).await?;
            }
            self.monitor
                .add_counter(
                    self.monitor.default_task_id(),
                    CounterType::DDLRecordTotal,
                    data_size.count,
                )
                .await;
            Ok((data_size, last_received_position, commit_positions))
        } else {
            Ok((
                DataSize::default(),
                last_received_position,
                commit_positions,
            ))
        }
    }

    async fn sink_dcl(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let (data, last_received_position, last_commit_position) =
            Self::fetch_dcl(all_data, &mut self.pending_snapshot_finished);
        let commit_positions = last_commit_position.into_iter().collect();
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: 0,
        };
        if data_size.count > 0 {
            self.parallelizer
                .sink_dcl(data, &self.sinkers)
                .await
                .stage(Stage::Parallelizer)?;
        }
        Ok((data_size, last_received_position, commit_positions))
    }

    pub fn fetch_raw(
        data: &[DtItem],
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> (u64, Option<Position>, Vec<Position>) {
        let mut data_count = 0;
        let mut last_received_position = Option::None;
        let mut commit_positions = HashMap::new();
        for i in data.iter() {
            match &i.dt_data {
                DtData::Commit { .. } => {
                    if Self::collect_snapshot_finished(&i.position, pending_snapshot_finished) {
                        continue;
                    }
                    Self::collect_commit_position(&mut commit_positions, &i.position);
                    last_received_position = Some(i.position.clone());
                    continue;
                }
                DtData::Heartbeat {} | DtData::Ddl { .. } => {
                    Self::collect_commit_position(&mut commit_positions, &i.position);
                    last_received_position = Some(i.position.clone());
                    continue;
                }
                DtData::Begin {} => {
                    continue;
                }

                DtData::Redis { .. } => {
                    last_received_position = Some(i.position.clone());
                    Self::collect_commit_position(&mut commit_positions, &i.position);
                    data_count += 1;
                }

                _ => {
                    last_received_position = Some(i.position.clone());
                    data_count += 1;
                }
            }
        }

        let mut commit_positions: Vec<(String, Position)> = commit_positions.into_iter().collect();
        commit_positions.sort_by(|left, right| left.0.cmp(&right.0));
        (
            data_count,
            last_received_position,
            commit_positions
                .into_iter()
                .map(|(_, position)| position)
                .collect(),
        )
    }

    fn collect_commit_position(
        commit_positions: &mut HashMap<String, Position>,
        position: &Position,
    ) {
        if matches!(position, Position::None) {
            return;
        }

        commit_positions.insert(
            ResumerUtil::get_key_from_position(position),
            position.clone(),
        );
    }

    fn fetch_dml(
        mut data: Vec<DtItem>,
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> (Vec<RowData>, Option<Position>, Option<Position>) {
        let mut dml_data = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } => {
                    if Self::collect_snapshot_finished(&i.position, pending_snapshot_finished) {
                        continue;
                    }
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }
                DtData::Heartbeat {} => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Dml { row_data } => {
                    last_received_position = Some(i.position);
                    dml_data.push(row_data);
                }

                _ => {}
            }
        }

        (dml_data, last_received_position, last_commit_position)
    }

    fn fetch_ddl(
        mut data: Vec<DtItem>,
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> (Vec<DdlData>, Option<Position>, Option<Position>) {
        let mut result = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } => {
                    if Self::collect_snapshot_finished(&i.position, pending_snapshot_finished) {
                        continue;
                    }
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }
                DtData::Heartbeat {} => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Ddl { ddl_data } => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    result.push(ddl_data);
                }

                _ => {}
            }
        }

        (result, last_received_position, last_commit_position)
    }

    fn fetch_dcl(
        mut data: Vec<DtItem>,
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> (Vec<DclData>, Option<Position>, Option<Position>) {
        let mut result = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } => {
                    if Self::collect_snapshot_finished(&i.position, pending_snapshot_finished) {
                        continue;
                    }
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                }
                DtData::Heartbeat {} => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                }

                DtData::Dcl { dcl_data } => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    result.push(dcl_data);
                }

                _ => {}
            }
        }

        (result, last_received_position, last_commit_position)
    }

    fn get_sink_method(&self, data: &Vec<DtItem>) -> SinkMethod {
        for i in data {
            match i.dt_data {
                DtData::Struct { .. } => return SinkMethod::Struct,
                DtData::Ddl { .. } => return SinkMethod::Ddl,
                DtData::Dcl { .. } => return SinkMethod::Dcl,
                DtData::Dml { .. } => return SinkMethod::Dml,
                DtData::Redis { .. } => return SinkMethod::Raw,
                DtData::Begin {} | DtData::Commit { .. } | DtData::Heartbeat {} => continue,
            }
        }
        SinkMethod::Raw
    }

    async fn try_finish_snapshot_tasks(&mut self) -> anyhow::Result<()> {
        let finished_task_ids: Vec<String> =
            self.pending_snapshot_finished.keys().cloned().collect();

        for task_id in finished_task_ids {
            let Some(finish_position) = self.pending_snapshot_finished.remove(&task_id) else {
                continue;
            };

            self.handle_snapshot_finished_control_item(&finish_position)
                .await?;

            self.monitor
                .with_type(MonitorType::Sinker)
                .unregister_monitor(&task_id);
            self.monitor
                .add_no_window_metrics(TaskMetricsType::FinishedProgressCount, 1);
            log_finished!("{}", finish_position.to_string());
            if let Some(handler) = &self.recorder {
                if let Err(err) = handler.record_position(&finish_position).await {
                    log_error!(
                        "failed to record finish position: {}, err: {:#}",
                        finish_position,
                        err
                    );
                }
            }
        }

        Ok(())
    }

    async fn handle_snapshot_finished_control_item(
        &mut self,
        finish_position: &Position,
    ) -> anyhow::Result<()> {
        if matches!(finish_position, Position::RdbSnapshotFinished { .. }) {
            // The table's data has already been sunk when the finished position reaches this
            // point. Forward it as a control item so sinkers and checker can react to snapshot
            // lifecycle events.
            let item = DtItem {
                dt_data: DtData::Commit { xid: String::new() },
                position: finish_position.clone(),
                data_origin_node: String::new(),
            };
            for sinker in self.sinkers.iter_mut() {
                sinker.lock().await.handle_control_item(&item).await?;
            }
        }
        Ok(())
    }

    fn collect_snapshot_finished(
        position: &Position,
        pending_snapshot_finished: &mut HashMap<String, Position>,
    ) -> bool {
        if let Position::RdbSnapshotFinished { db, schema, tb, .. } = position {
            pending_snapshot_finished.insert(
                TaskMonitorHandle::task_id_from_db_schema_tb(db, schema, tb),
                position.clone(),
            );
            true
        } else {
            false
        }
    }

    async fn record_checkpoint(
        &self,
        last_checkpoint_time: Option<Instant>,
    ) -> anyhow::Result<Instant> {
        if let Some(last) = last_checkpoint_time {
            if last.elapsed().as_secs() < self.checkpoint_interval_secs {
                return Ok(last);
            }
        }

        let (received_position, committed_position, last_commit_positions) = {
            let progress = self.progress.lock().await;
            (
                progress.received_position.clone(),
                progress.aggregate_committed_position(),
                progress.committed_positions.clone(),
            )
        };
        let last_received_position = &received_position;
        if !matches!(last_received_position, Position::None) {
            // extracting chunks will sink None position.
            log_position!("current_position | {}", last_received_position.to_string());
        }
        let mut commit_positions: Vec<(&String, &Position)> = last_commit_positions
            .iter()
            .filter(|(_, position)| !matches!(position, Position::None))
            .collect();
        commit_positions.sort_by(|left, right| left.0.cmp(right.0));
        for (_, position) in commit_positions.iter() {
            log_position!("checkpoint_position | {}", position.to_string());
        }

        let checker_position = &committed_position;

        if self.propagate_checkpoint_to_sinker && !matches!(checker_position, Position::None) {
            // Lifecycle-aware decorators are attached to the last sinker so their close hook
            // runs after every regular sinker has closed.
            if let Some(sinker) = self.sinkers.last() {
                if let Err(err) = sinker
                    .lock()
                    .await
                    .record_checkpoint(checker_position)
                    .await
                {
                    log_warn!("sinker checkpoint hook failed: {}", err);
                }
            }
        }
        if let Some(handler) = &self.recorder {
            if commit_positions.is_empty() && !matches!(last_received_position, Position::None) {
                if let Err(e) = handler.record_position(last_received_position).await {
                    log_error!(
                        "failed to record position: {}, err: {:#}",
                        last_received_position,
                        e
                    );
                }
            } else {
                for (_, position) in commit_positions.iter() {
                    if let Err(e) = handler.record_position(position).await {
                        log_error!("failed to record position: {}, err: {:#}", position, e);
                    }
                }
            }
        }

        if !matches!(checker_position, Position::None) {
            let mut progress = self.progress.lock().await;
            progress.committed_position = committed_position;
        }

        self.monitor.set_counter(
            self.monitor.default_task_id(),
            CounterType::Timestamp,
            last_received_position.to_timestamp(),
        );

        Ok(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{atomic::AtomicBool, Arc},
    };

    use async_trait::async_trait;
    use dt_common::meta::{
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
        position::{aggregator::redis::RedisPositionAggregator, Position, Progress},
        redis::redis_entry::RedisEntry,
    };
    use dt_connector::{
        extractor::resumer::{recorder::Recorder, utils::ResumerUtil},
        Sinker,
    };
    use dt_parallelizer::{DataSize, Parallelizer};
    use tokio::sync::Mutex;

    use super::BasePipeline;
    use crate::Pipeline;

    fn redis_node_position(node_id: &str, repl_offset: u64) -> Position {
        Position::Redis {
            node_id: Some(node_id.to_string()),
            address: Some(format!("127.0.0.1:{repl_offset}")),
            repl_id: format!("repl-{node_id}"),
            repl_port: 10008,
            repl_offset,
            now_db_id: 0,
            timestamp: Default::default(),
        }
    }

    fn redis_item(position: Position) -> DtItem {
        DtItem {
            dt_data: DtData::Redis {
                entry: RedisEntry::new(),
            },
            position,
            data_origin_node: String::new(),
        }
    }

    #[test]
    fn fetch_raw_collects_latest_position_per_redis_node() {
        let mut pending_snapshot_finished = HashMap::new();
        let node_1_old = redis_node_position("node-1", 10);
        let node_2 = redis_node_position("node-2", 20);
        let node_1_new = redis_node_position("node-1", 30);
        let data = vec![
            redis_item(node_1_old),
            redis_item(node_2.clone()),
            redis_item(node_1_new.clone()),
        ];

        let (_, _, commit_positions) =
            BasePipeline::fetch_raw(&data, &mut pending_snapshot_finished);

        assert_eq!(commit_positions.len(), 2);
        let by_key: HashMap<_, _> = commit_positions
            .into_iter()
            .map(|position| (ResumerUtil::get_key_from_position(&position), position))
            .collect();
        assert_eq!(by_key.get("redis-node-node-1"), Some(&node_1_new));
        assert_eq!(by_key.get("redis-node-node-2"), Some(&node_2));
    }

    struct RawTestParallelizer {
        fail_after: Option<usize>,
        sinks: usize,
        started: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait]
    impl Parallelizer for RawTestParallelizer {
        fn get_name(&self) -> String {
            "RawTestParallelizer".to_string()
        }

        async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
            Ok(vec![buffer.pop().await?])
        }

        async fn sink_raw(
            &mut self,
            _data: Vec<DtItem>,
            _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        ) -> anyhow::Result<DataSize> {
            if self.fail_after == Some(self.sinks) {
                anyhow::bail!("injected sink failure");
            }
            if let Some(started) = &self.started {
                started.notify_one();
                std::future::pending::<()>().await;
            }
            self.sinks += 1;
            Ok(DataSize::default())
        }
    }

    fn raw_test_pipeline(
        progress: Arc<Mutex<Progress>>,
        buffer: Arc<DtQueue>,
        parallelizer: RawTestParallelizer,
    ) -> BasePipeline {
        BasePipeline {
            buffer,
            parallelizer: Box::new(parallelizer),
            sinker_config: dt_common::config::sinker_config::SinkerConfig::Dummy,
            sinkers: Vec::new(),
            shut_down: Arc::new(AtomicBool::new(true)),
            checkpoint_interval_secs: 0,
            batch_sink_interval_secs: 0,
            progress,
            monitor: Default::default(),
            pending_snapshot_finished: HashMap::new(),
            data_marker: None,
            lua_processor: None,
            recorder: None,
            propagate_checkpoint_to_sinker: false,
        }
    }

    fn redis_progress(node_ids: &[&str]) -> Arc<Mutex<Progress>> {
        let mut progress = Progress::new(Box::new(RedisPositionAggregator));
        RedisPositionAggregator::register_nodes(
            &mut progress.committed_positions,
            node_ids.iter().copied(),
        );
        Arc::new(Mutex::new(progress))
    }

    #[tokio::test]
    async fn redis_timestamp_advances_only_after_successful_batches() {
        for fail_after in [None, Some(0), Some(2)] {
            let progress = redis_progress(&["fast", "slow"]);
            let buffer = Arc::new(DtQueue::new(3, 0, None, None));
            for (node_id, millis) in [("fast", 14_000), ("slow", 12_000), ("fast", 15_000)] {
                let mut position = redis_node_position(node_id, millis);
                if let Position::Redis { timestamp, .. } = &mut position {
                    *timestamp = Position::format_timestamp_millis(millis as i64);
                }
                buffer.push(redis_item(position)).await.unwrap();
            }
            let mut pipeline = raw_test_pipeline(
                progress.clone(),
                buffer,
                RawTestParallelizer {
                    fail_after,
                    sinks: 0,
                    started: None,
                },
            );
            let result = pipeline.start().await;
            let progress = progress.lock().await;
            if fail_after.is_some() {
                assert!(format!("{:#}", result.unwrap_err()).contains("injected sink failure"));
                if fail_after == Some(0) {
                    assert!(progress
                        .committed_positions
                        .values()
                        .all(|value| matches!(value, Position::None)));
                    assert_eq!(progress.received_position, Position::None);
                    assert_eq!(progress.committed_positions.len(), 2);
                } else {
                    assert_eq!(progress.received_position.to_timestamp(), 12_000);
                    assert_eq!(
                        progress.committed_positions["redis-node-fast"].to_timestamp(),
                        14_000
                    );
                    let Position::Redis { repl_offset, .. } =
                        &progress.committed_positions["redis-node-fast"]
                    else {
                        panic!("expected node offset");
                    };
                    assert_eq!(*repl_offset, 14_000);
                }
            } else {
                result.unwrap();
                assert_eq!(progress.received_position.to_timestamp(), 12_000);
                assert_eq!(
                    progress.committed_positions["redis-node-fast"].to_timestamp(),
                    15_000
                );
                assert_eq!(progress.committed_positions.len(), 2);
            }
        }
    }

    #[tokio::test]
    async fn redis_timestamp_cancellation_during_sink_does_not_publish_progress() {
        let progress = redis_progress(&["node"]);
        let buffer = Arc::new(DtQueue::new(1, 0, None, None));
        let mut position = redis_node_position("node", 14_000);
        if let Position::Redis { timestamp, .. } = &mut position {
            *timestamp = Position::format_timestamp_millis(14_000);
        }
        buffer.push(redis_item(position)).await.unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let mut pipeline = raw_test_pipeline(
            progress.clone(),
            buffer,
            RawTestParallelizer {
                fail_after: None,
                sinks: 0,
                started: Some(started.clone()),
            },
        );
        let worker = tokio::spawn(async move { pipeline.start().await });
        tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
            .await
            .unwrap();
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        let progress = progress.lock().await;
        assert_eq!(
            progress.committed_positions["redis-node-node"],
            Position::None
        );
        assert_eq!(progress.received_position, Position::None);
        assert_eq!(progress.committed_positions.len(), 1);
    }

    struct RecordingRecorder {
        positions: Arc<Mutex<Vec<Position>>>,
        progress: Arc<Mutex<Progress>>,
    }

    #[async_trait]
    impl Recorder for RecordingRecorder {
        async fn record_position(&self, position: &Position) -> anyhow::Result<()> {
            assert!(
                self.progress.try_lock().is_ok(),
                "progress must be unlocked during checkpoint I/O"
            );
            self.positions.lock().await.push(position.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn redis_checkpoint_skips_unreported_slots_and_keeps_node_offsets() {
        let progress = redis_progress(&["z", "a", "unreported"]);
        let buffer = Arc::new(DtQueue::new(2, 0, None, None));
        let mut expected = Vec::new();
        for (node, offset, millis) in [("z", 100, 14_000), ("a", 200, 12_000)] {
            let mut position = redis_node_position(node, offset);
            if let Position::Redis { timestamp, .. } = &mut position {
                *timestamp = Position::format_timestamp_millis(millis);
            }
            buffer.push(redis_item(position.clone())).await.unwrap();
            expected.push(position);
        }
        expected.reverse();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let mut pipeline = raw_test_pipeline(
            progress.clone(),
            buffer,
            RawTestParallelizer {
                fail_after: None,
                sinks: 0,
                started: None,
            },
        );
        pipeline.checkpoint_interval_secs = 60;
        pipeline.recorder = Some(Arc::new(RecordingRecorder {
            positions: recorded.clone(),
            progress: progress.clone(),
        }));
        pipeline.start().await.unwrap();

        assert_eq!(*recorded.lock().await, expected);
        let progress = progress.lock().await;
        assert_eq!(
            progress.committed_positions["redis-node-unreported"],
            Position::None
        );
        assert_eq!(progress.received_position.to_timestamp(), 0);
        assert_eq!(progress.committed_position.to_timestamp(), 0);
    }
}
