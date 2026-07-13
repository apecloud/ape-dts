use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{
    sync::{Mutex, RwLock},
    task::yield_now,
    time::{Duration, Instant},
};

use crate::{lua_processor::LuaProcessor, Pipeline};
use dt_common::{
    config::sinker_config::SinkerConfig,
    log_error, log_finished, log_info, log_position, log_warn,
    meta::{
        dcl_meta::dcl_data::DclData,
        ddl_meta::ddl_data::DdlData,
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
        position::Position,
        row_data::RowData,
        syncer::Syncer,
    },
    monitor::{
        counter_type::CounterType, task_metrics::TaskMetricsType, task_monitor::MonitorType,
        task_monitor_handle::TaskMonitorHandle,
    },
};
use dt_connector::{
    checker::CheckerHandle,
    data_marker::DataMarker,
    extractor::resumer::{recorder::Recorder, utils::ResumerUtil},
    Sinker,
};
use dt_parallelizer::{DataSize, Parallelizer};

pub struct BasePipeline {
    pub buffer: Arc<DtQueue>,
    pub parallelizer: Box<dyn Parallelizer + Send + Sync>,
    pub sinker_config: SinkerConfig,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: Arc<AtomicBool>,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub syncer: Arc<Mutex<Syncer>>,
    pub monitor: TaskMonitorHandle,
    pub pending_snapshot_finished: HashMap<String, Position>,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
    pub lua_processor: Option<LuaProcessor>,
    pub recorder: Option<Arc<dyn Recorder + Send + Sync>>,
    pub checker: Option<CheckerHandle>,
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
        for sinker in self.sinkers.iter_mut() {
            sinker.lock().await.close().await?;
        }
        let final_position = {
            let syncer = self.syncer.lock().await;
            Self::checker_close_position(&syncer)
        };
        if let Some(checker) = &mut self.checker {
            if let Err(err) = checker.close_with_position(final_position.as_ref()).await {
                log_warn!("checker close failed: {}", err);
            }
        }
        self.parallelizer.close().await
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
        let mut last_received_position = Position::None;
        let mut last_commit_positions = HashMap::new();
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
                self.parallelizer.drain(self.buffer.as_ref()).await?
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

            if let Some(position) = &last_received {
                self.syncer.lock().await.received_position = position.to_owned();
                last_received_position = position.to_owned();
            }
            for position in last_commits {
                last_commit_positions
                    .insert(ResumerUtil::get_key_from_position(&position), position);
            }

            last_checkpoint_time = self
                .record_checkpoint(
                    Some(last_checkpoint_time),
                    &last_received_position,
                    &last_commit_positions,
                )
                .await?;

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

            yield_now().await;
        }

        self.record_checkpoint(None, &last_received_position, &last_commit_positions)
            .await?;
        self.try_finish_snapshot_tasks().await?;
        Ok(())
    }
}

impl BasePipeline {
    fn checker_close_position(syncer: &Syncer) -> Option<Position> {
        (!matches!(syncer.committed_position, Position::None))
            .then_some(syncer.committed_position.clone())
    }

    async fn sink_raw(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Vec<Position>)> {
        let (data_count, last_received_position, commit_positions) =
            Self::fetch_raw(&all_data, &mut self.pending_snapshot_finished);
        if data_count > 0 {
            let data_size = self.parallelizer.sink_raw(all_data, &self.sinkers).await?;
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
            .sink_struct(data.clone(), &self.sinkers)
            .await?;

        if let Some(checker) = &mut self.checker {
            checker.check_struct(data).await?;
        }

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

        let data_size = self.parallelizer.sink_dml(data, &self.sinkers).await?;
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
                .await?;
            // only part of sinkers will execute sink_ddl, but all sinkers should refresh metadata
            for sinker in self.sinkers.iter_mut() {
                sinker.lock().await.refresh_meta(data.clone()).await?;
            }
            // cdc+check also needs refreshed table metadata after sink ddl changes the target schema
            if let Some(checker) = &self.checker {
                if let Err(err) = checker.refresh_meta(data.clone()).await {
                    log_warn!("checker refresh_meta failed: {}", err);
                }
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
            self.parallelizer.sink_dcl(data, &self.sinkers).await?;
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
            if let Some(checker) = &self.checker {
                if let Err(err) = checker.handle_control_item(&item).await {
                    log_warn!("checker handle_control_item failed: {}", err);
                }
            }
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
        if let Position::RdbSnapshotFinished { schema, tb, .. } = position {
            pending_snapshot_finished.insert(
                TaskMonitorHandle::task_id_from_schema_tb(schema, tb),
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
        last_received_position: &Position,
        last_commit_positions: &HashMap<String, Position>,
    ) -> anyhow::Result<Instant> {
        if let Some(last) = last_checkpoint_time {
            if last.elapsed().as_secs() < self.checkpoint_interval_secs {
                return Ok(last);
            }
        }

        if !matches!(last_received_position, Position::None) {
            // extracting chunks will sink None position.
            log_position!("current_position | {}", last_received_position.to_string());
        }
        let mut commit_positions: Vec<(&String, &Position)> =
            last_commit_positions.iter().collect();
        commit_positions.sort_by(|left, right| left.0.cmp(right.0));
        for (_, position) in commit_positions.iter() {
            log_position!("checkpoint_position | {}", position.to_string());
        }

        let checker_position = commit_positions
            .last()
            .map(|(_, position)| *position)
            .unwrap_or(last_received_position);

        if !matches!(checker_position, Position::None) {
            if let Some(checker) = &self.checker {
                if let Err(err) = checker.record_checkpoint(checker_position).await {
                    log_warn!("checker checkpoint failed: {}", err);
                }
            }
        }
        if let Some(handler) = &self.recorder {
            if commit_positions.is_empty() {
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
            let mut syncer = self.syncer.lock().await;
            syncer.committed_position = checker_position.to_owned();
            if !last_commit_positions.is_empty() {
                syncer.committed_positions = last_commit_positions.clone();
            }
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
    use std::collections::HashMap;

    use dt_common::meta::{
        dt_data::{DtData, DtItem},
        position::Position,
        redis::redis_entry::RedisEntry,
    };
    use dt_connector::extractor::resumer::utils::ResumerUtil;

    use super::BasePipeline;

    fn redis_node_position(node_id: &str, repl_offset: u64) -> Position {
        Position::Redis {
            node_id: Some(node_id.to_string()),
            address: Some(format!("127.0.0.1:{repl_offset}")),
            repl_id: format!("repl-{node_id}"),
            repl_port: 10008,
            repl_offset,
            now_db_id: 0,
            timestamp: String::new(),
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
}
