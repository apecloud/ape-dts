use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use tokio::{
    sync::{Mutex, RwLock},
    task::yield_now,
    time::Instant,
};

use crate::{lua_processor::LuaProcessor, Pipeline};
use dt_common::{
    config::sinker_config::SinkerConfig,
    log_error, log_info, log_position,
    meta::{
        dcl_meta::dcl_data::DclData,
        ddl_meta::ddl_data::DdlData,
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
        position::Position,
        row_data::RowData,
        syncer::Syncer,
    },
    monitor::{counter_type::CounterType, monitor::Monitor},
};
use dt_connector::{
    checker::CheckerHandle, data_marker::DataMarker, extractor::resumer::recorder::Recorder, Sinker,
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
    pub monitor: Arc<Monitor>,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
    pub lua_processor: Option<LuaProcessor>,
    pub recorder: Option<Arc<dyn Recorder + Send + Sync>>,
    pub checker: Option<Box<dyn CheckerHandle>>,
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
        if let Some(checker) = &mut self.checker {
            checker.close().await?;
        }
        self.checker.take();
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
        let mut last_commit_position = Position::None;
        let mut record_time = Instant::now();

        while !self.shut_down.load(Ordering::Acquire) || !self.buffer.is_empty() {
            // to avoid too many sub counters, only add counter when buffer is not empty
            if !self.buffer.is_empty() {
                self.monitor
                    .add_counter(CounterType::BufferSize, self.buffer.len() as u64)
                    .await;
            }
            if record_time.elapsed().as_secs() > 1 {
                let len = self.buffer.len() as u64;
                let size = self.buffer.get_curr_size();
                self.monitor
                    .set_counter(CounterType::QueuedRecordCurrent, len);
                self.monitor
                    .set_counter(CounterType::QueuedByteCurrent, size);
                record_time = Instant::now();
            }

            // some sinkers (foxlake) need to accumulate data to a big batch and sink
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
            let (data_size, last_received, last_commit) = match self.get_sink_method(&data) {
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
            if let Some(position) = &last_commit {
                last_commit_position = position.to_owned();
            }

            last_checkpoint_time = self
                .record_checkpoint(
                    Some(last_checkpoint_time),
                    &last_received_position,
                    &last_commit_position,
                )
                .await;

            self.monitor
                .add_counter(CounterType::SinkedRecordTotal, data_size.count)
                .await
                .add_counter(CounterType::SinkedByteTotal, data_size.bytes)
                .await;

            yield_now().await;
        }

        self.record_checkpoint(None, &last_received_position, &last_commit_position)
            .await;
        Ok(())
    }
}

impl BasePipeline {
    async fn sink_raw(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Option<Position>)> {
        let (data_count, last_received_position, last_commit_position) = Self::fetch_raw(&all_data);
        if data_count > 0 {
            let data_size = self.parallelizer.sink_raw(all_data, &self.sinkers).await?;
            Ok((data_size, last_received_position, last_commit_position))
        } else {
            Ok((
                DataSize::default(),
                last_received_position,
                last_commit_position,
            ))
        }
    }

    async fn sink_struct(
        &mut self,
        mut all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Option<Position>)> {
        let mut data = Vec::new();
        for i in all_data.drain(..) {
            if let DtData::Struct { struct_data } = i.dt_data {
                data.push(struct_data);
            }
        }
        let data_size = self.parallelizer.sink_struct(data, &self.sinkers).await?;
        Ok((data_size, None, None))
    }

    async fn sink_dml(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Option<Position>)> {
        let (mut data, last_received_position, last_commit_position) = Self::fetch_dml(all_data);
        if data.is_empty() {
            return Ok((
                DataSize::default(),
                last_received_position,
                last_commit_position,
            ));
        }

        // execute lua processor
        if let Some(lua_processor) = &self.lua_processor {
            data = lua_processor.process(data)?;
        }

        let data_for_check = if self.checker.is_some() {
            Some(data.clone())
        } else {
            None
        };
        let data_size = self.parallelizer.sink_dml(data, &self.sinkers).await?;
        if let (Some(checker), Some(data_for_check)) = (&self.checker, data_for_check) {
            checker.check(data_for_check).await?;
        }
        Ok((data_size, last_received_position, last_commit_position))
    }

    async fn sink_ddl(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Option<Position>)> {
        let (data, last_received_position, last_commit_position) = Self::fetch_ddl(all_data);
        if !data.is_empty() {
            let data_size = self
                .parallelizer
                .sink_ddl(data.clone(), &self.sinkers)
                .await?;
            // only part of sinkers will execute sink_ddl, but all sinkers should refresh metadata
            for sinker in self.sinkers.iter_mut() {
                sinker.lock().await.refresh_meta(data.clone()).await?;
            }
            self.monitor
                .add_counter(CounterType::DDLRecordTotal, data_size.count)
                .await;
            Ok((data_size, last_received_position, last_commit_position))
        } else {
            Ok((
                DataSize::default(),
                last_received_position,
                last_commit_position,
            ))
        }
    }

    async fn sink_dcl(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(DataSize, Option<Position>, Option<Position>)> {
        let (data, last_received_position, last_commit_position) = Self::fetch_dcl(all_data);
        let data_size = DataSize {
            count: data.len() as u64,
            bytes: 0,
        };
        if data_size.count > 0 {
            self.parallelizer.sink_dcl(data, &self.sinkers).await?;
        }
        Ok((data_size, last_received_position, last_commit_position))
    }

    pub fn fetch_raw(data: &[DtItem]) -> (u64, Option<Position>, Option<Position>) {
        let mut data_count = 0;
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.iter() {
            match &i.dt_data {
                DtData::Commit { .. } | DtData::Heartbeat {} | DtData::Ddl { .. } => {
                    last_commit_position = Some(i.position.clone());
                    last_received_position = last_commit_position.clone();
                    continue;
                }
                DtData::Begin {} => {
                    continue;
                }

                DtData::Redis { .. } => {
                    last_received_position = Some(i.position.clone());
                    last_commit_position = last_received_position.clone();
                    data_count += 1;
                }

                _ => {
                    last_received_position = Some(i.position.clone());
                    data_count += 1;
                }
            }
        }

        (data_count, last_received_position, last_commit_position)
    }

    fn fetch_dml(mut data: Vec<DtItem>) -> (Vec<Arc<RowData>>, Option<Position>, Option<Position>) {
        let mut dml_data = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } | DtData::Heartbeat {} => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Dml { row_data } => {
                    last_received_position = Some(i.position);
                    dml_data.push(Arc::new(row_data));
                }

                _ => {}
            }
        }

        (dml_data, last_received_position, last_commit_position)
    }

    fn fetch_ddl(mut data: Vec<DtItem>) -> (Vec<DdlData>, Option<Position>, Option<Position>) {
        let mut result = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } | DtData::Heartbeat {} => {
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

    fn fetch_dcl(mut data: Vec<DtItem>) -> (Vec<DclData>, Option<Position>, Option<Position>) {
        let mut result = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } | DtData::Heartbeat {} => {
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
                DtData::Dml { .. } => match self.sinker_config {
                    SinkerConfig::FoxlakePush { .. }
                    | SinkerConfig::FoxlakeMerge { .. }
                    | SinkerConfig::Foxlake { .. }
                    | SinkerConfig::Redis { .. } => return SinkMethod::Raw,
                    _ => return SinkMethod::Dml,
                },
                DtData::Redis { .. } | DtData::Foxlake { .. } => return SinkMethod::Raw,
                DtData::Begin {} | DtData::Commit { .. } | DtData::Heartbeat {} => {
                    continue;
                }
            }
        }
        SinkMethod::Raw
    }

    async fn record_checkpoint(
        &self,
        last_checkpoint_time: Option<Instant>,
        last_received_position: &Position,
        last_commit_position: &Position,
    ) -> Instant {
        if let Some(last) = last_checkpoint_time {
            if last.elapsed().as_secs() < self.checkpoint_interval_secs {
                return last;
            }
        }

        log_position!("current_position | {}", last_received_position.to_string());
        log_position!("checkpoint_position | {}", last_commit_position.to_string());

        // record position for recovery if necessary
        if let Some(handler) = &self.recorder {
            let record_position = if matches!(last_commit_position, Position::None) {
                last_received_position
            } else {
                last_commit_position
            };
            if let Err(e) = handler.record_position(record_position).await {
                log_error!("failed to record position: {}, err: {}", record_position, e);
            }
        }

        if !matches!(last_commit_position, Position::None) {
            self.syncer.lock().await.committed_position = last_commit_position.to_owned();
        }

        self.monitor.set_counter(
            CounterType::Timestamp,
            last_received_position.to_timestamp(),
        );

        Instant::now()
    }
}
