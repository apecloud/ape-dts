use std::{
    cmp,
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::bail;
use async_recursion::async_recursion;
use async_trait::async_trait;
use sqlx::{mysql::MySqlArguments, query::Query, MySql, Pool};
use tokio::{sync::Mutex, time::Instant};

use crate::{
    extractor::{
        base_extractor::BaseExtractor, mysql::binlog_util::BinlogUtil, resumer::recovery::Recovery,
    },
    Extractor,
};
use dt_common::{
    config::config_enums::DbType,
    error::Error,
    log_debug, log_error, log_info, log_warn,
    meta::{
        adaptor::mysql_col_value_convertor::MysqlColValueConvertor, col_value::ColValue,
        dt_data::DtData, mysql::mysql_meta_manager::MysqlMetaManager, position::Position,
        row_data::RowData, row_type::RowType, syncer::Syncer,
    },
    rdb_filter::RdbFilter,
    utils::time_util::TimeUtil,
};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    command::gtid_set::GtidSet,
    event::{
        event_data::EventData, event_header::EventHeader, query_event::QueryEvent,
        row_event::RowEvent, table_map_event::TableMapEvent,
    },
};

pub struct MysqlCdcExtractor {
    pub base_extractor: BaseExtractor,
    pub meta_manager: MysqlMetaManager,
    pub conn_pool: Pool<MySql>,
    pub filter: RdbFilter,
    pub url: String,
    pub binlog_filename: String,
    pub binlog_position: u32,
    pub server_id: u64,
    pub gtid_enabled: bool,
    pub gtid_set: String,
    pub binlog_heartbeat_interval_secs: u64,
    pub binlog_timeout_secs: u64,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_tb: String,
    pub syncer: Arc<Mutex<Syncer>>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

struct Context {
    binlog_filename: String,
    table_map_event_map: HashMap<u64, TableMapEvent>,
    gtid_set: Option<GtidSet>,
}

const QUERY_BEGIN: &str = "BEGIN";

#[async_trait]
impl Extractor for MysqlCdcExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if self.base_extractor.time_filter.start_timestamp > 0 {
            self.binlog_filename = BinlogUtil::find_last_binlog_before_timestamp(
                self.base_extractor.time_filter.start_timestamp,
                &self.url,
                self.server_id,
                &self.conn_pool,
            )
            .await?;
        }

        if let Some(recovery) = &self.recovery {
            if let Some(position) = recovery.get_cdc_resume_position().await {
                match &position {
                    Position::MysqlCdc {
                        binlog_filename,
                        next_event_position,
                        gtid_set,
                        ..
                    } => {
                        self.binlog_filename = binlog_filename.to_owned();
                        self.binlog_position = next_event_position.to_owned();
                        self.gtid_set = gtid_set.to_owned();
                        log_info!(
                            "cdc recovery from binlogfile:[{}], binlog_position:[{}], gtid_set:[{}]",
                            binlog_filename,
                            next_event_position,
                            gtid_set
                        );
                        self.base_extractor
                            .push_dt_data(DtData::Heartbeat {}, position)
                            .await?;
                    }
                    _ => {
                        log_warn!("position:{} is not a valid mysql cdc position", position);
                    }
                }
            }
        }

        log_info!(
            "MysqlCdcExtractor starts, binlog_filename: {}, binlog_position: {}, gtid_enabled: {}, gtid_set: {}, heartbeat_interval_secs: {}, heartbeat_tb: {}",
            self.binlog_filename,
            self.binlog_position,
            self.gtid_enabled,
            self.gtid_set,
            self.heartbeat_interval_secs,
            self.heartbeat_tb
        );
        self.extract_internal().await?;
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.meta_manager.close().await
    }
}

impl MysqlCdcExtractor {
    async fn extract_internal(&mut self) -> anyhow::Result<()> {
        let mut client = BinlogClient {
            url: self.url.clone(),
            binlog_filename: self.binlog_filename.clone(),
            binlog_position: self.binlog_position,
            server_id: self.server_id,
            gtid_enabled: self.gtid_enabled,
            gtid_set: self.gtid_set.clone(),
            heartbeat_interval_secs: self.binlog_heartbeat_interval_secs,
            timeout_secs: self.binlog_timeout_secs,
        };
        let mut stream = client.connect().await?;

        let mut ctx = Context {
            binlog_filename: self.binlog_filename.clone(),
            table_map_event_map: HashMap::new(),
            gtid_set: None,
        };
        if self.gtid_enabled {
            ctx.gtid_set = Some(GtidSet::new(&client.gtid_set)?);
        }

        // start heartbeat
        self.start_heartbeat(self.base_extractor.shut_down.clone())?;

        loop {
            if self.base_extractor.time_filter.ended {
                stream.close().await?;
                return Ok(());
            }

            let (header, data) = stream.read().await?;
            match data {
                EventData::Rotate(r) => {
                    ctx.binlog_filename = r.binlog_filename;
                }

                _ => self.parse_events(header, data, &mut ctx).await?,
            }
        }
    }

    #[async_recursion]
    async fn parse_events(
        &mut self,
        header: EventHeader,
        data: EventData,
        ctx: &mut Context,
    ) -> anyhow::Result<()> {
        log_debug!(
            "received binlog, event header: {:?}, event data: {:?}",
            header,
            data
        );

        // TODO, get server_id from source mysql
        let server_id = String::new();
        let timestamp = Position::format_timestamp_millis(header.timestamp as i64 * 1000);
        let mut gtid_set_str = String::new();
        if let Some(gtid_set) = &ctx.gtid_set {
            gtid_set_str = gtid_set.to_string();
        }
        let position = Position::MysqlCdc {
            server_id,
            binlog_filename: ctx.binlog_filename.clone(),
            next_event_position: header.next_event_position,
            gtid_set: gtid_set_str,
            timestamp,
        };

        match data {
            EventData::Gtid(g) => {
                if let Some(gtid_set) = ctx.gtid_set.as_mut() {
                    gtid_set.add(&g.gtid)?;
                }
            }

            EventData::TableMap(d) => {
                ctx.table_map_event_map.insert(d.table_id, d);
            }

            EventData::TransactionPayload(event) => {
                for (mut inner_header, data) in event.uncompressed_events {
                    // headers of uncompressed events have no next_event_position,
                    // use header of TransactionPayload instead
                    inner_header.next_event_position = header.next_event_position;
                    self.parse_events(inner_header, data, ctx).await?;
                }
            }

            EventData::WriteRows(mut w) => {
                for event in w.rows.iter_mut() {
                    let table_map_event = ctx.table_map_event_map.get(&w.table_id).unwrap();
                    let col_values = self
                        .parse_row_data(table_map_event, &w.included_columns, event)
                        .await?;
                    let row_data = RowData::new(
                        table_map_event.database_name.clone(),
                        table_map_event.table_name.clone(),
                        RowType::Insert,
                        None,
                        Some(col_values),
                    );
                    self.push_row_to_buf(row_data, position.clone()).await?;
                }
            }

            EventData::UpdateRows(mut u) => {
                for event in u.rows.iter_mut() {
                    let table_map_event = ctx.table_map_event_map.get(&u.table_id).unwrap();
                    let col_values_before = self
                        .parse_row_data(table_map_event, &u.included_columns_before, &mut event.0)
                        .await?;
                    let col_values_after = self
                        .parse_row_data(table_map_event, &u.included_columns_after, &mut event.1)
                        .await?;
                    let row_data = RowData::new(
                        table_map_event.database_name.clone(),
                        table_map_event.table_name.clone(),
                        RowType::Update,
                        Some(col_values_before),
                        Some(col_values_after),
                    );
                    self.push_row_to_buf(row_data, position.clone()).await?;
                }
            }

            EventData::DeleteRows(mut d) => {
                for event in d.rows.iter_mut() {
                    let table_map_event = ctx.table_map_event_map.get(&d.table_id).unwrap();
                    let col_values = self
                        .parse_row_data(table_map_event, &d.included_columns, event)
                        .await?;
                    let row_data = RowData::new(
                        table_map_event.database_name.clone(),
                        table_map_event.table_name.clone(),
                        RowType::Delete,
                        Some(col_values),
                        None,
                    );
                    self.push_row_to_buf(row_data, position.clone()).await?;
                }
            }

            EventData::Query(query) => {
                if query.query == QUERY_BEGIN {
                    BaseExtractor::update_time_filter(
                        &mut self.base_extractor.time_filter,
                        header.timestamp,
                        &position,
                    );
                }

                self.handle_query_event(query, position.clone()).await?;
            }

            EventData::Xid(xid) => {
                let commit = DtData::Commit {
                    xid: xid.xid.to_string(),
                };
                self.base_extractor
                    .push_dt_data(commit, position.clone())
                    .await?;
            }

            _ => {}
        }

        Ok(())
    }

    async fn push_row_to_buf(
        &mut self,
        row_data: RowData,
        position: Position,
    ) -> anyhow::Result<()> {
        self.base_extractor.push_row(row_data, position).await
    }

    async fn parse_row_data(
        &mut self,
        table_map_event: &TableMapEvent,
        included_columns: &[bool],
        event: &mut RowEvent,
    ) -> anyhow::Result<HashMap<String, ColValue>> {
        if !self.base_extractor.time_filter.started {
            return Ok(HashMap::new());
        }

        let db = &table_map_event.database_name;
        let tb = &table_map_event.table_name;
        let tb_meta = self.meta_manager.get_tb_meta(db, tb).await?;
        let ignore_cols = self.filter.get_ignore_cols(db, tb);

        if included_columns.len() != event.column_values.len() {
            bail! {Error::ExtractorError(
                "included_columns not match column_values in binlog".into(),
            )}
        }

        let mut data = HashMap::new();
        let col_count = cmp::min(tb_meta.basic.cols.len(), included_columns.len());
        for i in (0..col_count).rev() {
            let col = tb_meta.basic.cols.get(i).unwrap();
            if ignore_cols.is_some_and(|cols| cols.contains(col)) {
                continue;
            }

            if let Some(false) = included_columns.get(i) {
                data.insert(col.clone(), ColValue::None);
                continue;
            }

            let col_type = tb_meta.get_col_type(col)?;
            let raw_value = event.column_values.remove(i);
            let value = MysqlColValueConvertor::from_binlog(col_type, raw_value)?;
            data.insert(col.clone(), value);
        }
        Ok(data)
    }

    async fn handle_query_event(
        &mut self,
        query: QueryEvent,
        position: Position,
    ) -> anyhow::Result<()> {
        // TODO, currently we do not parse ddl if filtered,
        // but we should always try to parse ddl in the future
        if self.filter.filter_all_ddl() && self.filter.filter_all_dcl() {
            return Ok(());
        }

        if query.query == QUERY_BEGIN {
            return Ok(());
        }

        if !self.filter.filter_all_dcl() {
            if let Ok(Some(dcl_data)) = self
                .base_extractor
                .parse_dcl(&DbType::Mysql, &query.schema, &query.query)
                .await
            {
                if !self.filter.filter_dcl(&dcl_data.dcl_type) {
                    self.base_extractor
                        .push_dcl(dcl_data.clone(), position.clone())
                        .await?;
                }
                return Ok(());
            }
        }

        if !self.filter.filter_all_ddl() {
            if let Ok(Some(ddl_data)) = self
                .base_extractor
                .parse_ddl(&DbType::Mysql, &query.schema, &query.query)
                .await
            {
                for sub_ddl_data in ddl_data.clone().split_to_multi() {
                    let (db, tb) = sub_ddl_data.get_schema_tb();
                    // invalidate metadata cache
                    self.meta_manager.invalidate_cache(&db, &tb);
                    if !self.filter.filter_ddl(&db, &tb, &sub_ddl_data.ddl_type) {
                        self.base_extractor
                            .push_ddl(sub_ddl_data.clone(), position.clone())
                            .await?;
                    }
                }

                if let Some(meta_center) = &mut self.meta_manager.meta_center {
                    meta_center.sync_from_ddl(&ddl_data).await?;
                }

                return Ok(());
            }
        }

        Ok(())
    }

    fn filter_event(&mut self, table_map_event: &TableMapEvent, row_type: RowType) -> bool {
        let db = &table_map_event.database_name;
        let tb = &table_map_event.table_name;
        let filtered = self.filter.filter_event(db, tb, &row_type);
        if filtered {
            return !self.base_extractor.is_data_marker_info(db, tb);
        }
        filtered
    }

    fn start_heartbeat(&mut self, shut_down: Arc<AtomicBool>) -> anyhow::Result<()> {
        let db_tb = self.base_extractor.precheck_heartbeat(
            self.heartbeat_interval_secs,
            &self.heartbeat_tb,
            DbType::Pg,
        );
        if db_tb.len() != 2 {
            return Ok(());
        }

        self.filter.add_ignore_tb(&db_tb[0], &db_tb[1]);

        let (server_id, heartbeat_interval_secs, syncer, conn_pool) = (
            self.server_id,
            self.heartbeat_interval_secs,
            self.syncer.clone(),
            self.conn_pool.clone(),
        );

        tokio::spawn(async move {
            let mut start_time = Instant::now();
            while !shut_down.load(Ordering::Acquire) {
                if start_time.elapsed().as_secs() >= heartbeat_interval_secs {
                    Self::heartbeat(server_id, &db_tb[0], &db_tb[1], &syncer, &conn_pool)
                        .await
                        .unwrap();
                    start_time = Instant::now();
                }
                TimeUtil::sleep_millis(1000 * heartbeat_interval_secs).await;
            }
        });
        log_info!("heartbeat started");
        Ok(())
    }

    async fn heartbeat(
        server_id: u64,
        db: &str,
        tb: &str,
        syncer: &Arc<Mutex<Syncer>>,
        conn_pool: &Pool<MySql>,
    ) -> anyhow::Result<()> {
        let (received_binlog_filename, received_next_event_position, received_timestamp) =
            if let Position::MysqlCdc {
                binlog_filename,
                next_event_position,
                timestamp,
                ..
            } = &syncer.lock().await.received_position
            {
                (
                    binlog_filename.to_owned(),
                    *next_event_position,
                    timestamp.to_owned(),
                )
            } else {
                (String::new(), 0, String::new())
            };

        let (flushed_binlog_filename, flushed_next_event_position, flushed_timestamp) =
            if let Position::MysqlCdc {
                binlog_filename,
                next_event_position,
                timestamp,
                ..
            } = &syncer.lock().await.committed_position
            {
                (
                    binlog_filename.to_owned(),
                    *next_event_position,
                    timestamp.to_owned(),
                )
            } else {
                (String::new(), 0, String::new())
            };

        // CREATE TABLE test_db_1.ape_dts_heartbeat(
        //     server_id INT UNSIGNED,
        //     update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        //     received_binlog_filename VARCHAR(255),
        //     received_next_event_position INT UNSIGNED,
        //     received_timestamp VARCHAR(255),
        //     flushed_binlog_filename VARCHAR(255),
        //     flushed_next_event_position INT UNSIGNED,
        //     flushed_timestamp VARCHAR(255),
        //     PRIMARY KEY(server_id)
        // );
        let sql = format!(
            "REPLACE INTO `{}`.`{}` (server_id, update_timestamp, 
                received_binlog_filename, received_next_event_position, received_timestamp, 
                flushed_binlog_filename, flushed_next_event_position, flushed_timestamp) 
            VALUES ({}, now(), '{}', {}, '{}', '{}', {}, '{}')",
            db,
            tb,
            server_id,
            received_binlog_filename,
            received_next_event_position,
            received_timestamp,
            flushed_binlog_filename,
            flushed_next_event_position,
            flushed_timestamp,
        );

        let query: Query<MySql, MySqlArguments> = sqlx::query(&sql);
        if let Err(err) = query.execute(conn_pool).await {
            log_error!("heartbeat failed: {:?}", err);
        }
        Ok(())
    }
}
