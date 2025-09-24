use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::bail;

use dt_common::{
    config::{
        config_enums::DbType,
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
    },
    error::Error,
    log_debug, log_error, log_info,
    meta::{
        dcl_meta::{dcl_data::DclData, dcl_parser::DclParser},
        ddl_meta::ddl_data::DdlData,
        dt_queue::DtQueue,
        struct_meta::struct_data::StructData,
    },
    rdb_filter::RdbFilter,
    utils::{sql_util::SqlUtil, time_util::TimeUtil},
};
use dt_common::{
    meta::{
        ddl_meta::ddl_parser::DdlParser,
        dt_data::{DtData, DtItem},
        position::Position,
        row_data::RowData,
    },
    time_filter::TimeFilter,
};

use crate::{data_marker::DataMarker, rdb_router::RdbRouter};

use super::extractor_monitor::ExtractorMonitor;

pub struct BaseExtractor {
    pub buffer: Arc<DtQueue>,
    pub router: RdbRouter,
    pub shut_down: Arc<AtomicBool>,
    pub monitor: ExtractorMonitor,
    pub data_marker: Option<DataMarker>,
    pub time_filter: TimeFilter,
}

impl BaseExtractor {
    pub fn is_data_marker_info(&self, schema: &str, tb: &str) -> bool {
        if let Some(data_marker) = &self.data_marker {
            return data_marker.is_rdb_marker_info(schema, tb);
        }
        false
    }

    pub async fn push_dt_data(
        &mut self,
        dt_data: DtData,
        position: Position,
    ) -> anyhow::Result<()> {
        if !self.time_filter.started {
            return Ok(());
        }

        if self.refresh_and_check_data_marker(&dt_data) {
            return Ok(());
        }

        self.monitor.counters.pushed_record_count += 1;
        self.monitor.counters.pushed_data_size += dt_data.get_data_size();
        self.monitor.try_flush(false).await;

        let data_origin_node = if let Some(data_marker) = &mut self.data_marker {
            data_marker.data_origin_node.clone()
        } else {
            String::new()
        };

        let item = DtItem {
            dt_data,
            position,
            data_origin_node,
        };
        log_debug!("extracted item: {:?}", item);
        self.buffer.push(item).await
    }

    pub fn refresh_and_check_data_marker(&mut self, dt_data: &DtData) -> bool {
        // data_marker does not support DDL event yet.
        // user needs to ensure only one-way DDL replication exists in the topology
        if let Some(data_marker) = &mut self.data_marker {
            if dt_data.is_begin() || dt_data.is_commit() {
                data_marker.reset();
            } else if data_marker.reset {
                if data_marker.is_marker_info(dt_data) {
                    data_marker.refresh(dt_data);
                    // after data_marker refreshed, discard the marker data itself
                    return true;
                } else {
                    // the first dml/ddl after the last transaction commit is NOT marker_info,
                    // then current transaction should NOT be filtered by default.
                    // set reset = false, just to make sure is_marker_info won't be called again
                    // in current transaction
                    data_marker.filter = false;
                    data_marker.reset = false;
                }
            }

            // data from origin node are filtered
            if data_marker.filter {
                return true;
            }
        }
        false
    }

    pub async fn push_row(&mut self, row_data: RowData, position: Position) -> anyhow::Result<()> {
        let row_data = self.router.route_row(row_data);
        self.push_dt_data(DtData::Dml { row_data }, position).await
    }

    pub async fn push_ddl(&mut self, ddl_data: DdlData, position: Position) -> anyhow::Result<()> {
        let ddl_data = self.router.route_ddl(ddl_data);
        while !self.buffer.is_empty() {
            TimeUtil::sleep_millis(1).await;
        }
        self.push_dt_data(DtData::Ddl { ddl_data }, position).await
    }

    pub async fn push_dcl(&mut self, dcl_data: DclData, position: Position) -> anyhow::Result<()> {
        // Todo: route dcl data
        self.push_dt_data(DtData::Dcl { dcl_data }, position).await
    }

    pub async fn push_struct(&mut self, struct_data: StructData) -> anyhow::Result<()> {
        let struct_data = self.router.route_struct(struct_data);
        self.push_dt_data(DtData::Struct { struct_data }, Position::None)
            .await
    }

    pub async fn parse_ddl(
        &self,
        db_type: &DbType,
        schema: &str,
        query: &str,
    ) -> anyhow::Result<Option<DdlData>> {
        let parser = DdlParser::new(db_type.to_owned());
        let parse_result = parser.parse(query);
        if let Err(err) = parse_result {
            let error = format!("failed to parse ddl, will try ignore it, please execute the ddl manually in target, sql: {}, error: {}", query, err);
            log_error!("{}", error);
            bail! {Error::Unexpected(error)}
        }

        // case 1, execute: use db_1; create table tb_1(id int);
        // binlog query.schema == db_1, schema from DdlParser == None
        // case 2, execute: create table db_1.tb_1(id int);
        // binlog query.schema == empty, schema from DdlParser == db_1
        // case 3, execute: use db_1; create table db_2.tb_1(id int);
        // binlog query.schema == db_1, schema from DdlParser == db_2
        if let Some(mut ddl_data) = parse_result? {
            ddl_data.default_schema = schema.to_string();
            ddl_data.query = query.to_string();
            Ok(Some(ddl_data))
        } else {
            Ok(None)
        }
    }

    pub async fn parse_dcl(
        &self,
        db_type: &DbType,
        _schema: &str,
        query: &str,
    ) -> anyhow::Result<Option<DclData>> {
        let parser = DclParser::new(db_type.to_owned());
        let parse_result = parser.parse(query);

        if let Err(err) = parse_result {
            let error = format!(
                "failed to parse dcl, will try ignore it, sql: {}, error: {}",
                query, err
            );
            bail! {Error::Unexpected(error)}
        }

        if let Some(dcl_data) = parse_result? {
            Ok(Some(dcl_data))
        } else {
            Ok(None)
        }
    }

    pub fn get_where_sql(filter: &RdbFilter, schema: &str, tb: &str, condition: &str) -> String {
        let mut res: String = String::new();
        if let Some(where_condition) = filter.get_where_condition(schema, tb) {
            res = format!("WHERE {}", where_condition);
        }

        if condition.is_empty() {
            return res;
        }

        if res.is_empty() {
            format!("WHERE {}", condition)
        } else {
            format!("{} AND {}", res, condition)
        }
    }

    pub fn precheck_heartbeat(
        &self,
        heartbeat_interval_secs: u64,
        heartbeat_tb: &str,
        db_type: DbType,
    ) -> Vec<String> {
        log_info!(
            "try starting heartbeat, heartbeat_interval_secs: {}, heartbeat_tb: {}, ",
            heartbeat_interval_secs,
            heartbeat_tb
        );

        if heartbeat_interval_secs == 0 || heartbeat_tb.is_empty() {
            log_info!(
                "heartbeat disabled, heartbeat_tb: {}, heartbeat_interval_secs: {}",
                heartbeat_tb,
                heartbeat_interval_secs
            );
            return vec![];
        }

        let schema_tb = ConfigTokenParser::parse(
            heartbeat_tb,
            &['.'],
            &TokenEscapePair::from_char_pairs(SqlUtil::get_escape_pairs(&db_type)),
        );

        if schema_tb.len() < 2 {
            log_info!("heartbeat disabled, heartbeat_tb should be like schema.tb");
            return vec![];
        }
        schema_tb
    }

    pub fn update_time_filter(time_filter: &mut TimeFilter, timestamp: u32, position: &Position) {
        if !time_filter.started && timestamp >= time_filter.start_timestamp {
            time_filter.started = true;
            log_info!("time filter started, position: {}", position.to_string());
        }

        if !time_filter.ended && timestamp >= time_filter.end_timestamp {
            time_filter.ended = true;
            log_info!("time filter ended, position: {}", position.to_string());
        }
    }

    pub async fn wait_task_finish(&mut self) -> anyhow::Result<()> {
        // wait all data to be transferred
        while !self.buffer.is_empty() {
            TimeUtil::sleep_millis(1).await;
        }

        self.monitor.try_flush(true).await;
        self.shut_down.store(true, Ordering::Release);
        Ok(())
    }
}
