use std::{collections::HashMap, sync::atomic::AtomicBool};

use async_recursion::async_recursion;
use async_trait::async_trait;

use concurrent_queue::ConcurrentQueue;
use log::info;
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    event::{
        event_data::EventData, event_header::EventHeader, row_event::RowEvent,
        table_map_event::TableMapEvent,
    },
};

use crate::{
    adaptor::mysql_col_value_convertor::MysqlColValueConvertor,
    common::position_util::PositionUtil,
    error::Error,
    extractor::rdb_filter::RdbFilter,
    meta::{
        col_value::ColValue, mysql::mysql_meta_manager::MysqlMetaManager, row_data::RowData,
        row_type::RowType,
    },
    task::task_util::TaskUtil,
    traits::Extractor,
};

pub struct MysqlCdcExtractor<'a> {
    pub meta_manager: MysqlMetaManager,
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub filter: RdbFilter,
    pub url: String,
    pub binlog_filename: String,
    pub binlog_position: u32,
    pub server_id: u64,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for MysqlCdcExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        info!(
            "MysqlCdcExtractor starts, binlog_filename: {}, binlog_position: {}",
            self.binlog_filename, self.binlog_position
        );
        self.extract_internal().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl MysqlCdcExtractor<'_> {
    async fn extract_internal(&mut self) -> Result<(), Error> {
        let mut client = BinlogClient {
            url: self.url.clone(),
            binlog_filename: self.binlog_filename.clone(),
            binlog_position: self.binlog_position,
            server_id: self.server_id,
        };
        let mut stream = client.connect().await.unwrap();

        let mut table_map_event_map = HashMap::new();
        let mut binlog_filename = self.binlog_filename.clone();
        // get the actual starting binlog position from client,
        // since we may pass an empty binlog position in which case client will start from the latest position
        let mut last_xid_position = client.binlog_position;
        // no timestamp provided when starting client
        let mut last_xid_timestamp = 0;
        loop {
            let (header, data) = stream.read().await?;
            match data {
                EventData::Rotate(r) => {
                    binlog_filename = r.binlog_filename;
                }

                _ => {
                    (last_xid_position, last_xid_timestamp) = self
                        .parse_events(
                            header,
                            data,
                            &binlog_filename,
                            last_xid_position,
                            last_xid_timestamp,
                            &mut table_map_event_map,
                        )
                        .await?;
                }
            }
        }
    }

    #[async_recursion]
    async fn parse_events(
        &mut self,
        header: EventHeader,
        data: EventData,
        binlog_filename: &str,
        last_xid_position: u32,
        last_xid_timestamp: u32,
        table_map_event_map: &mut HashMap<u64, TableMapEvent>,
    ) -> Result<(u32, u32), Error> {
        let current_position = format!(
            "binlog_filename:{},next_event_position:{},timestamp:{}",
            binlog_filename,
            header.next_event_position,
            PositionUtil::format_timestamp_millis(header.timestamp as i64 * 1000)
        );
        let checkpoint_position = format!(
            "binlog_filename:{},last_xid_position:{},timestamp:{}",
            binlog_filename,
            last_xid_position,
            PositionUtil::format_timestamp_millis(last_xid_timestamp as i64 * 1000)
        );
        let mut last_xid_position = last_xid_position;
        let mut last_xid_timestamp = last_xid_timestamp;

        match data {
            EventData::TableMap(d) => {
                table_map_event_map.insert(d.table_id, d);
            }

            EventData::TransactionPayload(event) => {
                for event in event.uncompressed_events {
                    (last_xid_position, last_xid_timestamp) = self
                        .parse_events(
                            event.0,
                            event.1,
                            binlog_filename,
                            last_xid_position,
                            last_xid_timestamp,
                            table_map_event_map,
                        )
                        .await?;
                }
            }

            EventData::WriteRows(mut w) => {
                for event in w.rows.iter_mut() {
                    let table_map_event = table_map_event_map.get(&w.table_id).unwrap();
                    let col_values = self
                        .parse_row_data(&table_map_event, &w.included_columns, event)
                        .await?;
                    let row_data = RowData {
                        db: table_map_event.database_name.clone(),
                        tb: table_map_event.table_name.clone(),
                        row_type: RowType::Insert,
                        before: Option::None,
                        after: Some(col_values),
                        current_position: current_position.clone(),
                        checkpoint_position: checkpoint_position.clone(),
                    };
                    let _ = self.push_row_to_buf(row_data).await?;
                }
            }

            EventData::UpdateRows(mut u) => {
                for event in u.rows.iter_mut() {
                    let table_map_event = table_map_event_map.get(&u.table_id).unwrap();
                    let col_values_before = self
                        .parse_row_data(&table_map_event, &u.included_columns_before, &mut event.0)
                        .await?;
                    let col_values_after = self
                        .parse_row_data(&table_map_event, &u.included_columns_after, &mut event.1)
                        .await?;
                    let row_data = RowData {
                        db: table_map_event.database_name.clone(),
                        tb: table_map_event.table_name.clone(),
                        row_type: RowType::Update,
                        before: Some(col_values_before),
                        after: Some(col_values_after),
                        current_position: current_position.clone(),
                        checkpoint_position: checkpoint_position.clone(),
                    };
                    let _ = self.push_row_to_buf(row_data).await?;
                }
            }

            EventData::DeleteRows(mut d) => {
                for event in d.rows.iter_mut() {
                    let table_map_event = table_map_event_map.get(&d.table_id).unwrap();
                    let col_values = self
                        .parse_row_data(&table_map_event, &d.included_columns, event)
                        .await?;
                    let row_data = RowData {
                        db: table_map_event.database_name.clone(),
                        tb: table_map_event.table_name.clone(),
                        row_type: RowType::Delete,
                        before: Some(col_values),
                        after: Option::None,
                        current_position: current_position.clone(),
                        checkpoint_position: checkpoint_position.clone(),
                    };
                    let _ = self.push_row_to_buf(row_data).await?;
                }
            }

            EventData::Xid(_xid) => {
                last_xid_position = header.next_event_position;
                last_xid_timestamp = header.timestamp;
            }

            _ => {}
        }

        Ok((last_xid_position, last_xid_timestamp))
    }

    async fn push_row_to_buf(&mut self, row_data: RowData) -> Result<(), Error> {
        if self
            .filter
            .filter(&row_data.db, &row_data.tb, &row_data.row_type)
        {
            return Ok(());
        }

        while self.buffer.is_full() {
            TaskUtil::sleep_millis(1).await;
        }
        let _ = self.buffer.push(row_data);
        Ok(())
    }

    async fn parse_row_data(
        &mut self,
        table_map_event: &TableMapEvent,
        included_columns: &Vec<bool>,
        event: &mut RowEvent,
    ) -> Result<HashMap<String, ColValue>, Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&table_map_event.database_name, &table_map_event.table_name)
            .await?;

        if included_columns.len() != event.column_values.len() {
            return Err(Error::ColumnNotMatch);
        }

        let mut data = HashMap::new();
        for i in (0..tb_meta.basic.cols.len()).rev() {
            let key = tb_meta.basic.cols.get(i).unwrap();
            if let Some(false) = included_columns.get(i) {
                data.insert(key.clone(), ColValue::None);
                continue;
            }

            let col_type = tb_meta.col_type_map.get(key).unwrap();
            let raw_value = event.column_values.remove(i);
            let value = MysqlColValueConvertor::from_binlog(col_type, raw_value);
            data.insert(key.clone(), value);
        }
        Ok(data)
    }
}
