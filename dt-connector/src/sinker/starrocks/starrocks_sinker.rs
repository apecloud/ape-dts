use std::{cmp, collections::HashMap, str::FromStr, sync::Arc};

use crate::{call_batch_fn, sinker::base_sinker::BaseSinker, Sinker};
use anyhow::bail;
use async_trait::async_trait;
use chrono::Utc;
use reqwest::{header, Client, Method, Response, StatusCode};
use serde_json::{json, Value};
use tokio::time::Instant;

use dt_common::{
    config::config_enums::DbType,
    error::Error,
    log_error,
    meta::{
        col_value::ColValue,
        mysql::{
            mysql_col_type::MysqlColType, mysql_meta_manager::MysqlMetaManager,
            mysql_tb_meta::MysqlTbMeta,
        },
        row_data::RowData,
        row_type::RowType,
    },
    monitor::monitor::Monitor,
    utils::{limit_queue::LimitedQueue, sql_util::SqlUtil},
};

const SIGN_COL_NAME: &str = "_ape_dts_is_deleted";
const TIMESTAMP_COL_NAME: &str = "_ape_dts_timestamp";

#[derive(Clone)]
pub struct StarRocksSinker {
    pub db_type: DbType,
    pub batch_size: usize,
    pub http_client: Client,
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub meta_manager: MysqlMetaManager,
    pub monitor: Arc<Monitor>,
    pub sync_timestamp: i64,
    pub hard_delete: bool,
}

#[async_trait]
impl Sinker for StarRocksSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_sink(data).await?;
        } else {
            call_batch_fn!(self, data, Self::batch_sink);
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.meta_manager.close().await
    }
}

impl StarRocksSinker {
    async fn serial_sink(&mut self, mut data: Vec<RowData>) -> anyhow::Result<()> {
        let mut data_size = 0;

        let data = data.as_mut_slice();
        for i in 0..data.len() {
            data_size += data[i].get_data_size();
            self.send_data(data, i, 1).await?;
        }

        BaseSinker::update_serial_monitor(&self.monitor, data.len() as u64, data_size).await
    }

    async fn batch_sink(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let data_size = self.send_data(data, start_index, batch_size).await?;

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, data_size as u64).await
    }

    async fn send_data(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<usize> {
        let db = data[start_index].schema.clone();
        let tb = data[start_index].tb.clone();
        let first_row_type = data[start_index].row_type.clone();
        let tb_meta = self.meta_manager.get_tb_meta(&db, &tb).await?;
        self.sync_timestamp = cmp::max(Utc::now().timestamp_millis(), self.sync_timestamp + 1);

        let mut data_size = 0;
        let mut rts = LimitedQueue::new(1);
        // build stream load data
        let mut load_data = Vec::new();
        for row_data in data.iter().skip(start_index).take(batch_size) {
            data_size += row_data.get_data_size() as usize;
            let mut row = row_data.clone();

            Self::convert_row_data(&mut row, tb_meta)?;

            let mut col_values = if row.row_type == RowType::Delete {
                let mut before = row.require_before()?.clone();
                if self.db_type == DbType::StarRocks {
                    // SIGN_COL value
                    before.insert(SIGN_COL_NAME.into(), ColValue::Long(1));
                }
                before
            } else {
                row.require_after()?.clone()
            };

            if self.db_type == DbType::StarRocks {
                col_values.insert(
                    TIMESTAMP_COL_NAME.into(),
                    ColValue::LongLong(self.sync_timestamp),
                );
            }

            load_data.push(col_values);
        }

        let mut op = "";
        if self.db_type == DbType::StarRocks {
            let hard_delete = self.hard_delete
                || !tb_meta
                    .basic
                    .col_origin_type_map
                    .contains_key(SIGN_COL_NAME);
            if first_row_type == RowType::Delete && hard_delete {
                op = "delete";
            }
        } else if first_row_type == RowType::Delete {
            op = "delete";
        }

        let body = json!(load_data).to_string();
        // do stream load
        let url = format!(
            "http://{}:{}/api/{}/{}/_stream_load",
            self.host, self.port, db, tb
        );
        let request = self.build_request(&url, op, &body)?;

        let start_time = Instant::now();
        let response = self.http_client.execute(request).await?;
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;

        Self::check_response(response).await?;

        Ok(data_size)
    }

    fn convert_row_data(row_data: &mut RowData, tb_meta: &MysqlTbMeta) -> anyhow::Result<()> {
        if let Some(before) = &mut row_data.before {
            Self::convert_col_values(before, tb_meta)?;
        }
        if let Some(after) = &mut row_data.after {
            Self::convert_col_values(after, tb_meta)?;
        }
        Ok(())
    }

    fn convert_col_values(
        col_values: &mut HashMap<String, ColValue>,
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<()> {
        let mut new_col_values: HashMap<String, ColValue> = HashMap::new();
        for (col, col_value) in col_values.iter() {
            if let MysqlColType::Json = tb_meta.get_col_type(col)? {
                // ColValue::Json2 will be serialized to:
                // {"id": 1, "json_field": "{\"name\": \"Alice\", \"age\": 30}"}
                // ColValue::Json3 will be serialized to:
                // {"id": 5, "json_field": {"name": "Alice", "age": 30}}
                match col_value {
                    ColValue::Json2(v) | ColValue::String(v) => {
                        if let Ok(json_v) = serde_json::Value::from_str(v) {
                            new_col_values.insert(col.to_owned(), ColValue::Json3(json_v));
                        }
                    }
                    _ => {}
                }
            }

            match col_value {
                ColValue::Blob(v) | ColValue::RawString(v) => {
                    new_col_values.insert(
                        col.to_owned(),
                        ColValue::String(SqlUtil::binary_to_str(v).0),
                    );
                }

                ColValue::Bit(v) => {
                    new_col_values.insert(col.to_owned(), ColValue::LongLong(*v as i64));
                }

                _ => {}
            }
        }

        for (col, col_value) in new_col_values {
            col_values.insert(col, col_value);
        }
        Ok(())
    }

    fn build_request(&self, url: &str, op: &str, body: &str) -> anyhow::Result<reqwest::Request> {
        let password = if self.password.is_empty() {
            None
        } else {
            Some(self.password.clone())
        };

        let mut put = self
            .http_client
            .request(Method::PUT, url)
            .basic_auth(&self.username, password)
            .header(header::EXPECT, "100-continue")
            .header("format", "json")
            .header("strip_outer_array", "true")
            .header("timezone", "UTC")
            .body(body.to_string());
        // by default, the __op will be upsert
        if !op.is_empty() {
            match self.db_type {
                DbType::StarRocks => {
                    // https://docs.starrocks.io/docs/loading/Load_to_Primary_Key_tables/
                    // https://docs.starrocks.io/docs/loading/Stream_Load_transaction_interface/
                    let op = format!("__op='{}'", op);
                    put = put.header("columns", op);
                }
                DbType::Doris => {
                    // https://doris.apache.org/docs/1.2/data-operate/update-delete/batch-delete-manual
                    // https://doris.apache.org/docs/1.2/data-operate/import/import-way/stream-load-manual
                    // if bulk delete support is enabled (enable_batch_delete_by_default=true or ALTER TABLE tablename ENABLE FEATURE "BATCH_DELETE"),
                    // there will be 2 hidden columns for each table:
                    // Doris > DESC `test_db`.`tb_1`;
                    // +-----------------------+---------+------+-------+---------+-------+
                    // | Field                 | Type    | Null | Key   | Default | Extra |
                    // +-----------------------+---------+------+-------+---------+-------+
                    // | id                    | INT     | No   | true  | NULL    |       |
                    // | value                 | INT     | Yes  | false | NULL    | NONE  |
                    // | __DORIS_DELETE_SIGN__ | TINYINT | No   | false | 0       | NONE  |
                    // | __DORIS_VERSION_COL__ | BIGINT  | No   | false | 0       | NONE  |
                    // +-----------------------+---------+------+-------+---------+-------+
                    put = put.header("merge_type", op);
                }
                _ => {}
            }
        }
        Ok(put.build()?)
    }

    async fn check_response(response: Response) -> anyhow::Result<()> {
        let status_code = response.status();
        let response_text = &response.text().await?;
        if status_code != StatusCode::OK {
            bail! {Error::HttpError(format!(
                "data load request failed, status_code: {}, response_text: {:?}",
                status_code, response_text
            ))}
        }

        // response example:
        // {
        //     "TxnId": 2039,
        //     "Label": "54afc14e-9088-46df-b532-4deaf4437b42",
        //     "Status": "Success",
        //     "Message": "OK",
        //     "NumberTotalRows": 3,
        //     "NumberLoadedRows": 3,
        //     "NumberFilteredRows": 0,
        //     "NumberUnselectedRows": 0,
        //     "LoadBytes": 221,
        //     "LoadTimeMs": 228,
        //     "BeginTxnTimeMs": 34,
        //     "StreamLoadPlanTimeMs": 48,
        //     "ReadDataTimeMs": 0,
        //     "WriteDataTimeMs": 107,
        //     "CommitAndPublishTimeMs": 36
        // }
        let json_value: Value = serde_json::from_str(response_text)?;
        if json_value["Status"] != "Success" {
            let err = format!(
                "stream load request failed, status_code: {}, load_result: {}",
                status_code, response_text,
            );
            log_error!("{}", err);
            bail! {Error::HttpError(err)}
        }
        Ok(())
    }
}
