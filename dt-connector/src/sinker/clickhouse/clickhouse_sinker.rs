use std::{cmp, collections::HashMap, sync::Arc};

use anyhow::bail;
use async_trait::async_trait;
use chrono::Utc;
use reqwest::{Client, Method, Response, StatusCode};
use serde_json::json;
use tokio::time::Instant;

use crate::{call_batch_fn, sinker::base_sinker::BaseSinker, Sinker};
use dt_common::{
    config::config_enums::DbType,
    error::Error,
    meta::{col_value::ColValue, row_data::RowData, row_type::RowType},
    monitor::monitor::Monitor,
    utils::{limit_queue::LimitedQueue, sql_util::SqlUtil},
};

const SIGN_COL_NAME: &str = "_ape_dts_is_deleted";
const TIMESTAMP_COL_NAME: &str = "_ape_dts_timestamp";

#[derive(Clone)]
pub struct ClickhouseSinker {
    pub http_client: Client,
    pub batch_size: usize,
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub monitor: Arc<Monitor>,
    pub sync_timestamp: i64,
}

#[async_trait]
impl Sinker for ClickhouseSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        call_batch_fn!(self, data, Self::batch_sink);
        Ok(())
    }
}

impl ClickhouseSinker {
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
        let db = SqlUtil::escape_by_db_type(&data[start_index].schema, &DbType::ClickHouse);
        let tb = SqlUtil::escape_by_db_type(&data[start_index].tb, &DbType::ClickHouse);
        self.sync_timestamp = cmp::max(Utc::now().timestamp_millis(), self.sync_timestamp + 1);

        let mut data_size = 0;
        // build stream load data
        let mut load_data = Vec::new();
        for row_data in data.iter_mut().skip(start_index).take(batch_size) {
            data_size += row_data.data_size;

            Self::convert_row_data(row_data)?;

            let col_values = if row_data.row_type == RowType::Delete {
                let before = row_data.require_before_mut()?;
                // SIGN_COL value
                before.insert(SIGN_COL_NAME.into(), ColValue::Long(1));
                before
            } else {
                row_data.require_after_mut()?
            };

            col_values.insert(
                TIMESTAMP_COL_NAME.into(),
                ColValue::LongLong(self.sync_timestamp),
            );
            load_data.push(col_values);
        }

        // curl -X POST -d @data.json 'http://localhost:8123/?query=INSERT%20INTO%test_db.tb_1%20FORMAT%20JSON' --user admin:123456
        let body = json!(load_data).to_string();
        let url = format!(
            "http://{}:{}/?query=INSERT INTO {}.{} FORMAT JSON",
            self.host, self.port, db, tb
        );
        let request = self.build_request(&url, &body)?;

        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let response = self.http_client.execute(request).await?;
        rts.push((start_time.elapsed().as_millis() as u64, 1));
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;

        Self::check_response(response).await?;

        Ok(data_size)
    }

    fn convert_row_data(row_data: &mut RowData) -> anyhow::Result<()> {
        if let Some(before) = &mut row_data.before {
            Self::convert_col_values(before)?;
        }
        if let Some(after) = &mut row_data.after {
            Self::convert_col_values(after)?;
        }
        Ok(())
    }

    fn convert_col_values(col_values: &mut HashMap<String, ColValue>) -> anyhow::Result<()> {
        let mut new_col_values: HashMap<String, ColValue> = HashMap::new();
        for (col, col_value) in col_values.iter() {
            match col_value {
                ColValue::RawString(v) => {
                    let (str, is_hex) = SqlUtil::binary_to_str(v);
                    if is_hex {
                        new_col_values
                            .insert(col.to_owned(), ColValue::String(format!("0x{}", str)));
                    } else {
                        new_col_values.insert(col.to_owned(), ColValue::String(str));
                    }
                }

                ColValue::Blob(v) => {
                    let hex_str = hex::encode(v);
                    new_col_values
                        .insert(col.to_owned(), ColValue::String(format!("0x{}", hex_str)));
                }

                ColValue::Time(v) | ColValue::Timestamp(v) => {
                    // postgres: 10:20:12+00 -> 10:20:12, 2019-02-10 11:35:00+00 -> 2019-02-10 11:35:00
                    new_col_values.insert(
                        col.to_owned(),
                        ColValue::String(v.trim_end_matches("+00").to_string()),
                    );
                }

                _ => {}
            }
        }

        for (col, col_value) in new_col_values {
            col_values.insert(col, col_value);
        }
        Ok(())
    }

    fn build_request(&self, url: &str, body: &str) -> anyhow::Result<reqwest::Request> {
        let password = if self.password.is_empty() {
            None
        } else {
            Some(self.password.clone())
        };

        let post = self
            .http_client
            .request(Method::POST, url)
            .basic_auth(&self.username, password)
            .body(body.to_string());
        Ok(post.build()?)
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
        Ok(())
    }
}
