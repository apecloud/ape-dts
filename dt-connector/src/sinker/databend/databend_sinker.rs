use std::{
    cmp,
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::bail;
use async_trait::async_trait;
use databend_driver::Client;
use chrono::Utc;
use dt_common::{
    config::config_enums::DbType,
    error::Error,
    meta::{col_value::ColValue, row_data::RowData, row_type::RowType},
    monitor::monitor::Monitor,
    utils::sql_util::SqlUtil,
};

use crate::{call_batch_fn, sinker::base_sinker::BaseSinker, Sinker};

// 定义用于软删除和时间戳的列名
const SIGN_COL_NAME: &str = "_ape_dts_is_deleted";
const TIMESTAMP_COL_NAME: &str = "_ape_dts_timestamp";

// DatabendSinker结构体定义
#[derive(Clone)]
pub struct DatabendSinker {
    pub client: Client,                  // Databend客户端连接
    pub batch_size: usize,              // 批处理大小
    pub monitor: Arc<Mutex<Monitor>>,    // 监控对象
    pub sync_timestamp: i64,            // 同步时间戳
}

#[async_trait]
impl Sinker for DatabendSinker {
    // 实现DML数据写入方法
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        call_batch_fn!(self, data, Self::batch_sink);
        Ok(())
    }
}

impl DatabendSinker {
    // 批量写入数据
    async fn batch_sink(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();

        let data_size = self.send_data(data, start_index, batch_size).await?;

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time)
    }

    // 发送数据到Databend
    async fn send_data(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<usize> {
        let db = SqlUtil::escape_by_db_type(&data[start_index].schema, &DbType::Databend);
        let tb = SqlUtil::escape_by_db_type(&data[start_index].tb, &DbType::Databend);
        self.sync_timestamp = cmp::max(Utc::now().timestamp_millis(), self.sync_timestamp + 1);

        let mut data_size = 0;
        let mut values = Vec::new();
        let mut cols = Vec::new();

        // 处理第一行数据，获取列名
        if let Some(first_row) = data.get_mut(start_index) {
            data_size += first_row.data_size;
            let col_values = if first_row.row_type == RowType::Delete {
                let before = first_row.before.as_mut().unwrap();
                before.insert(SIGN_COL_NAME.into(), ColValue::Long(1));
                before
            } else {
                first_row.after.as_mut().unwrap()
            };
            col_values.insert(
                TIMESTAMP_COL_NAME.into(),
                ColValue::LongLong(self.sync_timestamp),
            );

            // 获取所有列名
            cols = col_values.keys().cloned().collect();
            let value_list = Self::build_value_list(col_values)?;
            values.push(value_list);
        }

        // 处理剩余数据
        for row_data in data.iter_mut().skip(start_index + 1).take(batch_size - 1) {
            data_size += row_data.data_size;

            let col_values = if row_data.row_type == RowType::Delete {
                let before = row_data.before.as_mut().unwrap();
                before.insert(SIGN_COL_NAME.into(), ColValue::Long(1));
                before
            } else {
                row_data.after.as_mut().unwrap()
            };
            col_values.insert(
                TIMESTAMP_COL_NAME.into(),
                ColValue::LongLong(self.sync_timestamp),
            );

            let value_list = Self::build_value_list(col_values)?;
            values.push(value_list);
        }

        // 构建并执行INSERT语句
        let sql = format!(
            "INSERT INTO {}.{} ({}) VALUES {}",
            db,
            tb,
            cols.iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<String>>()
                .join(","),
            values.join(",")
        );

        self.execute_sql(&sql).await?;

        Ok(data_size)
    }

    // 构建值列表
    fn build_value_list(col_values: &mut HashMap<String, ColValue>) -> anyhow::Result<String> {
        let mut values = Vec::new();
        for (_, value) in col_values {
            values.push(match value {
                ColValue::None => "NULL".to_string(),
                ColValue::Bool(v) => v.to_string(),
                ColValue::Tiny(v) => v.to_string(),
                ColValue::UnsignedTiny(v) => v.to_string(),
                ColValue::Short(v) => v.to_string(),
                ColValue::UnsignedShort(v) => v.to_string(),
                ColValue::Long(v) => v.to_string(),
                ColValue::UnsignedLong(v) => v.to_string(),
                ColValue::LongLong(v) => v.to_string(),
                ColValue::UnsignedLongLong(v) => v.to_string(),
                ColValue::Float(v) => v.to_string(),
                ColValue::Double(v) => v.to_string(),
                ColValue::Decimal(v) => v.to_string(),
                ColValue::Time(v) => format!("'{}'", v),
                ColValue::Date(v) => format!("'{}'", v),
                ColValue::DateTime(v) => format!("'{}'", v),
                ColValue::Timestamp(v) => format!("'{}'", v),
                ColValue::Year(v) => v.to_string(),
                ColValue::String(v) => format!("'{}'", v.replace('\'', "''")),
                ColValue::RawString(v) => {
                    let (str, is_hex) = SqlUtil::binary_to_str(v);
                    if is_hex {
                        format!("'{}'", format!("0x{}", str))
                    } else {
                        format!("'{}'", str)
                    }
                },
                ColValue::Blob(v) => format!("'{}'", format!("0x{}", hex::encode(v))),
                ColValue::Bit(v) => v.to_string(),
                ColValue::Set(v) => v.to_string(),
                ColValue::Enum(v) => v.to_string(),
                ColValue::Set2(v) => format!("'{}'", v),
                ColValue::Enum2(v) => format!("'{}'", v),
                ColValue::Json(v) => {
                    let v_str = String::from_utf8_lossy(v);
                    format!("'{}'", v_str.replace('\'', "''"))
                },
                ColValue::Json2(v) => format!("'{}'", v.replace('\'', "''")),
                ColValue::Json3(v) => format!("'{}'", v.to_string().replace('\'', "''")),
                ColValue::MongoDoc(v) => format!("'{}'", v.to_string().replace('\'', "''")),
            });
        }
        Ok(format!("({})", values.join(",")))
    }

    // 执行SQL语句
    async fn execute_sql(&self, sql: &str) -> anyhow::Result<()> {
        match self.client.get_conn().await?.exec(sql, ()).await {
            Ok(_) => Ok(()),
            Err(e) => bail!(anyhow::Error::msg(e.to_string())), 
        }
    }
}
