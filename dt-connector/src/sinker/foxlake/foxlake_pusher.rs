use std::{cmp, str::FromStr, sync::Arc};

use anyhow::{Context, Ok};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Timelike, Utc};
use opendal::Operator;
use orc_format::{
    schema::{Field, Schema},
    writer::{data::GenericData, Config, Writer},
};
use rust_decimal::Decimal;
use tokio::{sync::Mutex, time::Instant};
use uuid::Uuid;

use super::{
    decimal_util::DecimalUtil,
    orc_sequencer::{OrcSequenceInfo, OrcSequencer},
    unicode_util::UnicodeUtil,
};
use crate::{rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};
use dt_common::{
    config::{config_enums::ExtractType, s3_config::S3Config},
    log_info,
    meta::{
        col_value::ColValue,
        ddl_meta::ddl_data::DdlData,
        dt_data::{DtData, DtItem},
        foxlake::s3_file_meta::S3FileMeta,
        mysql::{
            mysql_col_type::MysqlColType, mysql_meta_manager::MysqlMetaManager,
            mysql_tb_meta::MysqlTbMeta,
        },
        row_data::RowData,
        row_type::RowType,
        time::dt_utc_time::DtNaiveTime,
    },
    monitor::monitor::Monitor,
    utils::{limit_queue::LimitedQueue, time_util::TimeUtil},
};

pub struct FoxlakePusher {
    pub url: String,
    pub batch_size: usize,
    pub meta_manager: MysqlMetaManager,
    pub monitor: Arc<Monitor>,
    pub s3_client: Operator,
    pub s3_config: S3Config,
    pub extract_type: ExtractType,
    pub batch_memory_bytes: usize,
    pub schema: Option<String>,
    pub tb: Option<String>,
    pub reverse_router: RdbRouter,
    pub orc_sequencer: Arc<Mutex<OrcSequencer>>,
}

const CDC_ACTION: &str = "cdc_action";
const CDC_LOG_SEQUENCE: &str = "cdc_log_sequence";

#[async_trait]
impl Sinker for FoxlakePusher {
    async fn sink_raw(&mut self, data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        self.batch_sink(data).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        // push finished signal
        if let Some(schema) = &self.schema {
            if let Some(tb) = &self.tb {
                let finished_meta = self.get_finished_meta_info(schema, tb);
                Self::put_to_s3(&self.s3_client, &finished_meta, Vec::new()).await?;
            }
        }

        self.meta_manager.close().await
    }

    async fn refresh_meta(&mut self, _data: Vec<DdlData>) -> anyhow::Result<()> {
        self.orc_sequencer.lock().await.update_epoch();
        Ok(())
    }
}

impl FoxlakePusher {
    async fn batch_sink(&mut self, items: Vec<DtItem>) -> anyhow::Result<()> {
        let batch_size = items.len();
        let (_, all_data_size) = self.batch_push(items, false).await?;

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, all_data_size as u64)
            .await
    }

    pub async fn batch_push(
        &mut self,
        items: Vec<DtItem>,
        async_push: bool,
    ) -> anyhow::Result<(Vec<S3FileMeta>, usize)> {
        let mut s3_file_metas = Vec::new();
        let mut futures = Vec::new();

        let mut all_data_size = 0;
        let item_count = items.len();
        let mut item_index = 0;

        let mut batch_data = Vec::new();
        let mut batch_last_position;
        let mut batch_row_count = 0;
        let mut batch_data_size = 0;

        let mut rts = LimitedQueue::new(cmp::min(100, items.len()));
        for item in items {
            item_index += 1;
            batch_last_position = item.position;
            let mut do_push = false;

            // there may be DtData::Commit items, ignore them
            if let DtData::Dml { row_data } = item.dt_data {
                batch_data_size += row_data.data_size;

                if row_data.row_type == RowType::Update {
                    let (delete_row_data, insert_row_data) = row_data.split_update_row_data();
                    batch_data.push(delete_row_data);
                    batch_data.push(insert_row_data);
                    batch_row_count += 2;
                } else {
                    batch_data.push(row_data);
                    batch_row_count += 1;
                }

                if self.batch_memory_bytes > 0 {
                    if batch_data_size >= self.batch_memory_bytes {
                        do_push = true;
                    }
                } else if batch_row_count >= self.batch_size {
                    do_push = true;
                }
            }

            do_push |= item_index >= item_count;
            if !do_push || batch_data.is_empty() {
                continue;
            }

            // push current batch
            let tb_meta = self
                .meta_manager
                .get_tb_meta_by_row_data(&batch_data[0])
                .await?
                .to_owned();

            let (orc_data, insert_only) = self.generate_orc_data(batch_data, &tb_meta).await?;

            let (src_schema, src_tb) = self
                .reverse_router
                .get_tb_map(&tb_meta.basic.schema, &tb_meta.basic.tb);
            let (data_file_name, meta_file_name, sequence_info) =
                self.get_s3_file_info(src_schema, src_tb).await;

            let s3_file_meta = S3FileMeta {
                schema: tb_meta.basic.schema.clone(),
                tb: tb_meta.basic.tb.clone(),
                insert_only,
                data_file_name,
                meta_file_name,
                data_size: batch_data_size,
                row_count: batch_row_count,
                last_position: batch_last_position.clone(),
                sequencer_id: sequence_info.sequencer_id,
                push_epoch: sequence_info.push_epoch,
                push_sequence: sequence_info.push_sequence,
            };

            // push to s3
            if async_push {
                let s3_client = self.s3_client.clone();
                let s3_file_meta = s3_file_meta.clone();
                let future = tokio::spawn(async move {
                    Self::push(&s3_client, &s3_file_meta, orc_data)
                        .await
                        .unwrap();
                });
                futures.push(future);
            } else {
                let start_time = Instant::now();
                Self::push(&self.s3_client, &s3_file_meta, orc_data).await?;
                rts.push((start_time.elapsed().as_millis() as u64, 1));
            }

            s3_file_metas.push(s3_file_meta);
            all_data_size += batch_data_size;

            // reset batch data
            batch_data = Vec::new();
            batch_data_size = 0;
            batch_row_count = 0;
        }

        for future in futures {
            let start_time = Instant::now();
            future.await.unwrap();
            rts.push((start_time.elapsed().as_millis() as u64, 1));
        }
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;

        Ok((s3_file_metas, all_data_size))
    }

    async fn push(
        s3_client: &Operator,
        s3_file_meta: &S3FileMeta,
        orc_data: Vec<u8>,
    ) -> anyhow::Result<()> {
        Self::put_to_s3(s3_client, &s3_file_meta.data_file_name, orc_data).await?;
        Self::put_to_s3(
            s3_client,
            &s3_file_meta.meta_file_name,
            s3_file_meta.to_string().as_bytes().to_vec(),
        )
        .await
    }
}

// ORC functions
impl FoxlakePusher {
    #[inline(always)]
    fn get_col_value<'a>(
        row_data: &'a RowData,
        col: &str,
    ) -> anyhow::Result<Option<&'a ColValue>> {
        if row_data.row_type == RowType::Delete {
            Ok(row_data.require_before()?.get(col))
        } else {
            Ok(row_data.require_after()?.get(col))
        }
    }

    async fn generate_orc_data(
        &self,
        data: Vec<RowData>,
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<(Vec<u8>, bool)> {
        let mut insert_only = true;
        let (tb_schema, fields) = self.get_tb_orc_schema(tb_meta)?;
        let col_count = tb_meta.basic.cols.len();

        let mut buffer = Vec::new();
        let mut writer = Writer::new(&mut buffer, &tb_schema, Config::new()).unwrap();
        let root = writer.data().unwrap_struct();

        // ignore cdc_action and cdc_log_sequence
        for (i, field) in fields.into_iter().take(col_count).enumerate() {
            let col = &field.0;
            let col_schema = field.1;
            match col_schema {
                Schema::Long => {
                    let field_data = root.child(i).unwrap_long();
                    for row_data in data.iter() {
                        match Self::get_col_value(row_data, col)? {
                            Some(ColValue::Tiny(v)) => field_data.write(*v as i64),
                            Some(ColValue::UnsignedTiny(v)) => field_data.write(*v as i64),
                            Some(ColValue::Short(v)) => field_data.write(*v as i64),
                            Some(ColValue::UnsignedShort(v)) => field_data.write(*v as i64),
                            Some(ColValue::Long(v)) => field_data.write(*v as i64),
                            Some(ColValue::UnsignedLong(v)) => field_data.write(*v as i64),
                            Some(ColValue::LongLong(v)) => field_data.write(*v),
                            Some(ColValue::UnsignedLongLong(v)) => field_data.write(*v as i64),
                            Some(ColValue::Year(v)) => field_data.write(*v as i64),
                            Some(ColValue::Bit(v)) => field_data.write(*v as i64),
                            Some(ColValue::Set(v)) => field_data.write(*v as i64),
                            Some(ColValue::Enum(v)) => field_data.write(*v as i64),

                            Some(ColValue::Time(v)) => {
                                let timestamp = Self::time_to_long(v)?;
                                field_data.write(timestamp)
                            }

                            Some(ColValue::Date(v)) => {
                                let timestamp = Self::date_to_long(v)?;
                                field_data.write(timestamp)
                            }

                            Some(ColValue::DateTime(v)) | Some(ColValue::Timestamp(v)) => {
                                field_data.write(Self::timestamp_to_long(v)?)
                            }

                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::Decimal(..) => {
                    let field_data = root.child(i).unwrap_decimal();
                    for row_data in data.iter() {
                        match Self::get_col_value(row_data, col)? {
                            Some(ColValue::Decimal(v)) => {
                                let decimal = Decimal::from_str(v)
                                    .with_context(|| format!("invalid decimal: {}", v))?;
                                field_data.write_i128(decimal.mantissa())
                            }
                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::Float => {
                    let field_data = root.child(i).unwrap_float();
                    for row_data in data.iter() {
                        match Self::get_col_value(row_data, col)? {
                            Some(ColValue::Float(v)) => field_data.write(*v),
                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::Double => {
                    let field_data = root.child(i).unwrap_double();
                    for row_data in data.iter() {
                        match Self::get_col_value(row_data, col)? {
                            Some(ColValue::Double(v)) => field_data.write(*v),
                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::String => {
                    let field_data = root.child(i).unwrap_string();
                    for row_data in data.iter() {
                        match Self::get_col_value(row_data, col)? {
                            Some(ColValue::String(v))
                            | Some(ColValue::Set2(v))
                            | Some(ColValue::Enum2(v))
                            | Some(ColValue::Json2(v)) => field_data.write(v),
                            _ => field_data.write_null(),
                        };
                    }
                }

                Schema::Binary => {
                    let field_data = root.child(i).unwrap_binary();
                    for row_data in data.iter() {
                        match Self::get_col_value(row_data, col)? {
                            Some(ColValue::Json(v))
                            | Some(ColValue::Blob(v))
                            | Some(ColValue::RawString(v)) => field_data.write(v),

                            Some(ColValue::Bit(v)) => {
                                let bit = Self::u64_to_bytes(*v);
                                field_data.write(&bit)
                            }

                            Some(ColValue::Decimal(v)) => match tb_meta.get_col_type(col)? {
                                MysqlColType::Decimal { precision, scale } => {
                                    let latin1_data = DecimalUtil::string_to_mysql_binlog(
                                        v,
                                        *precision as usize,
                                        *scale as usize,
                                    )?;
                                    let utf8_data = UnicodeUtil::latin1_to_utf8(&latin1_data);
                                    field_data.write(&utf8_data)
                                }
                                _ => field_data.write_null(),
                            },

                            _ => field_data.write_null(),
                        };
                    }
                }
                // never happen
                _ => continue,
            }
        }

        let cdc_action = root.child(col_count).unwrap_long();
        for row_data in data.iter() {
            let action = match row_data.row_type {
                RowType::Insert => 0,
                RowType::Update => 1,
                RowType::Delete => 2,
            };
            cdc_action.write(action);
            if action != 0 {
                insert_only = false;
            }
        }

        let cdc_log_sequence = root.child(col_count + 1).unwrap_long();
        for _ in 0..data.len() {
            cdc_log_sequence.write(0);
        }

        for _ in 0..data.len() {
            root.write();
        }

        writer.write_batch(data.len() as u64)?;
        let orc_data = writer.finish()?.to_owned();

        Ok((orc_data, insert_only))
    }

    fn get_tb_orc_schema(&self, tb_meta: &MysqlTbMeta) -> anyhow::Result<(Schema, Vec<Field>)> {
        let mut fields = Vec::new();
        for col in tb_meta.basic.cols.iter() {
            let col_type = tb_meta.get_col_type(col)?;
            let schema = self.get_col_orc_schema(col_type);
            fields.push(Field(col.to_owned(), schema))
        }
        fields.push(Field(CDC_ACTION.to_owned(), Schema::Int));
        fields.push(Field(CDC_LOG_SEQUENCE.to_owned(), Schema::Long));
        Ok((Schema::Struct(fields.clone()), fields))
    }

    fn get_col_orc_schema(&self, col_type: &MysqlColType) -> Schema {
        match *col_type {
            MysqlColType::TinyInt { unsigned: _ }
            | MysqlColType::SmallInt { unsigned: _ }
            | MysqlColType::MediumInt { unsigned: _ }
            | MysqlColType::Int { unsigned: _ }
            | MysqlColType::BigInt { unsigned: _ } => Schema::Long,

            MysqlColType::Float => Schema::Float,
            MysqlColType::Double => Schema::Double,
            MysqlColType::Decimal { precision, scale } => {
                // 2^127 > 10^38
                if precision > 38 {
                    Schema::Binary
                } else {
                    Schema::Decimal(precision, scale)
                }
            }

            MysqlColType::Year => Schema::Long,
            MysqlColType::Time { .. }
            | MysqlColType::Date { .. }
            | MysqlColType::DateTime { .. }
            | MysqlColType::Timestamp { .. } => Schema::Long,

            MysqlColType::Binary { .. }
            | MysqlColType::VarBinary { .. }
            | MysqlColType::Bit
            | MysqlColType::TinyBlob
            | MysqlColType::MediumBlob
            | MysqlColType::LongBlob
            | MysqlColType::Blob
            | MysqlColType::Unknown => Schema::Binary,

            MysqlColType::Char { .. }
            | MysqlColType::Varchar { .. }
            | MysqlColType::TinyText { .. }
            | MysqlColType::MediumText { .. }
            | MysqlColType::Text { .. }
            | MysqlColType::LongText { .. } => match self.extract_type {
                ExtractType::Cdc => Schema::Binary,
                _ => Schema::String,
            },

            MysqlColType::Enum { .. } | MysqlColType::Set { .. } | MysqlColType::Json => {
                Schema::String
            }
        }
    }

    #[inline(always)]
    fn timestamp_to_long(timestamp: &str) -> anyhow::Result<i64> {
        let datetime: DateTime<Utc> = TimeUtil::datetime_from_utc_str(timestamp)?;
        let ymd = ((datetime.year() as i64 * 13 + datetime.month() as i64) << 5)
            | (datetime.day() as i64);
        let hms = ((datetime.hour() as i64) << 12)
            | ((datetime.minute() as i64) << 6)
            | (datetime.second() as i64);
        let second_part = datetime.timestamp_micros() - datetime.timestamp() * 1_000_000;
        let l = (((ymd << 17) | hms) << 24) + second_part;
        Ok(l)
    }

    #[inline(always)]
    fn date_to_long(timestamp: &str) -> anyhow::Result<i64> {
        let date = TimeUtil::date_from_str(timestamp)?;
        let ymd = ((date.year() as i64 * 13 + date.month() as i64) << 5) | (date.day() as i64);
        let l = ymd << (24 + 17);
        Ok(l)
    }

    #[inline(always)]
    fn time_to_long(time: &str) -> anyhow::Result<i64> {
        let time = DtNaiveTime::from_str(time)?;
        Ok(time.timestamp_micros())
    }

    #[inline(always)]
    fn u64_to_bytes(value: u64) -> Vec<u8> {
        // Big Endian
        let mut bytes = [0; 8];
        let mut v = value;
        let mut i: i32 = 7;
        while i >= 0 {
            bytes[i as usize] = (v & 0xFF) as u8;
            v >>= 8;
            i -= 1;
        }
        bytes.to_vec()
    }
}

// s3 functions
impl FoxlakePusher {
    async fn get_s3_file_info(&self, schema: &str, tb: &str) -> (String, String, OrcSequenceInfo) {
        let dir = self.get_s3_file_dir(schema, tb);
        // currently we do not get sequence from position
        let log_sequence = "0_0";
        let uuid = Uuid::new_v4();
        let data_file_name = format!("log_dml_{}_{}.orc", log_sequence, uuid);

        let sequence_info = self.orc_sequencer.lock().await.get_sequence();
        let width = 10;
        let orc_sequence = format!(
            "{:0>width$}_{:0>width$}",
            sequence_info.sequencer_id, sequence_info.push_sequence
        );
        let meta_file_name = format!("{}_{}", orc_sequence, &data_file_name);

        (
            format!("{}/{}", dir, data_file_name),
            format!("{}/meta/{}", dir, meta_file_name),
            sequence_info,
        )
    }

    fn get_finished_meta_info(&self, schema: &str, tb: &str) -> String {
        format!("{}/meta/finished", self.get_s3_file_dir(schema, tb),)
    }

    fn get_s3_file_dir(&self, schema: &str, tb: &str) -> String {
        let mut dir = format!("{}/{}", schema, tb);
        if !self.s3_config.root_dir.is_empty() {
            dir = format!("{}/{}", self.s3_config.root_dir, dir);
        }
        dir
    }

    async fn put_to_s3(s3_client: &Operator, key: &str, data: Vec<u8>) -> anyhow::Result<()> {
        s3_client
            .write(key, data)
            .await
            .with_context(|| format!("failed to push: {key}"))?;
        log_info!("pushed: {key}");
        Ok(())
    }
}
