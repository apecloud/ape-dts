use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::UNIX_EPOCH,
};

use anyhow::bail;
use async_trait::async_trait;
use futures::StreamExt;
use postgres_protocol::message::backend::{
    DeleteBody, InsertBody,
    LogicalReplicationMessage::{
        Begin, Commit, Delete, Insert, Origin, Relation, Truncate, Type, Update,
    },
    RelationBody,
    ReplicationMessage::*,
    TupleData, UpdateBody,
};
use postgres_types::PgLsn;
use sqlx::{postgres::PgArguments, query::Query, Pool, Postgres};
use tokio::{sync::Mutex, time::Duration, time::Instant};
use tokio_postgres::replication::LogicalReplicationStream;

use crate::{
    close_conn_pool,
    extractor::{
        base_extractor::BaseExtractor, pg::pg_cdc_client::PgCdcClient, resumer::recovery::Recovery,
    },
    Extractor,
};
use dt_common::{
    config::{config_enums::DbType, config_token_parser::ConfigTokenParser},
    error::Error,
    log_error, log_info, log_warn,
    meta::{
        adaptor::pg_col_value_convertor::PgColValueConvertor,
        col_value::ColValue,
        dt_data::DtData,
        pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        position::Position,
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
        row_type::RowType,
        syncer::Syncer,
    },
    rdb_filter::RdbFilter,
    utils::time_util::TimeUtil,
};

pub struct PgCdcExtractor {
    pub base_extractor: BaseExtractor,
    pub meta_manager: PgMetaManager,
    pub conn_pool: Pool<Postgres>,
    pub filter: RdbFilter,
    pub url: String,
    pub slot_name: String,
    pub pub_name: String,
    pub start_lsn: String,
    pub recreate_slot_if_exists: bool,
    pub keepalive_interval_secs: u64,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_tb: String,
    pub ddl_meta_tb: String,
    pub syncer: Arc<Mutex<Syncer>>,
    pub recovery: Option<Arc<dyn Recovery + Send + Sync>>,
}

const SECS_FROM_1970_TO_2000: i64 = 946_684_800;

#[async_trait]
impl Extractor for PgCdcExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        if let Some(recovery) = &self.recovery {
            if let Some(position) = recovery.get_cdc_resume_position().await {
                match &position {
                    Position::PgCdc { lsn, .. } => {
                        self.start_lsn = lsn.to_owned();
                        log_info!("cdc recovery from lsn:[{}]", lsn);
                        self.base_extractor
                            .push_dt_data(DtData::Heartbeat {}, position)
                            .await?;
                    }
                    _ => {
                        log_warn!("position:{} is not a valid pg cdc position", position);
                    }
                }
            }
        }

        log_info!(
            "PgCdcExtractor starts, slot_name: {}, start_lsn: {}, keepalive_interval_secs: {}, heartbeat_interval_secs: {}, heartbeat_tb: {}, ddl_meta_tb: {}",
            self.slot_name,
            self.start_lsn,
            self.keepalive_interval_secs,
            self.heartbeat_interval_secs,
            self.heartbeat_tb,
            self.ddl_meta_tb,
        );
        self.extract_internal().await?;
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.meta_manager.close().await?;
        close_conn_pool!(self)
    }
}

impl PgCdcExtractor {
    async fn extract_internal(&mut self) -> anyhow::Result<()> {
        let mut cdc_client = PgCdcClient {
            url: self.url.clone(),
            pub_name: self.pub_name.clone(),
            slot_name: self.slot_name.clone(),
            start_lsn: self.start_lsn.clone(),
            recreate_slot_if_exists: self.recreate_slot_if_exists,
        };
        let (stream, actual_start_lsn) = cdc_client.connect().await?;
        tokio::pin!(stream);

        // setup ddl capture
        let ddl_meta =
            ConfigTokenParser::parse_config(&self.ddl_meta_tb, &DbType::Pg, &['.'], None)?;
        if ddl_meta.len() == 2 {
            self.filter.add_do_tb(&ddl_meta[0], &ddl_meta[1]);
        }

        // start heartbeat
        self.start_heartbeat(self.base_extractor.shut_down.clone())?;

        let mut last_tx_end_lsn = actual_start_lsn.clone();
        let mut xid = String::new();
        let mut start_time = Instant::now();

        let get_position = |lsn: &str, timestamp: i64| -> Position {
            Position::PgCdc {
                lsn: lsn.into(),
                timestamp: Position::format_timestamp_millis(
                    timestamp / 1000 + SECS_FROM_1970_TO_2000 * 1000,
                ),
            }
        };
        let mut position: Position = get_position("", 0);

        // refer: https://www.postgresql.org/docs/10/protocol-replication.html to get WAL data details
        loop {
            if self.base_extractor.time_filter.ended {
                // cdc stream will be dropped automatically if postgres receives no keepalive ack
                return Ok(());
            }

            if start_time.elapsed().as_secs() >= self.keepalive_interval_secs {
                self.keep_alive_ack(&mut stream, &actual_start_lsn).await?;
                start_time = Instant::now();
            }

            match stream.next().await {
                Some(Ok(XLogData(body))) => {
                    let data = body.into_data();
                    match data {
                        Relation(relation) => {
                            self.decode_relation(&relation).await?;
                        }

                        // do not push Begin into buffer to accelerate sinking
                        Begin(begin) => {
                            position = get_position(&last_tx_end_lsn, begin.timestamp());
                            xid = begin.xid().to_string();

                            let timestamp = begin.timestamp() / 1_000_000 + SECS_FROM_1970_TO_2000;
                            BaseExtractor::update_time_filter(
                                &mut self.base_extractor.time_filter,
                                timestamp as u32,
                                &position,
                            );
                        }

                        Commit(commit) => {
                            last_tx_end_lsn = PgLsn::from(commit.end_lsn()).to_string();
                            position = get_position(&last_tx_end_lsn, commit.timestamp());
                            let commit = DtData::Commit { xid: xid.clone() };
                            self.base_extractor
                                .push_dt_data(commit, position.clone())
                                .await?;
                        }

                        Origin(_origin) => {}

                        Truncate(_truncate) => {}

                        Type(_typee) => {}

                        Insert(insert) => {
                            if self.base_extractor.time_filter.started {
                                self.decode_insert(&insert, &position, &ddl_meta).await?;
                            }
                        }

                        Update(update) => {
                            if self.base_extractor.time_filter.started {
                                self.decode_update(&update, &position).await?;
                            }
                        }

                        Delete(delete) => {
                            if self.base_extractor.time_filter.started {
                                self.decode_delete(&delete, &position).await?;
                            }
                        }

                        _ => {}
                    }
                }

                Some(Ok(PrimaryKeepAlive(data))) => {
                    // Send a standby status update and require a keep alive response
                    if data.reply() == 1 {
                        self.keep_alive_ack(&mut stream, &actual_start_lsn).await?;
                        start_time = Instant::now();
                    }
                }

                Some(Ok(data)) => {
                    log_info!("received unknown replication data: {:?}", data);
                }

                Some(Err(error)) => panic!("unexpected replication stream error: {}", error),

                None => panic!("unexpected replication stream end"),
            }
        }
    }

    async fn keep_alive_ack(
        &mut self,
        stream: &mut Pin<&mut LogicalReplicationStream>,
        start_lsn: &str,
    ) -> anyhow::Result<()> {
        let lsn: PgLsn =
            if let Position::PgCdc { lsn, .. } = &self.syncer.lock().await.committed_position {
                if lsn.is_empty() {
                    start_lsn.parse().unwrap()
                } else {
                    lsn.parse().unwrap()
                }
            } else {
                start_lsn.parse().unwrap()
            };
        log_info!("confirmed flush lsn: {}", lsn.to_string());

        // Postgres epoch is 2000-01-01T00:00:00Z
        let pg_epoch = UNIX_EPOCH + Duration::from_secs(SECS_FROM_1970_TO_2000 as u64);
        let ts = pg_epoch.elapsed()?.as_micros() as i64;
        let result = stream
            .as_mut()
            .standby_status_update(lsn, lsn, lsn, ts, 1)
            .await;
        if let Err(error) = result {
            log_error!(
                "keepalive ack to postgres failed, lsn: {}, error: {}",
                lsn.to_string(),
                error
            );
        }
        Ok(())
    }

    async fn decode_relation(&mut self, event: &RelationBody) -> anyhow::Result<()> {
        let schema = event.namespace()?;
        let tb = event.name()?;
        // if the tb is filtered, we won't try to get the tb_meta since we may get privilege errors,
        // but we need to keep the oid —— tb_meta map which may be used for decoding events,
        // the built-in object used by datamarker, although it is not in filter config, still needs to get tb_meta.
        if self.filter.filter_tb(schema, tb) && !self.base_extractor.is_data_marker_info(schema, tb)
        {
            let tb_meta = Self::mock_pg_tb_meta(schema, tb, event.rel_id() as i32);
            self.meta_manager
                .update_tb_meta_by_oid(event.rel_id() as i32, tb_meta)?;
            return Ok(());
        }

        // todo, use event.rel_id()
        let mut tb_meta = self.meta_manager.get_tb_meta(schema, tb).await?.to_owned();
        let mut col_names = Vec::new();
        for column in event.columns() {
            // todo: check type_id in oid_to_type
            let col_type = self
                .meta_manager
                .type_registry
                .oid_to_type
                .get(&column.type_id())
                .unwrap();
            let col_name = column.name()?;
            // update meta
            tb_meta
                .col_type_map
                .insert(col_name.to_string(), col_type.clone());

            col_names.push(col_name.to_string());
        }

        // align the column order of tb_meta to that of the wal log
        tb_meta.basic.cols = col_names;
        self.meta_manager
            .update_tb_meta_by_oid(event.rel_id() as i32, tb_meta)?;
        Ok(())
    }

    async fn decode_insert(
        &mut self,
        event: &InsertBody,
        position: &Position,
        ddl_meta: &[String],
    ) -> anyhow::Result<()> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_oid(event.rel_id() as i32)?;
        if self.filter_event(&tb_meta, RowType::Insert) {
            return Ok(());
        }

        let col_values = self.parse_row_data(&tb_meta, event.tuple().tuple_data())?;
        let row_data = RowData::new(
            tb_meta.basic.schema,
            tb_meta.basic.tb,
            RowType::Insert,
            None,
            Some(col_values),
        );

        if ddl_meta.len() == 2 && row_data.schema == ddl_meta[0] && row_data.tb == ddl_meta[1] {
            return self.decode_ddl(&row_data, position).await;
        }

        self.push_row_to_buf(row_data, position.clone()).await
    }

    async fn decode_update(
        &mut self,
        event: &UpdateBody,
        position: &Position,
    ) -> anyhow::Result<()> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_oid(event.rel_id() as i32)?;
        if self.filter_event(&tb_meta, RowType::Update) {
            return Ok(());
        }

        let basic = &tb_meta.basic;
        let col_values_after = self.parse_row_data(&tb_meta, event.new_tuple().tuple_data())?;
        let col_values_before = if let Some(old_tuple) = event.old_tuple() {
            self.parse_row_data(&tb_meta, old_tuple.tuple_data())?
        } else if let Some(key_tuple) = event.key_tuple() {
            self.parse_row_data(&tb_meta, key_tuple.tuple_data())?
        } else if !basic.id_cols.is_empty() {
            let mut col_values_tmp = HashMap::new();
            for col in basic.id_cols.iter() {
                col_values_tmp.insert(col.to_string(), col_values_after.get(col).unwrap().clone());
            }
            col_values_tmp
        } else {
            HashMap::new()
        };

        let row_data = RowData::new(
            basic.schema.clone(),
            basic.tb.clone(),
            RowType::Update,
            Some(col_values_before),
            Some(col_values_after),
        );
        self.push_row_to_buf(row_data, position.clone()).await
    }

    async fn decode_delete(
        &mut self,
        event: &DeleteBody,
        position: &Position,
    ) -> anyhow::Result<()> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_oid(event.rel_id() as i32)?;
        if self.filter_event(&tb_meta, RowType::Delete) {
            return Ok(());
        }

        let col_values = if let Some(old_tuple) = event.old_tuple() {
            self.parse_row_data(&tb_meta, old_tuple.tuple_data())?
        } else if let Some(key_tuple) = event.key_tuple() {
            self.parse_row_data(&tb_meta, key_tuple.tuple_data())?
        } else {
            HashMap::new()
        };

        let row_data = RowData::new(
            tb_meta.basic.schema,
            tb_meta.basic.tb,
            RowType::Delete,
            Some(col_values),
            None,
        );
        self.push_row_to_buf(row_data, position.clone()).await
    }

    async fn decode_ddl(&mut self, row_data: &RowData, position: &Position) -> anyhow::Result<()> {
        if self.filter.filter_all_ddl() {
            return Ok(());
        }

        let get_string = |row_data: &RowData, col: &str| -> String {
            if let Some(col_value) = row_data.after.as_ref().unwrap().get(col) {
                if let Some(str) = col_value.to_option_string() {
                    return str;
                }
            }
            String::new()
        };

        // CREATE TABLE public.ape_dts_ddl_command
        // (
        //     ddl_text text COLLATE pg_catalog."default",
        //    id bigserial primary key,
        //    event text COLLATE pg_catalog."default",
        //    tag text COLLATE pg_catalog."default",
        //    username character varying COLLATE pg_catalog."default",
        //    database character varying COLLATE pg_catalog."default",
        //    schema character varying COLLATE pg_catalog."default",
        //    object_type character varying COLLATE pg_catalog."default",
        //    object_name character varying COLLATE pg_catalog."default",
        //    client_address character varying COLLATE pg_catalog."default",
        //    client_port integer,
        //    event_time timestamp with time zone,
        //    txid_current character varying(128) COLLATE pg_catalog."default",
        //    message text COLLATE pg_catalog."default"
        // );
        let ddl_text = get_string(row_data, "ddl_text");
        let _tag = get_string(row_data, "tag");
        let schema = get_string(row_data, "schema");

        if let Ok(Some(ddl_data)) = self
            .base_extractor
            .parse_ddl(&DbType::Pg, &schema, &ddl_text)
            .await
        {
            for ddl_data in ddl_data.split_to_multi() {
                // invalidate metadata cache
                self.meta_manager.invalidate_cache_by_ddl_data(&ddl_data);
                let (schema, tb) = ddl_data.get_schema_tb();

                if !self.filter.filter_ddl(&schema, &tb, &ddl_data.ddl_type) {
                    self.base_extractor
                        .push_ddl(ddl_data, position.clone())
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn parse_row_data(
        &mut self,
        tb_meta: &PgTbMeta,
        tuple_data: &[TupleData],
    ) -> anyhow::Result<HashMap<String, ColValue>> {
        let ignore_cols = self
            .filter
            .get_ignore_cols(&tb_meta.basic.schema, &tb_meta.basic.tb);
        let mut col_values: HashMap<String, ColValue> = HashMap::new();
        for i in 0..tuple_data.len() {
            let tuple_data = &tuple_data[i];
            let col = &tb_meta.basic.cols[i];
            if ignore_cols.is_some_and(|cols| cols.contains(col)) {
                continue;
            }

            let col_type = tb_meta.get_col_type(col)?;
            match tuple_data {
                TupleData::Null => {
                    col_values.insert(col.to_string(), ColValue::None);
                }

                TupleData::Text(value) => {
                    let col_value =
                        PgColValueConvertor::from_wal(col_type, value, &mut self.meta_manager)?;
                    col_values.insert(col.to_string(), col_value);
                }

                TupleData::UnchangedToast => {
                    bail! {Error::ExtractorError(
                        "unexpected UnchangedToast value received".into(),
                    )}
                }
            }
        }
        Ok(col_values)
    }

    async fn push_row_to_buf(
        &mut self,
        row_data: RowData,
        position: Position,
    ) -> anyhow::Result<()> {
        self.base_extractor.push_row(row_data, position).await
    }

    fn filter_event(&mut self, tb_meta: &PgTbMeta, row_type: RowType) -> bool {
        let schema = &tb_meta.basic.schema;
        let tb = &tb_meta.basic.tb;
        let filtered = self.filter.filter_event(schema, tb, &row_type);
        if filtered {
            return !self.base_extractor.is_data_marker_info(schema, tb);
        }
        filtered
    }

    fn mock_pg_tb_meta(schema: &str, tb: &str, oid: i32) -> PgTbMeta {
        PgTbMeta {
            basic: RdbTbMeta {
                schema: schema.into(),
                tb: tb.into(),
                ..Default::default()
            },
            oid,
            col_type_map: HashMap::new(),
        }
    }

    fn start_heartbeat(&mut self, shut_down: Arc<AtomicBool>) -> anyhow::Result<()> {
        let schema_tb = self.base_extractor.precheck_heartbeat(
            self.heartbeat_interval_secs,
            &self.heartbeat_tb,
            DbType::Pg,
        );
        if schema_tb.len() != 2 {
            return Ok(());
        }

        self.filter.add_ignore_tb(&schema_tb[0], &schema_tb[1]);

        let (slot_name, heartbeat_interval_secs, syncer, conn_pool) = (
            self.slot_name.clone(),
            self.heartbeat_interval_secs,
            self.syncer.clone(),
            self.conn_pool.clone(),
        );
        tokio::spawn(async move {
            let mut start_time = Instant::now();
            while !shut_down.load(Ordering::Acquire) {
                if start_time.elapsed().as_secs() >= heartbeat_interval_secs {
                    Self::heartbeat(
                        &slot_name,
                        &schema_tb[0],
                        &schema_tb[1],
                        &syncer,
                        &conn_pool,
                    )
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
        slot_name: &str,
        schema: &str,
        tb: &str,
        syncer: &Arc<Mutex<Syncer>>,
        conn_pool: &Pool<Postgres>,
    ) -> anyhow::Result<()> {
        let (received_lsn, received_timestamp) =
            if let Position::PgCdc { lsn, timestamp } = &syncer.lock().await.received_position {
                (lsn.clone(), timestamp.clone())
            } else {
                (String::new(), String::new())
            };

        let (flushed_lsn, flushed_timetimestamp) =
            if let Position::PgCdc { lsn, timestamp } = &syncer.lock().await.committed_position {
                (lsn.clone(), timestamp.clone())
            } else {
                (String::new(), String::new())
            };

        // create table ape_dts_heartbeat(
        //     slot_name character varying(64) not null,
        //     update_timestamp timestamp without time zone default (now() at time zone 'utc'),
        //     received_lsn character varying(64),
        //     received_timestamp character varying(64),
        //     flushed_lsn character varying(64),
        //     flushed_timestamp character varying(64),
        //     primary key(slot_name)
        // );
        let sql = format!(
            r#"INSERT INTO "{}"."{}" (slot_name, update_timestamp, received_lsn, received_timestamp, flushed_lsn, flushed_timestamp) 
                VALUES ('{}', now(), '{}', '{}', '{}', '{}')
                ON CONFLICT (slot_name) DO UPDATE 
                SET update_timestamp = now(),
                    received_lsn = '{}', 
                    received_timestamp = '{}',
                    flushed_lsn = '{}', 
                    flushed_timestamp = '{}'"#,
            schema,
            tb,
            slot_name,
            received_lsn,
            received_timestamp,
            flushed_lsn,
            flushed_timetimestamp,
            received_lsn,
            received_timestamp,
            flushed_lsn,
            flushed_timetimestamp
        );

        let query: Query<Postgres, PgArguments> = sqlx::query(&sql);
        if let Err(err) = query.execute(conn_pool).await {
            log_error!("heartbeat failed: {:?}", err);
        }
        Ok(())
    }
}
