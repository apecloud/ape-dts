use std::{cmp, str::FromStr, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Executor, Pool, Postgres,
};
use tokio::{sync::RwLock, time::Instant};

use crate::{
    call_batch_fn, data_marker::DataMarker, rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker,
};
use dt_common::{
    log_error, log_info, log_sql,
    meta::{
        ddl_meta::{ddl_data::DdlData, ddl_type::DdlType},
        pg::pg_meta_manager::PgMetaManager,
        row_data::RowData,
        row_type::RowType,
    },
    monitor::monitor::Monitor,
    utils::limit_queue::LimitedQueue,
};

#[derive(Clone)]
pub struct PgSinker {
    pub url: String,
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
    pub monitor: Arc<Monitor>,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
    pub replace: bool,
    pub monitor_interval: u64,
}

#[async_trait]
impl Sinker for PgSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_sink(&data).await?;
        } else {
            match data[0].row_type {
                RowType::Insert => {
                    call_batch_fn!(self, data, Self::batch_insert);
                }
                RowType::Delete => {
                    call_batch_fn!(self, data, Self::batch_delete);
                }
                _ => self.serial_sink(&data).await?,
            }
        }
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        let mut rts = LimitedQueue::new(cmp::min(100, data.len()));
        let monitor_interval = if self.monitor_interval > 0 {
            self.monitor_interval
        } else {
            10
        };
        let mut data_size = 0;
        let mut data_len = 0;
        let mut last_monitor_time = Instant::now();

        for ddl_data in data.iter() {
            let (schema, _tb) = ddl_data.get_schema_tb();
            data_size += ddl_data.get_data_size();
            data_len += 1;
            let conn_options = PgConnectOptions::from_str(&self.url)?;
            let mut pool_options = PgPoolOptions::new().max_connections(1);
            let sql = format!("SET search_path = '{}';", schema);

            if !schema.is_empty() {
                match ddl_data.ddl_type {
                    DdlType::CreateSchema | DdlType::DropSchema | DdlType::AlterSchema => {}
                    _ => {
                        pool_options = pool_options.after_connect(move |conn, _meta| {
                            let sql = sql.clone();
                            Box::pin(async move {
                                conn.execute(sql.as_str()).await?;
                                Ok(())
                            })
                        });
                    }
                }
            }

            let sql = ddl_data.to_sql();
            log_info!("sink ddl, schema: {}, sql: {}", schema, sql);

            let start_time = Instant::now();

            let conn_pool = pool_options.connect_with(conn_options).await?;
            let query = sqlx::query(&sql);
            query.execute(&conn_pool).await?;

            rts.push((start_time.elapsed().as_millis() as u64, 1));
            conn_pool.close().await;

            if last_monitor_time.elapsed().as_secs() >= monitor_interval {
                BaseSinker::update_serial_monitor(&self.monitor, data_len as u64, data_size)
                    .await?;
                BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;
                rts.clear();
                data_size = 0;
                data_len = 0;
                last_monitor_time = Instant::now();
            }
        }

        if data_len > 0 || data_size > 0 {
            BaseSinker::update_serial_monitor(&self.monitor, data_len as u64, data_size).await?;
            BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;
        }
        Ok(())
    }

    async fn refresh_meta(&mut self, data: Vec<DdlData>) -> anyhow::Result<()> {
        for ddl_data in data.iter() {
            self.meta_manager.invalidate_cache_by_ddl_data(ddl_data);
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl PgSinker {
    async fn serial_sink(&mut self, data: &[RowData]) -> anyhow::Result<()> {
        let monitor_interval = if self.monitor_interval > 0 {
            self.monitor_interval
        } else {
            10
        };
        let mut data_size = 0;
        let mut data_len = 0;
        let mut last_monitor_time = Instant::now();

        let mut tx = self.conn_pool.begin().await?;
        if let Some(sql) = self.get_data_marker_sql().await {
            sqlx::query(&sql)
                .execute(&mut tx)
                .await
                .with_context(|| format!("failed to execute data marker sql: [{}]", sql))?;
        }
        let mut rts = LimitedQueue::new(cmp::min(100, data.len()));
        for row_data in data.iter() {
            data_size += row_data.data_size;

            let tb_meta = self.meta_manager.get_tb_meta_by_row_data(row_data).await?;
            let query_builder = RdbQueryBuilder::new_for_pg(tb_meta, None);

            let query_info = query_builder.get_query_info(row_data, self.replace)?;
            let query = query_builder.create_pg_query(&query_info);

            let start_time = Instant::now();
            query
                .execute(&mut tx)
                .await
                .with_context(|| format!("serial sink failed, row_data: [{}]", row_data))?;

            rts.push((start_time.elapsed().as_millis() as u64, 1));
            if last_monitor_time.elapsed().as_secs() >= monitor_interval {
                BaseSinker::update_serial_monitor(&self.monitor, data_len as u64, data_size as u64)
                    .await?;
                BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;
                rts.clear();
                data_size = 0;
                data_len = 0;
                last_monitor_time = Instant::now();
            }
        }
        tx.commit().await?;

        if data_len > 0 || data_size > 0 {
            BaseSinker::update_serial_monitor(&self.monitor, data_len as u64, data_size as u64)
                .await?;
            BaseSinker::update_monitor_rt(&self.monitor, &rts).await?;
        }
        Ok(())
    }

    async fn batch_delete(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(tb_meta, None);

        let (query_info, data_size) =
            query_builder.get_batch_delete_query(data, start_index, batch_size)?;
        let query = query_builder.create_pg_query(&query_info);

        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        if let Some(sql) = self.get_data_marker_sql().await {
            let mut tx = self.conn_pool.begin().await?;
            sqlx::query(&sql).execute(&mut tx).await?;
            query.execute(&mut tx).await?;
            tx.commit().await?;
        } else {
            query.execute(&self.conn_pool).await?;
        }
        rts.push((start_time.elapsed().as_millis() as u64, 1));

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, data_size as u64)
            .await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }

    async fn batch_insert(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_row_data(&data[0])
            .await?
            .to_owned();
        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta, None);

        let (query_info, data_size) =
            query_builder.get_batch_insert_query(data, start_index, batch_size, self.replace)?;
        let query = query_builder.create_pg_query(&query_info);
        log_sql!(
            "batch insert sql: {}, batch_size: {}",
            query_info.sql,
            batch_size
        );
        let start_time = Instant::now();
        let mut rts = LimitedQueue::new(1);
        let exec_error = if let Some(sql) = self.get_data_marker_sql().await {
            let mut tx = self.conn_pool.begin().await?;
            sqlx::query(&sql).execute(&mut tx).await?;
            query.execute(&mut tx).await?;
            tx.commit().await
        } else {
            match query.execute(&self.conn_pool).await {
                Err(e) => Err(e),
                _ => Ok(()),
            }
        };

        if let Err(error) = exec_error {
            log_error!(
                "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                tb_meta.basic.schema,
                tb_meta.basic.tb,
                error.to_string()
            );
            let sub_data = &data[start_index..start_index + batch_size];
            self.serial_sink(sub_data).await?;
        } else {
            rts.push((start_time.elapsed().as_millis() as u64, 1));
        }

        BaseSinker::update_batch_monitor(&self.monitor, batch_size as u64, data_size as u64)
            .await?;
        BaseSinker::update_monitor_rt(&self.monitor, &rts).await
    }

    async fn get_data_marker_sql(&self) -> Option<String> {
        if let Some(data_marker) = &self.data_marker {
            let data_marker = data_marker.read().await;
            // CREATE TABLE ape_trans_pg.topo1 (
            //     data_origin_node varchar(255) NOT NULL,
            //     src_node varchar(255) NOT NULL,
            //     dst_node varchar(255) NOT NULL,
            //     n bigint DEFAULT NULL,
            //     PRIMARY KEY (data_origin_node, src_node, dst_node)
            //   );
            let sql = format!(
                r#"INSERT INTO "{}"."{}"(data_origin_node, src_node, dst_node, n)
                VALUES('{}', '{}', '{}', 1) 
                ON CONFLICT (data_origin_node, src_node, dst_node) 
                DO UPDATE SET n="{}"."{}".n+1"#,
                data_marker.marker_schema,
                data_marker.marker_tb,
                data_marker.data_origin_node,
                data_marker.src_node,
                data_marker.dst_node,
                data_marker.marker_schema,
                data_marker.marker_tb,
            );
            Some(sql)
        } else {
            None
        }
    }
}
