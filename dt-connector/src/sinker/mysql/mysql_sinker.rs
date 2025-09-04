use std::{
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use crate::{
    call_batch_fn, close_conn_pool, data_marker::DataMarker, rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker,
};

use anyhow::Context;
use dt_common::{
    log_error, log_info,
    meta::{
        dcl_meta::dcl_data::DclData,
        ddl_meta::{ddl_data::DdlData, ddl_type::DdlType},
    },
    monitor::monitor::Monitor,
};

use dt_common::meta::{
    mysql::mysql_meta_manager::MysqlMetaManager, row_data::RowData, row_type::RowType,
};

use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    MySql, Pool,
};

use async_trait::async_trait;

#[derive(Clone)]
pub struct MysqlSinker {
    pub url: String,
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
    pub monitor: Arc<Mutex<Monitor>>,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
    pub replace: bool,
}

#[async_trait]
impl Sinker for MysqlSinker {
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
        for ddl_data in data {
            let sql = ddl_data.to_sql();
            let query = sqlx::query(&sql);
            let (db, _tb) = ddl_data.get_schema_tb();
            log_info!("sink ddl, db: {}, sql: {}", db, sql);

            // create a tmp connection with databse since sqlx conn pool does NOT support `USE db`
            let mut conn_options = MySqlConnectOptions::from_str(&self.url)?;
            if !db.is_empty() {
                match ddl_data.ddl_type {
                    DdlType::CreateDatabase | DdlType::DropDatabase | DdlType::AlterDatabase => {}
                    _ => {
                        conn_options = conn_options.database(&db);
                    }
                }
            }

            let conn_pool = MySqlPoolOptions::new()
                .max_connections(1)
                .connect_with(conn_options)
                .await?;
            query.execute(&conn_pool).await?;
            conn_pool.close().await;
        }
        Ok(())
    }

    async fn sink_dcl(&mut self, data: Vec<DclData>, _batch: bool) -> anyhow::Result<()> {
        for dcl_data in data {
            let sql = dcl_data.to_sql();
            log_info!("sink dcl: {}", &sql);
            let query = sqlx::query(&sql).persistent(false);
            query.execute(&self.conn_pool).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.meta_manager.close().await?;
        return close_conn_pool!(self);
    }

    async fn refresh_meta(&mut self, data: Vec<DdlData>) -> anyhow::Result<()> {
        for ddl_data in data.iter() {
            self.meta_manager.invalidate_cache_by_ddl_data(ddl_data);
        }
        Ok(())
    }
}

impl MysqlSinker {
    async fn serial_sink(&mut self, data: &[RowData]) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let mut data_size = 0;

        let mut tx = self.conn_pool.begin().await?;
        if let Some(sql) = self.get_data_marker_sql() {
            sqlx::query(&sql)
                .execute(tx.as_mut())
                .await
                .with_context(|| format!("failed to execute data marker sql: [{}]", sql))?;
        }
        for row_data in data.iter() {
            data_size += row_data.data_size;
            let tb_meta = self.meta_manager.get_tb_meta_by_row_data(row_data).await?;
            let query_builder = RdbQueryBuilder::new_for_mysql(tb_meta, None);

            let query_info = query_builder.get_query_info(row_data, self.replace)?;
            let query = query_builder.create_mysql_query(&query_info);
            query
                .execute(tx.as_mut())
                .await
                .with_context(|| format!("serial sink failed, row_data: [{}]", row_data))?;
        }
        tx.commit().await?;

        BaseSinker::update_serial_monitor(&mut self.monitor, data.len(), data_size, start_time)
    }

    async fn batch_delete(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();

        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_row_data(&data[0])
            .await?
            .to_owned();
        let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta, None);
        let (query_info, data_size) =
            query_builder.get_batch_delete_query(data, start_index, batch_size)?;
        let query = query_builder.create_mysql_query(&query_info);

        if let Some(sql) = self.get_data_marker_sql() {
            let mut tx = self.conn_pool.begin().await?;
            sqlx::query(&sql).execute(tx.as_mut()).await?;
            query.execute(tx.as_mut()).await?;
            tx.commit().await?;
        } else {
            query.execute(&self.conn_pool).await?;
        }

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time)
    }

    async fn batch_insert(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();

        let tb_meta = self
            .meta_manager
            .get_tb_meta_by_row_data(&data[0])
            .await?
            .to_owned();
        let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta, None);

        let (query_info, data_size) =
            query_builder.get_batch_insert_query(data, start_index, batch_size, self.replace)?;
        let query = query_builder.create_mysql_query(&query_info);

        let exec_error = if let Some(sql) = self.get_data_marker_sql() {
            let mut tx = self.conn_pool.begin().await?;
            sqlx::query(&sql).execute(tx.as_mut()).await?;
            query.execute(tx.as_mut()).await?;
            match tx.commit().await {
                Err(e) => Some(e),
                _ => None,
            }
        } else {
            match query.execute(&self.conn_pool).await {
                Err(e) => Some(e),
                _ => None,
            }
        };

        if let Some(error) = exec_error {
            log_error!(
                "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                tb_meta.basic.schema,
                tb_meta.basic.tb,
                error.to_string()
            );
            // insert one by one
            let sub_data = &data[start_index..start_index + batch_size];
            self.serial_sink(sub_data).await?;
        }

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time)
    }

    fn get_data_marker_sql(&self) -> Option<String> {
        if let Some(data_marker) = &self.data_marker {
            let data_marker = data_marker.read().unwrap();
            // CREATE TABLE `ape_trans_mysql`.`topo1` (
            //     `data_origin_node` varchar(255) NOT NULL,
            //     `src_node` varchar(255) NOT NULL,
            //     `dst_node` varchar(255) NOT NULL,
            //     `n` bigint DEFAULT NULL,
            //     PRIMARY KEY (`data_origin_node`, `src_node`, `dst_node`)
            // ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
            let sql = format!(
                "INSERT INTO `{}`.`{}`(data_origin_node, src_node, dst_node, n) 
                VALUES('{}', '{}', '{}', 1) 
                ON DUPLICATE KEY UPDATE n=n+1",
                data_marker.marker_schema,
                data_marker.marker_tb,
                data_marker.data_origin_node,
                data_marker.src_node,
                data_marker.dst_node
            );
            Some(sql)
        } else {
            None
        }
    }
}
