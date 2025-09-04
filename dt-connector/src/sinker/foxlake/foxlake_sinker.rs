use std::{str::FromStr, sync::Arc};

use anyhow::Ok;
use async_trait::async_trait;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    MySql, Pool,
};

use super::{foxlake_merger::FoxlakeMerger, foxlake_pusher::FoxlakePusher};
use crate::{close_conn_pool, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};
use dt_common::{
    log_info,
    meta::{
        ddl_meta::{ddl_data::DdlData, ddl_type::DdlType},
        dt_data::{DtData, DtItem},
        mysql::mysql_meta_manager::MysqlMetaManager,
        position::Position,
    },
    monitor::monitor::Monitor,
};

pub struct FoxlakeSinker {
    pub url: String,
    pub batch_size: usize,
    pub meta_manager: MysqlMetaManager,
    pub monitor: Arc<Monitor>,
    pub conn_pool: Pool<MySql>,
    pub router: RdbRouter,
    pub pusher: FoxlakePusher,
    pub merger: FoxlakeMerger,
    pub engine: String,
}

#[async_trait]
impl Sinker for FoxlakeSinker {
    async fn sink_raw(&mut self, data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        self.batch_sink(data).await
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        for ddl_data in data {
            let mut sql = ddl_data.to_sql();
            if ddl_data.ddl_type == DdlType::CreateTable && !self.engine.is_empty() {
                sql = format!("/*+ ENGINE = {} */ {}", self.engine, sql);
            }

            log_info!(
                "sinking ddl, origin sql: {}, mapped sql: {}",
                ddl_data.query,
                sql
            );

            let (db, _tb) = ddl_data.get_schema_tb();
            let query = sqlx::query(&sql);

            // create a tmp connection with database since sqlx conn pool does NOT support `USE db`
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

            log_info!("ddl sinked");
        }
        Ok(())
    }

    async fn refresh_meta(&mut self, data: Vec<DdlData>) -> anyhow::Result<()> {
        for ddl_data in data.iter() {
            self.meta_manager.invalidate_cache_by_ddl_data(ddl_data);
            self.pusher
                .meta_manager
                .invalidate_cache_by_ddl_data(ddl_data);
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.meta_manager.close().await?;
        return close_conn_pool!(self);
    }
}

impl FoxlakeSinker {
    async fn batch_sink(&mut self, data: Vec<DtItem>) -> anyhow::Result<()> {
        // push to s3
        let (s3_file_metas, _) = self.pusher.batch_push(data, true).await?;

        // merge to foxlake
        let mut dt_items = Vec::new();
        for file_meta in s3_file_metas {
            let dt_item = DtItem {
                dt_data: DtData::Foxlake { file_meta },
                position: Position::None,
                data_origin_node: String::new(),
            };
            dt_items.push(dt_item);
        }
        let (all_data_size, all_row_count) = self.merger.batch_merge(dt_items).await?;

        BaseSinker::update_batch_monitor(&self.monitor, all_row_count as u64, all_data_size as u64)
            .await
    }
}
