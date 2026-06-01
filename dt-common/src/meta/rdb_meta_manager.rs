use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::Arc,
};

use anyhow::bail;

use crate::error::Error;

use super::{
    ddl_meta::ddl_data::DdlData, mysql::mysql_meta_manager::MysqlMetaManager,
    mysql::mysql_tb_meta::MysqlTbMeta, pg::pg_meta_manager::PgMetaManager,
    pg::pg_tb_meta::PgTbMeta, rdb_tb_meta::RdbTbMeta,
};

pub const RDB_PRIMARY_KEY_FLAG: &str = "primary";

#[derive(Clone)]
pub struct RdbMetaManager {
    pub mysql_meta_manager: Option<MysqlMetaManager>,
    pub pg_meta_manager: Option<PgMetaManager>,
}

// This is an owning handle, not a lock guard. It keeps the concrete MySQL/PG
// metadata Arc alive and derefs to the shared RdbTbMeta view.
pub enum RdbTbMetaGuard {
    Mysql(Arc<MysqlTbMeta>),
    Pg(Arc<PgTbMeta>),
}

impl RdbTbMetaGuard {
    pub fn basic(&self) -> &RdbTbMeta {
        match self {
            Self::Mysql(tb_meta) => &tb_meta.basic,
            Self::Pg(tb_meta) => &tb_meta.basic,
        }
    }
}

impl Deref for RdbTbMetaGuard {
    type Target = RdbTbMeta;

    fn deref(&self) -> &Self::Target {
        self.basic()
    }
}

impl AsRef<RdbTbMeta> for RdbTbMetaGuard {
    fn as_ref(&self) -> &RdbTbMeta {
        self.basic()
    }
}

impl RdbMetaManager {
    pub fn from_mysql(mysql_meta_manager: MysqlMetaManager) -> Self {
        Self {
            mysql_meta_manager: Some(mysql_meta_manager),
            pg_meta_manager: Option::None,
        }
    }

    pub fn from_pg(pg_meta_manager: PgMetaManager) -> Self {
        Self {
            mysql_meta_manager: Option::None,
            pg_meta_manager: Some(pg_meta_manager),
        }
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        if let Some(mysql_meta_manager) = &self.mysql_meta_manager {
            mysql_meta_manager.close().await?;
        }
        if let Some(pg_meta_manager) = &self.pg_meta_manager {
            pg_meta_manager.close().await?;
        }
        Ok(())
    }

    pub async fn get_tb_meta(&self, schema: &str, tb: &str) -> anyhow::Result<RdbTbMetaGuard> {
        if let Some(mysql_meta_manager) = self.mysql_meta_manager.as_ref() {
            let tb_meta = mysql_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok(RdbTbMetaGuard::Mysql(tb_meta));
        }

        if let Some(pg_meta_manager) = self.pg_meta_manager.as_ref() {
            let tb_meta = pg_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok(RdbTbMetaGuard::Pg(tb_meta));
        }

        bail! {Error::Unexpected(
            "no available meta_manager in partitioner".into(),
        )}
    }

    pub fn invalidate_cache_by_ddl_data(&self, ddl_data: &DdlData) {
        if let Some(mysql_meta_manager) = &self.mysql_meta_manager {
            mysql_meta_manager.invalidate_cache_by_ddl_data(ddl_data);
        }
        if let Some(pg_meta_manager) = &self.pg_meta_manager {
            pg_meta_manager.invalidate_cache_by_ddl_data(ddl_data);
        }
    }

    pub fn parse_rdb_cols(
        key_map: &HashMap<String, Vec<String>>,
        cols: &[String],
        nullable_cols: &HashSet<String>,
    ) -> anyhow::Result<(Vec<String>, String, Vec<String>)> {
        let mut id_cols = Vec::new();
        if let Some(cols) = key_map.get(RDB_PRIMARY_KEY_FLAG) {
            // use primary key
            id_cols = cols.clone();
        } else if !key_map.is_empty() {
            // use unique key
            // priority 1: use unique key with all non-nullable and least cols
            let non_nullable_keys = key_map
                .iter()
                .filter(|(_, cols)| !cols.iter().any(|col| nullable_cols.contains(col)))
                .map(|(_, cols)| cols)
                .collect::<Vec<&Vec<String>>>();

            for cols in non_nullable_keys {
                if id_cols.is_empty() || id_cols.len() > cols.len() {
                    id_cols = cols.clone();
                }
            }
            // priority 2: use unique key with nullable cols
            if id_cols.is_empty() {
                for key_cols in key_map.values() {
                    if id_cols.is_empty() || id_cols.len() > key_cols.len() {
                        id_cols = key_cols.clone();
                    }
                }
            }
        }

        let order_cols = if id_cols.is_empty() {
            Vec::new()
        } else {
            id_cols.clone()
        };

        if id_cols.is_empty() {
            id_cols = cols.to_owned();
        }

        let partition_col = id_cols[0].clone();

        Ok((order_cols, partition_col, id_cols))
    }
}
