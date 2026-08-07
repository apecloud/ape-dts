use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::meta::{ddl_meta::ddl_data::DdlData, row_data::RowData};

use super::{mssql_connection_pool::MssqlConnectionPool, mssql_tb_meta::MssqlTbMeta};

#[derive(Clone)]
pub struct MssqlMetaManager {
    pub connection_pool: MssqlConnectionPool,
    cache: Arc<RwLock<HashMap<(String, String), MssqlTbMeta>>>,
}

impl MssqlMetaManager {
    pub async fn new(connection_pool: MssqlConnectionPool) -> anyhow::Result<Self> {
        Ok(Self {
            connection_pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn get_tb_meta(&self, schema: &str, tb: &str) -> anyhow::Result<MssqlTbMeta> {
        if let Some(tb_meta) = self
            .cache
            .read()
            .expect("mssql metadata cache lock poisoned")
            .get(&(schema.to_string(), tb.to_string()))
            .cloned()
        {
            return Ok(tb_meta);
        }
        todo!("mssql table metadata catalog query is not implemented")
    }

    pub async fn get_tb_meta_by_row_data(&self, row_data: &RowData) -> anyhow::Result<MssqlTbMeta> {
        self.get_tb_meta(&row_data.schema, &row_data.tb).await
    }

    pub async fn list_schemas(&self) -> anyhow::Result<Vec<String>> {
        todo!("mssql schema catalog query is not implemented")
    }

    pub async fn list_tables(&self, _schema: &str) -> anyhow::Result<Vec<String>> {
        todo!("mssql table catalog query is not implemented")
    }

    pub fn invalidate_cache(&self, schema: &str, tb: &str) {
        self.cache
            .write()
            .expect("mssql metadata cache lock poisoned")
            .remove(&(schema.to_string(), tb.to_string()));
    }

    pub fn invalidate_cache_for_table(&self, schema: &str, tb: &str) {
        if !schema.is_empty() && !tb.is_empty() {
            self.invalidate_cache(schema, tb);
        }
    }

    pub fn invalidate_cache_by_ddl_data(&self, ddl_data: &DdlData) {
        let (schema, tb) = ddl_data.get_schema_tb();
        self.invalidate_cache(&schema, &tb);
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
