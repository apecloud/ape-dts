use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};
use std::collections::{HashMap, HashSet};

use crate::{
    meta_fetcher::mysql::mysql_struct_fetcher::MysqlStructFetcher,
    rdb_query_builder::RdbQueryBuilder,
    sinker::base_checker::{BaseChecker, CheckerBackend, CheckerCommon},
    Sinker,
};
use dt_common::meta::{
    mysql::mysql_meta_manager::MysqlMetaManager,
    row_data::RowData,
    struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
};

#[derive(Clone)]
pub struct MysqlChecker {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub common: CheckerCommon,
}

#[async_trait]
impl Sinker for MysqlChecker {
    async fn sink_dml(&mut self, data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        let mut backend = self.clone();
        BaseChecker::standard_sink_dml(&mut backend, &mut self.common, data, batch).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        BaseChecker::standard_close(&mut self.common).await
    }

    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let mut backend = self.clone();
        BaseChecker::standard_sink_struct(&mut backend, &mut self.common, data).await
    }
}

#[async_trait]
impl CheckerBackend for MysqlChecker {
    async fn get_tb_meta_by_row(
        &mut self,
        row: &RowData,
    ) -> anyhow::Result<crate::sinker::base_checker::CheckerTbMeta> {
        let meta = self
            .meta_manager
            .get_tb_meta_by_row_data(row)
            .await?
            .clone();
        Ok(crate::sinker::base_checker::CheckerTbMeta::Mysql(meta))
    }

    async fn fetch_single(
        &self,
        tb_meta: &crate::sinker::base_checker::CheckerTbMeta,
        row: &RowData,
    ) -> anyhow::Result<Option<RowData>> {
        match tb_meta {
            crate::sinker::base_checker::CheckerTbMeta::Mysql(mysql_meta) => {
                let qb = RdbQueryBuilder::new_for_mysql(mysql_meta, None);
                let q_info = qb.get_select_query(row)?;
                let query = qb.create_mysql_query(&q_info)?;
                let mut rows = query.fetch(&self.conn_pool);
                if let Some(row) = rows.try_next().await? {
                    let row_data = RowData::from_mysql_row(&row, mysql_meta, &None);
                    Ok(Some(row_data))
                } else {
                    Ok(None)
                }
            }
            _ => anyhow::bail!("Expected Mysql metadata for MysqlChecker"),
        }
    }

    async fn fetch_batch(
        &self,
        tb_meta: &crate::sinker::base_checker::CheckerTbMeta,
        _schema: &str,
        _tb: &str,
        data: &[RowData],
    ) -> anyhow::Result<HashMap<u128, RowData>> {
        match tb_meta {
            crate::sinker::base_checker::CheckerTbMeta::Mysql(mysql_meta) => {
                let qb = RdbQueryBuilder::new_for_mysql(mysql_meta, None);
                let query_info = qb.get_batch_select_query(data, 0, data.len())?;
                let query = qb.create_mysql_query(&query_info)?;
                let mut rows = query.fetch(&self.conn_pool);

                let mut map = HashMap::new();
                while let Some(row) = rows.try_next().await? {
                    let row_data = RowData::from_mysql_row(&row, mysql_meta, &None);
                    // Use basic meta part for hash code if it requires RdbTbMeta
                    let hash_code = row_data.get_hash_code(&mysql_meta.basic)?;
                    map.insert(hash_code, row_data);
                }
                Ok(map)
            }
            _ => anyhow::bail!("Expected Mysql metadata for MysqlChecker"),
        }
    }

    async fn fetch_dst_struct(&self, src: &StructStatement) -> anyhow::Result<StructStatement> {
        self.fetch_dst_struct_statement(src).await
    }
}

impl MysqlChecker {
    pub async fn fetch_dst_struct_statement(
        &self,
        src_statement: &StructStatement,
    ) -> anyhow::Result<StructStatement> {
        let schema = match src_statement {
            StructStatement::MysqlCreateDatabase(s) => s.database.name.clone(),
            StructStatement::MysqlCreateTable(s) => s.table.schema_name.clone(),
            _ => return Ok(StructStatement::Unknown),
        };

        let mut struct_fetcher = MysqlStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            dbs: HashSet::from([schema.clone()]),
            filter: None,
            meta_manager: self.meta_manager.clone(),
        };

        match src_statement {
            StructStatement::MysqlCreateDatabase(_) => {
                let mut statements = struct_fetcher
                    .get_create_database_statements(&schema)
                    .await?;
                if statements.is_empty() {
                    Ok(StructStatement::Unknown)
                } else {
                    Ok(StructStatement::MysqlCreateDatabase(statements.remove(0)))
                }
            }
            StructStatement::MysqlCreateTable(statement) => {
                let mut statements = struct_fetcher
                    .get_create_table_statements(&schema, &statement.table.table_name)
                    .await?;
                if statements.is_empty() {
                    Ok(StructStatement::Unknown)
                } else {
                    Ok(StructStatement::MysqlCreateTable(statements.remove(0)))
                }
            }
            _ => Ok(StructStatement::Unknown),
        }
    }
}
