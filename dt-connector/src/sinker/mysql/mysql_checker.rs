use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{MySql, Pool};
use std::collections::HashSet;

use crate::{
    meta_fetcher::mysql::mysql_struct_fetcher::MysqlStructFetcher,
    rdb_query_builder::RdbQueryBuilder,
    sinker::base_checker::{BaseChecker, Checker, CheckerCommon, CheckerTbMeta},
};
use dt_common::meta::{
    mysql::mysql_meta_manager::MysqlMetaManager, row_data::RowData,
    struct_meta::statement::struct_statement::StructStatement,
};

#[derive(Clone)]
pub struct MysqlChecker {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub common: CheckerCommon,
}

#[async_trait]
impl Checker for MysqlChecker {
    fn common_mut(&mut self) -> &mut CheckerCommon {
        &mut self.common
    }

    async fn get_tb_meta_by_row(&mut self, row: &RowData) -> anyhow::Result<CheckerTbMeta> {
        Ok(CheckerTbMeta::Mysql(
            self.meta_manager
                .get_tb_meta_by_row_data(row)
                .await?
                .clone(),
        ))
    }

    async fn fetch_batch(
        &self,
        tb_meta: &CheckerTbMeta,
        data: &[&RowData],
    ) -> anyhow::Result<Vec<RowData>> {
        let mysql_meta = tb_meta.mysql()?;
        let qb = RdbQueryBuilder::new_for_mysql(mysql_meta, None);

        let mut res = Vec::with_capacity(data.len());
        let mut batch_rows = Vec::with_capacity(data.len());

        for &row in data {
            if BaseChecker::has_null_key(row, &mysql_meta.basic.id_cols) {
                let query_info = qb.get_select_query(row)?;
                let query = qb.create_mysql_query(&query_info)?;
                let mut rows = query.fetch(&self.conn_pool);
                while let Some(r) = rows.try_next().await? {
                    res.push(RowData::from_mysql_row(&r, mysql_meta, &None));
                }
            } else {
                batch_rows.push(row);
            }
        }

        if !batch_rows.is_empty() {
            let query_info = qb.get_batch_select_query(&batch_rows, 0, batch_rows.len())?;
            let query = qb.create_mysql_query(&query_info)?;
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await? {
                res.push(RowData::from_mysql_row(&row, mysql_meta, &None));
            }
        }

        Ok(res)
    }

    async fn fetch_dst_struct(&self, src: &StructStatement) -> anyhow::Result<StructStatement> {
        let (schema, table) = match src {
            StructStatement::MysqlCreateDatabase(s) => {
                let schema = self.common.reverse_router.get_schema_map(&s.database.name);
                (schema, None)
            }
            StructStatement::MysqlCreateTable(s) => {
                let (schema, table) = self
                    .common
                    .reverse_router
                    .get_tb_map(&s.table.database_name, &s.table.table_name);
                (schema, Some(table))
            }
            _ => return Ok(StructStatement::Unknown),
        };

        let mut struct_fetcher = MysqlStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            dbs: HashSet::from([schema.to_string()]),
            filter: None,
            meta_manager: self.meta_manager.clone(),
        };

        match src {
            StructStatement::MysqlCreateDatabase(_) => {
                let statement = struct_fetcher
                    .get_create_database_statements(schema)
                    .await?
                    .into_iter()
                    .next();
                Ok(statement
                    .map(StructStatement::MysqlCreateDatabase)
                    .unwrap_or(StructStatement::Unknown))
            }
            StructStatement::MysqlCreateTable(_) => {
                if let Some(table_name) = table {
                    let statement = struct_fetcher
                        .get_create_table_statements(schema, table_name)
                        .await?
                        .into_iter()
                        .next();
                    Ok(statement
                        .map(StructStatement::MysqlCreateTable)
                        .unwrap_or(StructStatement::Unknown))
                } else {
                    Ok(StructStatement::Unknown)
                }
            }
            _ => Ok(StructStatement::Unknown),
        }
    }
}
