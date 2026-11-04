use std::collections::HashSet;

use dt_common::meta::{
    mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
    pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    row_data::RowData,
};
use dt_common::{config::config_enums::DbType, resource::RID, utils::sql_util::SqlUtil};
use dt_connector::rdb_query_builder::RdbQueryBuilder;
use futures::TryStreamExt;
use sqlx::{MySql, Pool, Postgres};

use super::mssql_test_endpoint::MssqlTestEndpoint;

pub struct RdbUtil {}

impl RdbUtil {
    pub async fn fetch_data_mysql(
        conn_pool: &Pool<MySql>,
        excluded_columns: Option<&HashSet<String>>,
        table: &RID,
        condition: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        Self::fetch_data_mysql_compatible(
            conn_pool,
            excluded_columns,
            table,
            &DbType::Mysql,
            condition,
        )
        .await
    }

    pub async fn fetch_data_mysql_compatible(
        conn_pool: &Pool<MySql>,
        excluded_columns: Option<&HashSet<String>>,
        table: &RID,
        db_type: &DbType,
        where_sql: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        let tb_meta = Self::get_tb_meta_mysql_compatible(conn_pool, table, db_type).await?;
        let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta, excluded_columns);
        let cols_str = query_builder.build_extract_cols_str().unwrap();
        let sql = format!(
            "SELECT {} FROM {}.{} {} ORDER BY {} ASC",
            cols_str,
            SqlUtil::escape_by_db_type(table.segment(0), db_type),
            SqlUtil::escape_by_db_type(table.segment(1), db_type),
            where_sql,
            SqlUtil::escape_by_db_type(&tb_meta.basic.cols[0], db_type),
        );

        let mut rows = if matches!(db_type, DbType::Mysql | DbType::Tidb) {
            sqlx::query(&sql).fetch(conn_pool)
        } else {
            sqlx::raw_sql(&sql).fetch(conn_pool)
        };
        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data =
                RowData::from_mysql_compatible_row(&row, &tb_meta, &None, db_type, None)?;
            result.push(row_data);
        }

        Ok(result)
    }

    pub async fn fetch_data_pg(
        conn_pool: &Pool<Postgres>,
        excluded_columns: Option<&HashSet<String>>,
        table: &RID,
        where_sql: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        let tb_meta = Self::get_tb_meta_pg(conn_pool, table).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta, excluded_columns);

        let cols_str = query_builder.build_extract_cols_str().unwrap();
        let sql = format!(
            "SELECT {} FROM {}.{} {} ORDER BY {} ASC",
            cols_str,
            SqlUtil::escape_by_db_type(table.segment(0), &DbType::Pg),
            SqlUtil::escape_by_db_type(table.segment(1), &DbType::Pg),
            where_sql,
            SqlUtil::escape_by_db_type(&tb_meta.basic.cols[0], &DbType::Pg),
        );
        let query = sqlx::query(&sql);
        let mut rows = query.fetch(conn_pool);

        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_pg_row(&row, &tb_meta, &None, None)?;
            result.push(row_data);
        }

        Ok(result)
    }

    pub async fn fetch_data_mssql(
        endpoint: &MssqlTestEndpoint,
        excluded_columns: Option<&HashSet<String>>,
        table: &RID,
        where_sql: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        endpoint
            .fetch_table(
                table.segment(0),
                table.segment(1),
                table.segment(2),
                excluded_columns,
                where_sql,
            )
            .await
    }

    pub async fn get_tb_cols_mssql(
        endpoint: &MssqlTestEndpoint,
        table: &RID,
    ) -> anyhow::Result<Vec<String>> {
        endpoint
            .get_table_columns(table.segment(0), table.segment(1), table.segment(2))
            .await
    }

    pub async fn get_tb_meta_mysql(
        conn_pool: &Pool<MySql>,
        table: &RID,
    ) -> anyhow::Result<MysqlTbMeta> {
        Self::get_tb_meta_mysql_compatible(conn_pool, table, &DbType::Mysql).await
    }

    pub async fn get_tb_meta_mysql_compatible(
        conn_pool: &Pool<MySql>,
        table: &RID,
        db_type: &DbType,
    ) -> anyhow::Result<MysqlTbMeta> {
        let mut meta_manager =
            MysqlMetaManager::new_mysql_compatible(conn_pool.to_owned(), db_type.to_owned())
                .await?;
        Ok(meta_manager
            .get_tb_meta(table.segment(0), table.segment(1))
            .await?
            .to_owned())
    }

    pub async fn get_tb_meta_pg(
        conn_pool: &Pool<Postgres>,
        table: &RID,
    ) -> anyhow::Result<PgTbMeta> {
        let mut meta_manager = PgMetaManager::new(conn_pool.clone()).await?;
        Ok(meta_manager
            .get_tb_meta(table.segment(0), table.segment(1))
            .await?
            .to_owned())
    }

    pub async fn execute_sqls_mysql(
        conn_pool: &Pool<MySql>,
        sqls: &Vec<String>,
    ) -> anyhow::Result<()> {
        for sql in sqls {
            println!("executing sql: {}", sql);
            let query = sqlx::raw_sql(sql);
            query.execute(conn_pool).await.unwrap();
        }
        Ok(())
    }

    pub async fn execute_sqls_pg(
        conn_pool: &Pool<Postgres>,
        sqls: &[String],
    ) -> anyhow::Result<()> {
        for sql in sqls.iter() {
            println!("executing sql: {}", sql);
            let query = sqlx::raw_sql(sql);
            query.execute(conn_pool).await.unwrap();
        }
        Ok(())
    }
}
