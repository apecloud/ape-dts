use std::collections::HashSet;

use dt_common::config::config_enums::DbType;
use dt_common::meta::{
    mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
    pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    row_data::RowData,
};
use dt_connector::rdb_query_builder::RdbQueryBuilder;
use futures::TryStreamExt;
use sqlx::{MySql, Pool, Postgres};

pub struct RdbUtil {}

impl RdbUtil {
    pub async fn fetch_data_mysql(
        conn_pool: &Pool<MySql>,
        ignore_cols: Option<&HashSet<String>>,
        db_tb: &(String, String),
        condition: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        Self::fetch_data_mysql_compatible(conn_pool, ignore_cols, db_tb, &DbType::Mysql, condition)
            .await
    }

    pub async fn fetch_data_mysql_compatible(
        conn_pool: &Pool<MySql>,
        ignore_cols: Option<&HashSet<String>>,
        db_tb: &(String, String),
        db_type: &DbType,
        where_sql: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        let tb_meta = Self::get_tb_meta_mysql_compatible(conn_pool, db_tb, db_type).await?;
        let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta, ignore_cols);
        let cols_str = query_builder.build_extract_cols_str().unwrap();
        let sql = format!(
            "SELECT {} FROM `{}`.`{}` {} ORDER BY `{}` ASC",
            cols_str, &db_tb.0, &db_tb.1, where_sql, &tb_meta.basic.cols[0],
        );

        let mut rows = sqlx::query(&sql).fetch(conn_pool);
        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_mysql_compatible_row(&row, &tb_meta, &None, db_type);
            result.push(row_data);
        }

        Ok(result)
    }

    pub async fn fetch_data_pg(
        conn_pool: &Pool<Postgres>,
        ignore_cols: Option<&HashSet<String>>,
        db_tb: &(String, String),
        where_sql: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        let tb_meta = Self::get_tb_meta_pg(conn_pool, db_tb).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta, ignore_cols);

        let tb_meta = Self::get_tb_meta_pg(conn_pool, db_tb).await?;
        let cols_str = query_builder.build_extract_cols_str().unwrap();
        let sql = format!(
            r#"SELECT {} FROM "{}"."{}" {} ORDER BY "{}" ASC"#,
            cols_str, &db_tb.0, &db_tb.1, where_sql, &tb_meta.basic.cols[0],
        );
        let query = sqlx::query(&sql);
        let mut rows = query.fetch(conn_pool);

        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_pg_row(&row, &tb_meta, &None);
            result.push(row_data);
        }

        Ok(result)
    }

    pub async fn get_tb_meta_mysql(
        conn_pool: &Pool<MySql>,
        db_tb: &(String, String),
    ) -> anyhow::Result<MysqlTbMeta> {
        Self::get_tb_meta_mysql_compatible(conn_pool, db_tb, &DbType::Mysql).await
    }

    pub async fn get_tb_meta_mysql_compatible(
        conn_pool: &Pool<MySql>,
        db_tb: &(String, String),
        db_type: &DbType,
    ) -> anyhow::Result<MysqlTbMeta> {
        let mut meta_manager =
            MysqlMetaManager::new_mysql_compatible(conn_pool.to_owned(), db_type.to_owned())
                .await?;
        Ok(meta_manager
            .get_tb_meta(&db_tb.0, &db_tb.1)
            .await?
            .to_owned())
    }

    pub async fn get_tb_meta_pg(
        conn_pool: &Pool<Postgres>,
        db_tb: &(String, String),
    ) -> anyhow::Result<PgTbMeta> {
        let mut meta_manager = PgMetaManager::new(conn_pool.clone()).await?;
        Ok(meta_manager
            .get_tb_meta(&db_tb.0, &db_tb.1)
            .await?
            .to_owned())
    }

    pub async fn execute_sqls_mysql(
        conn_pool: &Pool<MySql>,
        sqls: &Vec<String>,
    ) -> anyhow::Result<()> {
        for sql in sqls {
            println!("executing sql: {}", sql);
            let query = sqlx::query(sql);
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
            let query = sqlx::query(sql);
            query.execute(conn_pool).await.unwrap();
        }
        Ok(())
    }
}
