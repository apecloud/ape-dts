use futures::TryStreamExt;
use sqlx::{MySql, Pool, Row};

pub struct MysqlStructCheckFetcher {
    pub conn_pool: Pool<MySql>,
}

impl MysqlStructCheckFetcher {
    pub async fn fetch_table(&self, db: &str, tb: &str) -> String {
        let sql = format!("SHOW CREATE TABLE `{}`.`{}`", db, tb);
        self.execute_sql_and_get_one_result(&sql).await
    }

    pub async fn fetch_database(&self, db: &str) -> String {
        let sql = format!("SHOW CREATE DATABASE `{}`", db);
        self.execute_sql_and_get_one_result(&sql).await
    }

    async fn execute_sql_and_get_one_result(&self, sql: &str) -> String {
        let mut rows = sqlx::query(sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await.unwrap() {
            let value: Option<String> = row.try_get(1).unwrap();
            if let Some(v) = value {
                return v;
            }
        }
        String::new()
    }
}
