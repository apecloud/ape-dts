use std::collections::HashMap;

use anyhow::bail;
use async_trait::async_trait;
use dt_common::{
    config::connection_auth_config::ConnectionAuthConfig, error::Error, rdb_filter::RdbFilter,
};
use dt_task::task_util::TaskUtil;
use futures::{Stream, TryStreamExt};
use sqlx::{mysql::MySqlRow, query, MySql, Pool, Row};

use crate::{
    fetcher::traits::Fetcher,
    meta::database_mode::{Constraint, Database, Schema, Table},
};

pub struct MysqlFetcher {
    pub pool: Option<Pool<MySql>>,
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub is_source: bool,
    pub filter: RdbFilter,
}

#[async_trait]
impl Fetcher for MysqlFetcher {
    async fn build_connection(&mut self) -> anyhow::Result<()> {
        self.pool = Some(
            TaskUtil::create_mysql_conn_pool(&self.url, &self.connection_auth, 1, true, None)
                .await?,
        );
        Ok(())
    }

    async fn fetch_version(&mut self) -> anyhow::Result<String> {
        let sql = "SELECT version() AS VERSION".to_string();
        let mut version: String = String::from("");

        let result = self.fetch_all(sql, "mysql query database version").await;
        match result {
            Ok(rows) => {
                if !rows.is_empty() {
                    let version_str: String = rows.first().unwrap().get("VERSION");
                    version = version_str;
                }
            }
            Err(e) => bail! {e},
        }

        Ok(version)
    }

    async fn fetch_configuration(
        &mut self,
        config_keys: Vec<String>,
    ) -> anyhow::Result<HashMap<String, String>> {
        if config_keys.is_empty() {
            return Ok(HashMap::new());
        }
        let sql = format!(
            "SHOW variables WHERE variable_name IN ({})",
            config_keys
                .iter()
                .map(|c| format!("'{}'", c))
                .collect::<Vec<_>>()
                .join(",")
        );
        let mut result_map: HashMap<String, String> = config_keys
            .iter()
            .map(|c| (c.clone(), String::from("")))
            .collect();
        let result = self.fetch_all(sql, "mysql query config settings").await;
        match result {
            Ok(rows) => {
                for row in rows {
                    let (variable_name, value): (String, String) =
                        (row.get("Variable_name"), row.get("Value"));
                    if result_map.contains_key(variable_name.as_str()) {
                        result_map.insert(variable_name, value);
                    }
                }
            }
            Err(e) => bail! {e},
        }

        Ok(result_map)
    }

    async fn fetch_databases(&mut self) -> anyhow::Result<Vec<Database>> {
        let mut results: Vec<Database> = vec![];
        let query_db = "SELECT SCHEMA_NAME FROM information_schema.schemata";

        let rows_result = self.fetch_row(query_db, "mysql query dbs sql:");
        match rows_result {
            Ok(mut rows) => {
                while let Some(row) = rows.try_next().await.unwrap() {
                    let schema_name: String = row.get("SCHEMA_NAME");
                    if !self.filter.filter_schema(&schema_name) {
                        results.push(Database {
                            database_name: schema_name,
                        })
                    }
                }
            }
            Err(e) => bail! {e},
        }

        Ok(results)
    }

    async fn fetch_schemas(&mut self) -> anyhow::Result<Vec<Schema>> {
        Ok(vec![])
    }

    async fn fetch_tables(&mut self) -> anyhow::Result<Vec<Table>> {
        let mut results: Vec<Table> = vec![];
        let query_tb = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables where TABLE_TYPE = 'BASE TABLE'";

        let rows_result = self.fetch_row(query_tb, "mysql query tables sql:");
        match rows_result {
            Ok(mut rows) => {
                while let Some(row) = rows.try_next().await.unwrap() {
                    let (db, tb): (String, String) =
                        (row.get("TABLE_SCHEMA"), row.get("TABLE_NAME"));
                    if !self.filter.filter_tb(&db, &tb) {
                        results.push(Table {
                            database_name: db,
                            schema_name: String::from(""),
                            table_name: tb,
                        })
                    }
                }
            }
            Err(e) => bail! {e},
        }

        Ok(results)
    }

    async fn fetch_constraints(&mut self) -> anyhow::Result<Vec<Constraint>> {
        let mut results: Vec<Constraint> = vec![];
        let sys_dbs = MysqlFetcher::get_system_databases();

        let query_constraint = format!(
            "SELECT
              kcu.CONSTRAINT_NAME,
              tc.CONSTRAINT_TYPE,
              kcu.CONSTRAINT_SCHEMA,
              kcu.TABLE_NAME,
              kcu.COLUMN_NAME,
              kcu.REFERENCED_TABLE_SCHEMA,
              kcu.REFERENCED_TABLE_NAME,
              kcu.REFERENCED_COLUMN_NAME
            FROM
              INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
              ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA
            WHERE kcu.CONSTRAINT_SCHEMA not in ({})
        ",
            sys_dbs
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<String>>()
                .join(",")
        );

        let rows_result = self.fetch_row(query_constraint.as_str(), "mysql query constraints sql:");
        match rows_result {
            Ok(mut rows) => {
                while let Some(row) = rows.try_next().await.unwrap() {
                    let (db, table, rel_db, rel_table, constraint_name, constraint_type): (
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                    ) = (
                        Self::get_str_with_null(&row, "CONSTRAINT_SCHEMA").unwrap(),
                        Self::get_str_with_null(&row, "TABLE_NAME").unwrap(),
                        Self::get_str_with_null(&row, "REFERENCED_TABLE_SCHEMA").unwrap(),
                        Self::get_str_with_null(&row, "REFERENCED_TABLE_NAME").unwrap(),
                        Self::get_str_with_null(&row, "CONSTRAINT_NAME").unwrap(),
                        Self::get_str_with_null(&row, "CONSTRAINT_TYPE").unwrap(),
                    );
                    if !self.filter.filter_tb(&db, &table) {
                        results.push(Constraint {
                            database_name: db,
                            schema_name: String::from(""),
                            table_name: table,
                            column_name: String::from(""),
                            rel_database_name: rel_db,
                            rel_schema_name: String::from(""),
                            rel_table_name: rel_table,
                            rel_column_name: String::from(""),
                            constraint_name,
                            constraint_type,
                        })
                    }
                }
            }
            Err(e) => bail! {e},
        }

        Ok(results)
    }
}

impl MysqlFetcher {
    async fn fetch_all(&self, sql: String, mut sql_msg: &str) -> Result<Vec<MySqlRow>, Error> {
        let mysql_pool = match &self.pool {
            Some(pool) => pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        };

        sql_msg = if sql_msg.is_empty() { "sql" } else { sql_msg };
        println!("{}: {}", sql_msg, sql);

        let rows_result = query(&sql).fetch_all(mysql_pool).await;
        match rows_result {
            Ok(rows) => Ok(rows),
            Err(e) => Err(Error::from(e)),
        }
    }

    fn fetch_row<'a>(
        &self,
        sql: &'a str,
        mut sql_msg: &str,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<MySqlRow, sqlx::Error>> + 'a> {
        match &self.pool {
            Some(pool) => {
                sql_msg = if sql_msg.is_empty() { "sql" } else { sql_msg };
                println!("{}: {}", sql_msg, sql);
                Ok(query(sql).fetch(pool))
            }
            None => bail! {Error::from(sqlx::Error::PoolClosed)},
        }
    }

    fn get_system_databases() -> Vec<String> {
        let dbs = ["mysql", "performance_schema", "sys", "information_schema"];
        dbs.iter().map(|d| d.to_string()).collect()
    }

    fn get_str_with_null(row: &MySqlRow, col_name: &str) -> anyhow::Result<String> {
        let mut str_val = String::new();
        let str_val_option = row.get(col_name);
        if let Some(s) = str_val_option {
            str_val = s;
        }
        Ok(str_val)
    }
}
