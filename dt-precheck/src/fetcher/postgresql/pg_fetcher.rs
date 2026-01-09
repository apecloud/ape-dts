use std::collections::HashMap;

use anyhow::bail;
use async_trait::async_trait;
use dt_common::{
    config::connection_auth_config::ConnectionAuthConfig, error::Error, rdb_filter::RdbFilter,
};
use dt_task::task_util::TaskUtil;
use futures::{Stream, TryStreamExt};
use sqlx::{postgres::PgRow, query, Pool, Postgres, Row};

use crate::{
    fetcher::traits::Fetcher,
    meta::database_mode::{Constraint, Database, Schema, Table},
};

pub struct PgFetcher {
    pub pool: Option<Pool<Postgres>>,
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub is_source: bool,
    pub filter: RdbFilter,
}

#[async_trait]
impl Fetcher for PgFetcher {
    async fn build_connection(&mut self) -> anyhow::Result<()> {
        self.pool = Some(
            TaskUtil::create_pg_conn_pool(&self.url, &self.connection_auth, 1, true, false).await?,
        );
        Ok(())
    }

    async fn fetch_version(&mut self) -> anyhow::Result<String> {
        let sql = String::from("SELECT current_setting('server_version_num')::varchar");
        let mut version = String::from("");

        let results = self.fetch_all(sql, "pg query database version").await;
        match results {
            Ok(rows) => {
                if !rows.is_empty() {
                    version = rows.first().unwrap().get("current_setting");
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
            "SELECT name,setting::varchar FROM pg_settings WHERE name in ({})",
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
        let result = self.fetch_all(sql, "pg query config settings").await;
        match result {
            Ok(rows) => {
                for row in rows {
                    let (name, setting): (String, String) = (row.get("name"), row.get("setting"));
                    if result_map.contains_key(name.as_str()) {
                        result_map.insert(name, setting);
                    }
                }
            }
            Err(e) => bail! {e},
        }

        Ok(result_map)
    }

    async fn fetch_databases(&mut self) -> anyhow::Result<Vec<Database>> {
        Ok(vec![])
    }

    async fn fetch_schemas(&mut self) -> anyhow::Result<Vec<Schema>> {
        let mut schemas: Vec<Schema> = vec![];
        let sql = "select catalog_name,schema_name from information_schema.schemata";

        let rows_result = self.fetch_row(sql, "pg query schema sql");
        match rows_result {
            Ok(mut rows) => {
                while let Some(row) = rows.try_next().await.unwrap() {
                    let (database_name, schema_name): (String, String) =
                        (row.get("catalog_name"), row.get("schema_name"));
                    if !self.filter.filter_schema(&schema_name) {
                        schemas.push(Schema {
                            database_name,
                            schema_name,
                        })
                    }
                }
            }
            Err(e) => bail! {e},
        }

        Ok(schemas)
    }

    async fn fetch_tables(&mut self) -> anyhow::Result<Vec<Table>> {
        let mut tables: Vec<Table> = vec![];
        let table_sql = "SELECT table_catalog, table_schema, table_name 
                         FROM information_schema.tables 
                         WHERE table_type = 'BASE TABLE' 
                         AND table_schema NOT IN ('pg_catalog', 'information_schema')";

        let rows_result = self.fetch_row(table_sql, "pg query table sql");
        match rows_result {
            Ok(mut rows) => {
                while let Some(row) = rows.try_next().await.unwrap() {
                    let (database_name, schema_name, table_name): (String, String, String) = (
                        row.get("table_catalog"),
                        row.get("table_schema"),
                        row.get("table_name"),
                    );
                    if !self.filter.filter_tb(&schema_name, &table_name) {
                        tables.push(Table {
                            database_name,
                            schema_name,
                            table_name,
                        })
                    }
                }
            }
            Err(e) => bail! {e},
        }

        Ok(tables)
    }

    async fn fetch_constraints(&mut self) -> anyhow::Result<Vec<Constraint>> {
        let mut constraints: Vec<Constraint> = vec![];
        let sql = "SELECT
          con.conname,
          con.contype::varchar as contype,
          con.connamespace::regnamespace::text AS schema_name,
          ct.relname::text AS table_name,
          rt.relname::text AS ref_table_name,
          cs.relnamespace::regnamespace::text as ref_schema_name
        FROM
             pg_constraint con
        LEFT JOIN pg_class cs 
        ON   con.confrelid = cs.oid
        LEFT JOIN pg_class ct
        ON   con.conrelid = ct.oid
        LEFT JOIN pg_class rt
        ON   con.confrelid = rt.oid";

        let rows_result = self.fetch_row(sql, "pg query constraint sql");
        match rows_result {
            Ok(mut rows) => {
                while let Some(row) = rows.try_next().await.unwrap() {
                    let (
                        schema_name,
                        table_name,
                        rel_schema_name,
                        rel_table_name,
                        constraint_name,
                        constraint_type,
                    ): (String, String, String, String, String, String) = (
                        Self::get_text_with_null(&row, "schema_name").unwrap(),
                        Self::get_text_with_null(&row, "table_name").unwrap(),
                        Self::get_text_with_null(&row, "ref_schema_name").unwrap(),
                        Self::get_text_with_null(&row, "ref_table_name").unwrap(),
                        row.get("conname"),
                        row.get("contype"),
                    );
                    if !self.filter.filter_tb(&schema_name, &table_name) {
                        constraints.push(Constraint {
                            database_name: String::from(""),
                            schema_name,
                            table_name,
                            column_name: String::from(""),
                            rel_database_name: String::from(""),
                            rel_schema_name,
                            rel_table_name,
                            rel_column_name: String::from(""),
                            constraint_name,
                            constraint_type,
                        })
                    }
                }
            }
            Err(e) => bail! {e},
        }

        Ok(constraints)
    }
}

impl PgFetcher {
    async fn fetch_all(&self, sql: String, mut sql_msg: &str) -> anyhow::Result<Vec<PgRow>> {
        let pg_pool = match &self.pool {
            Some(pool) => pool,
            None => bail! {Error::from(sqlx::Error::PoolClosed)},
        };

        sql_msg = if sql_msg.is_empty() { "sql" } else { sql_msg };
        println!("{}: {}", sql_msg, sql);

        let rows_result = query(&sql).fetch_all(pg_pool).await;
        match rows_result {
            Ok(rows) => Ok(rows),
            Err(e) => bail! {Error::from(e)},
        }
    }

    fn fetch_row<'a>(
        &self,
        sql: &'a str,
        mut sql_msg: &str,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<PgRow, sqlx::Error>> + 'a> {
        match &self.pool {
            Some(pool) => {
                sql_msg = if sql_msg.is_empty() { "sql" } else { sql_msg };
                println!("{}: {}", sql_msg, sql);
                Ok(query(sql).fetch(pool))
            }
            None => bail! {Error::from(sqlx::Error::PoolClosed)},
        }
    }

    pub async fn fetch_slot_names(&self) -> anyhow::Result<Vec<String>> {
        let mut slots: Vec<String> = vec![];
        let slot_query = "select slot_name from pg_catalog.pg_replication_slots".to_string();

        let result = self.fetch_all(slot_query, "pg query slots").await;
        match result {
            Ok(rows) => {
                for row in rows {
                    let slot_name = row.get("slot_name");
                    slots.push(slot_name);
                }
            }
            Err(e) => bail! {e},
        }
        Ok(slots)
    }

    fn get_text_with_null(row: &PgRow, col_name: &str) -> anyhow::Result<String> {
        let mut str_val = String::new();

        if let Some(s) = row.get(col_name) {
            str_val = s
        }

        Ok(str_val)
    }
}
