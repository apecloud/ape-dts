use std::str::FromStr;

use anyhow::{bail, Context, Result};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::extractor::resumer::{
    ResumerDbPool, ResumerType, DEFAULT_POSITION_KEY, DEFAULT_RESUMER_SCHEMA, DEFAULT_RESUMER_TABLE,
};
use dt_common::{config::config_enums::DbType, meta::position::Position};

pub struct ResumerUtil {}

impl ResumerUtil {
    pub fn get_full_table_name(full_table_name: &str) -> Result<(String, String)> {
        if full_table_name.is_empty() {
            return Ok((
                DEFAULT_RESUMER_SCHEMA.to_string(),
                DEFAULT_RESUMER_TABLE.to_string(),
            ));
        }

        let parts = full_table_name.split('.').collect::<Vec<&str>>();
        if parts.len() != 2 {
            bail!("invalid full table name: {}", full_table_name)
        }
        let schema = parts[0];
        let table = parts[1];
        if schema.is_empty() || table.is_empty() {
            bail!("invalid full table name: {}", full_table_name)
        }
        Ok((schema.to_string(), table.to_string()))
    }

    pub async fn create_pool(
        url: &str,
        db_type: &DbType,
        max_connections: u32,
    ) -> anyhow::Result<ResumerDbPool> {
        match db_type {
            DbType::Mysql => {
                let conn_options = MySqlConnectOptions::from_str(url)
                    .context("failed to parse MySQL connection URL")?;

                let pool = MySqlPoolOptions::new()
                    .max_connections(max_connections)
                    .connect_with(conn_options)
                    .await
                    .context("failed to create MySQL connection pool")?;

                Ok(ResumerDbPool::MySql(pool))
            }
            DbType::Pg => {
                let conn_options = PgConnectOptions::from_str(url)
                    .context("failed to parse PostgreSQL connection URL")?;

                let pool = PgPoolOptions::new()
                    .max_connections(max_connections)
                    .connect_with(conn_options)
                    .await
                    .context("failed to create PostgreSQL connection pool")?;

                Ok(ResumerDbPool::Postgres(pool))
            }
            _ => {
                bail!(
                    "unsupported database type for DatabaseRecorder: {:?}",
                    db_type
                )
            }
        }
    }

    pub fn get_key_from_position(position: &Position) -> String {
        match position {
            Position::RdbSnapshot { schema, tb, .. }
            | Position::RdbSnapshotFinished { schema, tb, .. }
            | Position::FoxlakeS3 { schema, tb, .. } => {
                format!("{}-{}", schema, tb)
            }
            Position::Kafka {
                topic, partition, ..
            } => {
                format!("{}-{}", topic, partition)
            }
            _ => DEFAULT_POSITION_KEY.to_string(),
        }
    }

    pub fn get_key_from_base(
        (schema, tb, _col): (String, String, String),
        resumer_type: ResumerType,
    ) -> String {
        match resumer_type {
            ResumerType::SnapshotDoing | ResumerType::SnapshotFinished => {
                format!("{}-{}", schema, tb)
            }
            _ => DEFAULT_POSITION_KEY.to_string(),
        }
    }
}

#[cfg(test)]

mod tests {
    use crate::extractor::resumer::{
        utils::ResumerUtil, DEFAULT_RESUMER_SCHEMA, DEFAULT_RESUMER_TABLE,
    };

    #[test]
    fn test_get_full_table_name() {
        // Test default values
        let (schema, table) = ResumerUtil::get_full_table_name("").unwrap();
        assert_eq!(schema, DEFAULT_RESUMER_SCHEMA);
        assert_eq!(table, DEFAULT_RESUMER_TABLE);

        // Test valid full table name
        let (schema, table) = ResumerUtil::get_full_table_name("test_schema.test_table").unwrap();
        assert_eq!(schema, "test_schema");
        assert_eq!(table, "test_table");

        // Test invalid full table name - no dot
        let result = ResumerUtil::get_full_table_name("invalid_name");
        assert!(result.is_err());

        // Test invalid full table name - too many dots
        let result = ResumerUtil::get_full_table_name("too.many.dots");
        assert!(result.is_err());

        // Test invalid full table name - empty schema
        let result = ResumerUtil::get_full_table_name(".table_name");
        assert!(result.is_err());

        // Test invalid full table name - empty table
        let result = ResumerUtil::get_full_table_name("schema_name.");
        assert!(result.is_err());
    }
}
