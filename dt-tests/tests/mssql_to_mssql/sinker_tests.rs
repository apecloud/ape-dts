#[cfg(test)]
mod test {
    use std::{collections::HashMap, env};

    use anyhow::Context;
    use dt_common::{
        config::{
            connection_auth_config::ConnectionAuthConfig,
            ssl_config::{SslConfig, SslMode},
        },
        meta::{
            adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
            col_value::ColValue,
            mssql::{
                mssql_connection_pool::MssqlConnectionPool, mssql_meta_manager::MssqlMetaManager,
            },
            row_data::RowData,
            row_type::RowType,
        },
    };
    use dt_connector::{
        sinker::{base_sinker::BaseSinker, mssql::mssql_sinker::MssqlSinker},
        Sinker,
    };
    use serial_test::serial;

    use crate::{
        test_config_util::TestConfigUtil, test_runner::mssql_test_client::MssqlTestClient,
    };

    const TEST_SCHEMA: &str = "ape_dts_sinker_test";
    const TEST_TABLE: &str = "transaction_rows";

    fn required_env(key: &str) -> anyhow::Result<String> {
        env::var(key).with_context(|| format!("required MSSQL test environment variable {key}"))
    }

    fn auth(username: String, password: String) -> ConnectionAuthConfig {
        ConnectionAuthConfig::BasicSsl {
            username: Some(username),
            password: Some(password),
            ssl_config: SslConfig {
                ssl_mode: SslMode::Disable,
                ssl_ca_path: String::new(),
            },
        }
    }

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let env_path = TestConfigUtil::get_absolute_path(".env");
        dotenv::from_path(&env_path)
            .with_context(|| format!("failed to load MSSQL test environment {env_path}"))?;
        let connection_string = required_env("mssql_sinker_without_auth_url")?;
        let database = required_env("mssql_sinker_database")?;
        let connection_auth = auth(
            required_env("mssql_sinker_username")?,
            required_env("mssql_sinker_password")?,
        );
        MssqlTestClient::from_connection_string_and_auth(
            &connection_string,
            connection_auth.clone(),
        )?
        .ensure_database(&database)
        .await?;
        MssqlConnectionPool::from_config(&connection_string, &connection_auth, None, 1, 15).await
    }

    async fn execute_batch(pool: &MssqlConnectionPool, sql: &str) -> anyhow::Result<()> {
        let mut connection = pool.get().await?;
        connection
            .client_mut()
            .simple_query(sql)
            .await?
            .into_results()
            .await?;
        Ok(())
    }

    async fn cleanup(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        execute_batch(
            pool,
            &format!(
                "DROP TABLE IF EXISTS [{TEST_SCHEMA}].[{TEST_TABLE}];
                 IF SCHEMA_ID(N'{TEST_SCHEMA}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{TEST_SCHEMA}]');"
            ),
        )
        .await
    }

    fn row(id: i32, code: &str) -> RowData {
        RowData::new(
            TEST_SCHEMA.to_string(),
            TEST_TABLE.to_string(),
            0,
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(id)),
                ("code".to_string(), ColValue::String(code.to_string())),
            ])),
        )
    }

    async fn row_count(pool: &MssqlConnectionPool) -> anyhow::Result<i64> {
        let mut connection = pool.get().await?;
        let row = connection
            .client_mut()
            .query(
                &format!("SELECT COUNT_BIG(*) AS row_count FROM [{TEST_SCHEMA}].[{TEST_TABLE}]"),
                &[],
            )
            .await?
            .into_row()
            .await?
            .context("MSSQL sinker test row count query returned no row")?;
        MssqlColValueConvertor::from_query_required_i64(&row, "row_count")
    }

    #[tokio::test]
    #[serial]
    async fn rolls_back_failed_batch_and_clears_identity_insert() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        execute_batch(
            &pool,
            &format!(
                "EXEC(N'CREATE SCHEMA [{TEST_SCHEMA}]');
                 CREATE TABLE [{TEST_SCHEMA}].[{TEST_TABLE}] (
                    id int IDENTITY(1, 1) NOT NULL PRIMARY KEY,
                    code nvarchar(20) NOT NULL UNIQUE
                 );"
            ),
        )
        .await?;

        let result = async {
            let meta_manager = MssqlMetaManager::new(pool.clone()).await?;
            let mut sinker =
                MssqlSinker::new(pool.clone(), meta_manager, None, 2, BaseSinker::default());

            let error = sinker
                .sink_dml(vec![row(10, "duplicate"), row(11, "duplicate")], true)
                .await
                .expect_err("duplicate batch should fail");
            assert!(error.to_string().contains("duplicate") || error.to_string().contains("2601"));
            assert_eq!(row_count(&pool).await?, 0);

            execute_batch(
                &pool,
                &format!("INSERT INTO [{TEST_SCHEMA}].[{TEST_TABLE}] (code) VALUES (N'generated')"),
            )
            .await?;
            sinker.sink_dml(vec![row(20, "explicit")], true).await?;
            assert_eq!(row_count(&pool).await?, 2);
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }
}
