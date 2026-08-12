#[cfg(test)]
mod test {
    use std::env;

    use anyhow::Context;
    use dt_common::{
        config::{
            connection_auth_config::ConnectionAuthConfig,
            ssl_config::{SslConfig, SslMode},
        },
        meta::{
            adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
            mssql::{
                mssql_col_type::MssqlColType, mssql_connection_pool::MssqlConnectionPool,
                mssql_meta_manager::MssqlMetaManager,
            },
        },
    };
    use serial_test::serial;

    use crate::{
        test_config_util::TestConfigUtil, test_runner::mssql_test_client::MssqlTestClient,
    };

    const TEST_SCHEMA: &str = "ape_dts_meta_manager_test";
    const TEST_TABLE: &str = "catalog_types";

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

        let connection_string = required_env("mssql_extractor_without_auth_url")?;
        let database = required_env("mssql_extractor_database")?;
        let auth = auth(
            required_env("mssql_extractor_username")?,
            required_env("mssql_extractor_password")?,
        );

        MssqlTestClient::from_connection_string_and_auth(&connection_string, auth.clone())?
            .ensure_database(&database)
            .await?;
        MssqlConnectionPool::from_config(&connection_string, &auth, None, 1, 15).await
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

    #[tokio::test]
    #[serial]
    async fn reads_and_invalidates_real_mssql_catalog_metadata() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        execute_batch(
            &pool,
            &format!(
                "EXEC(N'CREATE SCHEMA [{TEST_SCHEMA}]');
                 CREATE TABLE [{TEST_SCHEMA}].[{TEST_TABLE}] (
                    [tenant_id] int NOT NULL,
                    [id] bigint NOT NULL,
                    [optional_name] nvarchar(100) NULL,
                    [score] float(24) NOT NULL,
                    [alias_name] sysname NOT NULL,
                    CONSTRAINT [pk_ape_dts_meta_manager] PRIMARY KEY ([tenant_id], [id]),
                    CONSTRAINT [uq_ape_dts_meta_manager_name] UNIQUE ([optional_name])
                 );"
            ),
        )
        .await?;

        let result = async {
            let mut manager = MssqlMetaManager::new(pool.clone()).await?;

            let mut connection = pool.get().await?;
            let row = connection
                .client_mut()
                .query(
                    "SELECT CAST(NULL AS nvarchar(10)) AS null_value, \
                            CAST(1 AS int) AS wrong_type",
                    &[],
                )
                .await?
                .into_row()
                .await?
                .context("MSSQL required value test returned no row")?;
            assert!(
                MssqlColValueConvertor::from_query_required_string(&row, "null_value").is_err()
            );
            assert!(
                MssqlColValueConvertor::from_query_required_string(&row, "wrong_type").is_err()
            );
            drop(connection);

            assert!(manager
                .list_schemas()
                .await?
                .contains(&TEST_SCHEMA.to_string()));
            assert_eq!(manager.list_tables(TEST_SCHEMA).await?, vec![TEST_TABLE]);

            let meta = manager.get_tb_meta(TEST_SCHEMA, TEST_TABLE).await?;
            assert_eq!(
                meta.basic.cols,
                ["tenant_id", "id", "optional_name", "score", "alias_name"]
            );
            assert_eq!(meta.basic.schema, TEST_SCHEMA);
            assert_eq!(meta.basic.tb, TEST_TABLE);
            assert!(meta.basic.nullable_cols.contains("optional_name"));
            assert!(!meta.basic.nullable_cols.contains("score"));
            assert_eq!(
                meta.basic.col_origin_type_map.get("alias_name"),
                Some(&"sysname".to_string())
            );
            assert_eq!(
                meta.basic.key_map.get("primary"),
                Some(&vec!["tenant_id".to_string(), "id".to_string()])
            );
            assert_eq!(
                meta.basic.key_map.get("uq_ape_dts_meta_manager_name"),
                Some(&vec!["optional_name".to_string()])
            );
            assert_eq!(meta.basic.order_cols, ["tenant_id", "id"]);
            assert_eq!(meta.basic.partition_col, "tenant_id");
            assert_eq!(meta.basic.id_cols, ["tenant_id", "id"]);
            assert_eq!(meta.get_col_type("tenant_id")?, &MssqlColType::Int4);
            assert_eq!(meta.get_col_type("id")?, &MssqlColType::Int8);
            assert_eq!(meta.get_col_type("optional_name")?, &MssqlColType::NVarchar);
            assert_eq!(meta.get_col_type("score")?, &MssqlColType::Float4);
            assert_eq!(meta.get_col_type("alias_name")?, &MssqlColType::NVarchar);

            execute_batch(
                &pool,
                &format!("ALTER TABLE [{TEST_SCHEMA}].[{TEST_TABLE}] ADD [added_later] int NULL;"),
            )
            .await?;

            assert!(!manager
                .get_tb_meta(TEST_SCHEMA, TEST_TABLE)
                .await?
                .basic
                .cols
                .contains(&"added_later".to_string()));

            manager.invalidate_cache_for_table(TEST_SCHEMA, TEST_TABLE);
            assert!(manager
                .get_tb_meta(TEST_SCHEMA, TEST_TABLE)
                .await?
                .basic
                .cols
                .contains(&"added_later".to_string()));

            assert!(manager
                .get_tb_meta(TEST_SCHEMA, "table_does_not_exist")
                .await
                .is_err());
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }
}
