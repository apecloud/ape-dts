#[cfg(test)]
mod test {
    use anyhow::Context;
    use dt_common::meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        mssql::{mssql_col_type::MssqlColType, mssql_connection_pool::MssqlConnectionPool},
    };
    use serial_test::serial;

    use super::super::TASK_CONFIG_FILE;
    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    const TEST_DATABASE: &str = "ape_dts";
    const TEST_SCHEMA: &str = "ape_dts_meta_manager_test";
    const TEST_TABLE: &str = "catalog_types";

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Extractor)?;
        endpoint.ensure_database(TEST_DATABASE).await?;
        endpoint.create_pool_with(1, 15).await
    }

    async fn cleanup(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "USE [{TEST_DATABASE}];
                 DROP TABLE IF EXISTS [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TEST_TABLE}];
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
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "USE [{TEST_DATABASE}];
                 EXEC(N'CREATE SCHEMA [{TEST_SCHEMA}]');
                 CREATE TABLE [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TEST_TABLE}] (
                    [tenant_id] int NOT NULL,
                    [id] bigint IDENTITY(1, 1) NOT NULL,
                    [optional_name] nvarchar(100) NULL,
                    [score] float(24) NOT NULL,
                    [alias_name] sysname NOT NULL,
                    [computed_value] AS ([tenant_id] + 1),
                    [valid_from] datetime2 GENERATED ALWAYS AS ROW START NOT NULL
                        DEFAULT SYSUTCDATETIME(),
                    [valid_to] datetime2 GENERATED ALWAYS AS ROW END NOT NULL
                        DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'),
                    [version] rowversion NOT NULL,
                    CONSTRAINT [pk_ape_dts_meta_manager] PRIMARY KEY ([tenant_id], [id]),
                    CONSTRAINT [uq_ape_dts_meta_manager_name] UNIQUE ([optional_name]),
                    PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])
                 );"
            ),
        )
        .await?;

        let result = async {
            let mut manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;

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
                .list_schemas(TEST_DATABASE)
                .await?
                .contains(&TEST_SCHEMA.to_string()));
            assert_eq!(
                manager.list_tables(TEST_DATABASE, TEST_SCHEMA).await?,
                vec![TEST_TABLE]
            );
            assert!(manager
                .list_schema_tables(TEST_DATABASE)
                .await?
                .contains(&(TEST_SCHEMA.to_string(), TEST_TABLE.to_string())));

            let meta = manager
                .get_tb_meta(TEST_DATABASE, TEST_SCHEMA, TEST_TABLE)
                .await?;
            assert_eq!(
                meta.basic.cols,
                [
                    "tenant_id",
                    "id",
                    "optional_name",
                    "score",
                    "alias_name",
                    "computed_value",
                    "valid_from",
                    "valid_to",
                    "version"
                ]
            );
            assert_eq!(meta.basic.db, TEST_DATABASE);
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
            assert_eq!(meta.identity_col.as_deref(), Some("id"));
            assert_eq!(meta.computed_cols, ["computed_value".to_string()].into());
            assert_eq!(meta.generated_always_type_map.get("valid_from"), Some(&1));
            assert_eq!(meta.generated_always_type_map.get("valid_to"), Some(&2));
            assert_eq!(meta.generated_always_type_map.get("tenant_id"), Some(&0));
            assert_eq!(meta.rowversion_cols, ["version".to_string()].into());
            assert_eq!(meta.get_col_type("tenant_id")?, &MssqlColType::Int4);
            assert_eq!(meta.get_col_type("id")?, &MssqlColType::Int8);
            assert_eq!(meta.get_col_type("optional_name")?, &MssqlColType::NVarchar);
            assert_eq!(meta.get_col_type("score")?, &MssqlColType::Float4);
            assert_eq!(meta.get_col_type("alias_name")?, &MssqlColType::NVarchar);
            assert_eq!(meta.get_col_type("computed_value")?, &MssqlColType::Int4);
            assert_eq!(meta.get_col_type("valid_from")?, &MssqlColType::Datetime2);
            assert_eq!(meta.get_col_type("version")?, &MssqlColType::BigVarBin);

            MssqlTestEndpoint::execute_batch(
                &pool,
                &format!(
                    "ALTER TABLE [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TEST_TABLE}] \
                     ADD [added_later] int NULL;"
                ),
            )
            .await?;

            assert!(!manager
                .get_tb_meta(TEST_DATABASE, TEST_SCHEMA, TEST_TABLE)
                .await?
                .basic
                .cols
                .contains(&"added_later".to_string()));

            manager.invalidate_cache_for_table(TEST_DATABASE, TEST_SCHEMA, TEST_TABLE);
            assert!(manager
                .get_tb_meta(TEST_DATABASE, TEST_SCHEMA, TEST_TABLE)
                .await?
                .basic
                .cols
                .contains(&"added_later".to_string()));

            assert!(manager
                .get_tb_meta(TEST_DATABASE, TEST_SCHEMA, "table_does_not_exist")
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
