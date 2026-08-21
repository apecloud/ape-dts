#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use anyhow::Context;
    use dt_common::{
        config::{config_enums::DbType, router_config::RouterConfig},
        meta::{
            adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
            col_value::ColValue,
            dt_data::{DtData, DtItem},
            mssql::mssql_connection_pool::MssqlConnectionPool,
            position::Position,
            row_data::RowData,
            row_type::RowType,
        },
    };
    use dt_connector::{
        rdb_router::RdbRouter,
        sinker::{base_sinker::BaseSinker, mssql::mssql_sinker::MssqlSinker},
        Sinker,
    };
    use serial_test::serial;

    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    use super::super::TASK_CONFIG_FILE;

    const TEST_DATABASE: &str = "ape_dts";
    const TEST_SCHEMA: &str = "ape_dts_sinker_test";
    const TEST_TABLE: &str = "transaction_rows";

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Sinker)?;
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

    async fn prepare(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        cleanup(pool).await?;
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "USE [{TEST_DATABASE}];
                 EXEC(N'CREATE SCHEMA [{TEST_SCHEMA}]');
                 CREATE TABLE [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TEST_TABLE}] (
                    id int IDENTITY(1, 1) NOT NULL PRIMARY KEY,
                    code nvarchar(20) NOT NULL UNIQUE,
                    computed_code AS UPPER(code),
                    valid_from datetime2 GENERATED ALWAYS AS ROW START NOT NULL
                        DEFAULT SYSUTCDATETIME(),
                    valid_to datetime2 GENERATED ALWAYS AS ROW END NOT NULL
                        DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'),
                    version rowversion NOT NULL,
                    PERIOD FOR SYSTEM_TIME (valid_from, valid_to)
                 );"
            ),
        )
        .await
    }

    fn row(id: i32, code: &str) -> RowData {
        RowData::new(
            TEST_DATABASE.to_string(),
            TEST_SCHEMA.to_string(),
            TEST_TABLE.to_string(),
            0,
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(id)),
                ("code".to_string(), ColValue::String(code.to_string())),
                (
                    "computed_code".to_string(),
                    ColValue::String(code.to_uppercase()),
                ),
                (
                    "valid_from".to_string(),
                    ColValue::DateTime("2026-08-13 00:00:00".to_string()),
                ),
                (
                    "valid_to".to_string(),
                    ColValue::DateTime("9999-12-31 23:59:59.9999999".to_string()),
                ),
                ("version".to_string(), ColValue::Blob(vec![0; 8])),
            ])),
        )
    }

    async fn row_count(pool: &MssqlConnectionPool) -> anyhow::Result<i64> {
        let mut connection = pool.get().await?;
        let row = connection
            .client_mut()
            .query(
                &format!(
                    "SELECT COUNT_BIG(*) AS row_count \
                     FROM [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TEST_TABLE}]"
                ),
                &[],
            )
            .await?
            .into_row()
            .await?
            .context("MSSQL sinker test row count query returned no row")?;
        MssqlColValueConvertor::from_query_required_i64(&row, "row_count")
    }

    async fn code_for_id(pool: &MssqlConnectionPool, id: i32) -> anyhow::Result<Option<String>> {
        let mut connection = pool.get().await?;
        let row = connection
            .client_mut()
            .query(
                &format!(
                    "SELECT code FROM [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TEST_TABLE}] \
                     WHERE id = @P1"
                ),
                &[&id],
            )
            .await?
            .into_row()
            .await?;
        Ok(row.and_then(|row| row.get::<&str, _>("code").map(str::to_owned)))
    }

    #[tokio::test]
    #[serial]
    async fn failed_batch_insert_falls_back_to_serial_insert() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        prepare(&pool).await?;

        let result = async {
            let meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
            let mut sinker = MssqlSinker::new(
                pool.clone(),
                meta_manager,
                None,
                2,
                false,
                BaseSinker::default(),
            );

            sinker.sink_dml(vec![row(10, "original")], true).await?;
            let error = sinker
                .sink_dml(vec![row(10, "updated"), row(11, "new")], true)
                .await
                .expect_err("serial insert fallback should preserve the primary-key conflict");
            let error = format!("{error:#}");
            assert!(error.contains("duplicate") || error.contains("2627"));
            assert_eq!(code_for_id(&pool, 10).await?.as_deref(), Some("original"));
            assert_eq!(code_for_id(&pool, 11).await?, None);
            assert_eq!(row_count(&pool).await?, 1);

            MssqlTestEndpoint::execute_batch(
                &pool,
                &format!(
                    "INSERT INTO [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TEST_TABLE}] (code) \
                     VALUES (N'generated')"
                ),
            )
            .await?;
            sinker.sink_dml(vec![row(20, "explicit")], true).await?;
            assert_eq!(row_count(&pool).await?, 3);
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }

    #[tokio::test]
    #[serial]
    async fn replace_fallback_runs_all_single_rows_in_one_transaction() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        prepare(&pool).await?;

        let result = async {
            let meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
            let mut sinker = MssqlSinker::new(
                pool.clone(),
                meta_manager,
                None,
                2,
                false,
                BaseSinker::default(),
            );

            sinker
                .sink_dml(vec![row(10, "original"), row(11, "second")], true)
                .await?;
            sinker.replace = true;

            // The multi-row insert conflicts on id=10. Both rows are then
            // replaced serially inside one fallback transaction.
            sinker
                .sink_dml(vec![row(10, "updated"), row(12, "third")], true)
                .await?;
            assert_eq!(code_for_id(&pool, 10).await?.as_deref(), Some("updated"));
            assert_eq!(code_for_id(&pool, 12).await?.as_deref(), Some("third"));
            assert_eq!(row_count(&pool).await?, 3);

            // Row 10 succeeds first, then row 11 violates the unique code
            // constraint. One shared transaction must roll both changes back.
            let error = sinker
                .sink_dml(vec![row(10, "duplicate"), row(11, "duplicate")], true)
                .await
                .expect_err("serial sink fallback should fail on the second replace row");
            let error = format!("{error:#}");
            assert!(error.contains("duplicate") || error.contains("2601"));
            assert_eq!(code_for_id(&pool, 10).await?.as_deref(), Some("updated"));
            assert_eq!(code_for_id(&pool, 11).await?.as_deref(), Some("second"));
            assert_eq!(row_count(&pool).await?, 3);

            MssqlTestEndpoint::execute_batch(
                &pool,
                &format!(
                    "INSERT INTO [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TEST_TABLE}] (code) \
                     VALUES (N'generated')"
                ),
            )
            .await?;
            assert_eq!(row_count(&pool).await?, 4);
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_finished_invalidates_routed_table_meta() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        prepare(&pool).await?;

        let result = async {
            let router = RdbRouter::from_config(
                &RouterConfig::Rdb {
                    schema_map: String::new(),
                    tb_map: format!(
                        "{TEST_DATABASE}.source_schema.source_table:\
                         {TEST_DATABASE}.{TEST_SCHEMA}.{TEST_TABLE}"
                    ),
                    col_map: String::new(),
                    topic_map: String::new(),
                },
                &DbType::Mssql,
            )?
            .context("MSSQL sinker test router has no route rules")?;
            let meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
            let mut sinker = MssqlSinker::new(
                pool.clone(),
                meta_manager,
                Some(router),
                1,
                false,
                BaseSinker::default(),
            );

            assert!(!sinker
                .meta_manager
                .get_tb_meta(TEST_DATABASE, TEST_SCHEMA, TEST_TABLE)
                .await?
                .basic
                .cols
                .contains(&"added_after_cache".to_string()));
            MssqlTestEndpoint::execute_batch(
                &pool,
                &format!(
                    "ALTER TABLE [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TEST_TABLE}] \
                     ADD [added_after_cache] int NULL;"
                ),
            )
            .await?;

            sinker
                .handle_control_item(&DtItem {
                    dt_data: DtData::Commit { xid: String::new() },
                    position: Position::RdbSnapshotFinished {
                        db_type: "mssql".to_string(),
                        db: TEST_DATABASE.to_string(),
                        schema: "source_schema".to_string(),
                        tb: "source_table".to_string(),
                    },
                    data_origin_node: String::new(),
                })
                .await?;

            assert!(sinker
                .meta_manager
                .get_tb_meta(TEST_DATABASE, TEST_SCHEMA, TEST_TABLE)
                .await?
                .basic
                .cols
                .contains(&"added_after_cache".to_string()));
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }
}
