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
            mssql::{
                mssql_connection_pool::MssqlConnectionPool, mssql_meta_manager::MssqlMetaManager,
            },
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

    use crate::{
        mssql_to_mssql::functionality::mssql_component_test_context::{
            MssqlComponentTestContext, TestTable, TestTableName,
        },
        test_runner::mssql_test_endpoint::MssqlTestEndpoint,
    };

    fn row(table: &TestTableName, id: i32, code: &str) -> RowData {
        RowData::new(
            table.db.clone(),
            table.schema.clone(),
            table.tb.clone(),
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

    fn bulk_row(table: &TestTableName, id: i32, code: &str) -> RowData {
        RowData::new(
            table.db.clone(),
            table.schema.clone(),
            table.tb.clone(),
            0,
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(id)),
                ("code".to_string(), ColValue::String(code.to_string())),
                (
                    "happened_at".to_string(),
                    ColValue::DateTime("2026-08-13 12:34:56.1234567".to_string()),
                ),
            ])),
        )
    }

    fn parameter_row(table: &TestTableName, id: i32) -> RowData {
        RowData::new(
            table.db.clone(),
            table.schema.clone(),
            table.tb.clone(),
            0,
            RowType::Insert,
            None,
            Some(HashMap::from([
                ("id".to_string(), ColValue::Long(id)),
                (
                    "datetime_value".to_string(),
                    ColValue::DateTime("2026-08-13 12:34:56.123".to_string()),
                ),
                (
                    "smalldatetime_value".to_string(),
                    ColValue::DateTime("2026-08-13 12:34:00".to_string()),
                ),
            ])),
        )
    }

    async fn row_count(pool: &MssqlConnectionPool, table: &TestTableName) -> anyhow::Result<i64> {
        let mut connection = pool.get().await?;
        let row = connection
            .client_mut()
            .query(
                &format!(
                    "SELECT COUNT_BIG(*) AS row_count FROM {}",
                    table.quoted_name()
                ),
                &[],
            )
            .await?
            .into_row()
            .await?
            .context("MSSQL sinker test row count query returned no row")?;
        MssqlColValueConvertor::from_query_required_i64(&row, "row_count")
    }

    async fn matching_parameter_row_count(
        pool: &MssqlConnectionPool,
        table: &TestTableName,
    ) -> anyhow::Result<i64> {
        let mut connection = pool.get().await?;
        let row = connection
            .client_mut()
            .query(
                &format!(
                    "SELECT COUNT_BIG(*) AS row_count \
                     FROM {} \
                     WHERE datetime_value = CONVERT(datetime, '2026-08-13T12:34:56.123') \
                       AND smalldatetime_value = CONVERT(smalldatetime, '2026-08-13T12:34:00')",
                    table.quoted_name()
                ),
                &[],
            )
            .await?
            .into_row()
            .await?
            .context("MSSQL sinker parameter row count query returned no row")?;
        MssqlColValueConvertor::from_query_required_i64(&row, "row_count")
    }

    async fn code_for_id(
        pool: &MssqlConnectionPool,
        table: &TestTableName,
        id: i32,
    ) -> anyhow::Result<Option<String>> {
        let mut connection = pool.get().await?;
        let row = connection
            .client_mut()
            .query(
                &format!("SELECT code FROM {} WHERE id = @P1", table.quoted_name()),
                &[&id],
            )
            .await?
            .into_row()
            .await?;
        Ok(row.and_then(|row| row.get::<&str, _>("code").map(str::to_owned)))
    }

    async fn test_context() -> anyhow::Result<MssqlComponentTestContext> {
        MssqlComponentTestContext::new(
            "sinker_test",
            &[],
            &[TestTable::new("ape_dts_sinker_component_test", "dbo", "*")],
        )
        .await
    }

    fn new_sinker(
        pool: &MssqlConnectionPool,
        meta_manager: MssqlMetaManager,
        router: Option<RdbRouter>,
        batch_size: usize,
    ) -> MssqlSinker {
        MssqlSinker::new(
            pool.clone(),
            meta_manager,
            router,
            batch_size,
            false,
            BaseSinker::default(),
        )
    }

    #[tokio::test]
    #[serial]
    async fn batch_insert_selects_bulk_or_parameter_binding_by_table() -> anyhow::Result<()> {
        let context = test_context().await?;
        let pool = context.sinker_pool.clone();
        let bulk_table =
            context.sinker_table("ape_dts_sinker_component_test", "dbo", "bulk_rows")?;
        let parameter_table =
            context.sinker_table("ape_dts_sinker_component_test", "dbo", "parameter_rows")?;
        let bulk_rows = vec![
            bulk_row(bulk_table, 1, "bulk-1"),
            bulk_row(bulk_table, 2, "bulk-2"),
        ];
        let parameter_rows = vec![
            parameter_row(parameter_table, 1),
            parameter_row(parameter_table, 2),
        ];
        let mut meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;

        let bulk_meta = meta_manager
            .get_tb_meta(&bulk_table.db, &bulk_table.schema, &bulk_table.tb)
            .await?
            .clone();
        {
            let mut bulk_session = pool.get_table_sink_session(&bulk_meta).await?;
            assert!(bulk_session.can_bulk_insert(&bulk_rows));
            bulk_session.finalize().await?;
        }

        let parameter_meta = meta_manager
            .get_tb_meta(
                &parameter_table.db,
                &parameter_table.schema,
                &parameter_table.tb,
            )
            .await?
            .clone();
        {
            let mut parameter_session = pool.get_table_sink_session(&parameter_meta).await?;
            assert!(!parameter_session.can_bulk_insert(&parameter_rows));
            parameter_session.finalize().await?;
        }

        let mut sinker = new_sinker(&pool, meta_manager, None, 2);
        sinker.sink_dml(bulk_rows, true).await?;
        sinker.sink_dml(parameter_rows, true).await?;

        assert_eq!(row_count(&pool, bulk_table).await?, 2);
        assert_eq!(row_count(&pool, parameter_table).await?, 2);
        assert_eq!(
            matching_parameter_row_count(&pool, parameter_table).await?,
            2
        );
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn failed_batch_insert_falls_back_to_serial_insert() -> anyhow::Result<()> {
        let context = test_context().await?;
        let pool = context.sinker_pool.clone();
        let table =
            context.sinker_table("ape_dts_sinker_component_test", "dbo", "transaction_rows")?;
        let meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
        let mut sinker = new_sinker(&pool, meta_manager, None, 2);

        sinker
            .sink_dml(vec![row(table, 10, "original")], true)
            .await?;
        let error = sinker
            .sink_dml(vec![row(table, 10, "updated"), row(table, 11, "new")], true)
            .await
            .expect_err("serial insert fallback should preserve the primary-key conflict");
        let error = format!("{error:#}");
        assert!(error.contains("duplicate") || error.contains("2627"));
        assert_eq!(
            code_for_id(&pool, table, 10).await?.as_deref(),
            Some("original")
        );
        assert_eq!(code_for_id(&pool, table, 11).await?, None);
        assert_eq!(row_count(&pool, table).await?, 1);

        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "INSERT INTO {} (code) VALUES (N'generated')",
                table.quoted_name()
            ),
        )
        .await?;
        sinker
            .sink_dml(vec![row(table, 20, "explicit")], true)
            .await?;
        assert_eq!(row_count(&pool, table).await?, 3);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn upsert_fallback_runs_all_single_rows_in_one_transaction() -> anyhow::Result<()> {
        let context = test_context().await?;
        let pool = context.sinker_pool.clone();
        let table =
            context.sinker_table("ape_dts_sinker_component_test", "dbo", "transaction_rows")?;
        let meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
        let mut sinker = new_sinker(&pool, meta_manager, None, 2);

        sinker
            .sink_dml(
                vec![row(table, 10, "original"), row(table, 11, "second")],
                true,
            )
            .await?;
        sinker.replace = true;

        // The multi-row insert conflicts on id=10. Both rows are then
        // upserted serially inside one fallback transaction.
        sinker
            .sink_dml(
                vec![row(table, 10, "updated"), row(table, 12, "third")],
                true,
            )
            .await?;
        assert_eq!(
            code_for_id(&pool, table, 10).await?.as_deref(),
            Some("updated")
        );
        assert_eq!(
            code_for_id(&pool, table, 12).await?.as_deref(),
            Some("third")
        );
        assert_eq!(row_count(&pool, table).await?, 3);

        // Row 10 succeeds first, then row 11 violates the unique code
        // constraint. One shared transaction must roll both changes back.
        let error = sinker
            .sink_dml(
                vec![row(table, 10, "duplicate"), row(table, 11, "duplicate")],
                true,
            )
            .await
            .expect_err("serial sink fallback should fail on the second upsert row");
        let error = format!("{error:#}");
        assert!(error.contains("duplicate") || error.contains("2601"));
        assert_eq!(
            code_for_id(&pool, table, 10).await?.as_deref(),
            Some("updated")
        );
        assert_eq!(
            code_for_id(&pool, table, 11).await?.as_deref(),
            Some("second")
        );
        assert_eq!(row_count(&pool, table).await?, 3);

        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "INSERT INTO {} (code) VALUES (N'generated')",
                table.quoted_name()
            ),
        )
        .await?;
        assert_eq!(row_count(&pool, table).await?, 4);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_finished_invalidates_routed_table_meta() -> anyhow::Result<()> {
        let context = test_context().await?;
        let pool = context.sinker_pool.clone();
        let table =
            context.sinker_table("ape_dts_sinker_component_test", "dbo", "transaction_rows")?;
        let router = RdbRouter::from_config(
            &RouterConfig::Rdb {
                schema_map: String::new(),
                tb_map: format!(
                    "{}.source_schema.source_table:{}.{}.{}",
                    table.db, table.db, table.schema, table.tb
                ),
                col_map: String::new(),
                topic_map: String::new(),
            },
            &DbType::Mssql,
        )?
        .context("MSSQL sinker test router has no route rules")?;
        let meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
        let mut sinker = new_sinker(&pool, meta_manager, Some(router), 1);

        assert!(!sinker
            .meta_manager
            .get_tb_meta(&table.db, &table.schema, &table.tb)
            .await?
            .basic
            .cols
            .contains(&"added_after_cache".to_string()));
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "ALTER TABLE {} ADD [added_after_cache] int NULL;",
                table.quoted_name()
            ),
        )
        .await?;

        sinker
            .handle_control_item(&DtItem {
                dt_data: DtData::Commit { xid: String::new() },
                position: Position::RdbSnapshotFinished {
                    db_type: "mssql".to_string(),
                    db: table.db.clone(),
                    schema: "source_schema".to_string(),
                    tb: "source_table".to_string(),
                },
                data_origin_node: String::new(),
            })
            .await?;

        assert!(sinker
            .meta_manager
            .get_tb_meta(&table.db, &table.schema, &table.tb)
            .await?
            .basic
            .cols
            .contains(&"added_after_cache".to_string()));
        Ok(())
    }
}
