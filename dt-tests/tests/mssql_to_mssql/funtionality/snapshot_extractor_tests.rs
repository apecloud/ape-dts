#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use anyhow::{ensure, Context};
    use dt_common::{
        config::{
            config_enums::{DbType, RdbParallelType},
            filter_config::FilterConfig,
        },
        meta::{
            col_value::ColValue,
            dt_data::{DtData, DtItem},
            dt_queue::DtQueue,
            mssql::mssql_connection_pool::MssqlConnectionPool,
            position::Position,
        },
        monitor::task_monitor_handle::TaskMonitorHandle,
        rdb_filter::RdbFilter,
        time_filter::TimeFilter,
    };
    use dt_connector::{
        extractor::{
            base_extractor::{BaseExtractor, ExtractState},
            extractor_monitor::ExtractorMonitor,
            mssql::{
                mssql_snapshot_extractor::{MssqlSnapshotExtractor, MssqlSnapshotShared},
                mssql_snapshot_splitter::MssqlSnapshotSplitter,
            },
        },
        Extractor,
    };
    use serial_test::serial;
    use tokio::time::timeout;

    use super::super::TASK_CONFIG_FILE;
    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    const TEST_DB: &str = "ape_dts";
    const CROSS_DB: &str = "ape_dts_snapshot_cross_db";
    const TEST_SCHEMA: &str = "ape_dts_snapshot_extractor_test";
    const TEST_TABLE: &str = "snapshot_rows";
    const TEST_COMPOSITE_TABLE: &str = "composite_rows";
    const GENERATED_ORDER_TABLE: &str = "generated_order_rows";
    const COMPUTED_ORDER_TABLE: &str = "computed_order_rows";
    const TIMESTAMP_ORDER_TABLE: &str = "timestamp_order_rows";

    struct InvalidOrderCase {
        table: &'static str,
        partition_col: Option<&'static str>,
        expected_col: &'static str,
        expected_kind: &'static str,
    }

    fn invalid_order_cases() -> [InvalidOrderCase; 4] {
        [
            InvalidOrderCase {
                table: COMPUTED_ORDER_TABLE,
                partition_col: None,
                expected_col: "computed_value",
                expected_kind: "computed",
            },
            InvalidOrderCase {
                table: GENERATED_ORDER_TABLE,
                partition_col: Some("valid_from"),
                expected_col: "valid_from",
                expected_kind: "generated always",
            },
            InvalidOrderCase {
                table: GENERATED_ORDER_TABLE,
                partition_col: Some("rowversion_value"),
                expected_col: "rowversion_value",
                expected_kind: "rowversion/timestamp",
            },
            InvalidOrderCase {
                table: TIMESTAMP_ORDER_TABLE,
                partition_col: Some("timestamp_value"),
                expected_col: "timestamp_value",
                expected_kind: "rowversion/timestamp",
            },
        ]
    }

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Extractor)?;
        endpoint.ensure_database(TEST_DB).await?;
        endpoint.create_pool().await
    }

    async fn cleanup(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "USE [{TEST_DB}];
                 DROP TABLE IF EXISTS [{TEST_DB}].[{TEST_SCHEMA}].[{TIMESTAMP_ORDER_TABLE}];
                 DROP TABLE IF EXISTS [{TEST_DB}].[{TEST_SCHEMA}].[{COMPUTED_ORDER_TABLE}];
                 DROP TABLE IF EXISTS [{TEST_DB}].[{TEST_SCHEMA}].[{GENERATED_ORDER_TABLE}];
                 DROP TABLE IF EXISTS [{TEST_DB}].[{TEST_SCHEMA}].[{TEST_COMPOSITE_TABLE}];
                 DROP TABLE IF EXISTS [{TEST_DB}].[{TEST_SCHEMA}].[{TEST_TABLE}];
                 IF SCHEMA_ID(N'{TEST_SCHEMA}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{TEST_SCHEMA}]');"
            ),
        )
        .await
    }

    async fn cleanup_cross_database(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "IF DB_ID(N'{CROSS_DB}') IS NOT NULL
                 BEGIN
                    ALTER DATABASE [{CROSS_DB}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{CROSS_DB}];
                 END;"
            ),
        )
        .await
    }

    async fn collect_extractor_output(
        mut extractor: MssqlSnapshotExtractor,
        buffer: Arc<DtQueue>,
    ) -> anyhow::Result<Vec<DtItem>> {
        let mut extractor_task = tokio::spawn(async move { extractor.extract().await });
        let collect_result = timeout(Duration::from_secs(30), async {
            let mut items = Vec::new();
            loop {
                while let Ok(item) = buffer.pop().await {
                    items.push(item);
                }
                if extractor_task.is_finished() {
                    (&mut extractor_task)
                        .await
                        .context("MSSQL snapshot extractor task failed to join")??;
                    while let Ok(item) = buffer.pop().await {
                        items.push(item);
                    }
                    return anyhow::Ok(items);
                }
                buffer.wait_for_data(Duration::from_millis(50)).await;
            }
        })
        .await;

        match collect_result {
            Ok(result) => result,
            Err(error) => {
                extractor_task.abort();
                let _ = extractor_task.await;
                Err(error).context("MSSQL snapshot extractor timed out")
            }
        }
    }

    async fn extractor_order_col_error(
        pool: &MssqlConnectionPool,
        table: &str,
        partition_col: Option<&str>,
    ) -> anyhow::Result<(anyhow::Error, Arc<DtQueue>)> {
        let buffer = Arc::new(DtQueue::new(16, 0, None, None));
        let partition_cols = partition_col
            .map(|col| {
                HashMap::from([(
                    (
                        TEST_DB.to_string(),
                        TEST_SCHEMA.to_string(),
                        table.to_string(),
                    ),
                    col.to_string(),
                )])
            })
            .unwrap_or_default();
        let mut extractor = MssqlSnapshotExtractor {
            shared: MssqlSnapshotShared {
                base_extractor: BaseExtractor {
                    buffer: Arc::clone(&buffer),
                    router: None,
                    shut_down: Arc::new(AtomicBool::new(false)),
                },
                connection_pool: pool.clone(),
                meta_manager: MssqlTestEndpoint::create_meta_manager(pool.clone()).await?,
                filter: Arc::new(RdbFilter::from_config(
                    &FilterConfig::default(),
                    &DbType::Mssql,
                )?),
                partition_cols: Arc::new(partition_cols),
                batch_size: 2,
                parallel_type: if partition_col.is_some() {
                    RdbParallelType::Chunk
                } else {
                    RdbParallelType::Table
                },
                recovery: None,
            },
            extract_state: ExtractState {
                monitor: ExtractorMonitor::new(TaskMonitorHandle::default(), String::new()).await,
                data_marker: None,
                time_filter: TimeFilter::default(),
            },
            parallel_size: 1,
            tbs: vec![(
                TEST_DB.to_string(),
                TEST_SCHEMA.to_string(),
                table.to_string(),
            )],
        };
        let error = extractor
            .extract()
            .await
            .expect_err("generated MSSQL order column should fail before table extraction");
        Ok((error, buffer))
    }

    #[tokio::test]
    #[serial]
    async fn rejects_server_generated_order_columns_before_extracting_table() -> anyhow::Result<()>
    {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "USE [{TEST_DB}];
                 EXEC(N'CREATE SCHEMA [{TEST_SCHEMA}]');
                 CREATE TABLE [{TEST_DB}].[{TEST_SCHEMA}].[{GENERATED_ORDER_TABLE}] (
                    [id] int NOT NULL PRIMARY KEY,
                    [rowversion_value] rowversion NOT NULL,
                    [valid_from] datetime2 GENERATED ALWAYS AS ROW START NOT NULL
                        DEFAULT SYSUTCDATETIME(),
                    [valid_to] datetime2 GENERATED ALWAYS AS ROW END NOT NULL
                        DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'),
                    PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])
                 );
                 INSERT INTO [{TEST_DB}].[{TEST_SCHEMA}].[{GENERATED_ORDER_TABLE}] ([id]) VALUES (1);

                 CREATE TABLE [{TEST_DB}].[{TEST_SCHEMA}].[{COMPUTED_ORDER_TABLE}] (
                    [base_value] int NOT NULL,
                    [computed_value] AS ([base_value] * 2) PERSISTED
                 );
                 CREATE UNIQUE INDEX [uk_computed_order_rows]
                    ON [{TEST_DB}].[{TEST_SCHEMA}].[{COMPUTED_ORDER_TABLE}] ([computed_value]);
                 INSERT INTO [{TEST_DB}].[{TEST_SCHEMA}].[{COMPUTED_ORDER_TABLE}] ([base_value]) VALUES (1);

                 CREATE TABLE [{TEST_DB}].[{TEST_SCHEMA}].[{TIMESTAMP_ORDER_TABLE}] (
                    [id] int NOT NULL PRIMARY KEY,
                    [timestamp_value] timestamp NOT NULL
                 );
                 INSERT INTO [{TEST_DB}].[{TEST_SCHEMA}].[{TIMESTAMP_ORDER_TABLE}] ([id]) VALUES (1);"
            ),
        )
        .await?;

        let result = async {
            for case in invalid_order_cases() {
                let (error, buffer) =
                    extractor_order_col_error(&pool, case.table, case.partition_col).await?;
                let error_chain = format!("{error:#}");
                ensure!(
                    error_chain.contains(case.expected_col)
                        && error_chain.contains(case.expected_kind),
                    "unexpected order column validation error: {error_chain}"
                );
                ensure!(
                    buffer.is_empty(),
                    "extractor emitted data before rejecting {}.{}",
                    case.table,
                    case.expected_col,
                );
            }
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn splits_and_extracts_real_mssql_snapshot() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "USE [{TEST_DB}];
                 EXEC(N'CREATE SCHEMA [{TEST_SCHEMA}]');
                 CREATE TABLE [{TEST_DB}].[{TEST_SCHEMA}].[{TEST_TABLE}] (
                    [id] int NOT NULL PRIMARY KEY,
                    [split_key] int NULL,
                    [name] nvarchar(20) NOT NULL
                 );
                 INSERT INTO [{TEST_DB}].[{TEST_SCHEMA}].[{TEST_TABLE}] ([id], [split_key], [name]) VALUES
                    (1, NULL, N'a'),
                    (2, NULL, N'b'),
                    (3, 10, N'c'),
                    (4, 20, N'd'),
                    (5, 30, N'e'),
                    (6, 40, N'f'),
                    (7, 50, N'g');
                 CREATE TABLE [{TEST_DB}].[{TEST_SCHEMA}].[{TEST_COMPOSITE_TABLE}] (
                    [tenant_id] int NOT NULL,
                    [id] int NOT NULL,
                    [name] nvarchar(20) NOT NULL,
                    CONSTRAINT [pk_ape_dts_snapshot_composite]
                        PRIMARY KEY ([tenant_id], [id])
                 );
                 INSERT INTO [{TEST_DB}].[{TEST_SCHEMA}].[{TEST_COMPOSITE_TABLE}]
                    ([tenant_id], [id], [name]) VALUES
                    (1, 1, N'a'),
                    (1, 2, N'b'),
                    (1, 3, N'c'),
                    (2, 1, N'd'),
                    (2, 2, N'e');"
            ),
        )
        .await?;

        let result = async {
            let mut meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
            let tb_meta = Arc::new(
                meta_manager
                    .get_tb_meta(TEST_DB, TEST_SCHEMA, TEST_TABLE)
                    .await?
                    .clone(),
            );

            let mut integer_splitter = MssqlSnapshotSplitter::new(
                Arc::clone(&tb_meta),
                pool.clone(),
                2,
                "split_key".to_string(),
            );
            integer_splitter.init(&HashMap::new())?;
            let integer_chunks = integer_splitter.get_next_chunks().await?;
            assert!(integer_chunks.len() > 1);
            assert!(matches!(
                integer_chunks.first().map(|chunk| &chunk.chunk_range.0),
                Some(ColValue::None)
            ));
            assert_eq!(
                integer_chunks.last().map(|chunk| &chunk.chunk_range.1),
                Some(&ColValue::Long(50))
            );

            let mut string_splitter = MssqlSnapshotSplitter::new(
                Arc::clone(&tb_meta),
                pool.clone(),
                2,
                "name".to_string(),
            );
            string_splitter.init(&HashMap::new())?;
            let mut string_ends = Vec::new();
            loop {
                let chunks = string_splitter.get_next_chunks().await?;
                if chunks.is_empty() {
                    break;
                }
                assert_eq!(chunks.len(), 1);
                string_ends.push(chunks[0].chunk_range.1.clone());
            }
            assert_eq!(
                string_ends,
                ["b", "d", "f", "g"]
                    .map(|value| ColValue::String(value.to_string()))
            );

            let buffer = Arc::new(DtQueue::new(128, 0, None, None));
            let shut_down = Arc::new(AtomicBool::new(false));
            let filter_config = FilterConfig {
                ignore_cols: format!(
                    r#"json:[{{"db":"{TEST_DB}","tb":"{TEST_SCHEMA}.{TEST_TABLE}","ignore_cols":["split_key"]}}]"#
                ),
                ..Default::default()
            };
            let filter = Arc::new(RdbFilter::from_config(&filter_config, &DbType::Mssql)?);
            let extractor = MssqlSnapshotExtractor {
                shared: MssqlSnapshotShared {
                    base_extractor: BaseExtractor {
                        buffer: Arc::clone(&buffer),
                        router: None,
                        shut_down: Arc::clone(&shut_down),
                    },
                    connection_pool: pool.clone(),
                    meta_manager: MssqlTestEndpoint::create_meta_manager(pool.clone()).await?,
                    filter,
                    partition_cols: Arc::new(HashMap::from([(
                        (
                            TEST_DB.to_string(),
                            TEST_SCHEMA.to_string(),
                            TEST_TABLE.to_string(),
                        ),
                        "split_key".to_string(),
                    )])),
                    batch_size: 2,
                    parallel_type: RdbParallelType::Chunk,
                    recovery: None,
                },
                extract_state: ExtractState {
                    monitor: ExtractorMonitor::new(TaskMonitorHandle::default(), String::new())
                        .await,
                    data_marker: None,
                    time_filter: TimeFilter::default(),
                },
                parallel_size: 3,
                tbs: vec![(
                    TEST_DB.to_string(),
                    TEST_SCHEMA.to_string(),
                    TEST_TABLE.to_string(),
                )],
            };

            let items = collect_extractor_output(extractor, Arc::clone(&buffer)).await?;
            assert!(shut_down.load(Ordering::Acquire));

            let mut ids = Vec::new();
            let mut snapshot_finished_count = 0;
            for item in items {
                match item.dt_data {
                    DtData::Dml { row_data } => {
                        let after = row_data.after.context("snapshot row has no after values")?;
                        assert!(!after.contains_key("split_key"));
                        assert!(after.contains_key("name"));
                        match after.get("id") {
                            Some(ColValue::Long(id)) => ids.push(*id),
                            value => anyhow::bail!("unexpected MSSQL snapshot id: {value:?}"),
                        }
                    }
                    DtData::Commit { .. }
                        if matches!(
                            item.position,
                            Position::RdbSnapshotFinished {
                                ref db_type,
                                ref db,
                                ref schema,
                                ref tb,
                            } if db_type == &DbType::Mssql.to_string()
                                && db == TEST_DB
                                && schema == TEST_SCHEMA
                                && tb == TEST_TABLE
                        ) =>
                    {
                        snapshot_finished_count += 1;
                    }
                    _ => {}
                }
            }
            ids.sort_unstable();
            assert_eq!(ids, [1, 2, 3, 4, 5, 6, 7]);
            assert_eq!(snapshot_finished_count, 1);

            let buffer = Arc::new(DtQueue::new(128, 0, None, None));
            let shut_down = Arc::new(AtomicBool::new(false));
            let filter = Arc::new(RdbFilter::from_config(
                &FilterConfig::default(),
                &DbType::Mssql,
            )?);
            let extractor = MssqlSnapshotExtractor {
                shared: MssqlSnapshotShared {
                    base_extractor: BaseExtractor {
                        buffer: Arc::clone(&buffer),
                        router: None,
                        shut_down: Arc::clone(&shut_down),
                    },
                    connection_pool: pool.clone(),
                    meta_manager: MssqlTestEndpoint::create_meta_manager(pool.clone()).await?,
                    filter,
                    partition_cols: Arc::new(HashMap::new()),
                    batch_size: 2,
                    parallel_type: RdbParallelType::Table,
                    recovery: None,
                },
                extract_state: ExtractState {
                    monitor: ExtractorMonitor::new(TaskMonitorHandle::default(), String::new())
                        .await,
                    data_marker: None,
                    time_filter: TimeFilter::default(),
                },
                parallel_size: 2,
                tbs: vec![(
                    TEST_DB.to_string(),
                    TEST_SCHEMA.to_string(),
                    TEST_COMPOSITE_TABLE.to_string(),
                )],
            };

            let items = collect_extractor_output(extractor, Arc::clone(&buffer)).await?;
            assert!(shut_down.load(Ordering::Acquire));
            let mut composite_ids = Vec::new();
            let mut composite_finished_count = 0;
            for item in items {
                match item.dt_data {
                    DtData::Dml { row_data } => {
                        let after = row_data.after.context("snapshot row has no after values")?;
                        let tenant_id = match after.get("tenant_id") {
                            Some(ColValue::Long(value)) => *value,
                            value => anyhow::bail!("unexpected MSSQL tenant_id: {value:?}"),
                        };
                        let id = match after.get("id") {
                            Some(ColValue::Long(value)) => *value,
                            value => anyhow::bail!("unexpected MSSQL id: {value:?}"),
                        };
                        composite_ids.push((tenant_id, id));
                    }
                    DtData::Commit { .. }
                        if matches!(
                            item.position,
                            Position::RdbSnapshotFinished {
                                ref db_type,
                                ref db,
                                ref schema,
                                ref tb,
                            } if db_type == &DbType::Mssql.to_string()
                                && db == TEST_DB
                                && schema == TEST_SCHEMA
                                && tb == TEST_COMPOSITE_TABLE
                        ) =>
                    {
                        composite_finished_count += 1;
                    }
                    _ => {}
                }
            }
            composite_ids.sort_unstable();
            assert_eq!(composite_ids, [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2)]);
            assert_eq!(composite_finished_count, 1);
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn extracts_from_database_other_than_connection_database() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup_cross_database(&pool).await?;
        MssqlTestEndpoint::execute_batch(&pool, &format!("CREATE DATABASE [{CROSS_DB}];")).await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "CREATE TABLE [{CROSS_DB}].[dbo].[cross_rows] (
                    [id] int NOT NULL PRIMARY KEY,
                    [name] nvarchar(20) NOT NULL
                 );
                 INSERT INTO [{CROSS_DB}].[dbo].[cross_rows] ([id], [name])
                 VALUES (1, N'one'), (2, N'two');"
            ),
        )
        .await?;

        let result = async {
            let buffer = Arc::new(DtQueue::new(32, 0, None, None));
            let shut_down = Arc::new(AtomicBool::new(false));
            let extractor = MssqlSnapshotExtractor {
                shared: MssqlSnapshotShared {
                    base_extractor: BaseExtractor {
                        buffer: Arc::clone(&buffer),
                        router: None,
                        shut_down: Arc::clone(&shut_down),
                    },
                    connection_pool: pool.clone(),
                    meta_manager: MssqlTestEndpoint::create_meta_manager(pool.clone()).await?,
                    filter: Arc::new(RdbFilter::from_config(
                        &FilterConfig::default(),
                        &DbType::Mssql,
                    )?),
                    partition_cols: Arc::new(HashMap::new()),
                    batch_size: 2,
                    parallel_type: RdbParallelType::Table,
                    recovery: None,
                },
                extract_state: ExtractState {
                    monitor: ExtractorMonitor::new(TaskMonitorHandle::default(), String::new())
                        .await,
                    data_marker: None,
                    time_filter: TimeFilter::default(),
                },
                parallel_size: 1,
                tbs: vec![(
                    CROSS_DB.to_string(),
                    "dbo".to_string(),
                    "cross_rows".to_string(),
                )],
            };

            let items = collect_extractor_output(extractor, Arc::clone(&buffer)).await?;
            assert!(shut_down.load(Ordering::Acquire));
            let mut ids = Vec::new();
            let mut finished = false;
            for item in items {
                match item.dt_data {
                    DtData::Dml { row_data } => {
                        assert_eq!(row_data.db, CROSS_DB);
                        assert_eq!(row_data.schema, "dbo");
                        assert_eq!(row_data.tb, "cross_rows");
                        match row_data.require_after()?.get("id") {
                            Some(ColValue::Long(id)) => ids.push(*id),
                            value => anyhow::bail!("unexpected cross database id: {value:?}"),
                        }
                    }
                    DtData::Commit { .. }
                        if matches!(
                            item.position,
                            Position::RdbSnapshotFinished {
                                ref db,
                                ref schema,
                                ref tb,
                                ..
                            } if db == CROSS_DB && schema == "dbo" && tb == "cross_rows"
                        ) =>
                    {
                        finished = true;
                    }
                    _ => {}
                }
            }
            ids.sort_unstable();
            assert_eq!(ids, [1, 2]);
            assert!(finished);
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup_cross_database(&pool).await;
        result?;
        cleanup_result
    }
}
