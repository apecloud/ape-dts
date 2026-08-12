#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        env,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use anyhow::Context;
    use dt_common::{
        config::{
            config_enums::{DbType, RdbParallelType},
            connection_auth_config::ConnectionAuthConfig,
            filter_config::FilterConfig,
            ssl_config::{SslConfig, SslMode},
        },
        meta::{
            col_value::ColValue,
            dt_data::{DtData, DtItem},
            dt_queue::DtQueue,
            mssql::{
                mssql_connection_pool::MssqlConnectionPool, mssql_meta_manager::MssqlMetaManager,
            },
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

    use crate::{
        test_config_util::TestConfigUtil, test_runner::mssql_test_client::MssqlTestClient,
    };

    const TEST_SCHEMA: &str = "ape_dts_snapshot_extractor_test";
    const TEST_TABLE: &str = "snapshot_rows";
    const TEST_COMPOSITE_TABLE: &str = "composite_rows";

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
        MssqlConnectionPool::from_config(&connection_string, &auth, None, 4, 15).await
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
                "DROP TABLE IF EXISTS [{TEST_SCHEMA}].[{TEST_COMPOSITE_TABLE}];
                 DROP TABLE IF EXISTS [{TEST_SCHEMA}].[{TEST_TABLE}];
                 IF SCHEMA_ID(N'{TEST_SCHEMA}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{TEST_SCHEMA}]');"
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn splits_and_extracts_real_mssql_snapshot() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        execute_batch(
            &pool,
            &format!(
                "EXEC(N'CREATE SCHEMA [{TEST_SCHEMA}]');
                 CREATE TABLE [{TEST_SCHEMA}].[{TEST_TABLE}] (
                    [id] int NOT NULL PRIMARY KEY,
                    [split_key] int NULL,
                    [name] nvarchar(20) NOT NULL
                 );
                 INSERT INTO [{TEST_SCHEMA}].[{TEST_TABLE}] ([id], [split_key], [name]) VALUES
                    (1, NULL, N'a'),
                    (2, NULL, N'b'),
                    (3, 10, N'c'),
                    (4, 20, N'd'),
                    (5, 30, N'e'),
                    (6, 40, N'f'),
                    (7, 50, N'g');
                 CREATE TABLE [{TEST_SCHEMA}].[{TEST_COMPOSITE_TABLE}] (
                    [tenant_id] int NOT NULL,
                    [id] int NOT NULL,
                    [name] nvarchar(20) NOT NULL,
                    CONSTRAINT [pk_ape_dts_snapshot_composite]
                        PRIMARY KEY ([tenant_id], [id])
                 );
                 INSERT INTO [{TEST_SCHEMA}].[{TEST_COMPOSITE_TABLE}]
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
            let mut meta_manager = MssqlMetaManager::new(pool.clone()).await?;
            let tb_meta = Arc::new(meta_manager.get_tb_meta(TEST_SCHEMA, TEST_TABLE).await?.clone());

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
                    r#"json:[{{"db":"{TEST_SCHEMA}","tb":"{TEST_TABLE}","ignore_cols":["split_key"]}}]"#
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
                    meta_manager: MssqlMetaManager::new(pool.clone()).await?,
                    filter,
                    partition_cols: Arc::new(HashMap::from([(
                        (TEST_SCHEMA.to_string(), TEST_TABLE.to_string()),
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
                schema_tbs: HashMap::from([(
                    TEST_SCHEMA.to_string(),
                    vec![TEST_TABLE.to_string()],
                )]),
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
                                ref schema,
                                ref tb,
                            } if db_type == &DbType::Mssql.to_string()
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
                    meta_manager: MssqlMetaManager::new(pool.clone()).await?,
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
                schema_tbs: HashMap::from([(
                    TEST_SCHEMA.to_string(),
                    vec![TEST_COMPOSITE_TABLE.to_string()],
                )]),
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
                                ref schema,
                                ref tb,
                            } if db_type == &DbType::Mssql.to_string()
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
}
