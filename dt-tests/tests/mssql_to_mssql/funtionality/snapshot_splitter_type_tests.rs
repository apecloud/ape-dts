#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        sync::{atomic::AtomicBool, Arc},
    };

    use anyhow::{ensure, Context};
    use dt_common::{
        config::{
            config_enums::{DbType, RdbParallelType},
            filter_config::FilterConfig,
        },
        meta::{
            adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
            col_value::ColValue,
            dt_queue::DtQueue,
            mssql::{mssql_connection_pool::MssqlConnectionPool, mssql_tb_meta::MssqlTbMeta},
        },
        monitor::task_monitor_handle::TaskMonitorHandle,
        rdb_filter::RdbFilter,
        time_filter::TimeFilter,
    };
    use dt_connector::{
        extractor::{
            base_extractor::{BaseExtractor, ExtractState},
            base_splitter::SnapshotChunk,
            extractor_monitor::ExtractorMonitor,
            mssql::{
                mssql_snapshot_extractor::{MssqlSnapshotExtractor, MssqlSnapshotShared},
                mssql_snapshot_splitter::MssqlSnapshotSplitter,
            },
        },
        Extractor,
    };
    use serial_test::serial;

    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    use super::super::TASK_CONFIG_FILE;

    const TEST_SCHEMA: &str = "ape_dts_snapshot_splitter_type_test";
    const ALL_TYPES_TABLE: &str = "all_types";
    const COMPUTED_ORDER_TABLE: &str = "computed_order_type";
    const TIMESTAMP_TABLE: &str = "timestamp_type";
    const BATCH_SIZE: usize = 2;

    const EVEN_INTEGER_COLS: &[&str] = &[
        "tinyint_value",
        "smallint_value",
        "int_value",
        "bigint_value",
    ];

    const UNEVEN_SPLIT_COLS: &[(&str, &str)] = &[
        (ALL_TYPES_TABLE, "real_value"),
        (ALL_TYPES_TABLE, "float_value"),
        (ALL_TYPES_TABLE, "smallmoney_value"),
        (ALL_TYPES_TABLE, "money_value"),
        (ALL_TYPES_TABLE, "decimal_value"),
        (ALL_TYPES_TABLE, "numeric_value"),
        (ALL_TYPES_TABLE, "char_value"),
        (ALL_TYPES_TABLE, "varchar_value"),
        (ALL_TYPES_TABLE, "nchar_value"),
        (ALL_TYPES_TABLE, "nvarchar_value"),
        (ALL_TYPES_TABLE, "binary_value"),
        (ALL_TYPES_TABLE, "varbinary_value"),
        (ALL_TYPES_TABLE, "uuid_value"),
        (ALL_TYPES_TABLE, "date_value"),
        (ALL_TYPES_TABLE, "time_value"),
        (ALL_TYPES_TABLE, "smalldatetime_value"),
        (ALL_TYPES_TABLE, "datetime_value"),
        (ALL_TYPES_TABLE, "datetime2_value"),
        (ALL_TYPES_TABLE, "datetimeoffset_value"),
        (ALL_TYPES_TABLE, "rowversion_value"),
        (TIMESTAMP_TABLE, "timestamp_value"),
    ];

    const NO_SPLIT_COLS: &[&str] = &[
        "bit_value",
        "text_value",
        "ntext_value",
        "image_value",
        "xml_value",
        "sql_variant_value",
        "geometry_value",
        "geography_value",
        "hierarchyid_value",
    ];

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Extractor)?;
        endpoint.ensure_database().await?;
        endpoint.create_pool().await
    }

    async fn cleanup(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "DROP TABLE IF EXISTS [{TEST_SCHEMA}].[{TIMESTAMP_TABLE}];
                 DROP TABLE IF EXISTS [{TEST_SCHEMA}].[{COMPUTED_ORDER_TABLE}];
                 DROP TABLE IF EXISTS [{TEST_SCHEMA}].[{ALL_TYPES_TABLE}];
                 IF SCHEMA_ID(N'{TEST_SCHEMA}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{TEST_SCHEMA}]');"
            ),
        )
        .await
    }

    async fn prepare(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "EXEC(N'CREATE SCHEMA [{TEST_SCHEMA}]');
                 CREATE TABLE [{TEST_SCHEMA}].[{ALL_TYPES_TABLE}] (
                    [id] int NOT NULL PRIMARY KEY,
                    [bit_value] bit NOT NULL,
                    [tinyint_value] tinyint NOT NULL,
                    [smallint_value] smallint NOT NULL,
                    [int_value] int NOT NULL,
                    [bigint_value] bigint NOT NULL,
                    [real_value] real NOT NULL,
                    [float_value] float NOT NULL,
                    [smallmoney_value] smallmoney NOT NULL,
                    [money_value] money NOT NULL,
                    [decimal_value] decimal(18, 4) NOT NULL,
                    [numeric_value] numeric(20, 6) NOT NULL,
                    [char_value] char(8) NOT NULL,
                    [varchar_value] varchar(20) NOT NULL,
                    [nchar_value] nchar(8) NOT NULL,
                    [nvarchar_value] nvarchar(20) NOT NULL,
                    [binary_value] binary(8) NOT NULL,
                    [varbinary_value] varbinary(8) NOT NULL,
                    [text_value] text NULL,
                    [ntext_value] ntext NULL,
                    [image_value] image NULL,
                    [uuid_value] uniqueidentifier NOT NULL,
                    [xml_value] xml NULL,
                    [date_value] date NOT NULL,
                    [time_value] time(7) NOT NULL,
                    [smalldatetime_value] smalldatetime NOT NULL,
                    [datetime_value] datetime NOT NULL,
                    [datetime2_value] datetime2(7) NOT NULL,
                    [datetimeoffset_value] datetimeoffset(7) NOT NULL,
                    [sql_variant_value] sql_variant NULL,
                    [geometry_value] geometry NULL,
                    [geography_value] geography NULL,
                    [hierarchyid_value] hierarchyid NULL,
                    [rowversion_value] rowversion NOT NULL,
                    [computed_value] AS ([id] * 2) PERSISTED,
                    [valid_from] datetime2 GENERATED ALWAYS AS ROW START NOT NULL
                        DEFAULT SYSUTCDATETIME(),
                    [valid_to] datetime2 GENERATED ALWAYS AS ROW END NOT NULL
                        DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'),
                    PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])
                 );
                 INSERT INTO [{TEST_SCHEMA}].[{ALL_TYPES_TABLE}] (
                    [id], [bit_value], [tinyint_value], [smallint_value], [int_value],
                    [bigint_value], [real_value], [float_value], [smallmoney_value],
                    [money_value], [decimal_value], [numeric_value], [char_value],
                    [varchar_value], [nchar_value], [nvarchar_value], [binary_value],
                    [varbinary_value], [text_value], [ntext_value], [image_value],
                    [uuid_value], [xml_value], [date_value], [time_value],
                    [smalldatetime_value], [datetime_value], [datetime2_value],
                    [datetimeoffset_value], [sql_variant_value], [geometry_value],
                    [geography_value], [hierarchyid_value]
                 )
                 SELECT
                    value_id,
                    CONVERT(bit, value_id % 2),
                    CONVERT(tinyint, value_id),
                    CONVERT(smallint, value_id),
                    CONVERT(int, value_id),
                    CONVERT(bigint, value_id),
                    CONVERT(real, value_id),
                    CONVERT(float, value_id),
                    CONVERT(smallmoney, value_id),
                    CONVERT(money, value_id),
                    CONVERT(decimal(18, 4), value_id),
                    CONVERT(numeric(20, 6), value_id),
                    CONVERT(char(8), CONCAT('c', value_id)),
                    CONVERT(varchar(20), CONCAT('v', value_id)),
                    CONVERT(nchar(8), CONCAT(N'nc', value_id)),
                    CONVERT(nvarchar(20), CONCAT(N'nv', value_id)),
                    CONVERT(binary(8), value_id),
                    CONVERT(varbinary(8), value_id),
                    CONVERT(varchar(20), CONCAT('text', value_id)),
                    CONVERT(nvarchar(20), CONCAT(N'ntext', value_id)),
                    CONVERT(varbinary(8), value_id),
                    CONVERT(uniqueidentifier, CONCAT(
                        '00000000-0000-0000-0000-',
                        RIGHT('000000000000' + CONVERT(varchar(12), value_id), 12)
                    )),
                    CONVERT(xml, CONCAT('<value>', value_id, '</value>')),
                    DATEADD(day, value_id, CONVERT(date, '2024-01-01')),
                    TIMEFROMPARTS(value_id, 0, 0, 0, 7),
                    DATEADD(day, value_id, CONVERT(smalldatetime, '2024-01-01')),
                    DATEADD(day, value_id, CONVERT(datetime, '2024-01-01')),
                    DATEADD(day, value_id, CONVERT(datetime2, '2024-01-01')),
                    TODATETIMEOFFSET(
                        DATEADD(day, value_id, CONVERT(datetime2, '2024-01-01')),
                        '+08:00'
                    ),
                    CONVERT(sql_variant, value_id),
                    geometry::Point(CONVERT(float, value_id), CONVERT(float, value_id), 0),
                    geography::Point(CONVERT(float, value_id), CONVERT(float, value_id), 4326),
                    hierarchyid::Parse(CONCAT('/', value_id, '/'))
                 FROM (VALUES (1), (2), (3), (4), (5)) AS values_to_insert(value_id);

                 CREATE TABLE [{TEST_SCHEMA}].[{COMPUTED_ORDER_TABLE}] (
                    [base_value] int NOT NULL,
                    [computed_value] AS ([base_value] * 2) PERSISTED
                 );
                 CREATE UNIQUE INDEX [uk_computed_order_type]
                    ON [{TEST_SCHEMA}].[{COMPUTED_ORDER_TABLE}] ([computed_value]);
                 INSERT INTO [{TEST_SCHEMA}].[{COMPUTED_ORDER_TABLE}] ([base_value])
                 VALUES (1), (2), (3);

                 CREATE TABLE [{TEST_SCHEMA}].[{TIMESTAMP_TABLE}] (
                    [id] int NOT NULL PRIMARY KEY,
                    [timestamp_value] timestamp NOT NULL
                 );
                 INSERT INTO [{TEST_SCHEMA}].[{TIMESTAMP_TABLE}] ([id])
                 VALUES (1), (2), (3), (4), (5);"
            ),
        )
        .await
    }

    async fn ordered_values(
        pool: &MssqlConnectionPool,
        table: &str,
        col: &str,
    ) -> anyhow::Result<Vec<ColValue>> {
        let sql = format!(
            "SELECT [{col}] AS [split_value] FROM [{TEST_SCHEMA}].[{table}] \
             WHERE [{col}] IS NOT NULL ORDER BY [{col}] ASC"
        );
        let mut connection = pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&sql, &[])
            .await?
            .into_first_result()
            .await?;
        rows.iter()
            .map(|row| MssqlColValueConvertor::from_query(row, "split_value"))
            .collect()
    }

    fn ensure_same(actual: &ColValue, expected: &ColValue, context: &str) -> anyhow::Result<()> {
        ensure!(
            actual.is_same_value(expected),
            "{context}: expected {expected:?}, got {actual:?}"
        );
        Ok(())
    }

    fn ensure_chunk_sequence(
        col: &str,
        chunks: &[SnapshotChunk],
        expected_ends: &[ColValue],
    ) -> anyhow::Result<()> {
        ensure!(
            chunks.len() == expected_ends.len(),
            "split column {col}: expected {} chunks, got {}",
            expected_ends.len(),
            chunks.len()
        );
        for (index, (chunk, expected_end)) in chunks.iter().zip(expected_ends).enumerate() {
            let expected_start = if index == 0 {
                &ColValue::None
            } else {
                &expected_ends[index - 1]
            };
            ensure_same(
                &chunk.chunk_range.0,
                expected_start,
                &format!("split column {col}, chunk {index} start"),
            )?;
            ensure_same(
                &chunk.chunk_range.1,
                expected_end,
                &format!("split column {col}, chunk {index} end"),
            )?;
        }
        Ok(())
    }

    fn new_splitter(
        pool: &MssqlConnectionPool,
        tb_meta: &MssqlTbMeta,
        col: &str,
    ) -> anyhow::Result<MssqlSnapshotSplitter> {
        let mut splitter = MssqlSnapshotSplitter::new(
            Arc::new(tb_meta.clone()),
            pool.clone(),
            BATCH_SIZE,
            col.to_string(),
        );
        splitter.init(&HashMap::new())?;
        Ok(splitter)
    }

    async fn verify_even_integer_split(
        pool: &MssqlConnectionPool,
        tb_meta: &MssqlTbMeta,
        col: &str,
    ) -> anyhow::Result<()> {
        let values = ordered_values(pool, ALL_TYPES_TABLE, col).await?;
        ensure!(values.len() == 5, "split column {col}: expected 5 values");
        let expected_ends = vec![values[2].clone(), values[4].clone()];
        let mut splitter = new_splitter(pool, tb_meta, col)?;
        let chunks = splitter
            .get_next_chunks()
            .await
            .with_context(|| format!("failed to evenly split {col}"))?;
        ensure_chunk_sequence(col, &chunks, &expected_ends)?;
        ensure!(
            splitter.get_next_chunks().await?.is_empty(),
            "split column {col}: integer splitter should be exhausted"
        );
        Ok(())
    }

    async fn verify_uneven_split(
        pool: &MssqlConnectionPool,
        tb_meta: &MssqlTbMeta,
        table: &str,
        col: &str,
    ) -> anyhow::Result<()> {
        let values = ordered_values(pool, table, col).await?;
        ensure!(values.len() == 5, "split column {col}: expected 5 values");
        let expected_ends = values
            .chunks(BATCH_SIZE)
            .filter_map(|values| values.last().cloned())
            .collect::<Vec<_>>();
        let mut splitter = new_splitter(pool, tb_meta, col)?;
        let mut chunks = Vec::with_capacity(expected_ends.len());
        for _ in 0..expected_ends.len() {
            let next = splitter
                .get_next_chunks()
                .await
                .with_context(|| format!("failed to unevenly split {table}.{col}"))?;
            ensure!(
                next.len() == 1,
                "split column {col}: uneven splitter must return one chunk at a time"
            );
            chunks.extend(next);
        }
        ensure!(
            splitter.get_next_chunks().await?.is_empty(),
            "split column {col}: uneven splitter should be exhausted"
        );
        ensure_chunk_sequence(col, &chunks, &expected_ends)
    }

    async fn verify_no_split(
        pool: &MssqlConnectionPool,
        tb_meta: &MssqlTbMeta,
        col: &str,
    ) -> anyhow::Result<()> {
        let mut splitter = new_splitter(pool, tb_meta, col)?;
        let chunks = splitter
            .get_next_chunks()
            .await
            .with_context(|| format!("failed to handle non-splittable column {col}"))?;
        ensure!(
            chunks.len() == 1,
            "split column {col}: non-splittable type must return one chunk"
        );
        ensure!(
            matches!(chunks[0].chunk_range, (ColValue::None, ColValue::None)),
            "split column {col}: non-splittable type must use the full-table range"
        );
        ensure!(
            splitter.get_next_chunks().await?.is_empty(),
            "split column {col}: non-splittable splitter should be exhausted"
        );
        Ok(())
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
                    (TEST_SCHEMA.to_string(), table.to_string()),
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
                batch_size: BATCH_SIZE,
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
            schema_tbs: HashMap::from([(TEST_SCHEMA.to_string(), vec![table.to_string()])]),
        };
        let error = extractor
            .extract()
            .await
            .expect_err("generated MSSQL order column should fail before table extraction");
        Ok((error, buffer))
    }

    #[tokio::test]
    #[serial]
    async fn splits_all_mssql_order_column_types_as_expected() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        prepare(&pool).await?;

        let result = async {
            let mut meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
            let all_types_meta = meta_manager
                .get_tb_meta(TEST_SCHEMA, ALL_TYPES_TABLE)
                .await?
                .clone();
            let timestamp_meta = meta_manager
                .get_tb_meta(TEST_SCHEMA, TIMESTAMP_TABLE)
                .await?
                .clone();

            for col in EVEN_INTEGER_COLS {
                verify_even_integer_split(&pool, &all_types_meta, col).await?;
            }
            for (table, col) in UNEVEN_SPLIT_COLS {
                let tb_meta = if *table == ALL_TYPES_TABLE {
                    &all_types_meta
                } else {
                    &timestamp_meta
                };
                verify_uneven_split(&pool, tb_meta, table, col).await?;
            }
            for col in NO_SPLIT_COLS {
                verify_no_split(&pool, &all_types_meta, col).await?;
            }
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }

    #[tokio::test]
    #[serial]
    async fn rejects_server_generated_order_columns_before_extracting_table() -> anyhow::Result<()>
    {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        prepare(&pool).await?;

        let result = async {
            for (table, partition_col, expected_col, expected_kind) in [
                (COMPUTED_ORDER_TABLE, None, "computed_value", "computed"),
                (
                    ALL_TYPES_TABLE,
                    Some("valid_from"),
                    "valid_from",
                    "generated always",
                ),
                (
                    ALL_TYPES_TABLE,
                    Some("rowversion_value"),
                    "rowversion_value",
                    "rowversion/timestamp",
                ),
                (
                    TIMESTAMP_TABLE,
                    Some("timestamp_value"),
                    "timestamp_value",
                    "rowversion/timestamp",
                ),
            ] {
                let (error, buffer) =
                    extractor_order_col_error(&pool, table, partition_col).await?;
                let error_chain = format!("{error:#}");
                ensure!(
                    error_chain.contains(expected_col) && error_chain.contains(expected_kind),
                    "unexpected order column validation error: {error_chain}"
                );
                ensure!(
                    buffer.is_empty(),
                    "extractor emitted data before rejecting {table}.{expected_col}"
                );
            }
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }
}
