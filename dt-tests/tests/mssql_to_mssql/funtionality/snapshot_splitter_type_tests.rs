#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use anyhow::{ensure, Context};
    use dt_common::meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        col_value::ColValue,
        mssql::{mssql_connection_pool::MssqlConnectionPool, mssql_tb_meta::MssqlTbMeta},
    };
    use dt_connector::extractor::{
        base_splitter::SnapshotChunk, mssql::mssql_snapshot_splitter::MssqlSnapshotSplitter,
    };
    use serial_test::serial;

    use super::super::TASK_CONFIG_FILE;
    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    const TEST_DATABASE: &str = "ape_dts";
    const TEST_SCHEMA: &str = "ape_dts_snapshot_splitter_type_test";
    const ALL_TYPES_TABLE: &str = "all_types";
    const TIMESTAMP_TABLE: &str = "timestamp_type";
    const BATCH_SIZE: usize = 2;
    const ROW_COUNT: usize = 5;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SplitMode {
        Even,
        Uneven,
        FullTable,
    }

    struct SplitCase {
        table: &'static str,
        col: &'static str,
        sql_type: &'static str,
        source_expr: Option<&'static str>,
        mode: SplitMode,
    }

    fn split_cases() -> Vec<SplitCase> {
        vec![
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "bit_value",
                sql_type: "bit NOT NULL",
                source_expr: Some("CONVERT(bit, value_id % 2)"),
                mode: SplitMode::FullTable,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "tinyint_value",
                sql_type: "tinyint NOT NULL",
                source_expr: Some("CONVERT(tinyint, value_id)"),
                mode: SplitMode::Even,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "smallint_value",
                sql_type: "smallint NOT NULL",
                source_expr: Some("CONVERT(smallint, value_id)"),
                mode: SplitMode::Even,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "int_value",
                sql_type: "int NOT NULL",
                source_expr: Some("CONVERT(int, value_id)"),
                mode: SplitMode::Even,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "bigint_value",
                sql_type: "bigint NOT NULL",
                source_expr: Some("CONVERT(bigint, value_id)"),
                mode: SplitMode::Even,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "real_value",
                sql_type: "real NOT NULL",
                source_expr: Some("CONVERT(real, value_id)"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "float_value",
                sql_type: "float NOT NULL",
                source_expr: Some("CONVERT(float, value_id)"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "smallmoney_value",
                sql_type: "smallmoney NOT NULL",
                source_expr: Some("CONVERT(smallmoney, value_id)"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "money_value",
                sql_type: "money NOT NULL",
                source_expr: Some("CONVERT(money, value_id)"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "decimal_value",
                sql_type: "decimal(18, 4) NOT NULL",
                source_expr: Some("CONVERT(decimal(18, 4), value_id)"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "numeric_value",
                sql_type: "numeric(20, 6) NOT NULL",
                source_expr: Some("CONVERT(numeric(20, 6), value_id)"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "char_value",
                sql_type: "char(8) NOT NULL",
                source_expr: Some("CONVERT(char(8), CONCAT('c', value_id))"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "varchar_value",
                sql_type: "varchar(20) NOT NULL",
                source_expr: Some("CONVERT(varchar(20), CONCAT('v', value_id))"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "nchar_value",
                sql_type: "nchar(8) NOT NULL",
                source_expr: Some("CONVERT(nchar(8), CONCAT(N'nc', value_id))"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "nvarchar_value",
                sql_type: "nvarchar(20) NOT NULL",
                source_expr: Some("CONVERT(nvarchar(20), CONCAT(N'nv', value_id))"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "binary_value",
                sql_type: "binary(8) NOT NULL",
                source_expr: Some("CONVERT(binary(8), value_id)"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "varbinary_value",
                sql_type: "varbinary(8) NOT NULL",
                source_expr: Some("CONVERT(varbinary(8), value_id)"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "text_value",
                sql_type: "text NULL",
                source_expr: Some("CONVERT(varchar(20), CONCAT('text', value_id))"),
                mode: SplitMode::FullTable,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "ntext_value",
                sql_type: "ntext NULL",
                source_expr: Some("CONVERT(nvarchar(20), CONCAT(N'ntext', value_id))"),
                mode: SplitMode::FullTable,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "image_value",
                sql_type: "image NULL",
                source_expr: Some("CONVERT(varbinary(8), value_id)"),
                mode: SplitMode::FullTable,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "uuid_value",
                sql_type: "uniqueidentifier NOT NULL",
                source_expr: Some(
                    "CONVERT(uniqueidentifier, CONCAT('00000000-0000-0000-0000-', \
                     RIGHT('000000000000' + CONVERT(varchar(12), value_id), 12)))",
                ),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "xml_value",
                sql_type: "xml NULL",
                source_expr: Some("CONVERT(xml, CONCAT('<value>', value_id, '</value>'))"),
                mode: SplitMode::FullTable,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "date_value",
                sql_type: "date NOT NULL",
                source_expr: Some("DATEADD(day, value_id, CONVERT(date, '2024-01-01'))"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "time_value",
                sql_type: "time(7) NOT NULL",
                source_expr: Some("TIMEFROMPARTS(value_id, 0, 0, 0, 7)"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "smalldatetime_value",
                sql_type: "smalldatetime NOT NULL",
                source_expr: Some("DATEADD(day, value_id, CONVERT(smalldatetime, '2024-01-01'))"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "datetime_value",
                sql_type: "datetime NOT NULL",
                source_expr: Some("DATEADD(day, value_id, CONVERT(datetime, '2024-01-01'))"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "datetime2_value",
                sql_type: "datetime2(7) NOT NULL",
                source_expr: Some("DATEADD(day, value_id, CONVERT(datetime2, '2024-01-01'))"),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "datetimeoffset_value",
                sql_type: "datetimeoffset(7) NOT NULL",
                source_expr: Some(
                    "TODATETIMEOFFSET(DATEADD(day, value_id, \
                     CONVERT(datetime2, '2024-01-01')), '+08:00')",
                ),
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: ALL_TYPES_TABLE,
                col: "rowversion_value",
                sql_type: "rowversion NOT NULL",
                source_expr: None,
                mode: SplitMode::Uneven,
            },
            SplitCase {
                table: TIMESTAMP_TABLE,
                col: "timestamp_value",
                sql_type: "timestamp NOT NULL",
                source_expr: None,
                mode: SplitMode::Uneven,
            },
        ]
    }

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Extractor)?;
        endpoint.ensure_database(TEST_DATABASE).await?;
        endpoint.create_pool().await
    }

    async fn cleanup(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "USE [{TEST_DATABASE}];
                 DROP TABLE IF EXISTS [{TEST_DATABASE}].[{TEST_SCHEMA}].[{TIMESTAMP_TABLE}];
                 DROP TABLE IF EXISTS [{TEST_DATABASE}].[{TEST_SCHEMA}].[{ALL_TYPES_TABLE}];
                 IF SCHEMA_ID(N'{TEST_SCHEMA}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{TEST_SCHEMA}]');"
            ),
        )
        .await
    }

    fn table_setup_sql(cases: &[SplitCase], table: &str) -> String {
        let table_cases = cases
            .iter()
            .filter(|case| case.table == table)
            .collect::<Vec<_>>();
        let mut definitions = vec!["[id] int NOT NULL PRIMARY KEY".to_string()];
        definitions.extend(
            table_cases
                .iter()
                .map(|case| format!("[{}] {}", case.col, case.sql_type)),
        );
        let inserted_cases = table_cases
            .iter()
            .filter_map(|case| case.source_expr.map(|source_expr| (*case, source_expr)))
            .collect::<Vec<_>>();
        let insert_cols = std::iter::once("[id]".to_string())
            .chain(
                inserted_cases
                    .iter()
                    .map(|(case, _)| format!("[{}]", case.col)),
            )
            .collect::<Vec<_>>();
        let select_exprs = std::iter::once("value_id".to_string())
            .chain(
                inserted_cases
                    .iter()
                    .map(|(_, source_expr)| (*source_expr).to_string()),
            )
            .collect::<Vec<_>>();
        let rows = (1..=ROW_COUNT)
            .map(|value| format!("({value})"))
            .collect::<Vec<_>>();

        format!(
            "CREATE TABLE [{TEST_DATABASE}].[{TEST_SCHEMA}].[{table}] ({}); \
             INSERT INTO [{TEST_DATABASE}].[{TEST_SCHEMA}].[{table}] ({}) \
             SELECT {} FROM (VALUES {}) AS values_to_insert(value_id);",
            definitions.join(", "),
            insert_cols.join(", "),
            select_exprs.join(", "),
            rows.join(", "),
        )
    }

    async fn prepare(pool: &MssqlConnectionPool, cases: &[SplitCase]) -> anyhow::Result<()> {
        let all_types_sql = table_setup_sql(cases, ALL_TYPES_TABLE);
        let timestamp_sql = table_setup_sql(cases, TIMESTAMP_TABLE);
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "USE [{TEST_DATABASE}];
                 EXEC(N'CREATE SCHEMA [{TEST_SCHEMA}]');
                 {all_types_sql}
                 {timestamp_sql}"
            ),
        )
        .await
    }

    async fn ordered_values(
        pool: &MssqlConnectionPool,
        tb_meta: &MssqlTbMeta,
        case: &SplitCase,
    ) -> anyhow::Result<Vec<ColValue>> {
        let col_type = tb_meta.get_col_type(case.col)?;
        let sql = format!(
            "SELECT [{0}] AS [split_value] \
             FROM [{TEST_DATABASE}].[{TEST_SCHEMA}].[{1}] \
             WHERE [{0}] IS NOT NULL ORDER BY [{0}] ASC",
            case.col, case.table,
        );
        let mut connection = pool.get().await?;
        let rows = connection
            .client_mut()
            .query(&sql, &[])
            .await?
            .into_first_result()
            .await?;
        rows.iter()
            .map(|row| MssqlColValueConvertor::from_query(row, "split_value", col_type))
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
        case: &SplitCase,
        chunks: &[SnapshotChunk],
        expected_ends: &[ColValue],
    ) -> anyhow::Result<()> {
        let label = format!("{}.{}", case.table, case.col);
        ensure!(
            chunks.len() == expected_ends.len(),
            "split column {label}: expected {} chunks, got {}",
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
                &format!("split column {label}, chunk {index} start"),
            )?;
            ensure_same(
                &chunk.chunk_range.1,
                expected_end,
                &format!("split column {label}, chunk {index} end"),
            )?;
        }
        Ok(())
    }

    fn new_splitter(
        pool: &MssqlConnectionPool,
        tb_meta: &MssqlTbMeta,
        case: &SplitCase,
    ) -> anyhow::Result<MssqlSnapshotSplitter> {
        let mut splitter = MssqlSnapshotSplitter::new(
            Arc::new(tb_meta.clone()),
            pool.clone(),
            BATCH_SIZE,
            case.col.to_string(),
        );
        splitter.init(&HashMap::new())?;
        Ok(splitter)
    }

    async fn verify_split_case(
        pool: &MssqlConnectionPool,
        tb_meta: &MssqlTbMeta,
        case: &SplitCase,
    ) -> anyhow::Result<()> {
        let label = format!("{}.{}", case.table, case.col);
        let mut splitter = new_splitter(pool, tb_meta, case)?;

        if case.mode == SplitMode::FullTable {
            let chunks = splitter
                .get_next_chunks()
                .await
                .with_context(|| format!("failed to split {label}"))?;
            ensure!(
                chunks.len() == 1
                    && matches!(chunks[0].chunk_range, (ColValue::None, ColValue::None)),
                "split column {label}: non-splittable type must return one full-table chunk"
            );
        } else {
            let values = ordered_values(pool, tb_meta, case).await?;
            ensure!(
                values.len() == ROW_COUNT,
                "split column {label}: expected {ROW_COUNT} values"
            );
            let expected_ends = match case.mode {
                SplitMode::Even => vec![
                    values[values.len() / 2].clone(),
                    values.last().unwrap().clone(),
                ],
                SplitMode::Uneven => values
                    .chunks(BATCH_SIZE)
                    .filter_map(|values| values.last().cloned())
                    .collect(),
                SplitMode::FullTable => unreachable!(),
            };
            let mut chunks = Vec::with_capacity(expected_ends.len());
            while chunks.len() < expected_ends.len() {
                let next = splitter
                    .get_next_chunks()
                    .await
                    .with_context(|| format!("failed to split {label} in {:?} mode", case.mode))?;
                ensure!(
                    !next.is_empty(),
                    "split column {label}: splitter was exhausted too early"
                );
                if case.mode == SplitMode::Uneven {
                    ensure!(
                        next.len() == 1,
                        "split column {label}: uneven splitter must return one chunk at a time"
                    );
                }
                chunks.extend(next);
                ensure!(
                    chunks.len() <= expected_ends.len(),
                    "split column {label}: splitter returned too many chunks"
                );
            }
            ensure_chunk_sequence(case, &chunks, &expected_ends)?;
        }

        ensure!(
            splitter.get_next_chunks().await?.is_empty(),
            "split column {label}: splitter should be exhausted"
        );
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn splits_all_mssql_order_column_types_as_expected() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        let cases = split_cases();
        prepare(&pool, &cases).await?;

        let result = async {
            let mut meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
            let all_types_meta = meta_manager
                .get_tb_meta(TEST_DATABASE, TEST_SCHEMA, ALL_TYPES_TABLE)
                .await?
                .clone();
            let timestamp_meta = meta_manager
                .get_tb_meta(TEST_DATABASE, TEST_SCHEMA, TIMESTAMP_TABLE)
                .await?
                .clone();

            for case in &cases {
                let tb_meta = if case.table == ALL_TYPES_TABLE {
                    &all_types_meta
                } else {
                    &timestamp_meta
                };
                verify_split_case(&pool, tb_meta, case).await?;
            }
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }
}
