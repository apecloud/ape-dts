#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use anyhow::{bail, ensure, Context};
    use dt_common::meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        col_value::ColValue,
        mssql::{mssql_connection_pool::MssqlConnectionPool, mssql_tb_meta::MssqlTbMeta},
    };
    use dt_connector::extractor::{
        base_splitter::SnapshotChunk, mssql::mssql_snapshot_splitter::MssqlSnapshotSplitter,
    };
    use serial_test::serial;

    use crate::{
        mssql_to_mssql::functionality::mssql_component_test_context::{
            MssqlComponentTestContext, TestTable, TestTableName,
        },
        test_runner::mssql_test_endpoint::MssqlTestEndpoint,
    };

    const PARTITION_COL: &str = "value";
    const BATCH_SIZE: usize = 2;
    const ROW_COUNT: usize = 5;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SplitMode {
        Even,
        Uneven,
        FullTable,
    }

    fn split_mode(table: &TestTableName, tb_meta: &MssqlTbMeta) -> anyhow::Result<SplitMode> {
        let col_type = tb_meta.get_col_type(PARTITION_COL)?;
        let mode = match table.schema.as_str() {
            "even_split" => SplitMode::Even,
            "uneven_split" => SplitMode::Uneven,
            "full_table" => SplitMode::FullTable,
            schema => bail!("unknown MSSQL snapshot splitter test mode schema: {schema}"),
        };
        let type_matches_mode = match mode {
            SplitMode::Even => col_type.is_integer(),
            SplitMode::Uneven => col_type.can_be_splitted() && !col_type.is_integer(),
            SplitMode::FullTable => !col_type.can_be_splitted(),
        };
        ensure!(
            type_matches_mode,
            "MSSQL type {col_type:?} does not match {:?} mode for {}",
            mode,
            table.quoted_name()
        );
        Ok(mode)
    }

    async fn ordered_values(
        pool: &MssqlConnectionPool,
        tb_meta: &MssqlTbMeta,
        table: &TestTableName,
    ) -> anyhow::Result<Vec<ColValue>> {
        let col_type = tb_meta.get_col_type(PARTITION_COL)?;
        let sql = format!(
            "SELECT [{PARTITION_COL}] AS [split_value] FROM {} \
             WHERE [{PARTITION_COL}] IS NOT NULL ORDER BY [{PARTITION_COL}] ASC",
            table.quoted_name()
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
        table: &TestTableName,
        chunks: &[SnapshotChunk],
        expected_ends: &[ColValue],
    ) -> anyhow::Result<()> {
        let label = format!("{}.{}", table.quoted_name(), PARTITION_COL);
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
    ) -> anyhow::Result<MssqlSnapshotSplitter> {
        let mut splitter = MssqlSnapshotSplitter::new(
            Arc::new(tb_meta.clone()),
            pool.clone(),
            BATCH_SIZE,
            PARTITION_COL.to_string(),
        );
        splitter.init(&HashMap::new())?;
        Ok(splitter)
    }

    async fn verify_split_table(
        pool: &MssqlConnectionPool,
        tb_meta: &MssqlTbMeta,
        table: &TestTableName,
    ) -> anyhow::Result<()> {
        let mode = split_mode(table, tb_meta)?;
        let label = format!("{}.{}", table.quoted_name(), PARTITION_COL);
        let mut splitter = new_splitter(pool, tb_meta)?;

        if mode == SplitMode::FullTable {
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
            let values = ordered_values(pool, tb_meta, table).await?;
            ensure!(
                values.len() == ROW_COUNT,
                "split column {label}: expected {ROW_COUNT} values"
            );
            let expected_ends = match mode {
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
                    .with_context(|| format!("failed to split {label} in {mode:?} mode"))?;
                ensure!(
                    !next.is_empty(),
                    "split column {label}: splitter was exhausted too early"
                );
                if mode == SplitMode::Uneven {
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
            ensure_chunk_sequence(table, &chunks, &expected_ends)?;
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
        let context = MssqlComponentTestContext::new(
            "snapshot_splitter_type_test",
            &[
                TestTable::new(
                    "ape_dts_snapshot_splitter_component_test",
                    "even_split",
                    "*",
                ),
                TestTable::new(
                    "ape_dts_snapshot_splitter_component_test",
                    "uneven_split",
                    "*",
                ),
                TestTable::new(
                    "ape_dts_snapshot_splitter_component_test",
                    "full_table",
                    "*",
                ),
            ],
            &[],
        )
        .await?;
        context.execute_test_sqls().await?;
        let mut meta_manager =
            MssqlTestEndpoint::create_meta_manager(context.source_pool.clone()).await?;

        for table in &context.source_tables {
            let tb_meta = meta_manager
                .get_tb_meta(&table.db, &table.schema, &table.tb)
                .await?
                .clone();
            verify_split_table(&context.source_pool, &tb_meta, table).await?;
        }
        Ok(())
    }
}
