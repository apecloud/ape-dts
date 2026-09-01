#[cfg(test)]
mod test {
    use anyhow::{ensure, Context};
    use dt_common::meta::{
        adaptor::{mssql_col_value_convertor::MssqlColValueConvertor, tiberius_ext::TiberiusExt},
        col_value::ColValue,
        mssql::{
            mssql_col_type::MssqlColType, mssql_connection_pool::MssqlClient,
            mssql_meta_manager::MssqlMetaManager,
        },
    };
    use serial_test::serial;
    use tiberius::Query;

    use crate::{
        mssql_to_mssql::functionality::mssql_component_test_context::{
            MssqlComponentTestContext, TestTable, TestTableName,
        },
        test_runner::mssql_test_endpoint::MssqlTestEndpoint,
    };

    struct ColumnCase {
        source_table: TestTableName,
        destination_table: TestTableName,
        col_type: MssqlColType,
    }

    async fn conversion_cases(
        source_tables: &[TestTableName],
        destination_database: &str,
        source_meta_manager: &mut MssqlMetaManager,
        destination_meta_manager: &mut MssqlMetaManager,
    ) -> anyhow::Result<Vec<ColumnCase>> {
        let mut cases = Vec::with_capacity(source_tables.len());
        for source_table in source_tables {
            let destination_table = source_table.with_database(destination_database);
            let source_meta = source_meta_manager
                .get_tb_meta(&source_table.db, &source_table.schema, &source_table.tb)
                .await?
                .clone();
            let destination_meta = destination_meta_manager
                .get_tb_meta(
                    &destination_table.db,
                    &destination_table.schema,
                    &destination_table.tb,
                )
                .await?
                .clone();
            let source_type = *source_meta.get_col_type("value")?;
            let destination_type = *destination_meta.get_col_type("value")?;
            let generated = !source_meta.is_writable_col("value");

            ensure!(
                generated || source_type == destination_type,
                "non-generated MSSQL column type differs between {} and {}: {source_type:?} != {destination_type:?}",
                source_table.quoted_name(),
                destination_table.quoted_name()
            );
            ensure!(
                destination_meta.is_writable_col("value"),
                "destination MSSQL value column is not writable: {}",
                destination_table.quoted_name()
            );
            cases.push(ColumnCase {
                source_table: source_table.clone(),
                destination_table,
                col_type: source_type,
            });
        }
        Ok(cases)
    }

    async fn round_trip_case(
        source_client: &mut MssqlClient,
        destination_client: &mut MssqlClient,
        case: &ColumnCase,
    ) -> anyhow::Result<()> {
        let source_rows = source_client
            .query(
                format!(
                    "SELECT [case_id], [value] FROM {} ORDER BY [case_id]",
                    case.source_table.quoted_name()
                ),
                &[],
            )
            .await?
            .into_first_result()
            .await?;
        ensure!(
            source_rows.len() == 4,
            "unexpected source row count for {}",
            case.source_table.quoted_name()
        );

        let mut expected_rows = Vec::with_capacity(source_rows.len());
        for row in source_rows {
            let case_id = row
                .try_get::<u8, _>("case_id")?
                .context("source case_id is NULL")?;
            let source_value = MssqlColValueConvertor::from_query(&row, "value", &case.col_type)?;
            let parsed_value = match source_value.to_option_string().as_deref() {
                Some(value) => MssqlColValueConvertor::from_str(&case.col_type, value)
                    .with_context(|| {
                        format!(
                            "failed to parse value {value:?} from {} row {case_id}",
                            case.source_table.quoted_name()
                        )
                    })?,
                None => ColValue::None,
            };

            let mut insert = Query::new(format!(
                "INSERT INTO {} ([case_id], [value]) VALUES (@P1, @P2)",
                case.destination_table.quoted_name()
            ));
            insert.bind(case_id);
            insert
                .bind_col_value(&parsed_value, &case.col_type)
                .with_context(|| {
                    format!(
                        "failed to bind value for {} row {case_id}",
                        case.source_table.quoted_name()
                    )
                })?;
            insert.execute(destination_client).await?;
            expected_rows.push((case_id, source_value));
        }

        let destination_rows = destination_client
            .query(
                format!(
                    "SELECT [case_id], [value] FROM {} ORDER BY [case_id]",
                    case.destination_table.quoted_name()
                ),
                &[],
            )
            .await?
            .into_first_result()
            .await?;
        ensure!(
            destination_rows.len() == expected_rows.len(),
            "unexpected destination row count for {}",
            case.destination_table.quoted_name()
        );

        for (row, (expected_case_id, expected_value)) in destination_rows.iter().zip(expected_rows)
        {
            let actual_case_id = row
                .try_get::<u8, _>("case_id")?
                .context("destination case_id is NULL")?;
            ensure!(actual_case_id == expected_case_id, "case_id changed");
            let actual_value = MssqlColValueConvertor::from_query(row, "value", &case.col_type)?;
            ensure!(
                actual_value.is_same_value(&expected_value),
                "{} row {} changed after from_query -> ColValue -> Option<String> -> from_str -> bind: expected {expected_value:?}, got {actual_value:?}",
                case.source_table.tb,
                actual_case_id
            );
        }
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn all_supported_values_round_trip_from_database_through_string_and_bind(
    ) -> anyhow::Result<()> {
        let context = MssqlComponentTestContext::new(
            "column_value_conversion_test",
            &[TestTable::new(
                "ape_dts_col_value_conversion_source",
                "dbo",
                "*",
            )],
            &[],
        )
        .await?;
        context.execute_test_sqls().await?;
        let mut source_meta_manager =
            MssqlTestEndpoint::create_meta_manager(context.source_pool.clone()).await?;
        let mut destination_meta_manager =
            MssqlTestEndpoint::create_meta_manager(context.sinker_pool.clone()).await?;
        let cases = conversion_cases(
            &context.source_tables,
            "ape_dts_col_value_conversion_destination",
            &mut source_meta_manager,
            &mut destination_meta_manager,
        )
        .await?;

        let mut source_connection = context.source_pool.get().await?;
        let mut destination_connection = context.sinker_pool.get().await?;
        for case in cases {
            round_trip_case(
                source_connection.client_mut(),
                destination_connection.client_mut(),
                &case,
            )
            .await?;
        }
        Ok(())
    }
}
