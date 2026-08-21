#[cfg(test)]
mod test {
    use anyhow::{ensure, Context};
    use dt_common::meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        col_value::ColValue,
        mssql::{
            mssql_col_type::{parse_mssql_col_type, MssqlColType},
            mssql_connection_pool::{MssqlClient, MssqlConnectionPool},
        },
    };
    use serial_test::serial;
    use tiberius::Query;

    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    use super::super::TASK_CONFIG_FILE;

    const TEST_DATABASE: &str = "ape_dts";
    const SOURCE_TABLE: &str = "[ape_dts].[dbo].[ape_dts_col_value_conversion_source]";
    const DESTINATION_TABLE: &str = "[ape_dts].[dbo].[ape_dts_col_value_conversion_destination]";
    const ROW_COUNT: usize = 4;

    struct ColumnCase {
        col: &'static str,
        source_sql_type: &'static str,
        destination_sql_type: &'static str,
        type_name: &'static str,
        source_values: [&'static str; ROW_COUNT],
        generated: bool,
    }

    fn conversion_cases() -> Vec<ColumnCase> {
        vec![
            ColumnCase {
                col: "bit_value",
                source_sql_type: "bit NULL",
                destination_sql_type: "bit NULL",
                type_name: "bit",
                source_values: ["0", "1", "1", "NULL"],
                generated: false,
            },
            ColumnCase {
                col: "tinyint_value",
                source_sql_type: "tinyint NULL",
                destination_sql_type: "tinyint NULL",
                type_name: "tinyint",
                source_values: ["0", "255", "128", "NULL"],
                generated: false,
            },
            ColumnCase {
                col: "smallint_value",
                source_sql_type: "smallint NULL",
                destination_sql_type: "smallint NULL",
                type_name: "smallint",
                source_values: ["-32768", "32767", "0", "NULL"],
                generated: false,
            },
            ColumnCase {
                col: "int_value",
                source_sql_type: "int NULL",
                destination_sql_type: "int NULL",
                type_name: "int",
                source_values: ["-2147483648", "2147483647", "0", "NULL"],
                generated: false,
            },
            ColumnCase {
                col: "bigint_value",
                source_sql_type: "bigint NULL",
                destination_sql_type: "bigint NULL",
                type_name: "bigint",
                source_values: [
                    "CONVERT(bigint, '-9223372036854775808')",
                    "CONVERT(bigint, '9223372036854775807')",
                    "0",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "real_value",
                source_sql_type: "real NULL",
                destination_sql_type: "real NULL",
                type_name: "real",
                source_values: [
                    "CONVERT(real, '-3.402823466E+38')",
                    "CONVERT(real, '3.402823466E+38')",
                    "CONVERT(real, '1.175494351E-38')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "float_value",
                source_sql_type: "float NULL",
                destination_sql_type: "float NULL",
                type_name: "float",
                source_values: [
                    "CONVERT(float, '-1.7976931348623157E+308')",
                    "CONVERT(float, '1.7976931348623157E+308')",
                    "CONVERT(float, '2.2250738585072014E-308')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "smallmoney_value",
                source_sql_type: "smallmoney NULL",
                destination_sql_type: "smallmoney NULL",
                type_name: "smallmoney",
                source_values: ["-214748.3648", "214748.3647", "0.0001", "NULL"],
                generated: false,
            },
            ColumnCase {
                col: "money_value",
                source_sql_type: "money NULL",
                destination_sql_type: "money NULL",
                type_name: "money",
                // Tiberius maps money to f64. The absolute endpoints cannot retain four
                // decimal places, so use the nearest stable values for this round trip.
                source_values: [
                    "CONVERT(money, '-922337203685477.5000')",
                    "CONVERT(money, '922337203685477.5000')",
                    "0.0001",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "decimal_value",
                source_sql_type: "decimal(38, 0) NULL",
                destination_sql_type: "decimal(38, 0) NULL",
                type_name: "decimal",
                source_values: [
                    "CONVERT(decimal(38, 0), '-99999999999999999999999999999999999999')",
                    "CONVERT(decimal(38, 0), '99999999999999999999999999999999999999')",
                    "0",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "numeric_value",
                source_sql_type: "numeric(38, 37) NULL",
                destination_sql_type: "numeric(38, 37) NULL",
                type_name: "numeric",
                source_values: [
                    "CONVERT(numeric(38, 37), '-9.9999999999999999999999999999999999999')",
                    "CONVERT(numeric(38, 37), '9.9999999999999999999999999999999999999')",
                    "CONVERT(numeric(38, 37), '0.0000000000000000000000000000000000001')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "char_latin1_value",
                source_sql_type: "char(16) COLLATE Latin1_General_100_CI_AS NULL",
                destination_sql_type: "char(16) COLLATE Latin1_General_100_CI_AS NULL",
                type_name: "char",
                source_values: ["''", "'0123456789ABCDEF'", "N'España français'", "NULL"],
                generated: false,
            },
            ColumnCase {
                col: "char_chinese_value",
                source_sql_type: "char(16) COLLATE Chinese_PRC_CI_AS NULL",
                destination_sql_type: "char(16) COLLATE Chinese_PRC_CI_AS NULL",
                type_name: "char",
                source_values: ["''", "N'中文边界'", "N'数据A'", "NULL"],
                generated: false,
            },
            ColumnCase {
                col: "varchar_latin1_value",
                source_sql_type: "varchar(32) COLLATE Latin1_General_100_CI_AS NULL",
                destination_sql_type: "varchar(32) COLLATE Latin1_General_100_CI_AS NULL",
                type_name: "varchar",
                source_values: [
                    "''",
                    "'0123456789ABCDEF0123456789ABCDEF'",
                    "N'España français'",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "varchar_chinese_value",
                source_sql_type: "varchar(32) COLLATE Chinese_PRC_CI_AS NULL",
                destination_sql_type: "varchar(32) COLLATE Chinese_PRC_CI_AS NULL",
                type_name: "varchar",
                source_values: ["''", "REPLICATE(N'中', 16)", "N'中文数据传输'", "NULL"],
                generated: false,
            },
            ColumnCase {
                col: "nchar_value",
                source_sql_type: "nchar(16) COLLATE Latin1_General_100_CI_AS_SC NULL",
                destination_sql_type: "nchar(16) COLLATE Latin1_General_100_CI_AS_SC NULL",
                type_name: "nchar",
                source_values: ["N''", "N'中文边界测试'", "N'中文😀'", "NULL"],
                generated: false,
            },
            ColumnCase {
                col: "nvarchar_value",
                source_sql_type: "nvarchar(32) COLLATE Latin1_General_100_CI_AS_SC NULL",
                destination_sql_type: "nvarchar(32) COLLATE Latin1_General_100_CI_AS_SC NULL",
                type_name: "nvarchar",
                source_values: [
                    "N''",
                    "REPLICATE(N'中', 32)",
                    "N'简体中文-繁體中文-😀'",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "text_latin1_value",
                source_sql_type: "text COLLATE Latin1_General_100_CI_AS NULL",
                destination_sql_type: "text COLLATE Latin1_General_100_CI_AS NULL",
                type_name: "text",
                source_values: [
                    "''",
                    "REPLICATE(CONVERT(varchar(max), 'z'), 1024)",
                    "N'España français'",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "text_chinese_value",
                source_sql_type: "text COLLATE Chinese_PRC_CI_AS NULL",
                destination_sql_type: "text COLLATE Chinese_PRC_CI_AS NULL",
                type_name: "text",
                source_values: [
                    "''",
                    "REPLICATE(CONVERT(varchar(max), N'中') COLLATE Chinese_PRC_CI_AS, 512)",
                    "N'中文数据传输'",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "ntext_value",
                source_sql_type: "ntext COLLATE Latin1_General_100_CI_AS NULL",
                destination_sql_type: "ntext COLLATE Latin1_General_100_CI_AS NULL",
                type_name: "ntext",
                source_values: [
                    "N''",
                    "REPLICATE(CONVERT(nvarchar(max), N'中'), 512)",
                    "N'中文-日本語-대한민국-😀'",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "binary_value",
                source_sql_type: "binary(8) NULL",
                destination_sql_type: "binary(8) NULL",
                type_name: "binary",
                source_values: [
                    "0x0000000000000000",
                    "0xFFFFFFFFFFFFFFFF",
                    "0x000102030405FEFF",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "varbinary_value",
                source_sql_type: "varbinary(32) NULL",
                destination_sql_type: "varbinary(32) NULL",
                type_name: "varbinary",
                source_values: [
                    "0x",
                    "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
                    "0x000102030405FEFF",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "image_value",
                source_sql_type: "image NULL",
                destination_sql_type: "image NULL",
                type_name: "image",
                source_values: [
                    "0x",
                    "CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), 'x'), 1024))",
                    "0x000102030405FEFF",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "uuid_value",
                source_sql_type: "uniqueidentifier NULL",
                destination_sql_type: "uniqueidentifier NULL",
                type_name: "uniqueidentifier",
                source_values: [
                    "'00000000-0000-0000-0000-000000000000'",
                    "'ffffffff-ffff-ffff-ffff-ffffffffffff'",
                    "'550e8400-e29b-41d4-a716-446655440000'",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "xml_value",
                source_sql_type: "xml NULL",
                destination_sql_type: "xml NULL",
                type_name: "xml",
                source_values: [
                    "CONVERT(xml, N'<a/>')",
                    "CONVERT(xml, N'<根 属性=\"边界\">中文😀</根>')",
                    "CONVERT(xml, N'<root><child>text &amp; value</child></root>')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "date_value",
                source_sql_type: "date NULL",
                destination_sql_type: "date NULL",
                type_name: "date",
                source_values: [
                    "CONVERT(date, '00010101')",
                    "CONVERT(date, '99991231')",
                    "CONVERT(date, '20240229')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "time_value",
                source_sql_type: "time(7) NULL",
                destination_sql_type: "time(7) NULL",
                type_name: "time",
                source_values: [
                    "CONVERT(time(7), '00:00:00')",
                    "CONVERT(time(7), '23:59:59.9999999')",
                    "CONVERT(time(7), '12:34:56.1234567')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "smalldatetime_value",
                source_sql_type: "smalldatetime NULL",
                destination_sql_type: "smalldatetime NULL",
                type_name: "smalldatetime",
                source_values: [
                    "CONVERT(smalldatetime, '19000101 00:00:00')",
                    "CONVERT(smalldatetime, '20790606 23:59:00')",
                    "CONVERT(smalldatetime, '20240814 12:34:00')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "datetime_value",
                source_sql_type: "datetime NULL",
                destination_sql_type: "datetime NULL",
                type_name: "datetime",
                source_values: [
                    "CONVERT(datetime, '17530101 00:00:00.000')",
                    "CONVERT(datetime, '99991231 23:59:59.997')",
                    "CONVERT(datetime, '20240814 12:34:56.123')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "datetime2_value",
                source_sql_type: "datetime2(7) NULL",
                destination_sql_type: "datetime2(7) NULL",
                type_name: "datetime2",
                source_values: [
                    "CONVERT(datetime2(7), '0001-01-01 00:00:00')",
                    "CONVERT(datetime2(7), '9999-12-31 23:59:59.9999999')",
                    "CONVERT(datetime2(7), '2024-08-14 12:34:56.1234567')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "offset_value",
                source_sql_type: "datetimeoffset(7) NULL",
                destination_sql_type: "datetimeoffset(7) NULL",
                type_name: "datetimeoffset",
                source_values: [
                    "CONVERT(datetimeoffset(7), '0001-01-01T00:00:00+00:00')",
                    "CONVERT(datetimeoffset(7), '9999-12-31T23:59:59.9999999+00:00')",
                    "CONVERT(datetimeoffset(7), '2024-08-14T12:34:56.1234567+14:00')",
                    "NULL",
                ],
                generated: false,
            },
            ColumnCase {
                col: "rowversion_value",
                source_sql_type: "rowversion NOT NULL",
                destination_sql_type: "binary(8) NOT NULL",
                type_name: "rowversion",
                source_values: ["", "", "", ""],
                generated: true,
            },
        ]
    }

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
                "DROP TABLE IF EXISTS {DESTINATION_TABLE};
                 DROP TABLE IF EXISTS {SOURCE_TABLE};"
            ),
        )
        .await
    }

    async fn prepare(pool: &MssqlConnectionPool, cases: &[ColumnCase]) -> anyhow::Result<()> {
        let source_columns = cases
            .iter()
            .map(|case| format!("[{}] {}", case.col, case.source_sql_type))
            .collect::<Vec<_>>();
        let destination_columns = cases
            .iter()
            .map(|case| format!("[{}] {}", case.col, case.destination_sql_type))
            .collect::<Vec<_>>();
        let inserted_cases = cases
            .iter()
            .filter(|case| !case.generated)
            .collect::<Vec<_>>();
        let inserted_columns = inserted_cases
            .iter()
            .map(|case| format!("[{}]", case.col))
            .collect::<Vec<_>>();
        let source_rows = (0..ROW_COUNT)
            .map(|row_index| {
                let values = inserted_cases
                    .iter()
                    .map(|case| case.source_values[row_index])
                    .collect::<Vec<_>>();
                format!("({}, {})", row_index + 1, values.join(", "))
            })
            .collect::<Vec<_>>();

        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "CREATE TABLE {SOURCE_TABLE} (
                    [case_id] tinyint NOT NULL PRIMARY KEY,
                    {}
                 );
                 CREATE TABLE {DESTINATION_TABLE} (
                    [case_id] tinyint NOT NULL PRIMARY KEY,
                    {}
                 );
                 INSERT INTO {SOURCE_TABLE} ([case_id], {}) VALUES {};",
                source_columns.join(",\n"),
                destination_columns.join(",\n"),
                inserted_columns.join(", "),
                source_rows.join(",\n")
            ),
        )
        .await
    }

    fn col_type(type_name: &str) -> anyhow::Result<MssqlColType> {
        parse_mssql_col_type(type_name)
    }

    fn column_names(cases: &[ColumnCase]) -> Vec<String> {
        cases.iter().map(|case| format!("[{}]", case.col)).collect()
    }

    async fn copy_source_rows(
        client: &mut MssqlClient,
        cases: &[ColumnCase],
    ) -> anyhow::Result<Vec<(u8, Vec<ColValue>)>> {
        let columns = column_names(cases);
        let rows = client
            .query(
                format!(
                    "SELECT [case_id], {} FROM {SOURCE_TABLE} ORDER BY [case_id]",
                    columns.join(", ")
                ),
                &[],
            )
            .await?
            .into_first_result()
            .await?;
        ensure!(rows.len() == ROW_COUNT, "unexpected source row count");

        let placeholders = (2..=cases.len() + 1)
            .map(|index| format!("@P{index}"))
            .collect::<Vec<_>>();
        let mut expected_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let case_id = row
                .try_get::<u8, _>("case_id")?
                .context("source case_id is NULL")?;
            let mut source_values = Vec::with_capacity(cases.len());
            let mut parsed_values = Vec::with_capacity(cases.len());
            for case in cases {
                let case_col_type = col_type(case.type_name)?;
                let source_value =
                    MssqlColValueConvertor::from_query(&row, case.col, &case_col_type)?;
                let option_string = source_value.to_option_string();
                let parsed_value = match option_string.as_deref() {
                    Some(value) => MssqlColValueConvertor::from_str(&case_col_type, value)
                        .with_context(|| {
                            format!(
                                "failed to parse {} value {value:?} from source row {case_id}",
                                case.col
                            )
                        })?,
                    None => ColValue::None,
                };
                source_values.push(source_value);
                parsed_values.push(parsed_value);
            }

            let mut insert = Query::new(format!(
                "INSERT INTO {DESTINATION_TABLE} ([case_id], {}) VALUES (@P1, {})",
                columns.join(", "),
                placeholders.join(", ")
            ));
            insert.bind(case_id);
            for (case, value) in cases.iter().zip(&parsed_values) {
                MssqlColValueConvertor::bind(&mut insert, value, &col_type(case.type_name)?)
                    .with_context(|| {
                        format!("failed to bind {} for source row {case_id}", case.col)
                    })?;
            }
            insert.execute(client).await?;
            expected_rows.push((case_id, source_values));
        }

        Ok(expected_rows)
    }

    async fn assert_destination_rows(
        client: &mut MssqlClient,
        cases: &[ColumnCase],
        expected_rows: &[(u8, Vec<ColValue>)],
    ) -> anyhow::Result<()> {
        let columns = column_names(cases);
        let rows = client
            .query(
                format!(
                    "SELECT [case_id], {} FROM {DESTINATION_TABLE} ORDER BY [case_id]",
                    columns.join(", ")
                ),
                &[],
            )
            .await?
            .into_first_result()
            .await?;
        ensure!(
            rows.len() == expected_rows.len(),
            "unexpected destination row count"
        );

        for (row, (expected_case_id, expected_values)) in rows.iter().zip(expected_rows) {
            let actual_case_id = row
                .try_get::<u8, _>("case_id")?
                .context("destination case_id is NULL")?;
            ensure!(actual_case_id == *expected_case_id, "case_id changed");
            for (case, expected) in cases.iter().zip(expected_values) {
                let actual =
                    MssqlColValueConvertor::from_query(row, case.col, &col_type(case.type_name)?)?;
                ensure!(
                    actual.is_same_value(expected),
                    "{} row {} changed after from_query -> ColValue -> Option<String> -> from_str -> bind: expected {expected:?}, got {actual:?}",
                    case.col,
                    actual_case_id
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn all_supported_values_round_trip_from_database_through_string_and_bind(
    ) -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        let cases = conversion_cases();
        prepare(&pool, &cases).await?;

        let result = async {
            let mut connection = pool.get().await?;
            let expected_rows = copy_source_rows(connection.client_mut(), &cases).await?;
            assert_destination_rows(connection.client_mut(), &cases, &expected_rows).await
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }
}
