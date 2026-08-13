#[cfg(test)]
mod test {
    use anyhow::{ensure, Context};
    use dt_common::meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor,
        col_value::ColValue,
        mssql::{
            mssql_col_type::{parse_mssql_col_type, MssqlColType},
            mssql_connection_pool::MssqlConnectionPool,
            mssql_tb_meta::MssqlTbMeta,
        },
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
    };
    use serial_test::serial;
    use tiberius::Query;

    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    use super::super::TASK_CONFIG_FILE;

    const TEST_TABLE: &str = "[dbo].[ape_dts_col_value_conversion_test]";
    const TIMESTAMP_TABLE: &str = "[dbo].[ape_dts_timestamp_conversion_test]";
    const BOUNDARY_TABLE: &str = "[dbo].[ape_dts_col_value_boundary_test]";

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Extractor)?;
        endpoint.ensure_database().await?;
        endpoint.create_pool_with(1, 15).await
    }

    fn col_type(type_name: &str) -> MssqlColType {
        parse_mssql_col_type(type_name).unwrap()
    }

    async fn cleanup(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "DROP TABLE IF EXISTS {TIMESTAMP_TABLE};
                 DROP TABLE IF EXISTS {BOUNDARY_TABLE};
                 DROP TABLE IF EXISTS {TEST_TABLE};"
            ),
        )
        .await
    }

    fn bindable_values() -> Vec<(&'static str, &'static str, ColValue)> {
        vec![
            ("id", "int", ColValue::Long(1)),
            ("bit_value", "bit", ColValue::Bool(true)),
            ("tinyint_value", "tinyint", ColValue::UnsignedTiny(u8::MAX)),
            ("smallint_value", "smallint", ColValue::Short(-123)),
            ("int_value", "int", ColValue::Long(456_789)),
            ("bigint_value", "bigint", ColValue::LongLong(9_876_543_210)),
            ("real_value", "real", ColValue::Float(1.25)),
            ("float_value", "float", ColValue::Double(2.5)),
            ("smallmoney_value", "smallmoney", ColValue::Double(12.34)),
            ("money_value", "money", ColValue::Double(-56.78)),
            (
                "decimal_value",
                "decimal",
                ColValue::Decimal("12345678901234.5678".to_string()),
            ),
            (
                "numeric_value",
                "numeric",
                ColValue::Decimal("12345678901234.123456".to_string()),
            ),
            (
                "char_value",
                "char",
                ColValue::String("char-001".to_string()),
            ),
            (
                "varchar_value",
                "varchar",
                ColValue::String("plain ' varchar".to_string()),
            ),
            (
                "nchar_value",
                "nchar",
                ColValue::String("nchar001".to_string()),
            ),
            (
                "nvarchar_value",
                "nvarchar",
                ColValue::String("Ape-DTS \u{6570}\u{636e}".to_string()),
            ),
            (
                "text_value",
                "text",
                ColValue::String("legacy text".to_string()),
            ),
            (
                "ntext_value",
                "ntext",
                ColValue::String("legacy \u{4e2d}\u{6587}".to_string()),
            ),
            (
                "binary_value",
                "binary",
                ColValue::Blob(vec![0, 1, 2, 3, 4, 5, 254, 255]),
            ),
            (
                "varbinary_value",
                "varbinary",
                ColValue::Blob(vec![0, 1, 2, 255]),
            ),
            (
                "image_value",
                "image",
                ColValue::Blob(vec![202, 254, 1, 35]),
            ),
            (
                "uuid_value",
                "uniqueidentifier",
                ColValue::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
            ),
            (
                "xml_value",
                "xml",
                ColValue::String("<root attr=\"value\"><child>text</child></root>".to_string()),
            ),
            (
                "date_value",
                "date",
                ColValue::Date("2026-08-11".to_string()),
            ),
            (
                "time_value",
                "time",
                ColValue::Time("12:34:56.1234567".to_string()),
            ),
            (
                "smalldatetime_value",
                "smalldatetime",
                ColValue::DateTime("2026-08-11 12:34:00".to_string()),
            ),
            (
                "datetime_value",
                "datetime",
                ColValue::DateTime("2026-08-11 12:34:56.123".to_string()),
            ),
            (
                "datetime2_value",
                "datetime2",
                ColValue::DateTime("2026-08-11 12:34:56.1234567".to_string()),
            ),
            (
                "offset_value",
                "datetimeoffset",
                ColValue::Timestamp("2026-08-11T12:34:56.1234567+08:00".to_string()),
            ),
        ]
    }

    fn expected_query_value(col: &str, value: &ColValue) -> ColValue {
        match col {
            "time_value" => ColValue::Time("12:34:56.123456700".to_string()),
            "datetime_value" => ColValue::DateTime("2026-08-11 12:34:56.123333333".to_string()),
            "datetime2_value" => ColValue::DateTime("2026-08-11 12:34:56.123456700".to_string()),
            "offset_value" => {
                ColValue::Timestamp("2026-08-11T12:34:56.123456700+08:00".to_string())
            }
            _ => value.clone(),
        }
    }

    struct BoundaryCase {
        col: &'static str,
        sql_type: &'static str,
        type_name: &'static str,
        min: &'static str,
        max: &'static str,
    }

    fn boundary_cases() -> Vec<BoundaryCase> {
        vec![
            BoundaryCase {
                col: "bit_value",
                sql_type: "bit",
                type_name: "bit",
                min: "0",
                max: "TRUE",
            },
            BoundaryCase {
                col: "tinyint_value",
                sql_type: "tinyint",
                type_name: "tinyint",
                min: "0",
                max: "255",
            },
            BoundaryCase {
                col: "smallint_value",
                sql_type: "smallint",
                type_name: "smallint",
                min: "-32768",
                max: "32767",
            },
            BoundaryCase {
                col: "int_value",
                sql_type: "int",
                type_name: "int",
                min: "-2147483648",
                max: "2147483647",
            },
            BoundaryCase {
                col: "bigint_value",
                sql_type: "bigint",
                type_name: "bigint",
                min: "-9223372036854775808",
                max: "9223372036854775807",
            },
            BoundaryCase {
                col: "real_value",
                sql_type: "real",
                type_name: "real",
                min: "-3.4028235e38",
                max: "3.4028235e38",
            },
            BoundaryCase {
                col: "float_value",
                sql_type: "float",
                type_name: "float",
                min: "-1.7976931348623157e308",
                max: "1.7976931348623157e308",
            },
            BoundaryCase {
                col: "smallmoney_value",
                sql_type: "smallmoney",
                type_name: "smallmoney",
                min: "-214748.3648",
                max: "214748.3647",
            },
            BoundaryCase {
                col: "money_value",
                sql_type: "money",
                type_name: "money",
                min: "-922337203685477.5808",
                max: "922337203685477.5807",
            },
            BoundaryCase {
                col: "decimal_value",
                sql_type: "decimal(38, 0)",
                type_name: "decimal",
                min: "-99999999999999999999999999999999999999",
                max: "99999999999999999999999999999999999999",
            },
            BoundaryCase {
                col: "numeric_value",
                sql_type: "numeric(38, 37)",
                type_name: "numeric",
                min: "-9.9999999999999999999999999999999999999",
                max: "9.9999999999999999999999999999999999999",
            },
            BoundaryCase {
                col: "char_value",
                sql_type: "char(8)",
                type_name: "char",
                min: "00000000",
                max: "zzzzzzzz",
            },
            BoundaryCase {
                col: "varchar_value",
                sql_type: "varchar(8)",
                type_name: "varchar",
                min: "",
                max: "zzzzzzzz",
            },
            BoundaryCase {
                col: "nchar_value",
                sql_type: "nchar(8)",
                type_name: "nchar",
                min: "00000000",
                max: "ZZZZZZZZ",
            },
            BoundaryCase {
                col: "nvarchar_value",
                sql_type: "nvarchar(8)",
                type_name: "nvarchar",
                min: "",
                max: "\u{6570}\u{636e}\u{8fb9}\u{754c}",
            },
            BoundaryCase {
                col: "text_value",
                sql_type: "text",
                type_name: "text",
                min: "",
                max: "legacy text",
            },
            BoundaryCase {
                col: "ntext_value",
                sql_type: "ntext",
                type_name: "ntext",
                min: "",
                max: "legacy \u{4e2d}\u{6587}",
            },
            BoundaryCase {
                col: "binary_value",
                sql_type: "binary(8)",
                type_name: "binary",
                min: "0000000000000000",
                max: "ffffffffffffffff",
            },
            BoundaryCase {
                col: "varbinary_value",
                sql_type: "varbinary(8)",
                type_name: "varbinary",
                min: "",
                max: "ffffffffffffffff",
            },
            BoundaryCase {
                col: "image_value",
                sql_type: "image",
                type_name: "image",
                min: "",
                max: "ffffffffffffffff",
            },
            BoundaryCase {
                col: "uuid_value",
                sql_type: "uniqueidentifier",
                type_name: "uniqueidentifier",
                min: "00000000-0000-0000-0000-000000000000",
                max: "ffffffff-ffff-ffff-ffff-ffffffffffff",
            },
            BoundaryCase {
                col: "xml_value",
                sql_type: "xml",
                type_name: "xml",
                min: "<a/>",
                max: "<z>text</z>",
            },
            BoundaryCase {
                col: "date_value",
                sql_type: "date",
                type_name: "date",
                min: "0001-01-01",
                max: "9999-12-31",
            },
            BoundaryCase {
                col: "time_value",
                sql_type: "time(7)",
                type_name: "time",
                min: "00:00:00",
                max: "23:59:59.9999999",
            },
            BoundaryCase {
                col: "smalldatetime_value",
                sql_type: "smalldatetime",
                type_name: "smalldatetime",
                min: "1900-01-01 00:00:00",
                max: "2079-06-06 23:59:00",
            },
            BoundaryCase {
                col: "datetime_value",
                sql_type: "datetime",
                type_name: "datetime",
                min: "1753-01-01 00:00:00",
                max: "9999-12-31 23:59:59.997",
            },
            BoundaryCase {
                col: "datetime2_value",
                sql_type: "datetime2(7)",
                type_name: "datetime2",
                min: "0001-01-01 00:00:00",
                max: "9999-12-31 23:59:59.9999999",
            },
            BoundaryCase {
                col: "offset_value",
                sql_type: "datetimeoffset(7)",
                type_name: "datetimeoffset",
                min: "0001-01-01T00:00:00+00:00",
                max: "9999-12-31T23:59:59.9999999+00:00",
            },
        ]
    }

    fn expected_boundary_query_value(type_name: &str, input: &str, parsed: &ColValue) -> ColValue {
        match (type_name, input) {
            ("datetime", "9999-12-31 23:59:59.997") => {
                ColValue::DateTime("9999-12-31 23:59:59.996666666".to_string())
            }
            ("money" | "smallmoney", _) => ColValue::Double(input.parse().unwrap()),
            _ => parsed.clone(),
        }
    }

    fn assert_bind_rejected(type_name: &str, value: ColValue) {
        let mut query = Query::new("SELECT @P1");
        assert!(
            MssqlColValueConvertor::bind(&mut query, &value, &col_type(type_name)).is_err(),
            "MSSQL {type_name} accepted invalid {value:?}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn all_mssql_column_types_have_explicit_conversion_behavior() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "CREATE TABLE {TEST_TABLE} (
                    [id] int NOT NULL PRIMARY KEY,
                    [bit_value] bit NULL,
                    [tinyint_value] tinyint NULL,
                    [smallint_value] smallint NULL,
                    [int_value] int NULL,
                    [bigint_value] bigint NULL,
                    [real_value] real NULL,
                    [float_value] float NULL,
                    [smallmoney_value] smallmoney NULL,
                    [money_value] money NULL,
                    [decimal_value] decimal(18, 4) NULL,
                    [numeric_value] numeric(20, 6) NULL,
                    [char_value] char(8) NULL,
                    [varchar_value] varchar(100) NULL,
                    [nchar_value] nchar(8) NULL,
                    [nvarchar_value] nvarchar(100) NULL,
                    [text_value] text NULL,
                    [ntext_value] ntext NULL,
                    [binary_value] binary(8) NULL,
                    [varbinary_value] varbinary(100) NULL,
                    [image_value] image NULL,
                    [uuid_value] uniqueidentifier NULL,
                    [xml_value] xml NULL,
                    [date_value] date NULL,
                    [time_value] time(7) NULL,
                    [smalldatetime_value] smalldatetime NULL,
                    [datetime_value] datetime NULL,
                    [datetime2_value] datetime2(7) NULL,
                    [offset_value] datetimeoffset(7) NULL,
                    [sql_variant_value] sql_variant NULL,
                    [geometry_value] geometry NULL,
                    [geography_value] geography NULL,
                    [hierarchyid_value] hierarchyid NULL,
                    [rowversion_value] rowversion NOT NULL
                 );
                 CREATE TABLE {TIMESTAMP_TABLE} (
                    [id] int NOT NULL PRIMARY KEY,
                    [timestamp_value] timestamp NOT NULL
                 );"
            ),
        )
        .await?;

        let result = async {
            let values = bindable_values();
            let cols = values
                .iter()
                .map(|(col, _, _)| format!("[{col}]"))
                .collect::<Vec<_>>();
            let placeholders = (1..=values.len())
                .map(|index| format!("@P{index}"))
                .collect::<Vec<_>>();
            let mut query = Query::new(format!(
                "INSERT INTO {TEST_TABLE} ({}) VALUES ({})",
                cols.join(", "),
                placeholders.join(", ")
            ));
            for (_, type_name, value) in &values {
                MssqlColValueConvertor::bind(&mut query, value, &col_type(type_name))?;
            }

            let mut connection = pool.get().await?;
            query.execute(connection.client_mut()).await?;
            connection
                .client_mut()
                .simple_query(format!(
                    "UPDATE {TEST_TABLE} SET
                        [sql_variant_value] = CONVERT(sql_variant, 42),
                        [geometry_value] = geometry::Point(1, 2, 0),
                        [geography_value] = geography::Point(1, 2, 4326),
                        [hierarchyid_value] = hierarchyid::Parse('/1/')
                     WHERE [id] = 1;
                     INSERT INTO {TIMESTAMP_TABLE} ([id]) VALUES (1);"
                ))
                .await?
                .into_results()
                .await?;

            let row = connection
                .client_mut()
                .query(
                    format!(
                        "SELECT {}, [rowversion_value] FROM {TEST_TABLE} WHERE [id] = 1",
                        cols.join(", ")
                    ),
                    &[],
                )
                .await?
                .into_row()
                .await?
                .context("MSSQL value conversion query returned no row")?;
            for (col, _, value) in &values {
                let actual = MssqlColValueConvertor::from_query(&row, col)?;
                let expected = expected_query_value(col, value);
                ensure!(
                    actual.is_same_value(&expected),
                    "unexpected converted value for {col}: expected {expected:?}, got {actual:?}"
                );
            }

            let rowversion = MssqlColValueConvertor::from_query(&row, "rowversion_value")?;
            ensure!(
                matches!(rowversion, ColValue::Blob(ref value) if value.len() == 8),
                "rowversion should convert to an 8-byte blob, got {rowversion:?}"
            );
            drop(connection);
            {
                let mut meta_manager = MssqlTestEndpoint::create_meta_manager(pool.clone()).await?;
                let tb_meta = meta_manager
                    .get_tb_meta("dbo", "ape_dts_col_value_conversion_test")
                    .await?;
                for (col, expected_type) in [
                    ("sql_variant_value", MssqlColType::SSVariant),
                    ("geometry_value", MssqlColType::Udt),
                    ("geography_value", MssqlColType::Udt),
                    ("hierarchyid_value", MssqlColType::Udt),
                ] {
                    ensure!(
                        tb_meta.get_col_type(col)? == &expected_type,
                        "unexpected metadata type for {col}"
                    );
                    let error = MssqlColValueConvertor::from_str(&expected_type, "unsupported")
                        .expect_err("unsupported MSSQL type should return a conversion error");
                    ensure!(
                        error.to_string().contains("not supported"),
                        "unexpected conversion error for {col}: {error:#}"
                    );
                }
            }
            let mut connection = pool.get().await?;

            let timestamp_row = connection
                .client_mut()
                .query(
                    format!("SELECT [timestamp_value] FROM {TIMESTAMP_TABLE} WHERE [id] = 1"),
                    &[],
                )
                .await?
                .into_row()
                .await?
                .context("MSSQL timestamp conversion query returned no row")?;
            let timestamp = MssqlColValueConvertor::from_query(&timestamp_row, "timestamp_value")?;
            ensure!(
                matches!(timestamp, ColValue::Blob(ref value) if value.len() == 8),
                "timestamp should convert to an 8-byte blob, got {timestamp:?}"
            );

            let tb_meta = MssqlTbMeta {
                basic: RdbTbMeta {
                    schema: "dbo".to_string(),
                    tb: "ape_dts_col_value_conversion_test".to_string(),
                    cols: vec![
                        "bit_value".to_string(),
                        "decimal_value".to_string(),
                        "nvarchar_value".to_string(),
                    ],
                    ..Default::default()
                },
                ..Default::default()
            };
            let row_data = RowData::from_mssql_row(&row, &tb_meta, &None, Some(7))?;
            ensure!(row_data.chunk_id == 7, "unexpected RowData chunk id");
            let after = row_data.require_after()?;
            ensure!(after.get("bit_value") == Some(&ColValue::Bool(true)));
            ensure!(
                after.get("nvarchar_value")
                    == Some(&ColValue::String("Ape-DTS \u{6570}\u{636e}".to_string()))
            );

            let null_values = values
                .iter()
                .map(|(col, type_name, _)| {
                    if *col == "id" {
                        (*col, *type_name, ColValue::Long(2))
                    } else {
                        (*col, *type_name, ColValue::None)
                    }
                })
                .collect::<Vec<_>>();
            let mut query = Query::new(format!(
                "INSERT INTO {TEST_TABLE} ({}) VALUES ({})",
                cols.join(", "),
                placeholders.join(", ")
            ));
            for (_, type_name, value) in &null_values {
                MssqlColValueConvertor::bind(&mut query, value, &col_type(type_name))?;
            }
            query.execute(connection.client_mut()).await?;

            let null_row = connection
                .client_mut()
                .query(
                    format!(
                        "SELECT {} FROM {TEST_TABLE} WHERE [id] = 2",
                        cols.join(", ")
                    ),
                    &[],
                )
                .await?
                .into_row()
                .await?
                .context("MSSQL typed NULL query returned no row")?;
            for (col, _, _) in null_values.iter().filter(|(col, _, _)| *col != "id") {
                ensure!(
                    MssqlColValueConvertor::from_query(&null_row, col)? == ColValue::None,
                    "unexpected typed NULL conversion for {col}"
                );
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
    async fn from_str_and_bind_round_trip_all_supported_type_boundaries() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        cleanup(&pool).await?;
        let cases = boundary_cases();
        let columns = cases
            .iter()
            .map(|case| format!("[{}] {} NULL", case.col, case.sql_type))
            .collect::<Vec<_>>();
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "CREATE TABLE {BOUNDARY_TABLE} (
                    [case_id] tinyint NOT NULL PRIMARY KEY,
                    {}
                 );",
                columns.join(",\n")
            ),
        )
        .await?;

        let result = async {
            let col_names = cases
                .iter()
                .map(|case| format!("[{}]", case.col))
                .collect::<Vec<_>>();
            let placeholders = (2..=cases.len() + 1)
                .map(|index| format!("@P{index}"))
                .collect::<Vec<_>>();

            for (case_id, select_value) in [(1_u8, false), (2_u8, true)] {
                let parsed_values = cases
                    .iter()
                    .map(|case| {
                        let input = if select_value { case.max } else { case.min };
                        MssqlColValueConvertor::from_str(&col_type(case.type_name), input)
                            .with_context(|| {
                                format!(
                                    "failed to parse {} boundary {input:?}",
                                    case.type_name
                                )
                            })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let mut query = Query::new(format!(
                    "INSERT INTO {BOUNDARY_TABLE} ([case_id], {}) VALUES (@P1, {})",
                    col_names.join(", "),
                    placeholders.join(", ")
                ));
                query.bind(case_id);
                for (case, value) in cases.iter().zip(&parsed_values) {
                    MssqlColValueConvertor::bind(
                        &mut query,
                        value,
                        &col_type(case.type_name),
                    )?;
                }
                let mut connection = pool.get().await?;
                query.execute(connection.client_mut()).await?;
            }

            let mut connection = pool.get().await?;
            for (case_id, select_value) in [(1_u8, false), (2_u8, true)] {
                let row = connection
                    .client_mut()
                    .query(
                        format!(
                            "SELECT {} FROM {BOUNDARY_TABLE} WHERE [case_id] = @P1",
                            col_names.join(", ")
                        ),
                        &[&case_id],
                    )
                    .await?
                    .into_row()
                    .await?
                    .context("MSSQL boundary query returned no row")?;
                for case in &cases {
                    let input = if select_value { case.max } else { case.min };
                    let parsed =
                        MssqlColValueConvertor::from_str(&col_type(case.type_name), input)?;
                    let expected = expected_boundary_query_value(case.type_name, input, &parsed);
                    let actual = MssqlColValueConvertor::from_query(&row, case.col)?;
                    ensure!(
                        actual.is_same_value(&expected),
                        "unexpected {} boundary round trip for {}: input {input:?}, expected {expected:?}, got {actual:?}",
                        if select_value { "maximum" } else { "minimum" },
                        case.type_name
                    );
                }
            }
            drop(connection);

            for type_name in ["rowversion", "timestamp"] {
                let expected = ColValue::Blob(vec![0, 1, 2, 3, 4, 5, 254, 255]);
                let parsed = MssqlColValueConvertor::from_str(
                    &col_type(type_name),
                    "000102030405feff",
                )?;
                ensure!(parsed == expected, "unexpected {type_name} string conversion");
                let mut query = Query::new("SELECT @P1 AS [value]");
                MssqlColValueConvertor::bind(&mut query, &parsed, &col_type(type_name))?;
                let mut connection = pool.get().await?;
                let row = query
                    .query(connection.client_mut())
                    .await?
                    .into_row()
                    .await?
                    .context("MSSQL binary alias query returned no row")?;
                ensure!(
                    MssqlColValueConvertor::from_query(&row, "value")? == expected,
                    "unexpected {type_name} bind conversion"
                );
            }

            for type_name in cases
                .iter()
                .map(|case| case.type_name)
                .chain(["rowversion", "timestamp"])
            {
                let null = ColValue::None;
                let mut query = Query::new(
                    "SELECT CASE WHEN @P1 IS NULL THEN CONVERT(bit, 1) ELSE CONVERT(bit, 0) END AS [is_null]",
                );
                MssqlColValueConvertor::bind(&mut query, &null, &col_type(type_name))?;
                let mut connection = pool.get().await?;
                let row = query
                    .query(connection.client_mut())
                    .await?
                    .into_row()
                    .await?
                    .context("MSSQL typed NULL boundary query returned no row")?;
                ensure!(
                    MssqlColValueConvertor::from_query_required_bool(&row, "is_null")?,
                    "typed NULL bind failed for {type_name}"
                );
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
    async fn invalid_from_str_and_bind_boundaries_are_rejected() -> anyhow::Result<()> {
        let invalid_strings = [
            ("bit", "2"),
            ("bit", " true "),
            ("tinyint", "-1"),
            ("tinyint", "256"),
            ("smallint", "-32769"),
            ("smallint", "32768"),
            ("int", "-2147483649"),
            ("int", "2147483648"),
            ("bigint", "-9223372036854775809"),
            ("bigint", "9223372036854775808"),
            ("real", "NaN"),
            ("real", "3.5e38"),
            ("float", "inf"),
            ("money", "-inf"),
            ("decimal", "999999999999999999999999999999999999999"),
            ("numeric", "0.11111111111111111111111111111111111111"),
            ("decimal", "1e2"),
            ("varbinary", "0"),
            ("binary", "zz"),
            ("uniqueidentifier", "not-a-uuid"),
            ("date", "2026-02-29"),
            ("time", "24:00:00"),
            ("datetime", "2026-08-11T12:34:56"),
            ("datetime2", "2026-02-29 00:00:00"),
            ("datetimeoffset", "2026-08-11 12:34:56"),
        ];
        for (type_name, input) in invalid_strings {
            ensure!(
                MssqlColValueConvertor::from_str(&col_type(type_name), input).is_err(),
                "MSSQL {type_name} accepted invalid string {input:?}"
            );
        }

        for (type_name, value) in [
            ("bit", ColValue::Long(2)),
            ("tinyint", ColValue::Long(-1)),
            ("tinyint", ColValue::UnsignedShort(256)),
            ("smallint", ColValue::Long(i16::MAX as i32 + 1)),
            ("int", ColValue::UnsignedLong(i32::MAX as u32 + 1)),
            ("bigint", ColValue::UnsignedLongLong(u64::MAX)),
            ("real", ColValue::Double(f64::MAX)),
            ("real", ColValue::Float(f32::NAN)),
            ("float", ColValue::Double(f64::INFINITY)),
            ("money", ColValue::Double(f64::NAN)),
            ("decimal", ColValue::Decimal("9".repeat(39))),
            (
                "numeric",
                ColValue::Decimal(format!("0.{}", "1".repeat(38))),
            ),
            ("decimal", ColValue::Decimal("1e2".to_string())),
            ("decimal", ColValue::Double(1.0)),
            ("nvarchar", ColValue::Double(f64::NAN)),
            ("varchar", ColValue::RawString(vec![0xff])),
            ("text", ColValue::Blob(vec![1])),
            ("varbinary", ColValue::String("00ff".to_string())),
            ("uniqueidentifier", ColValue::String("invalid".to_string())),
            ("xml", ColValue::Long(1)),
            ("date", ColValue::Date("2026-02-29".to_string())),
            ("time", ColValue::Time("24:00:00".to_string())),
            (
                "datetime",
                ColValue::DateTime("2026-08-11T12:34:56".to_string()),
            ),
            (
                "datetimeoffset",
                ColValue::Timestamp("2026-08-11 12:34:56".to_string()),
            ),
        ] {
            assert_bind_rejected(type_name, value);
        }

        for unsupported in [
            MssqlColType::Null,
            MssqlColType::Intn,
            MssqlColType::Floatn,
            MssqlColType::Udt,
            MssqlColType::SSVariant,
        ] {
            ensure!(
                MssqlColValueConvertor::from_str(&unsupported, "1").is_err(),
                "unsupported type {unsupported:?} was parsed"
            );
            let mut query = Query::new("SELECT @P1");
            ensure!(
                MssqlColValueConvertor::bind(&mut query, &ColValue::None, &unsupported).is_err(),
                "unsupported type {unsupported:?} was bound"
            );
        }

        assert_bind_rejected("int", ColValue::UnchangedToast);

        let pool = create_pool().await?;
        for (type_name, sql_type, input) in [
            ("smallmoney", "smallmoney", "214748.3648"),
            ("money", "money", "922337203685477.5808"),
            ("decimal", "decimal(5, 2)", "1000.00"),
            ("varchar", "varchar(8)", "123456789"),
            ("nvarchar", "nvarchar(8)", "123456789"),
            ("varbinary", "varbinary(8)", "000102030405060708"),
            ("binary", "binary(8)", "000102030405060708"),
            ("smalldatetime", "smalldatetime", "2079-06-07 00:00:00"),
            ("datetime", "datetime", "1752-12-31 23:59:59"),
            (
                "datetimeoffset",
                "datetimeoffset(7)",
                "2026-08-11T12:34:56+15:00",
            ),
            ("xml", "xml", "<unclosed>"),
        ] {
            let value = MssqlColValueConvertor::from_str(&col_type(type_name), input)?;
            let mut query = Query::new(format!(
                "DECLARE @values TABLE ([value] {sql_type});
                 INSERT INTO @values ([value]) VALUES (@P1);"
            ));
            MssqlColValueConvertor::bind(&mut query, &value, &col_type(type_name))?;
            let mut connection = pool.get().await?;
            let result = query.execute(connection.client_mut()).await;
            ensure!(
                result.is_err(),
                "SQL Server accepted out-of-range {type_name} value {input:?}"
            );
        }

        Ok(())
    }
}
