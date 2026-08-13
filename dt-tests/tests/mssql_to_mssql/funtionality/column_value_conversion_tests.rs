#[cfg(test)]
mod test {
    use anyhow::Context;
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
    use tiberius::{time::chrono::DateTime, Query};

    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    use super::super::TASK_CONFIG_FILE;

    const TEST_TABLE: &str = "[dbo].[ape_dts_col_value_conversion_test]";

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Extractor)?;
        endpoint.ensure_database().await?;
        endpoint.create_pool_with(1, 15).await
    }

    fn col_type(type_name: &str) -> MssqlColType {
        parse_mssql_col_type(type_name).unwrap()
    }

    #[tokio::test]
    #[serial]
    async fn values_and_typed_nulls_round_trip_through_real_mssql() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {TEST_TABLE};
                 CREATE TABLE {TEST_TABLE} (
                    [id] int NOT NULL PRIMARY KEY,
                    [bit_value] bit NULL,
                    [tinyint_value] tinyint NULL,
                    [smallint_value] smallint NULL,
                    [int_value] int NULL,
                    [bigint_value] bigint NULL,
                    [real_value] real NULL,
                    [float_value] float NULL,
                    [money_value] money NULL,
                    [decimal_value] decimal(38, 4) NULL,
                    [text_value] nvarchar(100) NULL,
                    [binary_value] varbinary(100) NULL,
                    [uuid_value] uniqueidentifier NULL,
                    [xml_value] xml NULL,
                    [date_value] date NULL,
                    [time_value] time(7) NULL,
                    [datetime_value] datetime2(7) NULL,
                    [offset_value] datetimeoffset(7) NULL
                 )"
            ),
        )
        .await?;

        let result = async {
            let values = vec![
                ("int", ColValue::Long(1)),
                ("bit", ColValue::Bool(true)),
                ("tinyint", ColValue::UnsignedTiny(255)),
                ("smallint", ColValue::Short(-123)),
                ("int", ColValue::Long(456_789)),
                ("bigint", ColValue::LongLong(9_876_543_210)),
                ("real", ColValue::Float(1.25)),
                ("float", ColValue::Double(2.5)),
                ("money", ColValue::Double(12.34)),
                (
                    "decimal",
                    ColValue::Decimal("1234567890123456789012345678901234.5678".to_string()),
                ),
                (
                    "nvarchar",
                    ColValue::String("Ape-DTS \u{6570}\u{636e}".to_string()),
                ),
                ("varbinary", ColValue::Blob(vec![0, 1, 2, 255])),
                (
                    "uniqueidentifier",
                    ColValue::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
                ),
                (
                    "xml",
                    ColValue::String("<root attr=\"value\"/>".to_string()),
                ),
                ("date", ColValue::Date("2026-08-11".to_string())),
                ("time", ColValue::Time("12:34:56.1234567".to_string())),
                (
                    "datetime2",
                    ColValue::DateTime("2026-08-11 12:34:56.1234567".to_string()),
                ),
                (
                    "datetimeoffset",
                    ColValue::Timestamp("2026-08-11T12:34:56.1234567+08:00".to_string()),
                ),
            ];
            let mut query = Query::new(format!(
                "INSERT INTO {TEST_TABLE} VALUES ({})",
                (1..=values.len())
                    .map(|index| format!("@P{index}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            for (type_name, value) in &values {
                MssqlColValueConvertor::bind(&mut query, value, &col_type(type_name))?;
            }

            let mut connection = pool.get().await?;
            query.execute(connection.client_mut()).await?;
            let row = connection
                .client_mut()
                .query(format!("SELECT * FROM {TEST_TABLE} WHERE [id] = 1"), &[])
                .await?
                .into_row()
                .await?
                .context("MSSQL value conversion query returned no row")?;

            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "bit_value")?,
                ColValue::Bool(true)
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "tinyint_value")?,
                ColValue::UnsignedTiny(255)
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "smallint_value")?,
                ColValue::Short(-123)
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "int_value")?,
                ColValue::Long(456_789)
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "bigint_value")?,
                ColValue::LongLong(9_876_543_210)
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "real_value")?,
                ColValue::Float(1.25)
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "float_value")?,
                ColValue::Double(2.5)
            );
            let money = MssqlColValueConvertor::from_query(&row, "money_value")?;
            assert!(
                matches!(money, ColValue::Double(value) if (value - 12.34).abs() < f64::EPSILON)
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "decimal_value")?,
                ColValue::Decimal("1234567890123456789012345678901234.5678".to_string())
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "text_value")?,
                ColValue::String("Ape-DTS \u{6570}\u{636e}".to_string())
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "binary_value")?,
                ColValue::Blob(vec![0, 1, 2, 255])
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "uuid_value")?,
                ColValue::String("550e8400-e29b-41d4-a716-446655440000".to_string())
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "xml_value")?,
                ColValue::String("<root attr=\"value\"/>".to_string())
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "date_value")?,
                ColValue::Date("2026-08-11".to_string())
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "time_value")?,
                ColValue::Time("12:34:56.123456700".to_string())
            );
            assert_eq!(
                MssqlColValueConvertor::from_query(&row, "datetime_value")?,
                ColValue::DateTime("2026-08-11 12:34:56.123456700".to_string())
            );
            let offset = MssqlColValueConvertor::from_query(&row, "offset_value")?;
            let ColValue::Timestamp(offset) = offset else {
                anyhow::bail!("expected Timestamp for datetimeoffset");
            };
            assert_eq!(
                DateTime::parse_from_rfc3339(&offset)?,
                DateTime::parse_from_rfc3339("2026-08-11T12:34:56.1234567+08:00")?
            );

            let tb_meta = MssqlTbMeta {
                basic: RdbTbMeta {
                    schema: "dbo".to_string(),
                    tb: "ape_dts_col_value_conversion_test".to_string(),
                    cols: vec![
                        "bit_value".to_string(),
                        "decimal_value".to_string(),
                        "text_value".to_string(),
                    ],
                    ..Default::default()
                },
                ..Default::default()
            };
            let row_data = RowData::from_mssql_row(&row, &tb_meta, &None, Some(7))?;
            assert_eq!(row_data.chunk_id, 7);
            let after = row_data.require_after()?;
            assert_eq!(after.get("bit_value"), Some(&ColValue::Bool(true)));
            assert_eq!(
                after.get("text_value"),
                Some(&ColValue::String("Ape-DTS \u{6570}\u{636e}".to_string()))
            );

            let null = ColValue::None;
            let null_values = [
                ("int", ColValue::Long(2)),
                ("bit", null.clone()),
                ("decimal", null.clone()),
                ("nvarchar", null.clone()),
                ("varbinary", null.clone()),
                ("uniqueidentifier", null.clone()),
                ("xml", null.clone()),
                ("date", null.clone()),
                ("datetimeoffset", null),
            ];
            let mut query = Query::new(format!(
                "INSERT INTO {TEST_TABLE} ([id], [bit_value], [decimal_value], [text_value],
                    [binary_value], [uuid_value], [xml_value], [date_value], [offset_value])
                 VALUES ({})",
                (1..=null_values.len())
                    .map(|index| format!("@P{index}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            for (type_name, value) in &null_values {
                MssqlColValueConvertor::bind(&mut query, value, &col_type(type_name))?;
            }
            query.execute(connection.client_mut()).await?;

            let row = connection
                .client_mut()
                .query(format!("SELECT * FROM {TEST_TABLE} WHERE [id] = 2"), &[])
                .await?
                .into_row()
                .await?
                .context("MSSQL typed NULL query returned no row")?;
            for col in [
                "bit_value",
                "decimal_value",
                "text_value",
                "binary_value",
                "uuid_value",
                "xml_value",
                "date_value",
                "offset_value",
            ] {
                assert_eq!(
                    MssqlColValueConvertor::from_query(&row, col)?,
                    ColValue::None,
                    "unexpected value for {col}"
                );
            }

            Ok::<_, anyhow::Error>(())
        }
        .await;

        MssqlTestEndpoint::execute_batch(&pool, &format!("DROP TABLE IF EXISTS {TEST_TABLE}"))
            .await?;
        result
    }
}
