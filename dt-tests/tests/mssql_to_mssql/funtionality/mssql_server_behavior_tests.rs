#[cfg(test)]
mod test {
    use anyhow::{bail, Context};
    use dt_common::meta::mssql::mssql_connection_pool::{MssqlClient, MssqlConnectionPool};
    use serial_test::serial;
    use tiberius::Query;

    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    use super::super::TASK_CONFIG_FILE;

    const TEST_DATABASE: &str = "ape_dts";
    const NON_IDENTITY_TABLE: &str = "[ape_dts].[dbo].[ape_dts_non_identity_behavior_test]";
    const PARAMETER_LIMIT_TABLE: &str = "[ape_dts].[dbo].[ape_dts_parameter_limit_behavior_test]";
    const IDENTITY_ROLLBACK_TABLE_1: &str = "[ape_dts].[dbo].[ape_dts_identity_rollback_test_1]";
    const IDENTITY_ROLLBACK_TABLE_2: &str = "[ape_dts].[dbo].[ape_dts_identity_rollback_test_2]";
    const NATIVE_TYPE_TABLE: &str = "[ape_dts].[dbo].[ape_dts_native_type_behavior_test]";

    #[derive(Debug, Eq, PartialEq)]
    struct ProjectedNativeValues {
        geometry_wkb: Vec<u8>,
        geography_wkb: Vec<u8>,
        hierarchyid_text: String,
        sql_variant_int: i32,
    }

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Sinker)?;
        endpoint.ensure_database(TEST_DATABASE).await?;
        endpoint.create_pool_with(1, 15).await
    }

    async fn execute_bound_parameter_insert(
        client: &mut MssqlClient,
        parameter_count: usize,
    ) -> tiberius::Result<()> {
        let mut sql = String::new();
        let mut parameter_index = 1;
        while parameter_index <= parameter_count {
            let chunk_end = usize::min(parameter_index + 999, parameter_count);
            let values = (parameter_index..=chunk_end)
                .map(|index| format!("(@P{index})"))
                .collect::<Vec<_>>()
                .join(",");
            sql.push_str(&format!(
                "INSERT INTO {PARAMETER_LIMIT_TABLE} ([value]) VALUES {values};"
            ));
            parameter_index = chunk_end + 1;
        }

        let mut query = Query::new(sql);
        for value in 0..parameter_count {
            query.bind(value as i32);
        }
        query.execute(client).await?;
        Ok(())
    }

    async fn execute_sql(client: &mut MssqlClient, sql: &str) -> tiberius::Result<()> {
        client.simple_query(sql).await?.into_results().await?;
        Ok(())
    }

    async fn prepare_native_type_table(pool: &MssqlConnectionPool) -> anyhow::Result<()> {
        MssqlTestEndpoint::execute_batch(
            pool,
            &format!(
                "DROP TABLE IF EXISTS {NATIVE_TYPE_TABLE};
                 CREATE TABLE {NATIVE_TYPE_TABLE} (
                    [id] int NOT NULL PRIMARY KEY,
                    [geometry_value] geometry NULL,
                    [geography_value] geography NULL,
                    [hierarchyid_value] hierarchyid NULL,
                    [sql_variant_value] sql_variant NULL
                 );
                 INSERT INTO {NATIVE_TYPE_TABLE} (
                    [id], [geometry_value], [geography_value],
                    [hierarchyid_value], [sql_variant_value]
                 ) VALUES (
                    1,
                    geometry::Point(1.25, 2.5, 0),
                    geography::Point(47.6062, -122.3321, 4326),
                    hierarchyid::Parse('/1/2/'),
                    CONVERT(sql_variant, 42)
                 );"
            ),
        )
        .await
    }

    async fn assert_native_value_decode_panics(column: &'static str) -> anyhow::Result<()> {
        let task = tokio::spawn(async move {
            let pool = create_pool().await?;
            let mut connection = pool.get().await?;
            connection
                .client_mut()
                .query(
                    format!("SELECT [{column}] FROM {NATIVE_TYPE_TABLE} WHERE [id] = 1"),
                    &[],
                )
                .await?
                .into_row()
                .await?;
            anyhow::Ok(())
        });

        match task.await {
            Err(error) if error.is_panic() => Ok(()),
            Err(error) => Err(error.into()),
            Ok(Err(error)) => Err(error).with_context(|| {
                format!("querying native MSSQL column {column} returned an ordinary error")
            }),
            Ok(Ok(())) => bail!("Tiberius unexpectedly decoded native MSSQL column {column}"),
        }
    }

    async fn projected_native_values(
        client: &mut MssqlClient,
        id: i32,
    ) -> anyhow::Result<ProjectedNativeValues> {
        let mut query = Query::new(format!(
            "SELECT
                [geometry_value].STAsBinary() AS [geometry_wkb],
                [geography_value].STAsBinary() AS [geography_wkb],
                [hierarchyid_value].ToString() AS [hierarchyid_text],
                CONVERT(int, [sql_variant_value]) AS [sql_variant_int]
             FROM {NATIVE_TYPE_TABLE}
             WHERE [id] = @P1"
        ));
        query.bind(id);
        let row = query
            .query(client)
            .await?
            .into_row()
            .await?
            .with_context(|| format!("native type test row {id} was not found"))?;

        Ok(ProjectedNativeValues {
            geometry_wkb: row
                .try_get::<&[u8], _>("geometry_wkb")?
                .context("geometry WKB projection is NULL")?
                .to_vec(),
            geography_wkb: row
                .try_get::<&[u8], _>("geography_wkb")?
                .context("geography WKB projection is NULL")?
                .to_vec(),
            hierarchyid_text: row
                .try_get::<&str, _>("hierarchyid_text")?
                .context("hierarchyid text projection is NULL")?
                .to_string(),
            sql_variant_int: row
                .try_get::<i32, _>("sql_variant_int")?
                .context("sql_variant int projection is NULL")?,
        })
    }

    #[tokio::test]
    #[serial]
    async fn parameterized_insert_cannot_bind_2100_values() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {PARAMETER_LIMIT_TABLE};
                 CREATE TABLE {PARAMETER_LIMIT_TABLE} ([value] int NOT NULL);"
            ),
        )
        .await?;

        let result = async {
            let mut connection = pool.get().await?;
            execute_bound_parameter_insert(connection.client_mut(), 2098).await?;
            drop(connection);

            MssqlTestEndpoint::execute_batch(
                &pool,
                &format!("TRUNCATE TABLE {PARAMETER_LIMIT_TABLE}"),
            )
            .await?;

            let mut connection = pool.get().await?;
            let error = execute_bound_parameter_insert(connection.client_mut(), 2100)
                .await
                .expect_err("SQL Server should reject 2100 Tiberius-bound INSERT values");
            assert_eq!(error.code(), Some(8003));
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = MssqlTestEndpoint::execute_batch(
            &pool,
            &format!("DROP TABLE IF EXISTS {PARAMETER_LIMIT_TABLE}"),
        )
        .await;
        result?;
        cleanup_result
    }

    #[tokio::test]
    #[serial]
    async fn rejects_identity_insert_for_table_without_identity_column() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {NON_IDENTITY_TABLE};
                 CREATE TABLE {NON_IDENTITY_TABLE} ([id] int NOT NULL PRIMARY KEY);"
            ),
        )
        .await?;

        let result = async {
            let mut connection = pool.get().await?;
            let error = execute_sql(
                connection.client_mut(),
                &format!("SET IDENTITY_INSERT {NON_IDENTITY_TABLE} ON"),
            )
            .await
            .expect_err("SQL Server should reject IDENTITY_INSERT for a non-identity table");
            assert_eq!(error.code(), Some(8106));
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = MssqlTestEndpoint::execute_batch(
            &pool,
            &format!("DROP TABLE IF EXISTS {NON_IDENTITY_TABLE}"),
        )
        .await;
        result?;
        cleanup_result
    }

    #[tokio::test]
    #[serial]
    async fn rollback_clears_identity_insert_after_batch_failure() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {IDENTITY_ROLLBACK_TABLE_1};
                 DROP TABLE IF EXISTS {IDENTITY_ROLLBACK_TABLE_2};
                 CREATE TABLE {IDENTITY_ROLLBACK_TABLE_1} (
                    [id] int IDENTITY(1, 1) NOT NULL PRIMARY KEY,
                    [code] nvarchar(20) NOT NULL UNIQUE
                 );
                 CREATE TABLE {IDENTITY_ROLLBACK_TABLE_2} (
                    [id] int IDENTITY(1, 1) NOT NULL PRIMARY KEY
                 );"
            ),
        )
        .await?;

        let result = async {
            let mut transaction = pool.begin().await?;
            let mut query = Query::new(format!(
                "SET IDENTITY_INSERT {IDENTITY_ROLLBACK_TABLE_1} ON;
                 INSERT INTO {IDENTITY_ROLLBACK_TABLE_1} ([id], [code])
                 VALUES (@P1, N'duplicate'), (@P2, N'duplicate');
                 SET IDENTITY_INSERT {IDENTITY_ROLLBACK_TABLE_1} OFF;"
            ));
            query.bind(10_i32);
            query.bind(11_i32);
            query
                .execute(transaction.client_mut())
                .await
                .expect_err("the duplicate batch should fail before IDENTITY_INSERT OFF");
            transaction.rollback().await?;

            let mut connection = pool.get().await?;
            execute_sql(
                connection.client_mut(),
                &format!("SET IDENTITY_INSERT {IDENTITY_ROLLBACK_TABLE_2} ON"),
            )
            .await?;
            execute_sql(
                connection.client_mut(),
                &format!("SET IDENTITY_INSERT {IDENTITY_ROLLBACK_TABLE_2} OFF"),
            )
            .await?;
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = MssqlTestEndpoint::execute_batch(
            &pool,
            &format!(
                "DROP TABLE IF EXISTS {IDENTITY_ROLLBACK_TABLE_1};
                 DROP TABLE IF EXISTS {IDENTITY_ROLLBACK_TABLE_2};"
            ),
        )
        .await;
        result?;
        cleanup_result
    }

    #[tokio::test]
    #[serial]
    async fn tiberius_panics_when_decoding_native_udt_and_sql_variant_values() -> anyhow::Result<()>
    {
        let pool = create_pool().await?;
        prepare_native_type_table(&pool).await?;

        let result = async {
            for column in [
                "geometry_value",
                "geography_value",
                "hierarchyid_value",
                "sql_variant_value",
            ] {
                assert_native_value_decode_panics(column).await?;
            }
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = MssqlTestEndpoint::execute_batch(
            &pool,
            &format!("DROP TABLE IF EXISTS {NATIVE_TYPE_TABLE}"),
        )
        .await;
        result?;
        cleanup_result
    }

    #[tokio::test]
    #[serial]
    async fn projected_native_values_can_be_extracted_and_bound() -> anyhow::Result<()> {
        let pool = create_pool().await?;
        prepare_native_type_table(&pool).await?;

        let result = async {
            let mut connection = pool.get().await?;
            let expected = projected_native_values(connection.client_mut(), 1).await?;
            assert!(!expected.geometry_wkb.is_empty());
            assert!(!expected.geography_wkb.is_empty());
            assert_eq!(expected.hierarchyid_text, "/1/2/");
            assert_eq!(expected.sql_variant_int, 42);

            let mut direct_geometry_bind = Query::new(format!(
                "INSERT INTO {NATIVE_TYPE_TABLE} ([id], [geometry_value]) VALUES (@P1, @P2)"
            ));
            direct_geometry_bind.bind(2_i32);
            direct_geometry_bind.bind(expected.geometry_wkb.as_slice());
            let error = direct_geometry_bind
                .execute(connection.client_mut())
                .await
                .expect_err("varbinary should not bind directly to a geometry column");
            assert_eq!(error.code(), Some(6522));

            let mut reconstructed_bind = Query::new(format!(
                "INSERT INTO {NATIVE_TYPE_TABLE} (
                    [id], [geometry_value], [geography_value],
                    [hierarchyid_value], [sql_variant_value]
                 ) VALUES (
                    @P1,
                    geometry::STGeomFromWKB(@P2, 0),
                    geography::STGeomFromWKB(@P3, 4326),
                    hierarchyid::Parse(@P4),
                    @P5
                 )"
            ));
            reconstructed_bind.bind(3_i32);
            reconstructed_bind.bind(expected.geometry_wkb.as_slice());
            reconstructed_bind.bind(expected.geography_wkb.as_slice());
            reconstructed_bind.bind(expected.hierarchyid_text.as_str());
            reconstructed_bind.bind(expected.sql_variant_int);
            reconstructed_bind.execute(connection.client_mut()).await?;

            let actual = projected_native_values(connection.client_mut(), 3).await?;
            assert_eq!(actual, expected);
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = MssqlTestEndpoint::execute_batch(
            &pool,
            &format!("DROP TABLE IF EXISTS {NATIVE_TYPE_TABLE}"),
        )
        .await;
        result?;
        cleanup_result
    }
}
