#[cfg(test)]
mod test {
    use dt_common::meta::mssql::mssql_connection_pool::{MssqlClient, MssqlConnectionPool};
    use serial_test::serial;
    use tiberius::Query;

    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    use super::super::TASK_CONFIG_FILE;

    const NON_IDENTITY_TABLE: &str = "[dbo].[ape_dts_non_identity_behavior_test]";
    const PARAMETER_LIMIT_TABLE: &str = "[dbo].[ape_dts_parameter_limit_behavior_test]";
    const IDENTITY_ROLLBACK_TABLE_1: &str = "[dbo].[ape_dts_identity_rollback_test_1]";
    const IDENTITY_ROLLBACK_TABLE_2: &str = "[dbo].[ape_dts_identity_rollback_test_2]";

    async fn create_pool() -> anyhow::Result<MssqlConnectionPool> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Sinker)?;
        endpoint.ensure_database().await?;
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
}
