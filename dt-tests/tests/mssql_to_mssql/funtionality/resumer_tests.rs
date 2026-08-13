#[cfg(test)]
mod test {
    use anyhow::Context;
    use dt_common::{
        config::{config_enums::DbType, resumer_config::ResumerConfig},
        meta::{order_key::OrderKey, position::Position},
    };
    use dt_connector::extractor::resumer::{
        recorder::{to_database::DatabaseRecorder, Recorder},
        recovery::{from_database::DatabaseRecovery, Recovery},
        utils::ResumerUtil,
        ResumerDbPool,
    };
    use serial_test::serial;

    use crate::test_runner::mssql_test_endpoint::{MssqlTestEndpoint, TaskConfigEndpoint};

    use super::super::TASK_CONFIG_FILE;

    const TEST_SCHEMA: &str = "ape_dts_resumer_test";
    const TEST_TABLE: &str = "positions";
    const TASK_ID: &str = "mssql-resumer-'quoted-task";

    async fn create_resumer_pool() -> anyhow::Result<(ResumerDbPool, ResumerConfig)> {
        let endpoint =
            MssqlTestEndpoint::from_config_file(TASK_CONFIG_FILE, TaskConfigEndpoint::Extractor)?;
        endpoint.ensure_database().await?;
        let connection_string = endpoint.connection_string().to_string();
        let connection_auth = endpoint.connection_auth().clone();

        let pool = ResumerUtil::create_pool(
            &connection_string,
            &connection_auth,
            &DbType::Mssql,
            2,
            None,
        )
        .await?;
        let config = ResumerConfig::FromDB {
            url: connection_string,
            connection_auth,
            db_type: DbType::Mssql,
            table_full_name: format!("{TEST_SCHEMA}.{TEST_TABLE}"),
            max_connections: 2,
            is_direct_connection: None,
        };
        Ok((pool, config))
    }

    fn mssql_pool(
        pool: &ResumerDbPool,
    ) -> anyhow::Result<&dt_common::meta::mssql::mssql_connection_pool::MssqlConnectionPool> {
        match pool {
            ResumerDbPool::Mssql(pool) => Ok(pool),
            other => anyhow::bail!("expected MSSQL resumer pool, got {other:?}"),
        }
    }

    async fn execute_batch(pool: &ResumerDbPool, sql: &str) -> anyhow::Result<()> {
        let mut connection = mssql_pool(pool)?.get().await?;
        connection
            .client_mut()
            .simple_query(sql)
            .await?
            .into_results()
            .await?;
        Ok(())
    }

    async fn cleanup(pool: &ResumerDbPool) -> anyhow::Result<()> {
        execute_batch(
            pool,
            &format!(
                "DROP TABLE IF EXISTS [{TEST_SCHEMA}].[{TEST_TABLE}];
                 IF SCHEMA_ID(N'{TEST_SCHEMA}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{TEST_SCHEMA}]');"
            ),
        )
        .await
    }

    async fn count_task_rows(pool: &ResumerDbPool) -> anyhow::Result<i64> {
        let mut connection = mssql_pool(pool)?.get().await?;
        let row = connection
            .client_mut()
            .query(
                &format!(
                    "SELECT COUNT_BIG(*) AS row_count
                     FROM [{TEST_SCHEMA}].[{TEST_TABLE}]
                     WHERE task_id = @P1"
                ),
                &[&TASK_ID],
            )
            .await?
            .into_row()
            .await?
            .context("MSSQL checkpoint row count query returned no row")?;
        row.try_get::<i64, _>("row_count")?
            .context("MSSQL checkpoint row count was NULL")
    }

    async fn clear_finished_position_data(pool: &ResumerDbPool) -> anyhow::Result<()> {
        let mut connection = mssql_pool(pool)?.get().await?;
        connection
            .client_mut()
            .execute(
                &format!(
                    "UPDATE [{TEST_SCHEMA}].[{TEST_TABLE}]
                     SET position_data = NULL
                     WHERE task_id = @P1 AND resumer_type = @P2"
                ),
                &[&TASK_ID, &"SnapshotFinished"],
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn records_and_recovers_real_mssql_checkpoints() -> anyhow::Result<()> {
        let (pool, config) = create_resumer_pool().await?;
        cleanup(&pool).await?;

        let result = async {
            let empty_recovery = DatabaseRecovery::new(TASK_ID, &config, pool.clone()).await?;
            assert!(empty_recovery
                .get_snapshot_resume_position("dbo", "orders", false)
                .await
                .is_none());

            let recorder = DatabaseRecorder::new(TASK_ID, &config, pool.clone(), true).await?;
            let first_position = Position::RdbSnapshot {
                db_type: DbType::Mssql.to_string(),
                schema: "dbo".to_string(),
                tb: "orders".to_string(),
                order_key: Some(OrderKey::Single(("id".to_string(), Some("1".to_string())))),
            };
            let latest_position = Position::RdbSnapshot {
                db_type: DbType::Mssql.to_string(),
                schema: "dbo".to_string(),
                tb: "orders".to_string(),
                order_key: Some(OrderKey::Single(("id".to_string(), Some("2".to_string())))),
            };
            let finished_position = Position::RdbSnapshotFinished {
                db_type: DbType::Mssql.to_string(),
                schema: "dbo".to_string(),
                tb: "orders".to_string(),
            };

            recorder.record_position(&first_position).await?;
            recorder.record_position(&latest_position).await?;
            recorder.record_position(&finished_position).await?;
            assert_eq!(count_task_rows(&pool).await?, 2);
            clear_finished_position_data(&pool).await?;

            let recovery = DatabaseRecovery::new(TASK_ID, &config, pool.clone()).await?;
            assert_eq!(
                recovery
                    .get_snapshot_resume_position("dbo", "orders", false)
                    .await,
                Some(latest_position)
            );
            assert!(recovery.check_snapshot_finished("dbo", "orders").await);

            DatabaseRecorder::new(TASK_ID, &config, pool.clone(), true).await?;
            assert_eq!(count_task_rows(&pool).await?, 0);
            let reset_recovery = DatabaseRecovery::new(TASK_ID, &config, pool.clone()).await?;
            assert!(reset_recovery
                .get_snapshot_resume_position("dbo", "orders", false)
                .await
                .is_none());
            assert!(
                !reset_recovery
                    .check_snapshot_finished("dbo", "orders")
                    .await
            );
            anyhow::Ok(())
        }
        .await;

        let cleanup_result = cleanup(&pool).await;
        result?;
        cleanup_result
    }
}
