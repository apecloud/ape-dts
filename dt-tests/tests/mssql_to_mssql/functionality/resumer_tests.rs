#[cfg(test)]
mod test {
    use anyhow::Context;
    use dt_common::{
        config::config_enums::DbType,
        meta::{
            mssql::mssql_connection_pool::MssqlConnectionPool, order_key::OrderKey,
            position::Position,
        },
    };
    use dt_connector::extractor::resumer::{
        recorder::{to_database::DatabaseRecorder, Recorder},
        recovery::{from_database::DatabaseRecovery, Recovery},
        ResumerDbPool,
    };
    use serial_test::serial;

    use crate::mssql_to_mssql::functionality::mssql_component_test_context::{
        MssqlComponentTestContext, TestTable,
    };

    async fn count_task_rows(
        pool: &MssqlConnectionPool,
        table: TestTable,
        task_id: &str,
    ) -> anyhow::Result<i64> {
        let mut connection = pool.get().await?;
        let row = connection
            .client_mut()
            .query(
                &format!(
                    "SELECT COUNT_BIG(*) AS row_count
                     FROM {}
                     WHERE task_id = @P1",
                    table.quoted_name()
                ),
                &[&task_id],
            )
            .await?
            .into_row()
            .await?
            .context("MSSQL checkpoint row count query returned no row")?;
        row.try_get::<i64, _>("row_count")?
            .context("MSSQL checkpoint row count was NULL")
    }

    async fn clear_finished_position_data(
        pool: &MssqlConnectionPool,
        table: TestTable,
        task_id: &str,
    ) -> anyhow::Result<()> {
        let mut connection = pool.get().await?;
        connection
            .client_mut()
            .execute(
                &format!(
                    "UPDATE {}
                     SET position_data = NULL
                     WHERE task_id = @P1 AND resumer_type = @P2",
                    table.quoted_name()
                ),
                &[&task_id, &"SnapshotFinished"],
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn records_and_recovers_real_mssql_checkpoints() -> anyhow::Result<()> {
        let tables = [TestTable::new(
            "ape_dts_resumer_component_test",
            "dbo",
            "positions",
        )];
        let context = MssqlComponentTestContext::new("resumer_test", &[], &tables).await?;
        let pool = context.sinker_pool.clone();
        let resumer_pool = ResumerDbPool::Mssql(pool.clone());
        let config = context.runner.config.resumer.clone();
        let [checkpoint_table] = tables;
        let source_database = "ape_dts";
        let task_id = "mssql-resumer-'quoted-task";

        let empty_recovery = DatabaseRecovery::new(task_id, &config, resumer_pool.clone()).await?;
        assert!(empty_recovery
            .get_snapshot_resume_position(source_database, "dbo", "orders", false)
            .await
            .is_none());

        let recorder = DatabaseRecorder::new(task_id, &config, resumer_pool.clone(), true).await?;
        let first_position = Position::RdbSnapshot {
            db_type: DbType::Mssql.to_string(),
            db: source_database.to_string(),
            schema: "dbo".to_string(),
            tb: "orders".to_string(),
            order_key: Some(OrderKey::Single(("id".to_string(), Some("1".to_string())))),
        };
        let latest_position = Position::RdbSnapshot {
            db_type: DbType::Mssql.to_string(),
            db: source_database.to_string(),
            schema: "dbo".to_string(),
            tb: "orders".to_string(),
            order_key: Some(OrderKey::Single(("id".to_string(), Some("2".to_string())))),
        };
        let finished_position = Position::RdbSnapshotFinished {
            db_type: DbType::Mssql.to_string(),
            db: source_database.to_string(),
            schema: "dbo".to_string(),
            tb: "orders".to_string(),
        };

        recorder.record_position(&first_position).await?;
        recorder.record_position(&latest_position).await?;
        recorder.record_position(&finished_position).await?;
        assert_eq!(count_task_rows(&pool, checkpoint_table, task_id).await?, 2);
        clear_finished_position_data(&pool, checkpoint_table, task_id).await?;

        let recovery = DatabaseRecovery::new(task_id, &config, resumer_pool.clone()).await?;
        assert_eq!(
            recovery
                .get_snapshot_resume_position(source_database, "dbo", "orders", false)
                .await,
            Some(latest_position)
        );
        assert!(
            recovery
                .check_snapshot_finished(source_database, "dbo", "orders")
                .await
        );

        DatabaseRecorder::new(task_id, &config, resumer_pool.clone(), true).await?;
        assert_eq!(count_task_rows(&pool, checkpoint_table, task_id).await?, 0);
        let reset_recovery = DatabaseRecovery::new(task_id, &config, resumer_pool).await?;
        assert!(reset_recovery
            .get_snapshot_resume_position(source_database, "dbo", "orders", false)
            .await
            .is_none());
        assert!(
            !reset_recovery
                .check_snapshot_finished(source_database, "dbo", "orders")
                .await
        );
        Ok(())
    }
}
