#[cfg(test)]
mod test {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use anyhow::{ensure, Context};
    use dt_common::{
        config::{
            config_enums::DbType, extractor_config::ExtractorConfig, filter_config::FilterConfig,
        },
        meta::{
            dt_data::{DtData, DtItem},
            dt_queue::DtQueue,
            position::Position,
        },
        monitor::task_monitor_handle::TaskMonitorHandle,
        rdb_filter::RdbFilter,
        time_filter::TimeFilter,
    };
    use dt_connector::{
        extractor::{
            base_extractor::{BaseExtractor, ExtractState},
            extractor_monitor::ExtractorMonitor,
            mssql::mssql_snapshot_extractor::{MssqlSnapshotExtractor, MssqlSnapshotShared},
        },
        Extractor,
    };
    use dt_task::extractor_util::ExtractorUtil;
    use serial_test::serial;
    use tokio::time::timeout;

    use crate::{
        mssql_to_mssql::functionality::mssql_component_test_context::{
            MssqlComponentTestContext, TestTable, TestTableName,
        },
        test_runner::mssql_test_endpoint::MssqlTestEndpoint,
    };

    async fn collect_extractor_output(
        mut extractor: MssqlSnapshotExtractor,
        buffer: Arc<DtQueue>,
    ) -> anyhow::Result<Vec<DtItem>> {
        let mut extractor_task = tokio::spawn(async move { extractor.extract().await });
        let collect_result = timeout(Duration::from_secs(30), async {
            let mut items = Vec::new();
            loop {
                while let Ok(item) = buffer.pop().await {
                    items.push(item);
                }
                if extractor_task.is_finished() {
                    (&mut extractor_task)
                        .await
                        .context("MSSQL snapshot extractor task failed to join")??;
                    while let Ok(item) = buffer.pop().await {
                        items.push(item);
                    }
                    return anyhow::Ok(items);
                }
                buffer.wait_for_data(Duration::from_millis(50)).await;
            }
        })
        .await;

        match collect_result {
            Ok(result) => result,
            Err(error) => {
                extractor_task.abort();
                let _ = extractor_task.await;
                Err(error).context("MSSQL snapshot extractor timed out")
            }
        }
    }

    async fn new_extractor(
        context: &MssqlComponentTestContext,
        filter_config: &FilterConfig,
        table: &TestTableName,
    ) -> anyhow::Result<(MssqlSnapshotExtractor, Arc<DtQueue>, Arc<AtomicBool>)> {
        let ExtractorConfig::MssqlSnapshot {
            parallel_size,
            parallel_type,
            batch_size,
            partition_cols,
            ..
        } = &context.runner.config.extractor
        else {
            anyhow::bail!("MSSQL snapshot extractor component test has wrong extractor config")
        };
        let buffer = Arc::new(DtQueue::new(128, 0, None, None));
        let shut_down = Arc::new(AtomicBool::new(false));
        let partition_cols = ExtractorUtil::parse_partition_cols(partition_cols, &DbType::Mssql)?;
        let extractor = MssqlSnapshotExtractor {
            shared: MssqlSnapshotShared {
                base_extractor: BaseExtractor {
                    buffer: Arc::clone(&buffer),
                    router: None,
                    shut_down: Arc::clone(&shut_down),
                },
                connection_pool: context.source_pool.clone(),
                meta_manager: MssqlTestEndpoint::create_meta_manager(context.source_pool.clone())
                    .await?,
                filter: Arc::new(RdbFilter::from_config(filter_config, &DbType::Mssql)?),
                partition_cols: Arc::new(partition_cols),
                batch_size: *batch_size,
                parallel_type: parallel_type.clone(),
                recovery: None,
            },
            extract_state: ExtractState {
                monitor: ExtractorMonitor::new(TaskMonitorHandle::default(), String::new()).await,
                data_marker: None,
                time_filter: TimeFilter::default(),
            },
            parallel_size: *parallel_size,
            tbs: vec![table.to_tuple()],
        };
        Ok((extractor, buffer, shut_down))
    }

    async fn extractor_order_col_error(
        context: &MssqlComponentTestContext,
        table: &TestTableName,
    ) -> anyhow::Result<(anyhow::Error, Arc<DtQueue>)> {
        let (mut extractor, buffer, _) =
            new_extractor(context, &context.runner.config.filter, table).await?;
        let error = extractor
            .extract()
            .await
            .expect_err("generated MSSQL order column should fail before table extraction");
        Ok((error, buffer))
    }

    #[tokio::test]
    #[serial]
    async fn rejects_server_generated_order_columns_before_extracting_table() -> anyhow::Result<()>
    {
        let context = MssqlComponentTestContext::new(
            "snapshot_extractor_test",
            &[TestTable::new(
                "ape_dts_snapshot_extractor_component_test",
                "invalid_order_columns",
                "*",
            )],
            &[],
        )
        .await?;
        for table in &context.source_tables {
            let (error, buffer) = extractor_order_col_error(&context, table).await?;
            let error_chain = format!("{error:#}");
            ensure!(
                error_chain.contains("cannot be migrated as an order column"),
                "unexpected order column validation error for {}: {error_chain}",
                table.quoted_name(),
            );
            ensure!(
                buffer.is_empty(),
                "extractor emitted data before rejecting {}",
                table.quoted_name(),
            );
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn emits_filtered_rows_and_snapshot_finished() -> anyhow::Result<()> {
        let context = MssqlComponentTestContext::new(
            "snapshot_extractor_test",
            &[TestTable::new(
                "ape_dts_snapshot_extractor_component_test",
                "snapshot_extractor_test",
                "snapshot_rows",
            )],
            &[],
        )
        .await?;
        let snapshot_table = context.source_table(
            "ape_dts_snapshot_extractor_component_test",
            "snapshot_extractor_test",
            "snapshot_rows",
        )?;

        let mut filter_config = context.runner.config.filter.clone();
        filter_config.ignore_cols = format!(
            r#"json:[{{"db":"{}","tb":"{}.{}","ignore_cols":["split_key"]}}]"#,
            snapshot_table.db, snapshot_table.schema, snapshot_table.tb
        );
        let (extractor, buffer, shut_down) =
            new_extractor(&context, &filter_config, snapshot_table).await?;

        let items = collect_extractor_output(extractor, Arc::clone(&buffer)).await?;
        assert!(shut_down.load(Ordering::Acquire));

        let mut row_count = 0;
        let mut snapshot_finished_count = 0;
        for item in items {
            match item.dt_data {
                DtData::Dml { row_data } => {
                    let after = row_data.after.context("snapshot row has no after values")?;
                    assert!(!after.contains_key("split_key"));
                    assert!(after.contains_key("name"));
                    row_count += 1;
                }
                DtData::Commit { .. }
                    if matches!(
                        item.position,
                        Position::RdbSnapshotFinished {
                            ref db_type,
                            ref db,
                            ref schema,
                            ref tb,
                        } if db_type == &DbType::Mssql.to_string()
                            && db == &snapshot_table.db
                            && schema == &snapshot_table.schema
                            && tb == &snapshot_table.tb
                    ) =>
                {
                    snapshot_finished_count += 1;
                }
                _ => {}
            }
        }
        assert!(row_count > 0, "extractor emitted no snapshot rows");
        assert_eq!(snapshot_finished_count, 1);
        Ok(())
    }
}
