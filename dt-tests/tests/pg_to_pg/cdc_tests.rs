#[cfg(test)]
mod test {

    use std::time::Duration;

    use serial_test::serial;

    use crate::test_runner::{
        rdb_cycle_test_runner::RdbCycleTestRunner, rdb_test_runner::RdbTestRunner,
        test_base::TestBase,
    };

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/basic_test", 3000, 9000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_expression_unique_key_conflict_test() {
        let runner = RdbTestRunner::new("pg_to_pg/cdc/basic_test/expression_uk_conflict")
            .await
            .unwrap();
        runner.execute_prepare_sqls().await.unwrap();
        let target = runner.dst_conn_pool_pg.as_ref().unwrap();
        let mut writer = target.begin().await.unwrap();
        let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *writer)
            .await
            .unwrap();
        sqlx::query("SELECT id FROM expression_uk_conflict.rows WHERE id=1 FOR UPDATE")
            .fetch_one(&mut *writer)
            .await
            .unwrap();

        let task = runner.spawn_cdc_task(3000, 3000).await.unwrap();
        let replace_during_delete = async {
            let blocked_sql: String = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let statement = sqlx::query_scalar::<_, String>(
                        "SELECT query FROM pg_stat_activity \
                         WHERE $1 = ANY(pg_blocking_pids(pid)) \
                         AND (query LIKE 'DELETE FROM%' OR query LIKE 'UPDATE%') \
                         AND query LIKE '%\"expression_uk_conflict\".\"rows\"%'",
                    )
                    .bind(writer_pid)
                    .fetch_optional(target)
                    .await
                    .unwrap();
                    if let Some(statement) = statement {
                        break statement;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("CDC write did not reach the locked target row");

            if blocked_sql.starts_with("DELETE FROM") {
                // Replace the locked tuple without changing its data. The waiting DELETE's
                // statement snapshot cannot see the new tuple; its derived INSERT conflicts.
                for sql in &runner.base.dst_test_sqls {
                    sqlx::query(sql).execute(&mut *writer).await.unwrap();
                }
            }
            // An unmerged UPDATE only needs the lock released; it must update the existing row.
            writer.commit().await.unwrap();
        };
        let replicate = async {
            runner
                .execute_src_sqls(&runner.base.src_test_sqls)
                .await
                .unwrap();
            task.await
                .expect("CDC task failed while applying the source UPDATE");
            let (source_tables, target_tables) = runner.get_compare_db_tbs().unwrap();
            runner
                .compare_data_for_tbs(&source_tables, &target_tables)
                .await
                .unwrap();
        };
        tokio::join!(replace_during_delete, replicate);
        runner.close().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn cdc_big_packet_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/big_packet_test", 3000, 20000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_nopk_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/nopk_test", 3000, 9000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_custom_type_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/custom_type_test", 3000, 9000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_postgis_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/postgis_test", 3000, 9000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_postgis_array_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/postgis_array_test", 3000, 9000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_charset_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/charset_test", 3000, 5000).await;
    }

    // #[tokio::test]
    #[serial]
    async fn cdc_charset_euc_cn_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/charset_euc_cn_test", 3000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_timezone_test() {
        println!("cdc_timezone_test can be covered by test: cdc_basic_test, table: timezone_table, the default_time_zone for source db is +08:00, the default_time_zone for target db is +07:00")
    }

    #[tokio::test]
    #[serial]
    async fn cdc_special_character_in_name_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/special_character_in_name_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_wildchar_filter_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/wildchar_filter_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_route_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/route_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_foreign_key_test() {
        TestBase::run_cdc_test("pg_to_pg/cdc/foreign_key_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_ddl_test() {
        TestBase::run_ddl_test("pg_to_pg/cdc/ddl_test", 3000, 5000).await;
    }

    #[tokio::test]
    #[serial]
    #[ignore = "known bug: PostgreSQL trigger rows contain the complete batch DDL query"]
    async fn cdc_batch_ddl_test() {
        let runner = RdbTestRunner::new("pg_to_pg/cdc/batch_ddl_test")
            .await
            .unwrap();
        runner.run_ddl_test(3000, 5000).await.unwrap();

        for table in ["batch_ddl_a", "batch_ddl_b"] {
            let target_table: Option<String> =
                sqlx::query_scalar(&format!("SELECT to_regclass('test_db_1.{table}')::text"))
                    .fetch_one(runner.dst_conn_pool_pg.as_ref().unwrap())
                    .await
                    .unwrap();
            assert_eq!(
                target_table.as_deref(),
                Some(format!("test_db_1.{table}").as_str()),
                "table created by batched DDL was not applied to the target"
            );
        }

        runner.close().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn cycle_basic_test() {
        let tx_check_data = vec![
            ("node1", "node2", "node1", "10"),
            ("node2", "node1", "node2", "10"),
        ];

        RdbCycleTestRunner::run_cycle_cdc_test(
            "pg_to_pg/cdc/cycle_basic_test",
            2000,
            4000,
            &tx_check_data,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cycle_star_test() {
        let tx_check_data = vec![
            ("node1", "node2", "node1", "10"),
            ("node1", "node2", "node3", "10"),
            ("node1", "node3", "node1", "10"),
            ("node1", "node3", "node2", "10"),
            ("node2", "node1", "node2", "10"),
            ("node3", "node1", "node3", "10"),
        ];

        RdbCycleTestRunner::run_cycle_cdc_test(
            "pg_to_pg/cdc/cycle_star_test",
            2000,
            4000,
            &tx_check_data,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cycle_net_test() {
        let tx_check_data = vec![
            ("node1", "node2", "node1", "10"),
            ("node1", "node3", "node1", "10"),
            ("node2", "node1", "node2", "10"),
            ("node2", "node3", "node2", "10"),
            ("node3", "node1", "node3", "10"),
            ("node3", "node2", "node3", "10"),
        ];

        RdbCycleTestRunner::run_cycle_cdc_test(
            "pg_to_pg/cdc/cycle_net_test",
            2000,
            4000,
            &tx_check_data,
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_heartbeat_test() {
        TestBase::run_heartbeat_test("pg_to_pg/cdc/heartbeat_test", 3000, 4000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_to_sql_test() {
        TestBase::run_cdc_to_sql_test("pg_to_pg/cdc/to_sql_test", false, 3000, 4000).await;
    }

    /// need postgres tables: ALTER TABLE table_name REPLICA IDENTITY FULL;
    #[tokio::test]
    #[serial]
    async fn cdc_to_reverse_sql_test() {
        TestBase::run_cdc_to_sql_test("pg_to_pg/cdc/to_sql_test", true, 3000, 4000).await;
    }
}
