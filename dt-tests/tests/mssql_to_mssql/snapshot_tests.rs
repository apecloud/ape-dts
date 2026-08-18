#[cfg(test)]
mod test {
    use dt_common::config::config_enums::DbType;
    use serial_test::serial;

    use crate::test_runner::{
        rdb_test_runner::{RdbTestRunner, DST},
        test_base::TestBase,
    };

    async fn run_resume_test(test_dir: &str, expected_counts: &[(&str, usize)]) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner.run_snapshot_test(false).await.unwrap();
        for (table, expected_count) in expected_counts {
            let db_tb = RdbTestRunner::parse_full_tb_name(table, &DbType::Mssql);
            let rows = runner.fetch_data(&db_tb, DST).await.unwrap();
            assert_eq!(
                rows.len(),
                *expected_count,
                "unexpected row count for {table}"
            );
        }
        runner.close().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_ssl_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/ssl_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_type_coverage_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/type_coverage_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_charset_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/charset_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_key_structure_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/key_structure_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_on_duplicate_replace_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/on_duplicate_replace_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_wildchar_filter_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/wildchar_filter_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_route_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_chunk_parallel_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/parallel_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_table_parallel_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/table_parallel_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_special_character_in_name_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/special_character_in_name_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_big_packet_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/big_packet_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_log_test() {
        run_resume_test(
            "mssql_to_mssql/snapshot/resume_log_test",
            &[
                ("resume_log_test.resume_rows", 5),
                ("resume_log_test.config_rows", 4),
                ("resume_log_test.composite_rows", 4),
                ("resume_log_test.binary_key_rows", 4),
                ("[resume_log_test].[resume table.*]", 3),
                ("resume_log_test.nullable_composite_unique_rows", 6),
                ("resume_log_test.string_key_rows", 5),
                ("resume_log_test.date_key_rows", 5),
                ("resume_log_test.no_key_rows", 4),
                ("resume_log_test.fresh_rows", 3),
                ("resume_log_test.finished_config_rows", 0),
                ("resume_log_test.finished_log_rows", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_db_test() {
        run_resume_test(
            "mssql_to_mssql/snapshot/resume_db_test",
            &[
                ("resume_test.resume_rows", 5),
                ("resume_test.composite_rows", 4),
                ("resume_test.binary_key_rows", 4),
                ("[resume_test].[resume table.*]", 3),
                ("resume_test.nullable_composite_unique_rows", 6),
                ("resume_test.string_key_rows", 5),
                ("resume_test.fresh_rows", 3),
                ("resume_test.finished_rows", 0),
                ("resume_test.finished_rows_2", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_parallel_resume_from_log_test() {
        run_resume_test(
            "mssql_to_mssql/snapshot/parallel_resume_log_test",
            &[
                ("parallel_resume_log.integer_rows", 12),
                ("parallel_resume_log.nullable_rows", 11),
                ("[parallel_resume_log].[string rows.*]", 8),
                ("parallel_resume_log.composite_rows", 8),
                ("parallel_resume_log.binary_rows", 8),
                ("parallel_resume_log.decimal_rows", 8),
                ("parallel_resume_log.date_rows", 8),
                ("parallel_resume_log.no_key_rows", 8),
                ("parallel_resume_log.unique_rows", 8),
                ("parallel_resume_log.position_log_rows", 8),
                ("parallel_resume_log.finished_rows", 0),
                ("parallel_resume_log.finished_config_rows", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_parallel_resume_from_db_test() {
        run_resume_test(
            "mssql_to_mssql/snapshot/parallel_resume_db_test",
            &[
                ("parallel_resume_db.integer_rows", 12),
                ("parallel_resume_db.nullable_rows", 11),
                ("[parallel_resume_db].[string rows.*]", 8),
                ("parallel_resume_db.composite_rows", 8),
                ("parallel_resume_db.binary_rows", 8),
                ("parallel_resume_db.decimal_rows", 8),
                ("parallel_resume_db.date_rows", 8),
                ("parallel_resume_db.no_key_rows", 8),
                ("parallel_resume_db.unique_rows", 8),
                ("parallel_resume_db.finished_rows", 0),
            ],
        )
        .await;
    }
}
