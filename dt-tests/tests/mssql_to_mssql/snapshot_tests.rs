#[cfg(test)]
mod test {
    use dt_common::config::config_enums::DbType;
    use serial_test::serial;

    use crate::test_runner::{
        rdb_test_runner::{RdbTestRunner, DST},
        test_base::TestBase,
    };

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_snapshot_test("mssql_to_mssql/snapshot/basic_test").await;
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
    async fn snapshot_resume_from_db_test() {
        let runner = RdbTestRunner::new("mssql_to_mssql/snapshot/resume_db_test")
            .await
            .unwrap();
        runner.run_snapshot_test(false).await.unwrap();
        for (table, expected_count) in [
            ("resume_test.resume_rows", 4),
            ("resume_test.fresh_rows", 2),
            ("resume_test.finished_rows", 0),
        ] {
            let db_tb = RdbTestRunner::parse_full_tb_name(table, &DbType::Mssql);
            let rows = runner.fetch_data(&db_tb, DST).await.unwrap();
            assert_eq!(
                rows.len(),
                expected_count,
                "unexpected row count for {table}"
            );
        }
        runner.close().await.unwrap();
    }
}
