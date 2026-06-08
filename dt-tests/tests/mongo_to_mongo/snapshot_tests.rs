#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn snapshot_basic_test() {
        TestBase::run_mongo_snapshot_test("mongo_to_mongo/snapshot/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_table_parallel_test() {
        TestBase::run_mongo_snapshot_test("mongo_to_mongo/snapshot/table_parallel_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_route_test() {
        TestBase::run_mongo_snapshot_test("mongo_to_mongo/snapshot/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_test() {
        TestBase::run_mongo_snapshot_test_and_check_dst_count(
            "mongo_to_mongo/snapshot/resume_log_test",
            resume_expected_counts(),
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_from_db_test() {
        TestBase::run_mongo_snapshot_test_and_check_dst_count(
            "mongo_to_mongo/snapshot/resume_db_test",
            resume_expected_counts(),
        )
        .await;
    }

    fn resume_expected_counts() -> HashMap<(&'static str, &'static str), usize> {
        let mut dst_expected_counts = HashMap::new();
        dst_expected_counts.insert(("test_db_1", "finish_tb_1"), 0);
        dst_expected_counts.insert(("test_db_1", "resume_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "non_resume_tb_1"), 3);
        dst_expected_counts.insert(("test_db_1", "finish_tb_in_log_1"), 0);
        dst_expected_counts.insert(("test_db_1", "resume_tb_in_log_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_string_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_int32_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_int64_in_log_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_datetime_in_log_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_binary_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_decimal_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_document_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "resume_minmax_in_log_tb_1"), 1);
        dst_expected_counts
    }
}
