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
    async fn snapshot_route_test() {
        TestBase::run_mongo_snapshot_test("mongo_to_mongo/snapshot/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_resume_test() {
        let mut dst_expected_counts = HashMap::new();
        dst_expected_counts.insert(("test_db_1", "finish_tb_1"), 0);
        dst_expected_counts.insert(("test_db_1", "resume_tb_1"), 1);
        dst_expected_counts.insert(("test_db_1", "non_resume_tb_1"), 3);
        dst_expected_counts.insert(("test_db_1", "finish_tb_in_log_1"), 0);
        dst_expected_counts.insert(("test_db_1", "resume_tb_in_log_1"), 1);

        TestBase::run_mongo_snapshot_test_and_check_dst_count(
            "mongo_to_mongo/snapshot/resume_log_test",
            dst_expected_counts,
        )
        .await;
    }
}
