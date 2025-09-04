#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_op_log_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/op_log_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_change_stream_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/change_stream_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_resume_test() {
        TestBase::run_mongo_cdc_resume_test("mongo_to_mongo/cdc/resume_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_idempotent_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/idempotent_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_serial_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/serial_sink_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_route_test() {
        TestBase::run_mongo_cdc_test("mongo_to_mongo/cdc/route_test", 3000, 3000).await;
    }

    #[tokio::test]
    #[serial]
    async fn cdc_heartbeat_test() {
        TestBase::run_mongo_heartbeat_test("mongo_to_mongo/cdc/heartbeat_test", 3000, 3000).await;
    }
}
