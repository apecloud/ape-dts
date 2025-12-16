#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn review_basic_test() {
        TestBase::run_mongo_review_test("mongo_to_mongo/review/basic_test").await;
    }
}
