#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test(
            "mock_test/pg_to_pg/13_3_4_to_13_3_4/cdc/basic_test",
            3000,
            5000,
        )
        .await;
    }
}
