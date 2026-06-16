#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn cdc_basic_test() {
        TestBase::run_cdc_test(
            "mock_test/mysql_to_mysql/5_7_to_5_7/cdc/basic_test",
            3000,
            4000,
        )
        .await;
    }
}
