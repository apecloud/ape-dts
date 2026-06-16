#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        TestBase::run_mock_struct_test("mock_test/mysql_to_mysql/5_7_to_5_7/struct/basic_test")
            .await;
    }
}
