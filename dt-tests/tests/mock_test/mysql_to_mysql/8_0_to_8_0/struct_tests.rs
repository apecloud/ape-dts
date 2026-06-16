#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        TestBase::run_mock_struct_test("mock_test/mysql_to_mysql/8_0_to_8_0/struct/basic_test")
            .await;
    }
}
