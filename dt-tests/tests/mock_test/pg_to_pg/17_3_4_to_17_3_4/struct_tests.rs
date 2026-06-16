#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        TestBase::run_mock_struct_test("mock_test/pg_to_pg/17_3_4_to_17_3_4/struct/basic_test")
            .await;
    }
}
