#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn struct_basic_test() {
        TestBase::run_mysql_struct_test("mysql_to_mysql/struct/basic_test").await;
    }

    // TODO: fix this test: index migration is not configured,
    //       and the target UK order may be inconsistent which cannot be verified through check
    // /// do_structures=database,table
    // #[tokio::test]
    // #[serial]
    // async fn struct_filter_test_1() {
    //     let mut runner = RdbStructTestRunner::new("mysql_to_mysql/struct/filter_test_1/src_to_dst")
    //         .await
    //         .unwrap();
    //     runner.run_struct_test_without_check().await.unwrap();
    //     TestBase::run_check_test("mysql_to_mysql/struct/filter_test_1/check").await;
    // }

    /// do_structures=constraint,index
    #[tokio::test]
    #[serial]
    async fn struct_filter_test_2() {
        TestBase::run_mysql_struct_test("mysql_to_mysql/struct/filter_test_2").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_charset_test() {
        TestBase::run_mysql_struct_test("mysql_to_mysql/struct/charset_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_route_test() {
        TestBase::run_mysql_struct_test("mysql_to_mysql/struct/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_8_0_basic_test() {
        TestBase::run_mysql_struct_test("mysql_to_mysql/struct/8_0_basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn struct_batch_test() {
        TestBase::run_mysql_struct_test("mysql_to_mysql/struct/batch_test").await;
    }
}
