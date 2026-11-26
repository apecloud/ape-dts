#[cfg(test)]
mod test {
    use serial_test::serial;

    use crate::test_runner::test_base::TestBase;

    #[tokio::test]
    #[serial]
    async fn check_basic_test() {
        TestBase::run_check_test("mssql_to_mssql/check/basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn snapshot_inline_basic_test() {
        TestBase::run_check_test("mssql_to_mssql/check/snapshot_inline_basic_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn basic_struct_test() {
        TestBase::run_check_test("mssql_to_mssql/check/basic_struct_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn consistent_struct_test() {
        TestBase::run_check_test("mssql_to_mssql/check/consistent_struct_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_route_test() {
        TestBase::run_check_test("mssql_to_mssql/check/route_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_output_full_row_test() {
        TestBase::run_check_test("mssql_to_mssql/check/output_full_row_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_sample_filter_test() {
        TestBase::run_check_test("mssql_to_mssql/check/sample_filter_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_snapshot_inline_diff_test() {
        TestBase::run_check_test("mssql_to_mssql/check/snapshot_inline_diff_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_recover() {
        TestBase::run_recheck_test("mssql_to_mssql/check/recheck_recover").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_recheck_struct_recover() {
        TestBase::run_recheck_test("mssql_to_mssql/check/recheck_struct_recover").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_revise_struct_test() {
        TestBase::run_check_test("mssql_to_mssql/check/revise_struct_test").await;
    }

    #[tokio::test]
    #[serial]
    async fn check_rules_filter_test() {
        TestBase::run_check_test("mssql_to_mssql/check/rules_filter_test").await;
    }
}
