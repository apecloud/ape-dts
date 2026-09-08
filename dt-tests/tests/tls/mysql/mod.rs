tls_task_tests!(disable, "mysql", Disable, false;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(require, "mysql", Require, false;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(verify_ca, "mysql", VerifyCa, false;
    structure => "struct", snapshot => "snapshot", checker => "checker");
tls_task_tests!(verify_full, "mysql", VerifyFull, false;
    structure => "struct", snapshot => "snapshot", checker => "checker");
tls_task_tests!(allow_invalid_hostnames, "mysql", VerifyCa, true;
    structure => "struct", snapshot => "snapshot", checker => "checker");

#[tokio::test]
#[serial_test::serial]
async fn tls_connection_validation() {
    super::common::tls_connection_validation("mysql").await;
}
