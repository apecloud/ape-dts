tls_task_tests!(disable, "mssql", Disable, false; snapshot => "snapshot");
tls_task_tests!(require, "mssql", Require, false; snapshot => "snapshot");
tls_task_tests!(verify_ca, "mssql", VerifyCa, false; snapshot => "snapshot");
tls_task_tests!(verify_full, "mssql", VerifyFull, false; snapshot => "snapshot");

#[tokio::test]
#[serial_test::serial]
async fn tls_connection_validation() {
    super::common::tls_connection_validation("mssql").await;
}
