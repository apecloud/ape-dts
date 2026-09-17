tls_task_tests!(disable, "mssql", Disable; snapshot => "snapshot");
tls_task_tests!(require, "mssql", Require; snapshot => "snapshot");
tls_task_tests!(verify_ca, "mssql", VerifyCa; snapshot => "snapshot");
tls_task_tests!(verify_full, "mssql", VerifyFull; snapshot => "snapshot");
