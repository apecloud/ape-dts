tls_task_tests!(require, "mongo", Require;
    structure => "struct", snapshot => "snapshot", cdc => "cdc");
tls_task_tests!(verify_full, "mongo", VerifyFull; snapshot => "snapshot");
