tls_task_tests!(disable, "pg", Disable;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(require, "pg", Require;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(verify_ca, "pg", VerifyCa;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(verify_full, "pg", VerifyFull;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
