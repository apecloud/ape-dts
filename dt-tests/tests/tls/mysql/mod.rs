tls_task_tests!(disable, "mysql", Disable;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(require, "mysql", Require;
    structure => "struct", snapshot => "snapshot", cdc => "cdc", checker => "checker");
tls_task_tests!(verify_ca, "mysql", VerifyCa;
    structure => "struct", snapshot => "snapshot", checker => "checker");
tls_task_tests!(verify_full, "mysql", VerifyFull;
    structure => "struct", snapshot => "snapshot", checker => "checker");
