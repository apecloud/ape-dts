use super::{assert_error_output, config_path};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn invalid_mysql_url_output_is_stable() {
    assert_error_output(
        &config_path("mysql_invalid_url.ini"),
        "ERROR [CN001]: The database endpoint could not be reached\n\
         TASK: error-code-mysql\n\
         AFFECTED: source mysql\n\
         HINT: Check the endpoint address, database service, network, firewall, and security group.",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn invalid_postgres_url_output_is_stable() {
    assert_error_output(
        &config_path("pg_invalid_url.ini"),
        "ERROR [CN001]: The database endpoint could not be reached\n\
         TASK: error-code-pg\n\
         AFFECTED: source postgres\n\
         HINT: Check the endpoint address, database service, network, firewall, and security group.",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn malformed_database_url_output_is_stable() {
    assert_error_output(
        &config_path("malformed_url.ini"),
        "ERROR [CF002]: database connection URL is invalid\n\
         TASK: error-code-malformed-url\n\
         AFFECTED: source\n\
         PHASE: loading task configuration\n\
         HINT: Correct the reported configuration value and start the task again.",
    )
    .await;
}
