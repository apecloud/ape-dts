use dt_common::error::{EndpointRole, ErrorCode, Stage};
use serial_test::serial;

use super::{assert_error_identity, config_path, error_report};

#[tokio::test]
#[serial]
async fn invalid_mysql_url_report_identifies_source_and_sqlx() {
    let report = error_report(&config_path("mysql_invalid_url.ini")).await;

    assert_error_identity(&report, ErrorCode::InvalidConfig);
    assert_eq!(report.task_id.as_deref(), Some("error-code-mysql"));
    assert_eq!(report.endpoint, Some(EndpointRole::Source));
    assert_eq!(
        report.origin.as_ref().map(|origin| origin.system.as_str()),
        Some("sqlx")
    );
}

#[tokio::test]
#[serial]
async fn invalid_postgres_url_report_identifies_source_and_sqlx() {
    let report = error_report(&config_path("pg_invalid_url.ini")).await;

    assert_error_identity(&report, ErrorCode::InvalidConfig);
    assert_eq!(report.task_id.as_deref(), Some("error-code-pg"));
    assert_eq!(report.endpoint, Some(EndpointRole::Source));
    assert_eq!(
        report.origin.as_ref().map(|origin| origin.system.as_str()),
        Some("sqlx")
    );
}

#[tokio::test]
#[serial]
async fn malformed_database_url_report_identifies_config_and_source() {
    let report = error_report(&config_path("malformed_url.ini")).await;

    assert_error_identity(&report, ErrorCode::InvalidConfig);
    assert_eq!(report.stage, Stage::Bootstrap);
    assert_eq!(report.task_id.as_deref(), Some("error-code-malformed-url"));
    assert_eq!(report.endpoint, Some(EndpointRole::Source));
}
