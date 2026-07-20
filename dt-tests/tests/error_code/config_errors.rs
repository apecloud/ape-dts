use dt_common::error::{ErrorCode, Stage};
use serial_test::serial;

use super::{assert_error_identity, config_path, error_report};

#[tokio::test]
#[serial]
async fn missing_config_file_report_has_expected_identity() {
    let report = error_report(&config_path("does-not-exist.ini")).await;

    assert_error_identity(&report, ErrorCode::MissingConfig);
    assert_eq!(report.stage, Stage::Bootstrap);
}

#[tokio::test]
#[serial]
async fn missing_required_item_report_has_expected_identity() {
    let report = error_report(&config_path("missing_required.ini")).await;

    assert_error_identity(&report, ErrorCode::MissingConfigItem);
    assert_eq!(report.stage, Stage::Bootstrap);
}

#[tokio::test]
#[serial]
async fn invalid_type_report_has_expected_identity() {
    let report = error_report(&config_path("invalid_type.ini")).await;

    assert_error_identity(&report, ErrorCode::InvalidConfig);
    assert_eq!(report.stage, Stage::Bootstrap);
}
