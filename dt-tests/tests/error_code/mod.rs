use std::path::{Path, PathBuf};

use dt_common::error::{ErrorCode, ErrorReport};

mod config_errors;
mod database_errors;

fn config_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/error_code/configs")
        .join(name)
}

async fn error_report(config: &Path) -> ErrorReport {
    let error = dt_main::run_config(
        config
            .to_str()
            .expect("error-code fixture path must be valid UTF-8"),
        false,
    )
    .await
    .expect_err("error-code fixture must fail");
    ErrorReport::from_anyhow(&error)
}

fn assert_error_identity(report: &ErrorReport, code: ErrorCode) {
    assert_eq!(report.schema_version, 1);
    assert_eq!(report.code, code);
}
