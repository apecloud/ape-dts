use std::path::{Path, PathBuf};

mod config_errors;
mod database_errors;

fn config_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/error_code/configs")
        .join(name)
}

async fn assert_error_output(config: &Path, expected: &str) {
    let error = dt_main::run_config(
        config
            .to_str()
            .expect("error-code fixture path must be valid UTF-8"),
        false,
    )
    .await
    .expect_err("error-code fixture must fail");
    let output = dt_main::format_error(&error, false);
    assert_eq!(output, expected);
    assert!(!output.contains("panicked"), "{output}");
}
