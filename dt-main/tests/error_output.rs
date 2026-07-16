use std::process::Command;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dt-main"))
        .args(args)
        .env_remove("APE_DTS_VERBOSE_ERRORS")
        .output()
        .unwrap()
}

fn run_with_verbose_env(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dt-main"))
        .args(args)
        .env("APE_DTS_VERBOSE_ERRORS", "1")
        .output()
        .unwrap()
}

fn assert_missing_config(output: std::process::Output) {
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("ERROR [CF001]"), "{stderr}");
    assert!(stderr.contains("HINT:"), "{stderr}");
    assert!(!stderr.contains("CONTEXT:"), "{stderr}");
    assert!(!stderr.contains("ORIGIN:"), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
}

#[test]
fn no_config_returns_structured_error() {
    assert_missing_config(run(&[]));
}

#[test]
fn missing_config_file_returns_structured_error() {
    assert_missing_config(run(&[
        "--config",
        "/tmp/ape-dts-cli-config-that-does-not-exist.ini",
    ]));
}

#[test]
fn verbose_errors_include_developer_location_without_composite_code() {
    let output = run(&["--verbose-errors"]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("DIAGNOSTIC [CF001]"), "{stderr}");
    assert!(stderr.contains("LOCATION:"), "{stderr}");
    assert!(stderr.contains("STAGE: bootstrap"), "{stderr}");
}

#[test]
fn verbose_errors_can_be_enabled_by_environment() {
    let output = run_with_verbose_env(&[]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("ERROR [CF001]"), "{stderr}");
    assert!(stderr.contains("DIAGNOSTIC [CF001]"), "{stderr}");
    assert!(stderr.contains("LOCATION:"), "{stderr}");
    assert!(stderr.contains("STAGE: bootstrap"), "{stderr}");
}
