use std::process::Command;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dt-main"))
        .args(args)
        .env_remove("RUST_BACKTRACE")
        .env_remove("RUST_LIB_BACKTRACE")
        .output()
        .unwrap()
}

fn run_with_backtrace(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dt-main"))
        .args(args)
        .env_remove("RUST_BACKTRACE")
        .env("RUST_LIB_BACKTRACE", "1")
        .output()
        .unwrap()
}

fn assert_missing_config(output: std::process::Output) {
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("ERROR [CF001]"), "{stderr}");
    assert!(stderr.contains("HINT:"), "{stderr}");
    assert!(stderr.contains("DIAGNOSTIC [CF001]"), "{stderr}");
    assert!(!stderr.contains("PHASE:"), "{stderr}");
    assert!(!stderr.contains("LOCATION:"), "{stderr}");
    assert!(!stderr.contains("OPERATION:"), "{stderr}");
    assert!(stderr.contains("STAGE: bootstrap"), "{stderr}");
    assert!(!stderr.contains("BACKTRACE:"), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
}

#[test]
fn no_config_returns_structured_error() {
    assert_missing_config(run(&[]));
}

#[test]
fn captured_backtrace_is_printed_without_a_cli_flag() {
    let output = run_with_backtrace(&[]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("DIAGNOSTIC [CF001]"), "{stderr}");
    assert!(stderr.contains("BACKTRACE:"), "{stderr}");
}
