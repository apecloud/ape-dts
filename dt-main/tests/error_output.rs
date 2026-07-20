use std::process::Command;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dt-main"))
        .args(args)
        .output()
        .unwrap()
}

fn assert_missing_config(output: std::process::Output) {
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("ERROR [CF001]"), "{stderr}");
    assert!(stderr.contains("HINT:"), "{stderr}");
    assert!(stderr.contains("DIAGNOSTIC [CF001]"), "{stderr}");
    assert!(stderr.contains("LOCATION:"), "{stderr}");
    assert!(stderr.contains("STAGE: bootstrap"), "{stderr}");
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
