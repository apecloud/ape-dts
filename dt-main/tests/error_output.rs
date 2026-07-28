use std::process::Command;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dt-main"))
        .args(args)
        .env_remove("RUST_BACKTRACE")
        .env_remove("RUST_LIB_BACKTRACE")
        .output()
        .unwrap()
}

#[test]
fn missing_config_panics_before_logger_initialization() {
    let output = run(&[]);
    assert_eq!(output.status.code(), Some(101));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("task config path is required"), "{stderr}");
    assert!(!stderr.contains("__APE_DTS_ERROR_CONTEXT__"), "{stderr}");
    assert!(!stderr.contains("DIAGNOSTIC"), "{stderr}");
}
