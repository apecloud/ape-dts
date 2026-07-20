use std::{env, panic, process::ExitCode};

use clap::Parser;

use dt_common::{
    error::{DtError, ErrorCode, Stage},
    log_error,
};
use dt_main::{format_error, run_config};

const ENV_SHUTDOWN_TIMEOUT_SECS: &str = "SHUTDOWN_TIMEOUT_SECS";
const ENV_VERBOSE_ERRORS: &str = "APE_DTS_VERBOSE_ERRORS";

#[derive(Debug, Parser)]
struct Args {
    #[arg(short = 'v', long = "version", alias = "versions")]
    version: bool,

    #[arg(short, long, value_name = "CONFIG", conflicts_with = "legacy_config")]
    config: Option<String>,

    #[arg(value_name = "CONFIG")]
    legacy_config: Option<String>,

    #[arg(long)]
    init: bool,

    #[arg(long)]
    verbose_errors: bool,
}

impl Args {
    fn config_path(&self) -> Option<&str> {
        self.config
            .as_deref()
            .or(self.legacy_config.as_deref())
            .filter(|config| !config.is_empty())
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    unsafe {
        env::set_var("RUST_BACKTRACE", "1");
    }
    install_panic_hook();

    let args = Args::parse();
    let verbose_errors = args.verbose_errors || env::var(ENV_VERBOSE_ERRORS).as_deref() == Ok("1");
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", format_error(&error, verbose_errors));
            ExitCode::FAILURE
        }
    }
}

fn install_panic_hook() {
    panic::set_hook(Box::new(|panic_info| {
        let backtrace = std::backtrace::Backtrace::capture();
        log_error!("panic: {}\nbacktrace:\n{}", panic_info, backtrace);
    }));
}

async fn run(args: Args) -> anyhow::Result<()> {
    if args.version || matches!(args.legacy_config.as_deref(), Some("version")) {
        println!("dt-main {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let config = args.config_path().ok_or_else(|| {
        DtError::new(ErrorCode::MissingConfig)
            .message("no task config was provided")
            .hint("pass --config <CONFIG> or a positional config path")
            .stage(Stage::Bootstrap)
    })?;

    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_err() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(
            std::env::var(ENV_SHUTDOWN_TIMEOUT_SECS)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
        ))
        .await;
        std::process::exit(0);
    });

    run_config(config, args.init).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_config_flag() {
        let args = Args::try_parse_from(["dt-main", "--config", "task_config.ini"]).unwrap();
        assert_eq!(args.config_path(), Some("task_config.ini"));
    }

    #[test]
    fn accepts_legacy_positional_config() {
        let args = Args::try_parse_from(["dt-main", "task_config.ini"]).unwrap();
        assert_eq!(args.config_path(), Some("task_config.ini"));
    }

    #[test]
    fn version_does_not_require_config() {
        let args = Args::try_parse_from(["dt-main", "--version"]).unwrap();
        assert!(args.version);
        assert_eq!(args.config_path(), None);
    }

    #[test]
    fn accepts_legacy_version_command() {
        let args = Args::try_parse_from(["dt-main", "version"]).unwrap();
        assert_eq!(args.legacy_config.as_deref(), Some("version"));
    }

    #[test]
    fn accepts_verbose_errors_flag() {
        let args = Args::try_parse_from(["dt-main", "--verbose-errors"]).unwrap();
        assert!(args.verbose_errors);
    }

    #[test]
    fn rejects_config_flag_and_positional_config_together() {
        let err =
            Args::try_parse_from(["dt-main", "--config", "new.ini", "legacy.ini"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }
}
