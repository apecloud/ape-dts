use std::{env, panic, process::ExitCode};

use anyhow::Context;
use clap::Parser;

use dt_common::{
    config::{ini_loader::IniLoader, task_config::TaskConfig},
    error::{DtResultExt, ErrorReport},
    log_error,
    logger::TaskLogger,
};
use dt_main::run_config;
use dt_precheck::config::task_config::PrecheckTaskConfig;

const ENV_SHUTDOWN_TIMEOUT_SECS: &str = "SHUTDOWN_TIMEOUT_SECS";

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

    let args = Args::parse();
    if args.version || matches!(args.legacy_config.as_deref(), Some("version")) {
        println!("dt-main {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }

    let config = args
        .config_path()
        .context("no task config was provided")
        .unwrap();
    let loader = IniLoader::new(config)
        .with_context(|| format!("failed to load task config from [{config}]"))
        .unwrap();
    let task_config = TaskConfig::from_loader(&loader)
        .with_context(|| format!("invalid task config in [{config}]"))
        .unwrap();
    let precheck_config = PrecheckTaskConfig::load_if_present(&loader)
        .with_context(|| format!("invalid precheck config in [{config}]"))
        .unwrap();
    TaskLogger::new(&task_config)
        .init(precheck_config.is_some())
        .await
        .context("failed to initialize task logger")
        .unwrap();

    install_panic_hook();

    match run(task_config, precheck_config, args.init).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let report = ErrorReport::from_anyhow(&error);
            log_error!("{report}");
            log::logger().flush();
            ExitCode::FAILURE
        }
    }
}

async fn run(
    task_config: TaskConfig,
    precheck_config: Option<PrecheckTaskConfig>,
    init: bool,
) -> anyhow::Result<()> {
    let task_id = task_config.global.task_id.clone();

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

    run_config(task_config, precheck_config, init)
        .await
        .task_id(task_id)
}

fn install_panic_hook() {
    panic::set_hook(Box::new(|panic_info| {
        let backtrace = std::backtrace::Backtrace::capture();
        log_error!("panic: {}\nbacktrace:\n{}", panic_info, backtrace);
        log::logger().flush();
    }));
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
    fn rejects_config_flag_and_positional_config_together() {
        let err =
            Args::try_parse_from(["dt-main", "--config", "new.ini", "legacy.ini"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }
}
