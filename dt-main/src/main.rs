use std::{
    backtrace::Backtrace,
    env, panic,
    process::{exit, ExitCode},
    time::Duration,
};

use clap::Parser;
use tokio::{signal::ctrl_c, spawn, time::sleep};

use dt_common::{
    config::{ini_loader::IniLoader, task_config::TaskConfig},
    error::{DtResultExt, ErrorReport, Stage},
    log_error, log_error_report,
    logger::TaskLogger,
};
use dt_precheck::{config::task_config::PrecheckTaskConfig, do_precheck};
use dt_task::task_runner::TaskRunner;

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

    let config = args.config_path().expect("task config path is required");
    let loader = IniLoader::new(config).expect("failed to load task config");
    let task_config = TaskConfig::from_loader(&loader).expect("failed to parse task config");
    let precheck_config =
        PrecheckTaskConfig::load_if_present(&loader).expect("failed to parse precheck config");
    TaskLogger::new(&task_config)
        .init(precheck_config.is_some())
        .await
        .expect("failed to initialize task logger");

    install_panic_hook();

    match run(task_config, precheck_config, args.init).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let report = ErrorReport::from_anyhow(&error);
            log_error!("{report}");
            log_error_report!("{}", report.to_log_json());
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

    spawn(async {
        if ctrl_c().await.is_err() {
            return;
        }
        sleep(Duration::from_secs(
            env::var(ENV_SHUTDOWN_TIMEOUT_SECS)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
        ))
        .await;
        exit(0);
    });

    run_config(task_config, precheck_config, init)
        .await
        .task_id(task_id)
}

async fn run_config(
    task_config: TaskConfig,
    precheck_config: Option<PrecheckTaskConfig>,
    init: bool,
) -> anyhow::Result<()> {
    match precheck_config {
        Some(precheck_config) => do_precheck(task_config, precheck_config)
            .await
            .stage(Stage::Precheck)?,
        None => {
            TaskRunner::new(task_config)
                .stage(Stage::Bootstrap)?
                .start_task(init)
                .await
                .stage(Stage::Task)?;
        }
    }
    Ok(())
}

fn install_panic_hook() {
    panic::set_hook(Box::new(|panic_info| {
        let backtrace = Backtrace::capture();
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
