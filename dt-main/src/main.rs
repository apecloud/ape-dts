use std::env;

use dt_precheck::{config::task_config::PrecheckTaskConfig, do_precheck};
use dt_task::task_runner::TaskRunner;

const ENV_SHUTDOWN_TIMEOUT_SECS: &str = "SHUTDOWN_TIMEOUT_SECS";

#[tokio::main]
async fn main() {
    unsafe {
        env::set_var("RUST_BACKTRACE", "1");
    }

    let mut args = env::args();
    let _ = args.next();
    let Some(task_config) = args.next() else {
        panic!("no task_config provided in args");
    };

    if matches!(task_config.as_str(), "-V" | "--version" | "version") {
        println!("dt-main {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    tokio::spawn(async {
        tokio::signal::ctrl_c().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(
            std::env::var(ENV_SHUTDOWN_TIMEOUT_SECS)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
        ))
        .await;
        std::process::exit(0);
    });

    if PrecheckTaskConfig::new(&task_config).is_ok() {
        do_precheck(&task_config).await;
    } else {
        let runner = TaskRunner::new(&task_config).unwrap();
        runner.start_task().await.unwrap()
    }
}
