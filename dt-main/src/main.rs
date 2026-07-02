use std::{env, panic};

use dt_precheck::{config::task_config::PrecheckTaskConfig, do_precheck};
use dt_task::task_runner::TaskRunner;

#[cfg(feature = "tokio-console")]
const ENV_TOKIO_CONSOLE: &str = "APE_DTS_TOKIO_CONSOLE";

#[tokio::main]
async fn main() {
    env::set_var("RUST_BACKTRACE", "1");
    init_tokio_console();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("no task_config provided in args");
    }

    let task_config = args[1].clone();

    if PrecheckTaskConfig::new(&task_config).is_ok() {
        do_precheck(&task_config).await;
    } else {
        let runner = TaskRunner::new(&task_config).unwrap();
        runner.start_task(true).await.unwrap()
    }
}

#[cfg(feature = "tokio-console")]
fn init_tokio_console() {
    if env::var(ENV_TOKIO_CONSOLE).as_deref() == Ok("1") {
        console_subscriber::init();
    }
}

#[cfg(not(feature = "tokio-console"))]
fn init_tokio_console() {}
