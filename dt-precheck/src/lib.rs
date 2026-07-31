use dt_common::config::task_config::TaskConfig;

use crate::{
    builder::prechecker_builder::PrecheckerBuilder, config::task_config::PrecheckTaskConfig,
};

pub mod builder;
pub mod config;
pub mod fetcher;
pub mod meta;
pub mod prechecker;

pub async fn do_precheck(
    task_config: TaskConfig,
    precheck_config: PrecheckTaskConfig,
) -> anyhow::Result<()> {
    let checker_connector = PrecheckerBuilder::build(precheck_config.precheck, task_config);
    if let Err(error) = checker_connector.verify_check_result().await {
        println!("precheck not passed.");
        return Err(error);
    }

    println!("precheck passed.");
    Ok(())
}
