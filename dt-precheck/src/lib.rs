use dt_common::config::task_config::TaskConfig;

use crate::{
    builder::prechecker_builder::PrecheckerBuilder, config::task_config::PrecheckTaskConfig,
};

pub mod builder;
pub mod config;
mod error;
pub mod fetcher;
pub mod meta;
pub mod prechecker;

pub async fn do_precheck(config: &str, precheck_config: PrecheckTaskConfig) -> anyhow::Result<()> {
    let task_config = TaskConfig::new(config)?;

    let checker_connector = PrecheckerBuilder::build(precheck_config.precheck, task_config);
    if let Err(error) = checker_connector.verify_check_result().await {
        println!("precheck not passed.");
        return Err(error);
    }

    println!("precheck passed.");
    Ok(())
}
