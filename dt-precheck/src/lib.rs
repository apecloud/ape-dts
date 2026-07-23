use dt_common::{
    config::task_config::TaskConfig,
    error::{DtErrorContextExt, Stage},
};

use crate::{
    builder::prechecker_builder::PrecheckerBuilder, config::task_config::PrecheckTaskConfig,
};

pub mod builder;
pub mod config;
pub mod fetcher;
pub mod meta;
pub mod prechecker;

pub async fn do_precheck(config: &str, precheck_config: PrecheckTaskConfig) -> anyhow::Result<()> {
    let task_config = TaskConfig::new(config)?;
    let task_id = task_config.global.task_id.clone();

    let checker_connector = PrecheckerBuilder::build(precheck_config.precheck, task_config);
    if let Err(error) = checker_connector
        .verify_check_result()
        .await
        .map_err(|error| error.stage(Stage::Precheck).task_id(task_id))
    {
        println!("precheck not passed.");
        return Err(error);
    }

    println!("precheck passed.");
    Ok(())
}
