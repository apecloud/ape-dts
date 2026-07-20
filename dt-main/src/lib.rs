use dt_common::error::DtError;
use dt_precheck::{config::task_config::PrecheckTaskConfig, do_precheck};
use dt_task::task_runner::TaskRunner;

pub async fn run_config(config: &str, init: bool) -> anyhow::Result<()> {
    match PrecheckTaskConfig::load_if_present(config)? {
        Some(precheck_config) => do_precheck(config, precheck_config).await?,
        None => {
            let runner = TaskRunner::new(config)?;
            let task_id = runner.task_id().to_string();
            runner.start_task(init).await.map_err(|mut error| {
                if let Some(error) = error.downcast_mut::<DtError>() {
                    if error.task_id.is_none() {
                        error.task_id = Some(task_id);
                    }
                }
                error
            })?;
        }
    }
    Ok(())
}
