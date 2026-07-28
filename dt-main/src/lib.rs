use dt_common::{
    config::task_config::TaskConfig,
    error::{DtResultExt, Stage},
};
use dt_precheck::{config::task_config::PrecheckTaskConfig, do_precheck};
use dt_task::task_runner::TaskRunner;

pub async fn run_config(
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
