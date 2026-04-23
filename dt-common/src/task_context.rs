use std::sync::{atomic::AtomicBool, Arc};

use crate::{
    config::{extractor_config::ExtractorConfig, task_config::TaskConfig},
    monitor::{
        group_monitor::GroupMonitor,
        monitor::Monitor,
        monitor_util::MonitorUtil,
        task_monitor::{MonitorType, TaskMonitor},
    },
};

#[derive(Clone)]
pub struct TaskContext {
    pub task_config: TaskConfig,
    pub extractor_monitor: Arc<GroupMonitor>,
    pub pipeline_monitor: Arc<GroupMonitor>,
    pub sinker_monitor: Arc<GroupMonitor>,
    pub task_monitor: Arc<TaskMonitor>,
    pub shut_down: Arc<AtomicBool>,
}

impl TaskContext {
    pub async fn add_monitor(&self, id: &str) -> anyhow::Result<()> {
        let monitor_time_window_secs = self.task_config.pipeline.counter_time_window_secs;
        let monitor_max_sub_count = self.task_config.pipeline.counter_max_sub_count;
        let monitor_count_window = self.task_config.pipeline.capacity_limiter.buffer_size as u64;
        let extractor_monitor = Arc::new(Monitor::new(
            "extractor",
            id,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        ));
        let sinker_monitor = Arc::new(Monitor::new(
            "sinker",
            id,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        ));
        let pipeline_monitor = Arc::new(Monitor::new(
            "pipeline",
            id,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        ));
        // add monitors to global monitors
        tokio::join!(
            async {
                self.extractor_monitor
                    .add_monitor(id, extractor_monitor.clone());
            },
            async {
                self.pipeline_monitor
                    .add_monitor(id, pipeline_monitor.clone());
            },
            async {
                self.sinker_monitor.add_monitor(id, sinker_monitor.clone());
            },
            async {
                self.task_monitor.register(
                    id,
                    vec![
                        (MonitorType::Extractor, extractor_monitor.clone()),
                        (MonitorType::Pipeline, pipeline_monitor.clone()),
                        (MonitorType::Sinker, sinker_monitor.clone()),
                    ],
                );
            }
        );

        let interval_secs = self.task_config.pipeline.checkpoint_interval_secs;
        let shut_down = self.shut_down.clone();
        tokio::spawn(async move {
            MonitorUtil::flush_monitors_generic::<Monitor, TaskMonitor>(
                interval_secs,
                shut_down,
                &[extractor_monitor, pipeline_monitor, sinker_monitor],
                &[],
            )
            .await
        });
        Ok(())
    }

    pub async fn remove_monitor(&self, id: &str) -> anyhow::Result<()> {
        // remove monitors from global monitors
        tokio::join!(
            async {
                self.extractor_monitor.remove_monitor(id);
            },
            async {
                self.pipeline_monitor.remove_monitor(id);
            },
            async {
                self.sinker_monitor.remove_monitor(id);
            },
            async {
                self.task_monitor.unregister(
                    id,
                    vec![
                        MonitorType::Extractor,
                        MonitorType::Pipeline,
                        MonitorType::Sinker,
                    ],
                );
            }
        );
        Ok(())
    }

    pub fn generate_monitor_id(&self, schema: &str, table: &str) -> String {
        match &self.task_config.extractor {
            ExtractorConfig::MysqlSnapshot { .. }
            | ExtractorConfig::PgSnapshot { .. }
            | ExtractorConfig::MongoSnapshot { .. } => format!("{}.{}", schema, table),
            _ => String::new(),
        }
    }

    // TODO(wl)
    // pub async fn recorder_flush(&self, recorder: &Arc<dyn Recorder + Send + Sync>, schema: &str, table: &str) -> anyhow::Result<()> {
    //     // finished log
    //     let (schema, tb) = match &self.task_config.extractor {
    //         ExtractorConfig::MysqlSnapshot { .. }
    //         | ExtractorConfig::PgSnapshot { .. }
    //         | ExtractorConfig::MongoSnapshot { .. } => (schema.to_string(), table.to_string()),
    //         _ => (String::new(), String::new()),
    //     };
    //     if !tb.is_empty() {
    //         let finish_position = Position::RdbSnapshotFinished {
    //             db_type: self.task_config.extractor_basic.db_type.to_string(),
    //             schema,
    //             tb,
    //         };
    //         log_finished!("{}", finish_position.to_string());
    //         self.task_monitor
    //             .add_no_window_metrics(TaskMetricsType::FinishedProgressCount, 1);

    //         if let Some(handler) = &recorder {
    //             if let Err(e) = handler.record_position(&finish_position).await {
    //                 log_error!("failed to record position: {}, err: {}", finish_position, e);
    //             }
    //         }
    //     }
    //     Ok(())
    // }
}
