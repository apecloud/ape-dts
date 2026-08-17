use std::{
    env::current_dir,
    fs::canonicalize,
    io::ErrorKind,
    path::{Component, Path},
    sync::Mutex,
};

use log4rs::config::{Config, Deserializers, RawConfig};
use tokio::{
    fs::{self, metadata, File},
    io::AsyncReadExt,
};

use crate::{
    config::{
        checker_config::DEFAULT_CHECK_LOG_FILE_SIZE, config_enums::TaskType,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::DtError,
    log_filter::{RowLimitFilterDeserializer, SizeLimitFilterDeserializer},
};

static LOG_HANDLE: Mutex<Option<log4rs::Handle>> = Mutex::new(None);

const CHECK_LOG_DIR_PLACEHOLDER: &str = "CHECK_LOG_DIR_PLACEHOLDER";
const STATISTIC_LOG_DIR_PLACEHOLDER: &str = "STATISTIC_LOG_DIR_PLACEHOLDER";
const LOG_LEVEL_PLACEHOLDER: &str = "LOG_LEVEL_PLACEHOLDER";
const LOG_DIR_PLACEHOLDER: &str = "LOG_DIR_PLACEHOLDER";
const CHECK_LOG_FILE_SIZE_PLACEHOLDER: &str = "CHECK_LOG_FILE_SIZE_PLACEHOLDER";
const CHECK_LOG_MAX_ROWS_PLACEHOLDER: &str = "CHECK_LOG_MAX_ROWS_PLACEHOLDER";
const RUNTIME_STDOUT_APPENDER_PLACEHOLDER: &str = "RUNTIME_STDOUT_APPENDER_PLACEHOLDER";
const CHECK_RESULT_STDOUT_APPENDER_PLACEHOLDER: &str = "CHECK_RESULT_STDOUT_APPENDER_PLACEHOLDER";
const DEFAULT_CHECK_LOG_DIR_PLACEHOLDER: &str = "LOG_DIR_PLACEHOLDER/check";
const DEFAULT_STATISTIC_LOG_DIR_PLACEHOLDER: &str = "LOG_DIR_PLACEHOLDER/statistic";

fn log_deserializers() -> Deserializers {
    let mut deserializers = Deserializers::default();
    deserializers.insert("size_limit", SizeLimitFilterDeserializer);
    deserializers.insert("row_limit", RowLimitFilterDeserializer);
    deserializers
}

pub struct TaskLogger<'a> {
    task_config: &'a TaskConfig,
}

impl<'a> TaskLogger<'a> {
    pub fn new(task_config: &'a TaskConfig) -> Self {
        Self { task_config }
    }

    pub async fn init(&self, is_precheck: bool) -> anyhow::Result<()> {
        if !is_precheck {
            self.clear_check_logs().await?;
        }
        self.init_log4rs().await
    }

    async fn clear_check_logs(&self) -> anyhow::Result<()> {
        let Some(cfg) = self.task_config.checker.as_ref() else {
            return Ok(());
        };
        let check_log_dir = if cfg.log_dir().is_empty() {
            format!("{}/check", self.task_config.runtime.log_dir)
        } else {
            cfg.log_dir().to_string()
        };
        if Self::check_log_replay_reads_from_dir(&self.task_config.extractor, &check_log_dir)
            || !Self::should_clear_check_logs_before_log4rs(self.task_config.task_type())
        {
            return Ok(());
        }

        fs::create_dir_all(&check_log_dir).await?;
        for file_name in ["miss.log", "diff.log", "summary.log", "sql.log"] {
            Self::remove_file_if_exists(&format!("{check_log_dir}/{file_name}")).await?;
        }
        Ok(())
    }

    fn should_clear_check_logs_before_log4rs(task_type: Option<TaskType>) -> bool {
        match task_type {
            Some(task_type) => task_type.has_check() && !task_type.is_cdc_inline_check(),
            None => true,
        }
    }

    fn check_log_replay_reads_from_dir(extractor: &ExtractorConfig, check_log_dir: &str) -> bool {
        let replay_dir = match extractor {
            ExtractorConfig::MysqlCheck { check_log_dir, .. }
            | ExtractorConfig::PgCheck { check_log_dir, .. }
            | ExtractorConfig::MongoCheck { check_log_dir, .. } => check_log_dir,
            _ => return false,
        };
        Self::same_check_log_dir(replay_dir, check_log_dir)
    }

    fn same_check_log_dir(left: &str, right: &str) -> bool {
        let normalize = |path: &str| {
            canonicalize(path).unwrap_or_else(|_| {
                let path = current_dir().unwrap_or_default().join(Path::new(path));
                path.components()
                    .fold(Path::new("").into(), |mut acc, component| {
                        match component {
                            Component::CurDir => {}
                            Component::ParentDir => {
                                acc.pop();
                            }
                            _ => acc.push(component.as_os_str()),
                        }
                        acc
                    })
            })
        };
        normalize(left) == normalize(right)
    }

    async fn remove_file_if_exists(path: &str) -> anyhow::Result<()> {
        match fs::remove_file(path).await {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn init_log4rs(&self) -> anyhow::Result<()> {
        let log4rs_file = &self.task_config.runtime.log4rs_file;
        match metadata(log4rs_file).await {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }

        let mut config_str = String::new();
        let mut file = File::open(log4rs_file).await?;
        file.read_to_string(&mut config_str).await?;

        match &self.task_config.sinker {
            SinkerConfig::RedisStatistic {
                statistic_log_dir, ..
            } => {
                if !statistic_log_dir.is_empty() {
                    config_str =
                        config_str.replace(STATISTIC_LOG_DIR_PLACEHOLDER, statistic_log_dir);
                }
            }
            _ => {
                if let Some(cfg) = self.task_config.checker.as_ref() {
                    if !cfg.log_dir().is_empty() {
                        config_str = config_str.replace(CHECK_LOG_DIR_PLACEHOLDER, cfg.log_dir());
                    }
                    config_str =
                        config_str.replace(CHECK_LOG_FILE_SIZE_PLACEHOLDER, cfg.log_file_size());
                    config_str = config_str.replace(
                        CHECK_LOG_MAX_ROWS_PLACEHOLDER,
                        &cfg.log_max_rows().max(1).to_string(),
                    );
                }
            }
        }

        config_str = config_str
            .replace(CHECK_LOG_DIR_PLACEHOLDER, DEFAULT_CHECK_LOG_DIR_PLACEHOLDER)
            .replace(
                STATISTIC_LOG_DIR_PLACEHOLDER,
                DEFAULT_STATISTIC_LOG_DIR_PLACEHOLDER,
            )
            .replace(CHECK_LOG_FILE_SIZE_PLACEHOLDER, DEFAULT_CHECK_LOG_FILE_SIZE)
            .replace(
                CHECK_LOG_MAX_ROWS_PLACEHOLDER,
                &crate::config::checker_config::DEFAULT_CHECK_LOG_MAX_ROWS.to_string(),
            )
            .replace(LOG_DIR_PLACEHOLDER, &self.task_config.runtime.log_dir)
            .replace(LOG_LEVEL_PLACEHOLDER, &self.task_config.runtime.log_level);

        if self.task_config.runtime.check_result_stdout_only {
            config_str = config_str
                .replace(
                    RUNTIME_STDOUT_APPENDER_PLACEHOLDER,
                    "silent_stdout_appender",
                )
                .replace(
                    CHECK_RESULT_STDOUT_APPENDER_PLACEHOLDER,
                    "check_stdout_appender",
                );
        } else {
            config_str = config_str
                .replace(RUNTIME_STDOUT_APPENDER_PLACEHOLDER, "stdout")
                .replace(
                    CHECK_RESULT_STDOUT_APPENDER_PLACEHOLDER,
                    "silent_stdout_appender",
                );
        }

        let raw: RawConfig = serde_yaml::from_str(&config_str)?;
        let deserializers = log_deserializers();
        let (appenders, errors) = raw.appenders_lossy(&deserializers);
        if !errors.is_empty() {
            log::error!(target: "default_logger", "errors deserializing log appenders: {errors:?}");
            return Err(DtError::InvalidConfig(
                "one or more logging appenders are invalid".to_string(),
            )
            .into());
        }

        let config = Config::builder()
            .appenders(appenders)
            .loggers(raw.loggers())
            .build(raw.root())?;
        let mut handle_guard = LOG_HANDLE.lock().map_err(|_| {
            DtError::InvariantViolated("the logging configuration lock is poisoned".to_string())
        })?;
        if let Some(handle) = handle_guard.as_ref() {
            handle.set_config(config);
        } else {
            let handle = log4rs::init_config(config)?;
            *handle_guard = Some(handle);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env::current_dir;

    use log4rs::config::RawConfig;

    use super::{log_deserializers, TaskLogger};
    use crate::config::{
        config_enums::{CheckMode, TaskKind, TaskType},
        connection_auth_config::ConnectionAuthConfig,
        extractor_config::ExtractorConfig,
    };

    #[test]
    fn should_clear_standalone_check_logs_and_tasks_without_a_type() {
        assert!(TaskLogger::should_clear_check_logs_before_log4rs(None));
        assert!(TaskLogger::should_clear_check_logs_before_log4rs(Some(
            TaskType::new(TaskKind::Snapshot, Some(CheckMode::Standalone))
        )));
    }

    #[test]
    fn check_log_replay_input_output_same_dir_is_detected() {
        let extractor = ExtractorConfig::MysqlCheck {
            url: String::new(),
            connection_auth: ConnectionAuthConfig::NoAuth,
            check_log_dir: "/tmp/ape-dts/check/".to_string(),
            batch_size: 1,
        };

        assert!(TaskLogger::check_log_replay_reads_from_dir(
            &extractor,
            "/tmp/ape-dts/check"
        ));
        assert!(TaskLogger::check_log_replay_reads_from_dir(
            &extractor,
            "/tmp/ape-dts/./check"
        ));
        assert!(TaskLogger::same_check_log_dir(
            "logs/check",
            &current_dir().unwrap().join("logs/check").to_string_lossy()
        ));
        assert!(!TaskLogger::check_log_replay_reads_from_dir(
            &extractor,
            "/tmp/ape-dts/other"
        ));
    }

    #[test]
    fn bundled_log4rs_config_accepts_check_log_limits() {
        let bundled_config = include_str!("../../../log4rs.yaml");
        let yaml: serde_yaml::Value = serde_yaml::from_str(bundled_config).unwrap();
        for appender in ["miss_appender", "diff_appender"] {
            let filters = yaml["appenders"][appender]["filters"]
                .as_sequence()
                .unwrap();
            assert_eq!(filters[0]["kind"].as_str(), Some("size_limit"));
            assert_eq!(filters[1]["kind"].as_str(), Some("row_limit"));
        }

        let config = bundled_config
            .replace("CHECK_LOG_DIR_PLACEHOLDER", "/tmp/ape-dts/check")
            .replace("STATISTIC_LOG_DIR_PLACEHOLDER", "/tmp/ape-dts/statistic")
            .replace("CHECK_LOG_FILE_SIZE_PLACEHOLDER", "100mb")
            .replace("CHECK_LOG_MAX_ROWS_PLACEHOLDER", "1000")
            .replace("LOG_DIR_PLACEHOLDER", "/tmp/ape-dts")
            .replace("LOG_LEVEL_PLACEHOLDER", "info")
            .replace("RUNTIME_STDOUT_APPENDER_PLACEHOLDER", "stdout")
            .replace(
                "CHECK_RESULT_STDOUT_APPENDER_PLACEHOLDER",
                "silent_stdout_appender",
            );
        let raw: RawConfig = serde_yaml::from_str(&config).unwrap();
        let (_, errors) = raw.appenders_lossy(&log_deserializers());

        assert!(errors.is_empty(), "{errors:?}");
    }
}
