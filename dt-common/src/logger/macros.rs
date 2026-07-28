#[macro_export(local_inner_macros)]
macro_rules! log_miss {
    ($($arg:tt)+) => (log::log!(target: "miss_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_diff {
    ($($arg:tt)+) => (log::log!(target: "diff_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_position {
    ($($arg:tt)+) => (log::log!(target: "position_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_monitor {
    ($($arg:tt)+) => (log::log!(target: "monitor_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_statistic {
    ($($arg:tt)+) => (log::log!(target: "statistic_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_finished {
    ($($arg:tt)+) => (log::log!(target: "finished_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_sql {
    ($($arg:tt)+) => (log::log!(target: "sql_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_summary {
    ($($arg:tt)+) => (log::log!(target: "summary_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_runtime_trace {
    ($($arg:tt)+) => (log::log!(target: "runtime_trace_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_error {
    ($($arg:tt)+) => (log::log!(target: "default_logger", log::Level::Error, $($arg)+))
}

#[macro_export(local_inner_macros)]
macro_rules! log_error_report {
    ($($arg:tt)+) => (log::log!(target: "error_report_logger", log::Level::Error, $($arg)+))
}

#[macro_export(local_inner_macros)]
macro_rules! log_info {
    ($($arg:tt)+) => (log::log!(target: "default_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_warn {
    ($($arg:tt)+) => (log::log!(target: "default_logger", log::Level::Warn, $($arg)+))
}

#[macro_export(local_inner_macros)]
macro_rules! log_debug {
    ($($arg:tt)+) => (log::log!(target: "default_logger", log::Level::Debug, $($arg)+))
}

#[macro_export(local_inner_macros)]
macro_rules! log_task {
    ($($arg:tt)+) => (log::log!(target: "task_logger", log::Level::Info, $($arg)+));
}

#[cfg(test)]
mod tests {
    #[test]
    fn error_report_log_starts_with_a_utc_timestamp() {
        let config: serde_yaml::Value =
            serde_yaml::from_str(include_str!("../../../log4rs.yaml")).unwrap();
        let appender = &config["appenders"]["error_report_appender"];
        let logger = &config["loggers"]["error_report_logger"];

        assert_eq!(
            appender["path"].as_str(),
            Some("LOG_DIR_PLACEHOLDER/error_report.log")
        );
        assert_eq!(
            appender["encoder"]["pattern"].as_str(),
            Some("{d(%Y-%m-%d %H:%M:%S.%6f)(utc)} | {m}{n}")
        );
        assert_eq!(logger["level"].as_str(), Some("trace"));
        assert_eq!(logger["additive"].as_bool(), Some(false));
    }
}
