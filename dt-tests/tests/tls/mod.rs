macro_rules! tls_task_tests {
    ($module:ident, $engine:literal, $mode:ident; $($task:ident => $kind:literal),+ $(,)?) => {
        mod $module {
            $(
                #[tokio::test]
                #[serial_test::serial]
                async fn $task() {
                    crate::tls::common::run_tls_task_test(
                        $engine, $kind, dt_common::config::ssl_config::SslMode::$mode,
                    ).await;
                }
            )+
        }
    };
}

mod common;
mod mongo;
mod mongo_shard;
mod mssql;
mod mysql;
mod pg;
mod redis_7_0;
mod redis_8_0;
mod redis_cluster_6_2;
mod redis_cluster_7_0;
