use super::{check_util::CheckUtil, rdb_test_runner::RdbTestRunner};

pub struct RdbCheckTestRunner {
    base: RdbTestRunner,
    dst_check_log_dir: String,
    expect_check_log_dir: String,
}

impl RdbCheckTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = RdbTestRunner::new(relative_test_dir).await.unwrap();
        let version = base.get_dst_mysql_version().await;
        let (expect_check_log_dir, dst_check_log_dir) =
            CheckUtil::get_check_log_dir(&base.base, &version);
        Ok(Self {
            base,
            dst_check_log_dir,
            expect_check_log_dir,
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.base.close().await
    }

    pub async fn run_check_test(&self) -> anyhow::Result<()> {
        // clear existed check logs
        CheckUtil::clear_check_log(&self.dst_check_log_dir);

        // prepare src and dst tables
        self.base.execute_prepare_sqls().await?;
        self.base.execute_test_sqls().await?;

        // execute post dst sqls after task starts to simulate delayed arrival
        let mut post_dst_task = None;
        if !self.base.base.dst_post_test_sqls.is_empty() {
            let dst_sqls = self.base.base.dst_post_test_sqls.clone();
            let delay_ms = match &self.base.config.sinker {
                dt_common::config::sinker_config::SinkerConfig::MysqlCheck { .. }
                | dt_common::config::sinker_config::SinkerConfig::PgCheck { .. }
                | dt_common::config::sinker_config::SinkerConfig::MongoCheck { .. } => 0,
                _ => 0,
            };

            let dst_pool_mysql = self.base.dst_conn_pool_mysql.clone();
            let dst_pool_pg = self.base.dst_conn_pool_pg.clone();
            post_dst_task = Some(tokio::spawn(async move {
                if delay_ms > 0 {
                    dt_common::utils::time_util::TimeUtil::sleep_millis(delay_ms).await;
                }
                if let Some(pool) = dst_pool_mysql {
                    super::rdb_util::RdbUtil::execute_sqls_mysql(&pool, &dst_sqls)
                        .await
                        .unwrap();
                }
                if let Some(pool) = dst_pool_pg {
                    super::rdb_util::RdbUtil::execute_sqls_pg(&pool, &dst_sqls)
                        .await
                        .unwrap();
                }
            }));
        }

        // start task
        self.base.base.start_task().await?;
        if let Some(handle) = post_dst_task {
            handle.await.unwrap();
        }

        CheckUtil::validate_check_log(&self.expect_check_log_dir, &self.dst_check_log_dir)?;

        self.base.execute_clean_sqls().await?;

        Ok(())
    }

    pub async fn run_revise_test(&self) -> anyhow::Result<()> {
        CheckUtil::clear_check_log(&self.dst_check_log_dir);
        self.base.run_snapshot_test(true).await
    }

    pub async fn run_review_test(&self) -> anyhow::Result<()> {
        CheckUtil::clear_check_log(&self.dst_check_log_dir);
        self.run_check_test().await
    }
}
