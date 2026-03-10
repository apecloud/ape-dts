use super::{check_util::CheckUtil, rdb_test_runner::RdbTestRunner};
use dt_common::utils::time_util::TimeUtil;

pub struct RdbCheckTestRunner {
    base: RdbTestRunner,
    dst_check_log_dir: String,
    expect_check_log_dir: String,
}

impl RdbCheckTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = RdbTestRunner::new(relative_test_dir).await?;
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

        // start task
        self.base.base.start_task().await?;

        CheckUtil::validate_check_log(&self.expect_check_log_dir, &self.dst_check_log_dir)?;

        self.base.execute_clean_sqls().await?;

        Ok(())
    }

    pub async fn run_cdc_check_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        CheckUtil::clear_check_log(&self.dst_check_log_dir);

        self.base.execute_prepare_sqls().await?;
        self.base
            .execute_dst_sqls(&self.base.base.dst_test_sqls)
            .await?;

        let task = self.base.spawn_cdc_task(start_millis, parse_millis).await?;
        self.base
            .execute_src_sqls(&self.base.base.src_test_sqls)
            .await?;
        self.base.base.wait_task_finish(&task).await?;
        TimeUtil::sleep_millis(3000).await;

        CheckUtil::validate_check_log(&self.expect_check_log_dir, &self.dst_check_log_dir)?;
        self.base.execute_clean_sqls().await?;
        Ok(())
    }

    pub async fn run_recheck_test(&self) -> anyhow::Result<()> {
        CheckUtil::clear_check_log(&self.dst_check_log_dir);
        self.base.execute_prepare_sqls().await?;
        self.base.execute_test_sqls().await?;

        let retry_interval_secs = self
            .base
            .base
            .get_config()
            .checker
            .as_ref()
            .map(|checker| checker.retry_interval_secs)
            .unwrap_or(0);
        let delay_secs = std::cmp::max(1, retry_interval_secs / 2);

        let pool_mysql = self.base.dst_conn_pool_mysql.clone();
        let pool_pg = self.base.dst_conn_pool_pg.clone();
        let sqls = self.base.base.src_test_sqls.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
            if let Some(pool) = pool_mysql {
                for sql in &sqls {
                    sqlx::query(sql).execute(&pool).await.unwrap();
                }
            }
            if let Some(pool) = pool_pg {
                for sql in &sqls {
                    sqlx::query(sql).execute(&pool).await.unwrap();
                }
            }
        });

        self.base.base.start_task().await?;
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
