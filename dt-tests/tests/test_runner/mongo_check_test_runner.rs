use super::{check_util::CheckUtil, mongo_test_runner::MongoTestRunner};

pub struct MongoCheckTestRunner {
    base: std::sync::Arc<MongoTestRunner>,
    dst_check_log_dir: String,
    expect_check_log_dir: String,
}

impl MongoCheckTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = MongoTestRunner::new(relative_test_dir).await.unwrap();
        let (expect_check_log_dir, dst_check_log_dir) =
            CheckUtil::get_check_log_dir(&base.base, "");
        Ok(Self {
            base: std::sync::Arc::new(base),
            dst_check_log_dir,
            expect_check_log_dir,
        })
    }

    pub async fn run_check_test(&self) -> anyhow::Result<()> {
        // clear existed check logs
        CheckUtil::clear_check_log(&self.dst_check_log_dir);

        // prepare src and dst data
        self.base.execute_prepare_sqls().await.unwrap();
        self.base.execute_test_sqls().await.unwrap();

        // start task
        self.base.base.start_task().await?;
        CheckUtil::validate_check_log(&self.expect_check_log_dir, &self.dst_check_log_dir)
    }

    pub async fn run_recheck_test(&self) -> anyhow::Result<()> {
        CheckUtil::clear_check_log(&self.dst_check_log_dir);
        self.base.execute_prepare_sqls().await?;
        self.base.execute_test_sqls().await?;

        let retry_interval_secs = self.base.base.get_config().sinker.get_retry_interval_secs();
        let delay_secs = std::cmp::max(1, retry_interval_secs / 2);

        let sqls = self.base.base.src_test_sqls.clone();
        let client = self.base.dst_mongo_client().clone();
        let runner = self.base.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
            runner
                .execute_sqls_with_client(&client, &sqls)
                .await
                .unwrap();
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
