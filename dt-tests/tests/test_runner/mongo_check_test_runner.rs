use super::{check_util::CheckUtil, mongo_test_runner::MongoTestRunner};

pub struct MongoCheckTestRunner {
    base: MongoTestRunner,
    dst_check_log_dir: String,
    expect_check_log_dir: String,
}

impl MongoCheckTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = MongoTestRunner::new(relative_test_dir).await.unwrap();
        let (expect_check_log_dir, dst_check_log_dir) =
            CheckUtil::get_check_log_dir(&base.base, "");
        Ok(Self {
            base,
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

    pub async fn run_revise_test(&self) -> anyhow::Result<()> {
        CheckUtil::clear_check_log(&self.dst_check_log_dir);
        self.base.run_snapshot_test(true).await
    }

    pub async fn run_review_test(&self) -> anyhow::Result<()> {
        CheckUtil::clear_check_log(&self.dst_check_log_dir);
        self.run_check_test().await
    }
}
