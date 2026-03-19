use super::{
    base_test_runner::BaseTestRunner, check_util::CheckUtil, rdb_test_runner::RdbTestRunner,
    rdb_util::RdbUtil,
};
use crate::test_config_util::TestConfigUtil;
use anyhow::Context;
use dt_common::config::{config_enums::DbType, resumer_config::ResumerConfig};
use dt_common::utils::time_util::TimeUtil;
use dt_connector::checker::check_log::CheckSummaryLog;
use dt_task::task_util::TaskUtil;
use sqlx::{MySql, Pool, Postgres, Row};
use std::{fs::File, path::Path};

pub struct RdbCheckTestRunner {
    base: RdbTestRunner,
    dst_check_log_dir: String,
    expect_check_log_dir: String,
    checker_conn_pool_mysql: Option<Pool<MySql>>,
    checker_conn_pool_pg: Option<Pool<Postgres>>,
}

impl RdbCheckTestRunner {
    fn set_tb_parallel_size(&self, tb_parallel_size: usize) {
        TestConfigUtil::update_task_config(
            &self.base.base.task_config_file,
            &self.base.base.task_config_file,
            &[(
                "runtime".to_string(),
                "tb_parallel_size".to_string(),
                tb_parallel_size.to_string(),
            )],
        );
    }

    fn task_log_file(&self) -> String {
        format!("{}/task.log", self.base.config.runtime.log_dir)
    }

    fn clear_task_log(&self) -> anyhow::Result<()> {
        let task_log_file = self.task_log_file();
        if let Some(parent) = Path::new(&task_log_file).parent() {
            std::fs::create_dir_all(parent)?;
        }
        File::create(&task_log_file)?.set_len(0)?;
        Ok(())
    }

    fn load_summary_log(&self) -> anyhow::Result<CheckSummaryLog> {
        let summary_log_file = format!("{}/summary.log", self.dst_check_log_dir);
        let summary_raw = BaseTestRunner::load_file(&summary_log_file)
            .into_iter()
            .last()
            .with_context(|| format!("summary log is empty: {}", summary_log_file))?;
        serde_json::from_str(&summary_raw)
            .with_context(|| format!("failed to parse summary log: {}", summary_raw))
    }

    fn load_task_metrics(&self) -> anyhow::Result<serde_json::Value> {
        let task_log_file = self.task_log_file();
        let metrics_raw = BaseTestRunner::load_file(&task_log_file)
            .into_iter()
            .last()
            .with_context(|| format!("task log is empty: {}", task_log_file))?;
        serde_json::from_str(&metrics_raw)
            .with_context(|| format!("failed to parse task log: {}", metrics_raw))
    }

    fn assert_task_metrics_match_summary(&self) -> anyhow::Result<()> {
        let summary = self.load_summary_log()?;
        let task_metrics = self.load_task_metrics()?;

        let miss_count = task_metrics
            .get("checker_miss_count")
            .and_then(serde_json::Value::as_u64)
            .context("task metrics missing checker_miss_count")?;
        let diff_count = task_metrics
            .get("checker_diff_count")
            .and_then(serde_json::Value::as_u64)
            .context("task metrics missing checker_diff_count")?;

        assert_eq!(miss_count, summary.miss_count as u64);
        assert_eq!(diff_count, summary.diff_count as u64);
        Ok(())
    }

    fn get_resumer_tables(&self) -> anyhow::Result<(String, String)> {
        match &self.base.config.resumer {
            ResumerConfig::FromDB {
                table_full_name, ..
            } => {
                let (schema, table) = table_full_name.split_once('.').with_context(|| {
                    format!("invalid resumer table_full_name: {table_full_name}")
                })?;
                Ok((schema.to_string(), table.to_string()))
            }
            other => anyhow::bail!(
                "resumer config must be FromDB/from_target, got: {:?}",
                other
            ),
        }
    }

    async fn execute_checker_sqls(&self, sqls: &[String]) -> anyhow::Result<()> {
        if let Some(pool) = &self.checker_conn_pool_mysql {
            RdbUtil::execute_sqls_mysql(pool, &sqls.to_vec()).await?;
        }
        if let Some(pool) = &self.checker_conn_pool_pg {
            RdbUtil::execute_sqls_pg(pool, sqls).await?;
        }
        Ok(())
    }

    async fn reset_resumer_backend(&self) -> anyhow::Result<()> {
        let (schema, _) = self.get_resumer_tables()?;
        if let Some(pool) = &self.base.dst_conn_pool_mysql {
            let sqls = vec![format!("DROP DATABASE IF EXISTS `{}`", schema)];
            RdbUtil::execute_sqls_mysql(pool, &sqls).await?;
        }
        if let Some(pool) = &self.base.dst_conn_pool_pg {
            let sqls = vec![format!("DROP SCHEMA IF EXISTS {} CASCADE", schema)];
            RdbUtil::execute_sqls_pg(pool, &sqls).await?;
        }
        Ok(())
    }

    async fn get_resumer_unresolved_row_count(&self) -> anyhow::Result<i64> {
        let (schema, _) = self.get_resumer_tables()?;
        let task_id = &self.base.config.global.task_id;
        if let Some(pool) = &self.base.dst_conn_pool_mysql {
            let sql = format!(
                "SELECT COUNT(*) AS cnt FROM `{}`.`apedts_unconsistent_rows` WHERE task_id = ?",
                schema
            );
            let row = sqlx::query(&sql).bind(task_id).fetch_one(pool).await?;
            return Ok(row.get::<i64, _>("cnt"));
        }
        if let Some(pool) = &self.base.dst_conn_pool_pg {
            let sql = format!(
                "SELECT COUNT(*) AS cnt FROM {}.apedts_unconsistent_rows WHERE task_id = $1",
                schema
            );
            let row = sqlx::query(&sql).bind(task_id).fetch_one(pool).await?;
            return Ok(row.get::<i64, _>("cnt"));
        }
        anyhow::bail!("no sinker pool available for querying resumer unresolved rows")
    }

    async fn get_resumer_cdc_position_count(&self) -> anyhow::Result<i64> {
        let (schema, table) = self.get_resumer_tables()?;
        let task_id = &self.base.config.global.task_id;
        if let Some(pool) = &self.base.dst_conn_pool_mysql {
            let sql = format!(
                "SELECT COUNT(*) AS cnt FROM `{}`.`{}` WHERE task_id = ? AND resumer_type = ?",
                schema, table
            );
            let row = sqlx::query(&sql)
                .bind(task_id)
                .bind("CdcDoing")
                .fetch_one(pool)
                .await?;
            return Ok(row.get::<i64, _>("cnt"));
        }
        if let Some(pool) = &self.base.dst_conn_pool_pg {
            let sql = format!(
                "SELECT COUNT(*) AS cnt FROM {}.{} WHERE task_id = $1 AND resumer_type = $2",
                schema, table
            );
            let row = sqlx::query(&sql)
                .bind(task_id)
                .bind("CdcDoing")
                .fetch_one(pool)
                .await?;
            return Ok(row.get::<i64, _>("cnt"));
        }
        anyhow::bail!("no sinker pool available for querying resumer position rows")
    }

    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = RdbTestRunner::new(relative_test_dir).await?;
        let version = base.get_dst_mysql_version().await;
        let (expect_check_log_dir, dst_check_log_dir) =
            CheckUtil::get_check_log_dir(&base.base, &version);
        let mut checker_conn_pool_mysql = None;
        let mut checker_conn_pool_pg = None;

        if let Some(checker) = &base.config.checker {
            let sink_target_url = base.config.sink_target().map(|target| target.url);
            let is_override = match sink_target_url.as_ref() {
                Some(sink_url) => &checker.url != sink_url,
                None => true,
            };

            if is_override {
                match checker.db_type {
                    DbType::Mysql => {
                        checker_conn_pool_mysql = Some(
                            TaskUtil::create_mysql_conn_pool(
                                &checker.url,
                                &checker.connection_auth,
                                5,
                                false,
                                Some(vec!["SET FOREIGN_KEY_CHECKS=0"]),
                            )
                            .await?,
                        );
                    }
                    DbType::Pg => {
                        checker_conn_pool_pg = Some(
                            TaskUtil::create_pg_conn_pool(
                                &checker.url,
                                &checker.connection_auth,
                                5,
                                false,
                                true,
                            )
                            .await?,
                        );
                    }
                    _ => {}
                }
            }
        }

        Ok(Self {
            base,
            dst_check_log_dir,
            expect_check_log_dir,
            checker_conn_pool_mysql,
            checker_conn_pool_pg,
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.base.close().await?;
        if let Some(pool) = &self.checker_conn_pool_mysql {
            pool.close().await;
        }
        if let Some(pool) = &self.checker_conn_pool_pg {
            pool.close().await;
        }
        Ok(())
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

    pub async fn run_check_test_and_validate_task_metrics(
        &self,
        tb_parallel_size: usize,
    ) -> anyhow::Result<()> {
        self.set_tb_parallel_size(tb_parallel_size);
        self.clear_task_log()?;
        self.run_check_test().await?;
        self.assert_task_metrics_match_summary()
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

    pub async fn run_cdc_check_resume_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        CheckUtil::clear_check_log(&self.dst_check_log_dir);

        self.reset_resumer_backend().await?;
        self.base.execute_prepare_sqls().await?;
        self.execute_checker_sqls(&self.base.base.dst_prepare_sqls)
            .await?;
        self.base
            .execute_dst_sqls(&self.base.base.dst_test_sqls)
            .await?;
        self.execute_checker_sqls(&self.base.base.dst_test_sqls)
            .await?;

        let task = self.base.spawn_cdc_task(start_millis, parse_millis).await?;
        self.base
            .execute_src_sqls(&self.base.base.src_test_sqls)
            .await?;
        self.base.base.wait_task_finish(&task).await?;
        TimeUtil::sleep_millis(3000).await;

        let unresolved_rows = self.get_resumer_unresolved_row_count().await?;
        assert!(
            unresolved_rows > 0,
            "expected unresolved checker rows to persist into resumer backend after first run"
        );
        let cdc_positions = self.get_resumer_cdc_position_count().await?;
        assert!(
            cdc_positions > 0,
            "expected CDC position to persist into resumer backend after first run"
        );

        self.execute_checker_sqls(&self.base.base.src_test_sqls)
            .await?;
        CheckUtil::clear_check_log(&self.dst_check_log_dir);

        let resumed_task = self.base.spawn_cdc_task(start_millis, parse_millis).await?;
        self.base.base.wait_task_finish(&resumed_task).await?;
        TimeUtil::sleep_millis(3000).await;

        let unresolved_rows_after_resume = self.get_resumer_unresolved_row_count().await?;
        assert_eq!(
            unresolved_rows_after_resume, 0,
            "expected resumed CDC+check run to clear persisted unresolved rows"
        );

        CheckUtil::validate_check_log(&self.expect_check_log_dir, &self.dst_check_log_dir)?;
        self.base.execute_clean_sqls().await?;
        self.reset_resumer_backend().await?;
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
