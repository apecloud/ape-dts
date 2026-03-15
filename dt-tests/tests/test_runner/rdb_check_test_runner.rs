use super::{check_util::CheckUtil, rdb_test_runner::RdbTestRunner, rdb_util::RdbUtil};
use anyhow::Context;
use dt_common::config::{config_enums::DbType, resumer_config::ResumerConfig};
use dt_common::utils::time_util::TimeUtil;
use dt_task::task_util::TaskUtil;
use sqlx::{MySql, Pool, Postgres, Row};

pub struct RdbCheckTestRunner {
    base: RdbTestRunner,
    dst_check_log_dir: String,
    expect_check_log_dir: String,
    checker_conn_pool_mysql: Option<Pool<MySql>>,
    checker_conn_pool_pg: Option<Pool<Postgres>>,
}

impl RdbCheckTestRunner {
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
            let checker_db_type = checker.db_type.as_ref();
            let checker_url = checker.url.as_ref();
            let checker_auth = checker.connection_auth.as_ref();
            let is_override = checker_url.is_some_and(|url| url != &base.config.sinker_basic.url);

            if is_override {
                match (checker_db_type, checker_url, checker_auth) {
                    (Some(DbType::Mysql), Some(url), Some(auth)) => {
                        checker_conn_pool_mysql = Some(
                            TaskUtil::create_mysql_conn_pool(
                                url,
                                auth,
                                5,
                                false,
                                Some(vec!["SET FOREIGN_KEY_CHECKS=0"]),
                            )
                            .await?,
                        );
                    }
                    (Some(DbType::Pg), Some(url), Some(auth)) => {
                        checker_conn_pool_pg =
                            Some(TaskUtil::create_pg_conn_pool(url, auth, 5, false, true).await?);
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
