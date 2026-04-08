use super::{
    base_test_runner::BaseTestRunner, check_util::CheckUtil, rdb_test_runner::RdbTestRunner,
    rdb_util::RdbUtil,
};
use crate::test_config_util::TestConfigUtil;
use anyhow::Context;
use chrono::Utc;
use dt_common::utils::time_util::TimeUtil;
use dt_common::{
    config::resumer_config::ResumerConfig,
    meta::{col_value::ColValue, position::Position},
};
use dt_connector::checker::{
    check_log::CheckSummaryLog,
    state_store::{CheckerCheckpointCommit, CheckerStateRow},
    CheckerStateStore,
};
use dt_connector::extractor::resumer::{
    recorder::to_database::DatabaseRecorder, utils::ResumerUtil, ResumerType,
};
use serde_json::Value;
use sqlx::{query, Row};
use std::{collections::BTreeMap, fs::File, path::Path};

pub struct CheckTestRunner {
    base: RdbTestRunner,
    dst_check_log_dir: String,
    expect_check_log_dir: String,
}

impl CheckTestRunner {
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

    async fn create_checker_state_store(&self) -> anyhow::Result<CheckerStateStore> {
        let ResumerConfig::FromDB {
            url,
            connection_auth,
            db_type,
            max_connections,
            ..
        } = &self.base.config.resumer
        else {
            anyhow::bail!("checker state store requires ResumerConfig::FromDB");
        };
        let pool = ResumerUtil::create_pool(url, connection_auth, db_type, *max_connections as u32)
            .await?;
        CheckerStateStore::new(pool, &self.base.config.resumer).await
    }

    async fn ensure_resumer_tables(&self) -> anyhow::Result<()> {
        let ResumerConfig::FromDB {
            url,
            connection_auth,
            db_type,
            max_connections,
            ..
        } = &self.base.config.resumer
        else {
            anyhow::bail!("resumer tables require ResumerConfig::FromDB");
        };
        let pool = ResumerUtil::create_pool(url, connection_auth, db_type, *max_connections as u32)
            .await?;
        let _recorder = DatabaseRecorder::new(
            &self.base.config.global.task_id,
            &self.base.config.resumer,
            pool,
        )
        .await?;
        Ok(())
    }

    async fn fetch_current_mysql_cdc_position(&self) -> anyhow::Result<Position> {
        let pool = self
            .base
            .src_conn_pool_mysql
            .as_ref()
            .context("source MySQL pool is required for CDC resume tests")?;
        let row = query("show master status").fetch_one(pool).await?;
        let binlog_file: String = row.try_get(0)?;
        let binlog_position: u32 = row.try_get(1)?;
        Ok(Position::MysqlCdc {
            server_id: String::new(),
            binlog_filename: binlog_file,
            next_event_position: binlog_position,
            gtid_set: String::new(),
            timestamp: Position::format_timestamp_millis(Utc::now().timestamp_millis()),
        })
    }

    fn compute_seed_row_key(id: i32) -> anyhow::Result<u128> {
        let col_hash_code = ColValue::Long(id).hash_code()?;
        Ok(31u128 + u128::from(col_hash_code))
    }

    fn build_seed_unresolved_row() -> anyhow::Result<CheckerStateRow> {
        let payload = serde_json::to_string(&serde_json::json!({
            "schema": "test_db_1",
            "tb": "check_test",
            "is_delete": false,
            "pk": {
                "id": {"Long": 2}
            }
        }))?;

        let identity_json = serde_json::to_string(&serde_json::json!({
            "schema": "test_db_1",
            "tb": "check_test",
            "id_col_values": BTreeMap::from([("id".to_string(), Some("2".to_string()))]),
        }))?;

        Ok(CheckerStateRow {
            row_key: Self::compute_seed_row_key(2)?,
            identity_key: "seed-unresolved-row-2".to_string(),
            identity_json,
            payload,
        })
    }

    async fn seed_cdc_position_only(&self, position: Position) -> anyhow::Result<()> {
        self.ensure_resumer_tables().await?;
        let (schema, table) = self.get_resumer_tables()?;
        let task_id = &self.base.config.global.task_id;
        if let Some(pool) = &self.base.dst_conn_pool_mysql {
            let sql = format!(
                "INSERT INTO `{}`.`{}` (task_id, resumer_type, position_key, position_data) \
                 VALUES (?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE position_data = VALUES(position_data), updated_at = CURRENT_TIMESTAMP",
                schema, table
            );
            sqlx::query(&sql)
                .bind(task_id)
                .bind(ResumerType::CdcDoing.to_string())
                .bind(ResumerUtil::get_key_from_position(&position))
                .bind(position.to_string())
                .execute(pool)
                .await?;
            return Ok(());
        }
        anyhow::bail!("only MySQL resume seed is implemented in this test runner")
    }

    async fn seed_checker_state_resume_metadata(&self) -> anyhow::Result<()> {
        self.ensure_resumer_tables().await?;
        let store = self.create_checker_state_store().await?;
        let position = self.fetch_current_mysql_cdc_position().await?;
        let row = Self::build_seed_unresolved_row()?;
        let commit = CheckerCheckpointCommit {
            task_id: self.base.config.global.task_id.clone(),
            position,
            upserts: vec![row],
            deletes: Vec::new(),
        };
        store.commit_checkpoint(&commit).await
    }

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
        self.base.close().await?;
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

    pub async fn run_cdc_position_resume_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        self.reset_resumer_backend().await?;
        self.base.execute_prepare_sqls().await?;
        let (src_db_tbs, dst_db_tbs) = self.base.get_compare_db_tbs()?;
        let split_at = std::cmp::max(1, self.base.base.src_test_sqls.len() / 2);
        let (first_half, second_half) = self.base.base.src_test_sqls.split_at(split_at);
        self.base.execute_src_sqls(&first_half.to_vec()).await?;
        self.base.execute_dst_sqls(&first_half.to_vec()).await?;
        let seeded_position = self.fetch_current_mysql_cdc_position().await?;
        self.seed_cdc_position_only(seeded_position).await?;
        let cdc_positions = self.get_resumer_cdc_position_count().await?;
        assert!(
            cdc_positions > 0,
            "expected seeded CDC position to persist before resume"
        );

        let resumed_task = self.base.spawn_cdc_task(start_millis, parse_millis).await?;
        self.base.execute_src_sqls(&second_half.to_vec()).await?;
        self.base.base.wait_task_finish(&resumed_task).await?;
        TimeUtil::sleep_millis(3000).await;
        assert!(
            self.base
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?
        );
        self.base.execute_clean_sqls().await?;
        self.reset_resumer_backend().await?;
        Ok(())
    }

    pub async fn run_cdc_checker_state_resume_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        CheckUtil::clear_check_log(&self.dst_check_log_dir);

        self.reset_resumer_backend().await?;
        self.base.execute_prepare_sqls().await?;
        self.base
            .execute_src_sqls(&self.base.base.src_test_sqls)
            .await?;
        self.base
            .execute_dst_sqls(&self.base.base.src_test_sqls)
            .await?;
        self.seed_checker_state_resume_metadata().await?;

        let unresolved_rows = self.get_resumer_unresolved_row_count().await?;
        assert!(
            unresolved_rows > 0,
            "expected seeded unresolved checker rows to exist before resume"
        );

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
        let (src_db_tbs, dst_db_tbs) = self.base.get_compare_db_tbs()?;
        assert!(
            self.base
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?
        );
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

#[test]
fn seed_unresolved_row_payload_only_contains_recheck_key() {
    let row = CheckTestRunner::build_seed_unresolved_row().unwrap();
    let Value::Object(payload) = serde_json::from_str(&row.payload).unwrap() else {
        panic!("payload should be a json object");
    };

    assert!(payload.contains_key("schema"));
    assert!(payload.contains_key("tb"));
    assert!(payload.contains_key("is_delete"));
    assert!(payload.contains_key("pk"));
    assert_eq!(payload.len(), 4);
}
