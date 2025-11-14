use std::{collections::HashSet, str::FromStr};

use chrono::{Duration, Utc};
use dt_common::{
    config::{
        config_enums::DbType,
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
        extractor_config::ExtractorConfig,
        meta_center_config::MetaCenterConfig,
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    meta::{ddl_meta::ddl_type::DdlType, time::dt_utc_time::DtNaiveTime},
    rdb_filter::RdbFilter,
    utils::{sql_util::SqlUtil, time_util::TimeUtil},
};

use dt_common::meta::{
    col_value::ColValue, ddl_meta::ddl_parser::DdlParser,
    mysql::mysql_meta_manager::MysqlMetaManager, row_data::RowData,
};
use dt_connector::{
    meta_fetcher::mysql::mysql_struct_check_fetcher::MysqlStructCheckFetcher, rdb_router::RdbRouter,
};
use dt_task::{task_runner::TaskRunner, task_util::TaskUtil};

use sqlx::{query, types::BigDecimal, MySql, Pool, Postgres, Row};
use tokio::task::JoinHandle;

use crate::test_config_util::TestConfigUtil;

use super::{base_test_runner::BaseTestRunner, rdb_util::RdbUtil};

pub struct RdbTestRunner {
    pub base: BaseTestRunner,
    pub src_conn_pool_mysql: Option<Pool<MySql>>,
    pub dst_conn_pool_mysql: Option<Pool<MySql>>,
    pub src_conn_pool_pg: Option<Pool<Postgres>>,
    pub dst_conn_pool_pg: Option<Pool<Postgres>>,
    pub meta_center_pool_mysql: Option<Pool<MySql>>,
    pub config: TaskConfig,
    pub router: RdbRouter,
    pub filter: RdbFilter,
}

pub const SRC: &str = "src";
pub const DST: &str = "dst";
pub const PUBLIC: &str = "public";

const UTC_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[allow(dead_code)]
impl RdbTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();

        // prepare conn pools
        let mut src_conn_pool_mysql = None;
        let mut dst_conn_pool_mysql = None;
        let mut src_conn_pool_pg = None;
        let mut dst_conn_pool_pg = None;

        let config = TaskConfig::new(&base.task_config_file).unwrap();
        let src_db_type = &config.extractor_basic.db_type;
        let dst_db_type = &config.sinker_basic.db_type;
        let src_url = &config.extractor_basic.url;
        let dst_url = &config.sinker_basic.url;

        let mysql_conn_settings = Some(vec!["SET FOREIGN_KEY_CHECKS=0"]);

        match src_db_type {
            DbType::Mysql => {
                src_conn_pool_mysql = Some(
                    TaskUtil::create_mysql_conn_pool(
                        src_url,
                        1,
                        false,
                        mysql_conn_settings.clone(),
                    )
                    .await?,
                );
            }
            DbType::Pg => {
                src_conn_pool_pg =
                    Some(TaskUtil::create_pg_conn_pool(src_url, 1, false, true).await?);
            }
            _ => {}
        }

        if !dst_url.is_empty() {
            match dst_db_type {
                DbType::Mysql
                | DbType::Foxlake
                | DbType::StarRocks
                | DbType::Doris
                | DbType::Tidb => {
                    dst_conn_pool_mysql = Some(
                        TaskUtil::create_mysql_conn_pool(
                            dst_url,
                            1,
                            false,
                            mysql_conn_settings.clone(),
                        )
                        .await?,
                    );
                }
                DbType::Pg => {
                    dst_conn_pool_pg =
                        Some(TaskUtil::create_pg_conn_pool(dst_url, 1, false, true).await?);
                }
                _ => {}
            }
        }

        let config = TaskConfig::new(&base.task_config_file).unwrap();
        let router = RdbRouter::from_config(&config.router, dst_db_type).unwrap();
        let filter = RdbFilter::from_config(&config.filter, dst_db_type).unwrap();
        let meta_center_pool_mysql = match &config.meta_center {
            Some(MetaCenterConfig::MySqlDbEngine { url, .. }) => Some(
                TaskUtil::create_mysql_conn_pool(url, 1, false, mysql_conn_settings.clone())
                    .await
                    .unwrap(),
            ),
            _ => None,
        };

        Ok(Self {
            src_conn_pool_mysql,
            dst_conn_pool_mysql,
            src_conn_pool_pg,
            dst_conn_pool_pg,
            meta_center_pool_mysql,
            config,
            router,
            filter,
            base,
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        if let Some(pool) = &self.src_conn_pool_mysql {
            pool.close().await;
        }
        if let Some(pool) = &self.dst_conn_pool_mysql {
            pool.close().await;
        }
        if let Some(pool) = &self.src_conn_pool_pg {
            pool.close().await;
        }
        if let Some(pool) = &self.dst_conn_pool_pg {
            pool.close().await;
        }
        Ok(())
    }

    pub async fn get_dst_mysql_version(&self) -> String {
        if let Some(conn_pool) = &self.dst_conn_pool_mysql {
            let meta_manager = MysqlMetaManager::new(conn_pool.clone()).await.unwrap();
            return meta_manager.meta_fetcher.version;
        }
        String::new()
    }

    pub async fn run_snapshot_test(&self, compare_data: bool) -> anyhow::Result<()> {
        // prepare src and dst tables
        self.execute_prepare_sqls().await?;
        self.execute_test_sqls().await?;

        // start task
        self.base.start_task().await?;

        // compare data
        let (src_db_tbs, dst_db_tbs) = self.get_compare_db_tbs()?;
        if compare_data {
            assert!(self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?)
        }
        Ok(())
    }

    pub async fn run_ddl_test(&self, start_millis: u64, parse_millis: u64) -> anyhow::Result<()> {
        self.execute_prepare_sqls().await?;

        self.update_cdc_task_config(start_millis, parse_millis)
            .await?;
        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        self.execute_src_sqls(&self.base.src_test_sqls).await?;
        self.base.wait_task_finish(&task).await?;

        let (src_db_tbs, dst_db_tbs) = self.get_compare_db_tbs()?;
        assert!(
            self.compare_data_for_tbs_ignore_filtered(&src_db_tbs, &dst_db_tbs)
                .await?
        );
        Ok(())
    }

    pub async fn run_ddl_meta_center_test(
        &mut self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        self.execute_prepare_sqls().await?;
        self.update_cdc_task_config(start_millis, parse_millis)
            .await?;

        self.execute_src_sqls(&self.base.src_test_sqls).await?;

        // run_ddl_test: start cdc task BEFORE src_test_sqls executed
        // run_ddl_meta_center_test: start cdc task AFTER src_test_sqls executed
        let task: JoinHandle<()> = self.base.spawn_task().await?;
        self.base.wait_task_finish(&task).await?;

        // compare table data
        let (src_db_tbs, dst_db_tbs) = self.get_compare_db_tbs()?;
        assert!(
            self.compare_data_for_tbs_ignore_filtered(&src_db_tbs, &dst_db_tbs)
                .await?
        );

        // compare show create table
        let src_fetcher = MysqlStructCheckFetcher {
            conn_pool: self.src_conn_pool_mysql.as_mut().unwrap().clone(),
        };
        let meta_center_fetcher = MysqlStructCheckFetcher {
            conn_pool: self.meta_center_pool_mysql.as_mut().unwrap().clone(),
        };

        let filtered_db_tbs = self.get_filtered_db_tbs();
        for i in 0..src_db_tbs.len() {
            if filtered_db_tbs.contains(&src_db_tbs[i]) {
                continue;
            }
            let src_ddl_sql = src_fetcher
                .fetch_table(&src_db_tbs[i].0, &src_db_tbs[i].1)
                .await;
            let meta_center_ddl_sql = meta_center_fetcher
                .fetch_table(&dst_db_tbs[i].0, &dst_db_tbs[i].1)
                .await;
            assert_eq!(src_ddl_sql, meta_center_ddl_sql);
        }
        Ok(())
    }

    pub async fn run_dcl_test(&self, start_millis: u64, parse_millis: u64) -> anyhow::Result<()> {
        self.execute_prepare_sqls().await?;

        self.update_cdc_task_config(start_millis, parse_millis)
            .await?;
        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        self.execute_src_sqls(&self.base.src_test_sqls).await?;
        self.base.wait_task_finish(&task).await?;

        self.dcl_check_sql_execution().await?;

        Ok(())
    }

    pub async fn dcl_check_sql_execution(&self) -> anyhow::Result<()> {
        self.check_sql_execution("check_with_succeed.txt", true)
            .await?;
        self.check_sql_execution("check_with_failed.txt", false)
            .await?;
        Ok(())
    }

    async fn check_sql_execution(
        &self,
        filename: &str,
        expect_success: bool,
    ) -> anyhow::Result<()> {
        let file_path = format!("{}/{}", &self.base.test_dir, filename);
        if !BaseTestRunner::check_path_exists(&file_path) {
            return Ok(());
        }

        let lines = BaseTestRunner::load_file(&file_path);
        let mut mysql_pool: Option<Pool<MySql>> = None;
        let mut pg_pool: Option<Pool<Postgres>> = None;
        let is_mysql = self.dst_conn_pool_mysql.is_some();

        for line in lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if line.starts_with("--") {
                let parts: Vec<&str> = line[2..].trim().split('/').collect();
                let current_user = parts[0].trim().to_string();
                let current_pwd = parts[1].trim().to_string();

                if let Some(old_pool) = mysql_pool.take() {
                    old_pool.close().await;
                }
                if let Some(old_pool) = pg_pool.take() {
                    old_pool.close().await;
                }

                let url = &self.config.sinker_basic.url;
                let conn_str = url.replace(
                    &url[url.find("://").unwrap() + 3..url.find('@').unwrap()],
                    &format!("{}:{}", current_user, current_pwd),
                );

                if is_mysql {
                    let pool_connect = Pool::<MySql>::connect(&conn_str).await;
                    match pool_connect {
                        Ok(p) => {
                            mysql_pool = Some(p);
                        }
                        Err(e) => {
                            if expect_success {
                                assert!(
                                    false,
                                    "MySQL pool connect failed: {} with user={}, password={}, but expect success",
                                    e, current_user, current_pwd
                                );
                            }
                        }
                    }
                } else {
                    let pool_connect = Pool::<Postgres>::connect(&conn_str).await;
                    match pool_connect {
                        Ok(p) => {
                            pg_pool = Some(p);
                        }
                        Err(e) => {
                            if expect_success {
                                assert!(
                                    false,
                                    "PostgreSQL pool connect failed: {} with user={}, password={}, but expect success",
                                    e, current_user, current_pwd
                                );
                            }
                        }
                    }
                }
                continue;
            }

            if is_mysql {
                if let Some(ref pool) = mysql_pool {
                    let query = sqlx::query(line);
                    let result = query.execute(pool).await;

                    if expect_success {
                        assert!(
                            result.is_ok(),
                            "Expected success but got error: {:?}",
                            result.err()
                        );
                    } else {
                        assert!(
                            result.is_err(),
                            "Expected error but got success, sql: {}",
                            line
                        );
                    }
                }
            } else {
                if let Some(ref pool) = pg_pool {
                    let query = sqlx::query(line);
                    let result = query.execute(pool).await;

                    if expect_success {
                        assert!(
                            result.is_ok(),
                            "Expected success but got error: {:?}",
                            result.err()
                        );
                    } else {
                        assert!(
                            result.is_err(),
                            "Expected error but got success, sql: {}",
                            line
                        );
                    }
                }
            }
        }

        if let Some(pool) = mysql_pool {
            pool.close().await;
        }
        if let Some(pool) = pg_pool {
            pool.close().await;
        }

        Ok(())
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> anyhow::Result<()> {
        // prepare src and dst tables
        self.execute_prepare_sqls().await?;

        // start task
        let task = self.spawn_cdc_task(start_millis, parse_millis).await?;

        self.execute_test_sqls_and_compare(parse_millis).await?;

        self.base.wait_task_finish(&task).await
    }

    pub async fn run_heartbeat_test(
        &self,
        _start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        let config = TaskConfig::new(&self.base.task_config_file).unwrap();
        let (heartbeat_tb, db_type) = match config.extractor {
            ExtractorConfig::PgCdc { heartbeat_tb, .. } => (heartbeat_tb.clone(), DbType::Pg),
            ExtractorConfig::MysqlCdc { heartbeat_tb, .. } => (heartbeat_tb.clone(), DbType::Mysql),
            _ => (String::new(), DbType::Mysql),
        };

        let tokens = ConfigTokenParser::parse(
            &heartbeat_tb,
            &['.'],
            &TokenEscapePair::from_char_pairs(SqlUtil::get_escape_pairs(&db_type)),
        );
        let db_tb = (tokens[0].clone(), tokens[1].clone());

        self.execute_prepare_sqls().await?;

        // start task
        self.update_cdc_task_config(0, parse_millis).await?;
        let task = self.base.spawn_task().await?;
        self.base.wait_task_finish(&task).await.unwrap();

        let src_data = self.fetch_data(&db_tb, SRC).await?;
        assert_eq!(src_data.len(), 1);
        Ok(())
    }

    pub async fn update_cdc_task_config(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        let duration = Duration::try_milliseconds((start_millis + parse_millis) as i64).unwrap();
        let end_time_utc = (Utc::now() + duration).format(UTC_FORMAT).to_string();
        let (binlog_file, binlog_position) = self.fetch_mysql_binlog_position().await.unwrap();
        let binlog_position = binlog_position.to_string();
        let update_configs = vec![
            ("extractor", "end_time_utc", end_time_utc.as_str()),
            ("extractor", "binlog_filename", binlog_file.as_str()),
            ("extractor", "binlog_position", binlog_position.as_str()),
        ];
        TestConfigUtil::update_task_config_2(
            &self.base.task_config_file,
            &self.base.task_config_file,
            &update_configs,
        );
        Ok(())
    }

    pub async fn spawn_cdc_task(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<JoinHandle<()>> {
        // start task
        let total_parse_millis = self.get_total_parse_millis(parse_millis);
        self.update_cdc_task_config(start_millis, total_parse_millis)
            .await?;
        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;
        Ok(task)
    }

    fn get_total_parse_millis(&self, parse_millis: u64) -> u64 {
        let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
            Self::split_dml_sqls(&self.base.src_test_sqls);
        // parse_millis * 2 for: time to parse binlog + time to compare data
        (!src_insert_sqls.is_empty() as u64
            + !src_update_sqls.is_empty() as u64
            + !src_delete_sqls.is_empty() as u64)
            * parse_millis
            * 2
    }

    pub async fn execute_test_sqls_and_compare(&self, parse_millis: u64) -> anyhow::Result<()> {
        // load dml sqls
        let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
            Self::split_dml_sqls(&self.base.src_test_sqls);

        let (src_db_tbs, dst_db_tbs) = self.get_compare_db_tbs()?;

        // insert src data
        if !src_insert_sqls.is_empty() {
            self.execute_src_sqls(&src_insert_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?;
        }

        // update src data
        if !src_update_sqls.is_empty() {
            self.execute_src_sqls(&src_update_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?;
        }

        // delete src data
        if !src_delete_sqls.is_empty() {
            self.execute_src_sqls(&src_delete_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_data_for_tbs(&src_db_tbs, &dst_db_tbs).await?;
        }

        Ok(())
    }

    pub fn split_dml_sqls(dml_sqls: &[String]) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut insert_sqls = Vec::new();
        let mut update_sqls = Vec::new();
        let mut delete_sqls = Vec::new();

        for sql in dml_sqls.iter() {
            if sql.to_lowercase().starts_with("insert") {
                insert_sqls.push(sql.clone());
            } else if sql.to_lowercase().starts_with("update") {
                update_sqls.push(sql.clone());
            } else {
                delete_sqls.push(sql.clone());
            }
        }
        (insert_sqls, update_sqls, delete_sqls)
    }

    pub async fn execute_test_sqls(&self) -> anyhow::Result<()> {
        self.execute_src_sqls(&self.base.src_test_sqls).await?;
        self.execute_dst_sqls(&self.base.dst_test_sqls).await
    }

    pub async fn execute_prepare_sqls(&self) -> anyhow::Result<()> {
        self.execute_src_sqls(&self.base.src_prepare_sqls).await?;
        self.execute_dst_sqls(&self.base.dst_prepare_sqls).await?;
        self.execute_meta_center_prepare_sqls(&self.base.meta_center_prepare_sqls)
            .await?;

        // migrate database/table structures to target if needed
        if !self.base.struct_task_config_file.is_empty() {
            TaskRunner::new(&self.base.struct_task_config_file)?
                .start_task(BaseTestRunner::get_enable_log4rs())
                .await?;
        }
        Ok(())
    }

    pub async fn execute_meta_center_prepare_sqls(&self, sqls: &Vec<String>) -> anyhow::Result<()> {
        if let Some(pool) = &self.meta_center_pool_mysql {
            RdbUtil::execute_sqls_mysql(pool, sqls).await?
        }
        Ok(())
    }

    pub async fn execute_clean_sqls(&self) -> anyhow::Result<()> {
        self.execute_src_sqls(&self.base.src_clean_sqls).await?;
        self.execute_dst_sqls(&self.base.dst_clean_sqls).await
    }

    pub async fn execute_src_sqls(&self, sqls: &Vec<String>) -> anyhow::Result<()> {
        if let Some(pool) = &self.src_conn_pool_mysql {
            RdbUtil::execute_sqls_mysql(pool, sqls).await?;
        }
        if let Some(pool) = &self.src_conn_pool_pg {
            RdbUtil::execute_sqls_pg(pool, sqls).await?;
        }
        Ok(())
    }

    pub async fn execute_dst_sqls(&self, sqls: &Vec<String>) -> anyhow::Result<()> {
        if let Some(pool) = &self.dst_conn_pool_mysql {
            RdbUtil::execute_sqls_mysql(pool, sqls).await?;
        }
        if let Some(pool) = &self.dst_conn_pool_pg {
            RdbUtil::execute_sqls_pg(pool, sqls).await?;
        }
        Ok(())
    }

    pub async fn compare_data_for_tbs_ignore_filtered(
        &self,
        src_db_tbs: &[(String, String)],
        dst_db_tbs: &[(String, String)],
    ) -> anyhow::Result<bool> {
        let filtered_db_tbs = self.get_filtered_db_tbs();
        for i in 0..src_db_tbs.len() {
            if filtered_db_tbs.contains(&src_db_tbs[i]) {
                continue;
            }
            assert!(self.compare_tb_data(&src_db_tbs[i], &dst_db_tbs[i]).await?)
        }
        Ok(true)
    }

    pub async fn compare_data_for_tbs(
        &self,
        src_db_tbs: &[(String, String)],
        dst_db_tbs: &[(String, String)],
    ) -> anyhow::Result<bool> {
        let filtered_db_tbs = self.get_filtered_db_tbs();
        for i in 0..src_db_tbs.len() {
            if filtered_db_tbs.contains(&src_db_tbs[i]) {
                let dst_data = self.fetch_data(&dst_db_tbs[i], DST).await?;
                if !dst_data.is_empty() {
                    println!("tb: {:?} is filtered but dst is not empty", dst_db_tbs[i]);
                    panic!()
                }
            } else {
                assert!(self.compare_tb_data(&src_db_tbs[i], &dst_db_tbs[i]).await?)
            }
        }
        Ok(true)
    }

    async fn compare_tb_data(
        &self,
        src_db_tb: &(String, String),
        dst_db_tb: &(String, String),
    ) -> anyhow::Result<bool> {
        let src_data = self.fetch_data(src_db_tb, SRC).await?;
        let dst_data = self.fetch_data(dst_db_tb, DST).await?;
        println!(
            "comparing row data for src_tb: {:?}, dst_tb: {:?}, src_data count: {}, dst_data count: {}",
            src_db_tb,
            dst_db_tb,
            src_data.len(),
            dst_data.len(),
        );

        if !self.compare_row_data(&src_data, &dst_data, src_db_tb) {
            println!(
                "compare tb data failed, src_tb: {:?}, dst_tb: {:?}",
                src_db_tb, dst_db_tb,
            );
            return Ok(false);
        }
        Ok(true)
    }

    fn compare_row_data(
        &self,
        src_data: &[RowData],
        dst_data: &[RowData],
        src_db_tb: &(String, String),
    ) -> bool {
        assert_eq!(src_data.len(), dst_data.len());
        let src_db_type = self.get_db_type(SRC);
        let dst_db_type = self.get_db_type(DST);

        // router: col_map
        let col_map = self.router.get_col_map(&src_db_tb.0, &src_db_tb.1);
        // filter: ignore_cols
        let ignore_cols = self.filter.get_ignore_cols(&src_db_tb.0, &src_db_tb.1);

        for i in 0..src_data.len() {
            let src_col_values = src_data[i].after.as_ref().unwrap();
            let dst_col_values = dst_data[i].after.as_ref().unwrap();

            for (src_col, src_col_value) in src_col_values {
                let dst_col = if let Some(col_map) = col_map {
                    col_map.get(src_col).unwrap_or(src_col)
                } else {
                    src_col
                };

                let dst_col_value = dst_col_values.get(dst_col).unwrap();

                // ignored cols were NOT synced to target
                if ignore_cols.is_some_and(|cols| cols.contains(src_col)) {
                    assert_eq!(*dst_col_value, ColValue::None);
                    continue;
                }

                // TODO
                // issue: https://github.com/apecloud/foxlake/issues/2108
                // sqlx will execute: "SET time_zone='+00:00',NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;"
                // to initialize each connection.
                // but it doesn't work on Foxlake
                if matches!(self.base.get_config().sinker, SinkerConfig::Foxlake { .. })
                    && matches!(dst_col_value, ColValue::Timestamp(..))
                {
                    continue;
                }

                println!(
                    "row index: {}, col: {}, src_col_value: {:?}, dst_col_value: {:?}",
                    i, src_col, src_col_value, dst_col_value
                );

                if Self::compare_col_value(src_col_value, dst_col_value, &src_db_type, &dst_db_type)
                {
                    continue;
                }
                return false;
            }
        }
        true
    }

    fn compare_col_value(
        src_col_value: &ColValue,
        dst_col_value: &ColValue,
        src_db_type: &DbType,
        dst_db_type: &DbType,
    ) -> bool {
        if src_col_value == dst_col_value {
            return true;
        }

        if src_col_value.is_nan() && dst_col_value.is_nan() {
            return true;
        }

        if src_db_type == dst_db_type {
            return false;
        }

        if src_col_value.to_option_string() == dst_col_value.to_option_string() {
            return true;
        }

        // different databases support different column types,
        // for example: we use Year in mysql, but INT in StarRocks,
        // so try to compare after both converted to string.
        match src_col_value {
            // mysql 00:00:00 == foxlake 00:00:00.000000
            ColValue::Time(_) => {
                DtNaiveTime::from_str(&src_col_value.to_string()).unwrap()
                    == DtNaiveTime::from_str(&dst_col_value.to_string()).unwrap()
            }
            ColValue::Date(_) => {
                TimeUtil::date_from_str(&src_col_value.to_string()).unwrap()
                    == TimeUtil::date_from_str(&dst_col_value.to_string()).unwrap()
            }
            ColValue::DateTime(_) => {
                TimeUtil::datetime_from_utc_str(&src_col_value.to_string()).unwrap()
                    == TimeUtil::datetime_from_utc_str(&dst_col_value.to_string()).unwrap()
            }
            ColValue::Timestamp(_) => {
                TimeUtil::datetime_from_utc_str(&src_col_value.to_string()).unwrap()
                    == TimeUtil::datetime_from_utc_str(&dst_col_value.to_string()).unwrap()
            }
            ColValue::Json2(src_v) => match dst_col_value {
                // for snapshot/cdc task: mysql -> starrocks
                // in src mysql, json_test.f_1 type == JSON -> ColValue::Json2
                // in dst starrocks, json_test.f_1 type == STRING/VARCHAR/CHAR -> ColValue::String
                ColValue::Json2(dst_v) | ColValue::String(dst_v) => {
                    serde_json::Value::from_str(src_v).unwrap()
                        == serde_json::Value::from_str(dst_v).unwrap()
                }
                // for snapshot task: mysql -> starrocks
                // in src mysql, json_test.f_1 type == JSON
                // INSERT INTO json_test VALUES (11, NULL),(12, 'NULL');
                // +-----+------+
                // | f_0 | f_1  |
                // +-----+------+
                // |  11 | NULL |  ->  ColValue::None  ->  serde_json::Value::Null
                // |  12 | null |  ->  ColValue::Json2("null")  ->  serde_json::Value::Null
                // +-----+------+
                // in dst starrocks, json_test.f_1 type == JSON,
                // +-----+------+
                // | f_0 | f_1  |
                // +-----+------+
                // |  11 | NULL |  ->  ColValue::None  ->  serde_json::Value::Null
                // |  12 | NULL |  ->  ColValue::None  ->  serde_json::Value::Null
                ColValue::None => serde_json::Value::from_str(src_v).unwrap().is_null(),
                _ => false,
            },
            ColValue::Bit(src_v) => match dst_col_value {
                // in dst starrocks, BIT is stored as BIGINT
                ColValue::LongLong(dst_v) => *src_v == *dst_v as u64,
                _ => false,
            },
            ColValue::Decimal(src_v) => match dst_col_value {
                // src_v = 30.00000, dst_v = 30.000000000
                ColValue::Decimal(dst_v) => {
                    BigDecimal::from_str(src_v) == BigDecimal::from_str(dst_v)
                }
                _ => false,
            },
            ColValue::Bool(src_v) => match dst_col_value {
                // src_v, postgres, boolean
                // dst_v, starrocks, boolean == tiny(1)
                ColValue::Tiny(dst_v) => *src_v == (*dst_v == 1),
                _ => false,
            },

            _ => src_col_value.to_option_string() == dst_col_value.to_option_string(),
        }
    }

    pub async fn fetch_data(
        &self,
        db_tb: &(String, String),
        from: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        self.fetch_data_with_condition(db_tb, from, "").await
    }

    pub async fn fetch_data_with_condition(
        &self,
        db_tb: &(String, String),
        from: &str,
        condition: &str,
    ) -> anyhow::Result<Vec<RowData>> {
        let where_sql = self.get_where_sql(&db_tb.0, &db_tb.1, condition);
        let (conn_pool_mysql, conn_pool_pg) = self.get_conn_pool(from);
        let data = if let Some(pool) = conn_pool_mysql {
            let db_type = self.get_db_type(from);
            RdbUtil::fetch_data_mysql_compatible(pool, None, db_tb, &db_type, &where_sql).await?
        } else if let Some(pool) = conn_pool_pg {
            RdbUtil::fetch_data_pg(pool, None, db_tb, &where_sql).await?
        } else {
            Vec::new()
        };
        Ok(data)
    }

    pub fn parse_full_tb_name(full_tb_name: &str, db_type: &DbType) -> (String, String) {
        let escape_pairs = SqlUtil::get_escape_pairs(db_type);
        let tokens = ConfigTokenParser::parse(
            full_tb_name,
            &['.'],
            &TokenEscapePair::from_char_pairs(escape_pairs.clone()),
        );
        let (db, tb) = if tokens.len() > 1 {
            (tokens[0].to_string(), tokens[1].to_string())
        } else {
            (String::new(), full_tb_name.to_string())
        };

        (
            SqlUtil::unescape(&db, &escape_pairs[0]),
            SqlUtil::unescape(&tb, &escape_pairs[0]),
        )
    }

    /// get compare tbs
    #[allow(clippy::type_complexity)]
    pub fn get_compare_db_tbs(
        &self,
    ) -> anyhow::Result<(Vec<(String, String)>, Vec<(String, String)>)> {
        let db_type = self.get_db_type(SRC);
        let mut src_db_tbs =
            Self::get_compare_db_tbs_from_sqls(&db_type, &self.base.src_prepare_sqls)?;
        // since tables may be created/dropped in src_test.sql for ddl tests,
        // we also need to parse src_test.sql.
        src_db_tbs.extend_from_slice(&Self::get_compare_db_tbs_from_sqls(
            &db_type,
            &self.base.src_test_sqls,
        )?);

        let mut dst_db_tbs = vec![];
        for (db, tb) in src_db_tbs.iter() {
            let (dst_db, dst_tb) = self.router.get_tb_map(db, tb);
            dst_db_tbs.push((dst_db.into(), dst_tb.into()));
        }

        Ok((src_db_tbs, dst_db_tbs))
    }

    pub fn get_compare_db_tbs_from_sqls(
        db_type: &DbType,
        sqls: &[String],
    ) -> anyhow::Result<Vec<(String, String)>> {
        let mut db_tbs = vec![];
        let parser = DdlParser::new(db_type.to_owned());

        for sql in sqls.iter() {
            let sql = sql.trim().to_string();
            let tokens: Vec<&str> = sql.split(" ").collect();
            if tokens[0].trim().to_lowercase() != "create"
                || tokens[1].trim().to_lowercase() != "table"
            {
                continue;
            }

            let ddl = parser.parse(&sql).unwrap().unwrap();
            if ddl.ddl_type == DdlType::CreateTable {
                let (mut db, tb) = ddl.get_schema_tb();
                if db.is_empty() {
                    db = PUBLIC.to_string();
                }
                db_tbs.push((db, tb));
            }
        }

        Ok(db_tbs)
    }

    fn get_filtered_db_tbs(&self) -> HashSet<(String, String)> {
        let mut filtered_db_tbs = HashSet::new();
        let db_type = &self.get_db_type(SRC);
        let delimiters = vec!['.'];
        let escape_pairs = SqlUtil::get_escape_pairs(db_type);
        let filtered_tbs_file = format!("{}/filtered_tbs.txt", &self.base.test_dir);

        if BaseTestRunner::check_path_exists(&filtered_tbs_file) {
            let lines = BaseTestRunner::load_file(&filtered_tbs_file);
            for line in lines.iter() {
                let db_tb =
                    ConfigTokenParser::parse_config(line, db_type, &delimiters, None).unwrap();
                if db_tb.len() == 2 {
                    let db = SqlUtil::unescape(&db_tb[0], &escape_pairs[0]);
                    let tb = SqlUtil::unescape(&db_tb[1], &escape_pairs[0]);
                    filtered_db_tbs.insert((db, tb));
                }
            }
        }
        filtered_db_tbs
    }

    pub async fn get_tb_cols(
        &self,
        db_tb: &(String, String),
        from: &str,
    ) -> anyhow::Result<Vec<String>> {
        let (conn_pool_mysql, conn_pool_pg) = self.get_conn_pool(from);
        let cols = if let Some(conn_pool) = conn_pool_mysql {
            let tb_meta = RdbUtil::get_tb_meta_mysql(conn_pool, db_tb).await?;
            tb_meta.basic.cols.clone()
        } else if let Some(conn_pool) = conn_pool_pg {
            let tb_meta = RdbUtil::get_tb_meta_pg(conn_pool, db_tb).await?;
            tb_meta.basic.cols.clone()
        } else {
            vec![]
        };
        Ok(cols)
    }

    fn get_conn_pool(&self, from: &str) -> (&Option<Pool<MySql>>, &Option<Pool<Postgres>>) {
        if from == SRC {
            (&self.src_conn_pool_mysql, &self.src_conn_pool_pg)
        } else {
            (&self.dst_conn_pool_mysql, &self.dst_conn_pool_pg)
        }
    }

    pub fn get_db_type(&self, from: &str) -> DbType {
        let config = TaskConfig::new(&self.base.task_config_file).unwrap();
        if from == SRC {
            config.extractor_basic.db_type
        } else {
            config.sinker_basic.db_type
        }
    }

    async fn fetch_mysql_binlog_position(&self) -> anyhow::Result<(String, u32)> {
        if let Some(pool) = &self.src_conn_pool_mysql {
            let row = query("show master status").fetch_one(pool).await?;
            let binlog_file: String = row.try_get(0)?;
            let binlog_position = row.try_get(1)?;
            Ok((binlog_file, binlog_position))
        } else {
            Ok((String::new(), 0))
        }
    }

    pub fn get_where_sql(&self, schema: &str, tb: &str, condition: &str) -> String {
        let mut res: String = String::new();
        if let Some(where_condition) = self.filter.get_where_condition(schema, tb) {
            res = format!("WHERE {}", where_condition);
        }

        if condition.is_empty() {
            return res;
        }

        if res.is_empty() {
            format!("WHERE {}", condition)
        } else {
            format!("{} AND {}", res, condition)
        }
    }
}
