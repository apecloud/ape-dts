use anyhow::bail;
use dt_common::config::config_enums::DbType;
use dt_common::utils::redis_util::RedisUtil;
use dt_common::{
    config::{sinker_config::SinkerConfig, task_config::TaskConfig},
    error::Error,
    utils::time_util::TimeUtil,
};
use dt_task::task_util::TaskUtil;
use redis::{Connection, Value};
use sqlx::{MySql, Pool};

use crate::test_runner::rdb_util::RdbUtil;

use super::{
    base_test_runner::BaseTestRunner, rdb_test_runner::RdbTestRunner,
    redis_test_util::RedisTestUtil,
};

pub struct RdbRedisTestRunner {
    pub base: BaseTestRunner,
    mysql_conn_pool: Option<Pool<MySql>>,
    redis_conn: Connection,
    redis_util: RedisTestUtil,
}

impl RdbRedisTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();
        let config = TaskConfig::new(&base.task_config_file).unwrap();

        let mysql_conn_pool = Some(
            TaskUtil::create_mysql_conn_pool(&config.extractor_basic.url, 1, false, None).await?,
        );
        let redis_conn = match config.sinker {
            SinkerConfig::Redis { url, .. } => RedisUtil::create_redis_conn(&url).await.unwrap(),
            _ => {
                bail! {Error::ConfigError("unsupported sinker config".into())};
            }
        };
        let redis_util = RedisTestUtil::new_default();

        Ok(Self {
            base,
            mysql_conn_pool,
            redis_conn,
            redis_util,
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        if let Some(pool) = &self.mysql_conn_pool {
            pool.close().await;
        }
        Ok(())
    }

    pub async fn run_snapshot_test(&mut self) -> anyhow::Result<()> {
        // src prepare
        self.execute_sqls(&self.base.src_prepare_sqls).await?;
        // dst prepare
        self.redis_util
            .execute_cmds(&mut self.redis_conn, &self.base.dst_prepare_sqls.clone());

        self.execute_sqls(&self.base.src_test_sqls).await?;
        self.base.start_task().await?;

        let db_tbs = RdbTestRunner::get_compare_db_tbs_from_sqls(
            &DbType::Mysql,
            &self.base.src_prepare_sqls,
        )?;
        self.compare_data_for_tbs(&db_tbs).await;
        Ok(())
    }

    pub async fn run_cdc_test(
        &mut self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        self.execute_sqls(&self.base.src_prepare_sqls).await?;
        self.redis_util
            .execute_cmds(&mut self.redis_conn, &self.base.dst_prepare_sqls.clone());

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        // src dml
        let mut src_insert_sqls = Vec::new();
        let mut src_update_sqls = Vec::new();
        let mut src_delete_sqls = Vec::new();
        for sql in self.base.src_test_sqls.iter() {
            if sql.to_lowercase().starts_with("insert") {
                src_insert_sqls.push(sql.clone());
            } else if sql.to_lowercase().starts_with("update") {
                src_update_sqls.push(sql.clone());
            } else {
                src_delete_sqls.push(sql.clone());
            }
        }

        let db_tbs = RdbTestRunner::get_compare_db_tbs_from_sqls(
            &DbType::Mysql,
            &self.base.src_prepare_sqls,
        )?;

        // insert
        self.execute_sqls(&src_insert_sqls).await?;
        TimeUtil::sleep_millis(parse_millis).await;
        self.compare_data_for_tbs(&db_tbs).await;

        // update
        self.execute_sqls(&src_update_sqls).await?;
        TimeUtil::sleep_millis(parse_millis).await;
        self.compare_data_for_tbs(&db_tbs).await;

        // delete
        self.execute_sqls(&src_delete_sqls).await?;
        TimeUtil::sleep_millis(parse_millis).await;
        self.compare_data_for_tbs(&db_tbs).await;

        self.base.abort_task(&task).await
    }

    async fn execute_sqls(&self, sqls: &Vec<String>) -> anyhow::Result<()> {
        if let Some(conn_pool) = &self.mysql_conn_pool {
            RdbUtil::execute_sqls_mysql(conn_pool, sqls).await?;
        }
        Ok(())
    }

    pub async fn compare_data_for_tbs(&mut self, db_tbs: &Vec<(String, String)>) {
        for db_tb in db_tbs {
            assert!(self.compare_tb_data(db_tb).await.unwrap())
        }
    }

    async fn compare_tb_data(&mut self, db_tb: &(String, String)) -> anyhow::Result<bool> {
        if let Some(conn_pool) = &self.mysql_conn_pool {
            // mysql data
            let tb_meta = RdbUtil::get_tb_meta_mysql(conn_pool, db_tb).await?;
            if tb_meta.basic.order_col.is_none() {
                return Ok(true);
            }
            let key_col = tb_meta.basic.order_col.as_ref().unwrap();

            let results = RdbUtil::fetch_data_mysql(conn_pool, None, db_tb, "").await?;
            for row_data in results {
                let key = row_data
                    .after
                    .as_ref()
                    .unwrap()
                    .get(key_col)
                    .unwrap()
                    .to_option_string()
                    .unwrap();

                // redis data
                let redis_k = format!("{}.{}.{}", db_tb.0, db_tb.1, key);
                let redis_kvs = self
                    .redis_util
                    .get_hash_entry(&mut self.redis_conn, &redis_k);

                for (col, db_v) in row_data.after.unwrap() {
                    // check redis key exists
                    assert!(redis_kvs.contains_key(&col));
                    // check redis value = db value
                    if let Value::BulkString(v) = redis_kvs.get(&col).unwrap() {
                        let redis_v_str = String::from_utf8(v.clone()).unwrap();
                        if let Some(db_v_str) = db_v.to_option_string() {
                            if redis_v_str != db_v_str {
                                println!("compare db: {}, tb: {}, col: {}", db_tb.0, db_tb.1, col);
                            }
                            assert_eq!(redis_v_str, db_v_str)
                        } else {
                            assert_eq!(redis_v_str, "")
                        }
                    }
                }
            }
        }
        Ok(true)
    }
}
