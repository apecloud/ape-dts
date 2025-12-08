use clickhouse::{Client, Row};
use dt_common::{meta::col_value::ColValue, utils::time_util::TimeUtil};
use dt_task::task_runner::TaskRunner;
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::Url;

use super::{
    rdb_struct_test_runner::RdbStructTestRunner,
    rdb_test_runner::{RdbTestRunner, SRC},
};

pub struct RdbClickHouseTestRunner {
    rdb_test_runner: RdbTestRunner,
    rdb_struct_test_runner: RdbStructTestRunner,
    client: Client,
}

impl RdbClickHouseTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let rdb_test_runner = RdbTestRunner::new(relative_test_dir).await?;
        let rdb_struct_test_runner = RdbStructTestRunner::new(relative_test_dir).await?;

        let url_info = Url::parse(&rdb_test_runner.base.get_config().sinker_basic.url)?;
        let host = url_info.host_str().unwrap().to_string();
        let port = format!("{}", url_info.port().unwrap());

        let client = clickhouse::Client::default()
            .with_url(format!("http://{}:{}", host, port))
            .with_user(url_info.username())
            .with_password(url_info.password().unwrap_or(""));
        Ok(Self {
            rdb_test_runner,
            rdb_struct_test_runner,
            client,
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.rdb_test_runner.close().await
    }

    pub async fn run_struct_test(&self) -> anyhow::Result<()> {
        self.prepare_task().await?;
        self.rdb_test_runner.base.start_task().await?;

        let expect_ddl_sqls = self.rdb_struct_test_runner.load_expect_ddl_sqls().await;
        let (_, dst_db_tbs) = self.rdb_test_runner.get_compare_db_tbs().unwrap();
        for (db, tb) in dst_db_tbs.iter() {
            let sql = format!("SHOW CREATE TABLE `{}`.`{}`", db, tb);

            let mut cursor = self.client.query(&sql).fetch()?;
            let dst_ddl_sql: String = cursor.next().await?.unwrap();
            println!("dst_ddl_sql: {}\n", dst_ddl_sql);

            let key = format!("{}.{}", db, tb);
            let expect_ddl_sql = expect_ddl_sqls.get(&key).unwrap().to_owned();
            println!("expect_ddl_sql: {}\n", expect_ddl_sql);

            assert_eq!(dst_ddl_sql, expect_ddl_sql);
        }
        Ok(())
    }

    pub async fn run_snapshot_test<'a, T: Row + Serialize + for<'b> Deserialize<'b>>(
        &self,
    ) -> anyhow::Result<()> {
        self.prepare_task().await?;
        self.rdb_test_runner.execute_test_sqls().await?;
        self.rdb_test_runner.base.start_task().await?;
        self.compare_data_for_tbs::<T>().await
    }

    pub async fn run_cdc_test<'a, T: Row + Serialize + for<'b> Deserialize<'b>>(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        let runner = &self.rdb_test_runner;
        let basic = &runner.base;

        self.prepare_task().await?;

        // start task
        let task = runner.spawn_cdc_task(start_millis, parse_millis).await?;

        // load dml sqls
        let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
            RdbTestRunner::split_dml_sqls(&basic.src_test_sqls);

        // insert src data
        if !src_insert_sqls.is_empty() {
            runner.execute_src_sqls(&src_insert_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_data_for_tbs::<T>().await?;
        }

        // update src data
        if !src_update_sqls.is_empty() {
            runner.execute_src_sqls(&src_update_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_data_for_tbs::<T>().await?;
        }

        // delete src data
        if !src_delete_sqls.is_empty() {
            runner.execute_src_sqls(&src_delete_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_data_for_tbs::<T>().await?;
        }

        basic.wait_task_finish(&task).await
    }

    async fn prepare_task(&self) -> anyhow::Result<()> {
        let basic = &self.rdb_test_runner.base;
        self.rdb_test_runner
            .execute_src_sqls(&basic.src_prepare_sqls)
            .await?;
        for sql in basic.dst_prepare_sqls.iter() {
            self.client.query(sql).execute().await?;
        }

        // migrate database/table structures to target if needed
        if !basic.struct_task_config_file.is_empty() {
            TaskRunner::new(&basic.struct_task_config_file)?
                .start_task()
                .await?;
        }
        Ok(())
    }

    pub async fn compare_data_for_tbs<'a, T: Row + Serialize + for<'b> Deserialize<'b>>(
        &self,
    ) -> anyhow::Result<()> {
        let (src_db_tbs, dst_db_tbs) = self.rdb_test_runner.get_compare_db_tbs()?;
        for i in 0..src_db_tbs.len() {
            self.compare_tb_data::<T>(&src_db_tbs[i], &dst_db_tbs[i])
                .await?;
        }
        Ok(())
    }

    async fn compare_tb_data<'a, T: Row + Serialize + for<'b> Deserialize<'b>>(
        &self,
        src_db_tb: &(String, String),
        dst_db_tb: &(String, String),
    ) -> anyhow::Result<()> {
        let src_data = self.rdb_test_runner.fetch_data(src_db_tb, SRC).await?;

        let sql = format!("OPTIMIZE TABLE {}.{} FINAL", dst_db_tb.0, dst_db_tb.1);
        self.client.query(&sql).execute().await?;
        // Must use `SELECT ?fields` instead of `SELECT *`, otherwise the results is unpredictable.
        let src_tb_cols = self.rdb_test_runner.get_tb_cols(src_db_tb, SRC).await?;
        let sql = format!(
            "SELECT ?fields FROM {}.{} WHERE _ape_dts_is_deleted=0 ORDER BY {}",
            dst_db_tb.0, dst_db_tb.1, src_tb_cols[0]
        );
        let dst_data = self.client.query(&sql).fetch_all::<T>().await?;

        println!(
            "comparing row data for src_tb: {:?}, dst_tb: {:?}, src_data count: {}, dst_data count: {}",
            src_db_tb,
            dst_db_tb,
            src_data.len(),
            dst_data.len(),
        );
        assert_eq!(src_data.len(), dst_data.len());

        for i in 0..dst_data.len() {
            let src_row = &src_data[i];
            let dst_json_row = json!(dst_data[i]);
            let src_after = src_row.require_after()?;

            for (col, dst_col_value) in dst_json_row.as_object().unwrap() {
                let src_col_value = src_after.get(col);
                println!(
                    "row index: {}, col: {}, src_col_value: {:?}, dst_col_value: {}",
                    i, col, src_col_value, dst_col_value
                );
                self.compare_col_value(&src_col_value, dst_col_value);
            }
        }
        Ok(())
    }

    fn compare_col_value(
        &self,
        src_col_value: &Option<&ColValue>,
        dst_col_value: &serde_json::Value,
    ) {
        if src_col_value.is_none() || *src_col_value.unwrap() == ColValue::None {
            assert!(dst_col_value.is_null());
        } else {
            match src_col_value.unwrap() {
                ColValue::Blob(src_col_value_bytes) => {
                    if let Some(dst_col_value_str) = dst_col_value.as_str() {
                        if dst_col_value_str.starts_with("0x") {
                            let dst_col_value_bytes =
                                hex::decode(dst_col_value_str.trim_start_matches(r#"0x"#)).unwrap();
                            assert_eq!(*src_col_value_bytes, dst_col_value_bytes);
                        } else {
                            panic!("dst_col_value should be a binary string");
                        }
                    } else {
                        panic!("dst_col_value should be a binary string");
                    }
                }

                _ => {
                    let src_col_value_str = src_col_value.unwrap().to_string();
                    if let Some(dst_col_value_str) = dst_col_value.as_str() {
                        assert_eq!(src_col_value_str, dst_col_value_str);
                    } else {
                        assert_eq!(src_col_value_str, dst_col_value.to_string());
                    }
                }
            }
        }
    }
}
