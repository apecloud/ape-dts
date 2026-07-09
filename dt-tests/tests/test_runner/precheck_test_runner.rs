use std::collections::{HashMap, HashSet};

use dt_common::config::{config_enums::DbType, task_config::TaskConfig};

use dt_precheck::{
    builder::prechecker_builder::PrecheckerBuilder, config::task_config::PrecheckTaskConfig,
    meta::check_result::CheckResult,
};

use super::{
    base_test_runner::BaseTestRunner, mongo_test_runner::MongoTestRunner,
    rdb_test_runner::RdbTestRunner, redis_test_runner::RedisTestRunner,
};

pub struct PrecheckTestRunner {
    pub db_type: DbType,
    checker_connector: PrecheckerBuilder,
    test_dir: String,
}

impl PrecheckTestRunner {
    pub async fn new(test_dir: &str) -> anyhow::Result<Self> {
        let base = BaseTestRunner::new(test_dir).await.unwrap();
        let task_config = TaskConfig::new(&base.task_config_file).unwrap();
        let precheck_config = PrecheckTaskConfig::new(&base.task_config_file).unwrap();
        let checker_connector =
            PrecheckerBuilder::build(precheck_config.precheck.clone(), task_config.clone());

        Ok(Self {
            checker_connector,
            db_type: task_config.extractor_basic.db_type.clone(),
            test_dir: test_dir.to_owned(),
        })
    }

    pub async fn run_check(
        &self,
        ignore_check_items: &HashSet<String>,
        src_expected_results: &HashMap<String, bool>,
        dst_expected_results: &HashMap<String, bool>,
    ) -> anyhow::Result<()> {
        self.before_check().await?;

        let results: Vec<anyhow::Result<dt_precheck::meta::check_result::CheckResult>> =
            self.checker_connector.check().await?;

        let compare = |result: &CheckResult, expected_results: &HashMap<String, bool>| {
            if let Some(expected) = expected_results.get(&result.check_type_name) {
                if &result.is_validate != expected {
                    println!(
                        "{} checker: {} expect {}, found {}, dismatch",
                        if result.is_source { "source" } else { "target" },
                        result.check_type_name,
                        expected,
                        result.is_validate
                    );
                    assert!(false)
                }
            } else {
                // by default, is_validate == true
                if !&result.is_validate {
                    println!(
                        "{} checker: {} failed: {}",
                        if result.is_source { "source" } else { "target" },
                        result.check_type_name,
                        result.error_msg
                    );
                    assert!(false)
                }
            }
        };

        for i in results.iter() {
            let result = i.as_ref().unwrap();
            if ignore_check_items.contains(&result.check_type_name) {
                continue;
            }

            println!(
                "comparing precheck result, item: {}, is_source: {}",
                result.check_type_name, result.is_source
            );

            if result.is_source {
                compare(result, src_expected_results);
            } else {
                compare(result, dst_expected_results);
            }
        }

        Ok(())
    }

    async fn before_check(&self) -> anyhow::Result<()> {
        match self.db_type {
            DbType::Mysql | DbType::Pg => {
                let base = RdbTestRunner::new(&self.test_dir).await?;
                base.execute_prepare_sqls().await?;
            }

            DbType::Mongo => {
                let base = MongoTestRunner::new(&self.test_dir).await?;
                base.execute_prepare_sqls().await?;
            }

            DbType::Redis => {
                let mut base = RedisTestRunner::new_default(&self.test_dir).await?;
                base.execute_prepare_sqls()?;
            }

            _ => {}
        }
        Ok(())
    }
}
