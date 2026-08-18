use std::collections::HashMap;

use dt_common::utils::time_util::TimeUtil;
use tokio::task::JoinHandle;

use super::redis_test_runner::RedisTestRunner;
use crate::test_config_util::TestConfigUtil;

pub struct RedisCycleTestRunner {
    base: RedisTestRunner,
}

impl RedisCycleTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        Ok(Self {
            base: RedisTestRunner::new_default(relative_test_dir).await?,
        })
    }

    pub async fn run_cycle_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let sub_paths = TestConfigUtil::get_absolute_sub_dir(test_dir);
        let mut handlers: Vec<JoinHandle<()>> = vec![];
        let mut runner_map: HashMap<String, RedisCycleTestRunner> = HashMap::new();

        // execute ddls for all sub tasks
        for sub_path in &sub_paths {
            let mut runner = Self::new(format!("{}/{}", test_dir, sub_path.1).as_str())
                .await
                .unwrap();
            runner.base.execute_prepare_sqls().unwrap();
            runner_map.insert(sub_path.1.to_owned(), runner);
        }

        // start all sub tasks
        for sub_path in &sub_paths {
            let runner = runner_map.get(sub_path.1.as_str()).unwrap();
            handlers.push(runner.base.base.spawn_task().await.unwrap());
            // If there are two tasks doing PSYNC from the same redis at the same time,
            // the latter task may get an "invalid rdb format" error.
            // To avoid this, wait for a while after each task starts.
            TimeUtil::sleep_millis(start_millis).await;
        }

        // execute dmls for all sub tasks
        for sub_path in &sub_paths {
            let runner = runner_map.get_mut(sub_path.1.as_str()).unwrap();
            runner.base.execute_test_sqls().unwrap();
        }
        TimeUtil::sleep_millis(parse_millis).await;

        // do check
        for sub_path in &sub_paths {
            let runner = runner_map.get_mut(sub_path.1.as_str()).unwrap();
            runner.base.compare_all_data().unwrap();
        }

        for handler in handlers {
            handler.abort();
            while !handler.is_finished() {
                TimeUtil::sleep_millis(1).await;
            }
        }
    }
}
