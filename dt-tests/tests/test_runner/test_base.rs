use std::collections::{HashMap, HashSet};

use dt_common::config::config_enums::DbType;

use futures::executor::block_on;

use crate::test_runner::rdb_test_runner::DST;

use super::{
    mongo_check_test_runner::MongoCheckTestRunner, mongo_test_runner::MongoTestRunner,
    precheck_test_runner::PrecheckTestRunner, rdb_check_test_runner::RdbCheckTestRunner,
    rdb_kafka_rdb_test_runner::RdbKafkaRdbTestRunner, rdb_lua_test_runner::RdbLuaTestRunner,
    rdb_redis_test_runner::RdbRedisTestRunner, rdb_sql_test_runner::RdbSqlTestRunner,
    rdb_starrocks_test_runner::RdbStarRocksTestRunner, rdb_struct_test_runner::RdbStructTestRunner,
    rdb_test_runner::RdbTestRunner, redis_statistic_runner::RedisStatisticTestRunner,
    redis_test_runner::RedisTestRunner,
};

pub struct TestBase {}

#[allow(dead_code)]
impl TestBase {
    pub async fn run_snapshot_test(test_dir: &str) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner.run_snapshot_test(true).await.unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_snapshot_test_and_check_dst_count(
        test_dir: &str,
        db_type: &DbType,
        dst_expected_counts: HashMap<&str, usize>,
    ) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner.run_snapshot_test(false).await.unwrap();

        let assert_dst_count = |db_tb: &(String, String), count: usize| {
            let dst_data = block_on(runner.fetch_data(db_tb, DST)).unwrap();
            println!(
                "check dst table {:?} record count, expect: {}, actual: {}",
                db_tb,
                count,
                dst_data.len()
            );
            assert_eq!(dst_data.len(), count);
        };

        for (db_tb, count) in dst_expected_counts {
            let db_tb = RdbTestRunner::parse_full_tb_name(db_tb, db_type);
            assert_dst_count(&db_tb, count);
        }
        runner.close().await.unwrap();
    }

    pub async fn run_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_cdc_to_sql_test(
        test_dir: &str,
        reverse: bool,
        start_millis: u64,
        parse_millis: u64,
    ) {
        let runner = RdbSqlTestRunner::new(test_dir, reverse).await.unwrap();
        runner
            .run_cdc_to_sql_test(start_millis, parse_millis)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_cdc_lua_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = RdbLuaTestRunner::new(test_dir).await.unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_snapshot_lua_test(test_dir: &str) {
        let runner = RdbLuaTestRunner::new(test_dir).await.unwrap();
        runner.run_snapshot_test().await.unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_heartbeat_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner
            .run_heartbeat_test(start_millis, parse_millis)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_ddl_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner
            .run_ddl_test(start_millis, parse_millis)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_ddl_meta_center_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let mut runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner
            .run_ddl_meta_center_test(start_millis, parse_millis)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_check_test(test_dir: &str) {
        let runner = RdbCheckTestRunner::new(test_dir).await.unwrap();
        runner.run_check_test().await.unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_review_test(test_dir: &str) {
        let runner = RdbCheckTestRunner::new(test_dir).await.unwrap();
        runner.run_review_test().await.unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_revise_test(test_dir: &str) {
        let runner = RdbCheckTestRunner::new(test_dir).await.unwrap();
        runner.run_revise_test().await.unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_mongo_snapshot_test(test_dir: &str) {
        let runner = MongoTestRunner::new(test_dir).await.unwrap();
        runner.run_snapshot_test(true).await.unwrap();
    }

    pub async fn run_mongo_snapshot_test_and_check_dst_count(
        test_dir: &str,
        dst_expected_counts: HashMap<(&str, &str), usize>,
    ) {
        let runner = MongoTestRunner::new(test_dir).await.unwrap();
        runner.run_snapshot_test(false).await.unwrap();

        let assert_dst_count = |db: &str, tb: &str, count: usize| {
            let dst_data = block_on(runner.fetch_data(db, tb, DST));
            assert_eq!(dst_data.len(), count);
        };

        for ((db, tb), count) in dst_expected_counts.iter() {
            assert_dst_count(db, tb, *count);
        }
    }

    pub async fn run_mongo_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = MongoTestRunner::new(test_dir).await.unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_mongo_cdc_resume_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = MongoTestRunner::new(test_dir).await.unwrap();
        runner
            .run_cdc_resume_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_mongo_heartbeat_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = MongoTestRunner::new(test_dir).await.unwrap();
        runner
            .run_heartbeat_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_mongo_check_test(test_dir: &str) {
        let runner = MongoCheckTestRunner::new(test_dir).await.unwrap();
        runner.run_check_test().await.unwrap();
    }

    pub async fn run_mongo_revise_test(test_dir: &str) {
        let runner = MongoCheckTestRunner::new(test_dir).await.unwrap();
        runner.run_revise_test().await.unwrap();
    }

    pub async fn run_mongo_review_test(test_dir: &str) {
        let runner = MongoCheckTestRunner::new(test_dir).await.unwrap();
        runner.run_review_test().await.unwrap();
    }

    pub async fn run_redis_snapshot_test(test_dir: &str) {
        let mut runner = RedisTestRunner::new_default(test_dir).await.unwrap();
        runner.run_snapshot_test().await.unwrap();
    }

    pub async fn run_redis_rejson_snapshot_test(test_dir: &str) {
        let mut runner = RedisTestRunner::new(test_dir, vec![('\'', '\'')])
            .await
            .unwrap();
        runner.run_snapshot_test().await.unwrap();
    }

    pub async fn run_redis_redisearch_snapshot_test(test_dir: &str) {
        let mut runner = RedisTestRunner::new(test_dir, vec![('\'', '\'')])
            .await
            .unwrap();
        runner.run_snapshot_test().await.unwrap();
    }

    pub async fn run_redis_graph_snapshot_test(test_dir: &str) {
        let mut runner = RedisTestRunner::new_default(test_dir).await.unwrap();
        runner.run_snapshot_test().await.unwrap();
    }

    pub async fn run_redis_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let mut runner = RedisTestRunner::new_default(test_dir).await.unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_redis_heartbeat_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let mut runner = RedisTestRunner::new_default(test_dir).await.unwrap();
        runner
            .run_heartbeat_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_redis_rejson_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let mut runner = RedisTestRunner::new(test_dir, vec![('\'', '\'')])
            .await
            .unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_redis_graph_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let mut runner = RedisTestRunner::new_default(test_dir).await.unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_redis_statistic_test(test_dir: &str) {
        let mut runner = RedisStatisticTestRunner::new(test_dir).await.unwrap();
        runner.run_statistic_test().await.unwrap();
    }

    pub async fn run_mysql_struct_test(test_dir: &str) {
        let mut runner = RdbStructTestRunner::new(test_dir).await.unwrap();
        runner.run_mysql_struct_test().await.unwrap();
        runner.base.execute_clean_sqls().await.unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_pg_struct_test(test_dir: &str) {
        let mut runner = RdbStructTestRunner::new(test_dir).await.unwrap();
        runner.run_pg_struct_test().await.unwrap();
        runner.base.execute_clean_sqls().await.unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_precheck_test(
        test_dir: &str,
        ignore_check_items: &HashSet<String>,
        src_expected_results: &HashMap<String, bool>,
        dst_expected_results: &HashMap<String, bool>,
    ) {
        let runner = PrecheckTestRunner::new(test_dir).await.unwrap();
        runner
            .run_check(
                ignore_check_items,
                src_expected_results,
                dst_expected_results,
            )
            .await
            .unwrap();
    }

    pub async fn run_rdb_kafka_rdb_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = RdbKafkaRdbTestRunner::new(test_dir).await.unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_rdb_kafka_rdb_snapshot_test(
        test_dir: &str,
        start_millis: u64,
        parse_millis: u64,
    ) {
        let runner = RdbKafkaRdbTestRunner::new(test_dir).await.unwrap();
        runner
            .run_snapshot_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_rdb_redis_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let mut runner = RdbRedisTestRunner::new(test_dir).await.unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_rdb_redis_snapshot_test(test_dir: &str) {
        let mut runner = RdbRedisTestRunner::new(test_dir).await.unwrap();
        runner.run_snapshot_test().await.unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_rdb_starrocks_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = RdbStarRocksTestRunner::new(test_dir).await.unwrap();
        runner
            .run_cdc_soft_delete_test(start_millis, parse_millis)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_dcl_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner
            .run_dcl_test(start_millis, parse_millis)
            .await
            .unwrap();
        runner.close().await.unwrap();
    }

    pub async fn run_dcl_check_test(test_dir: &str) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner.dcl_check_sql_execution().await.unwrap();
        runner.close().await.unwrap();
    }
}
