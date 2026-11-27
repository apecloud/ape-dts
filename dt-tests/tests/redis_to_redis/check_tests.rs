#[cfg(test)]
mod test {

    use serial_test::serial;

    use crate::test_runner::redis_check_test_runner::RedisCheckTestRunner;

    #[tokio::test]
    #[serial]
    async fn standalone_check_basic_test() {
        RedisCheckTestRunner::new("redis_to_redis/check/basic_test")
            .await
            .unwrap()
            .run()
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn standalone_check_empty_source_test() {
        RedisCheckTestRunner::new("redis_to_redis/check/empty_source_test")
            .await
            .unwrap()
            .run()
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn standalone_check_route_test() {
        RedisCheckTestRunner::new("redis_to_redis/check/route_test")
            .await
            .unwrap()
            .run()
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn standalone_check_cluster_test() {
        RedisCheckTestRunner::new("redis_to_redis/check/cluster_test")
            .await
            .unwrap()
            .run()
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn standalone_check_binary_large_test() {
        let mut test = RedisCheckTestRunner::new("redis_to_redis/check/binary_large_test")
            .await
            .unwrap();
        let key = [0u8, 255, 7];
        let payload = vec![255u8; 512 * 1024];
        let before;
        {
            let (source, target) = test.base.default_connections();
            for conn in [source, &mut *target] {
                redis::cmd("SET")
                    .arg(key.as_slice())
                    .arg(&payload)
                    .query::<()>(conn)
                    .unwrap();
                for index in 0u8..32 {
                    let member = vec![0, 255, index];
                    redis::cmd("HSET")
                        .arg("binary-hash")
                        .arg(&member)
                        .arg(&member)
                        .query::<()>(conn)
                        .unwrap();
                    redis::cmd("RPUSH")
                        .arg("binary-list")
                        .arg(&member)
                        .query::<()>(conn)
                        .unwrap();
                    redis::cmd("SADD")
                        .arg("binary-set")
                        .arg(&member)
                        .query::<()>(conn)
                        .unwrap();
                    redis::cmd("ZADD")
                        .arg("binary-zset")
                        .arg(f64::from(index) / 3.0)
                        .arg(&member)
                        .query::<()>(conn)
                        .unwrap();
                    redis::cmd("XADD")
                        .arg("binary-stream")
                        .arg(format!("{}-0", index + 1))
                        .arg(&member)
                        .arg(&member)
                        .arg(&member)
                        .arg("duplicate-field")
                        .query::<()>(conn)
                        .unwrap();
                }
            }
            before = redis::cmd("DUMP")
                .arg(key.as_slice())
                .query::<Vec<u8>>(target)
                .unwrap();
        }
        test.run().await.unwrap();
        let (_, target) = test.base.default_connections();
        assert_eq!(
            redis::cmd("DUMP")
                .arg(key.as_slice())
                .query::<Vec<u8>>(target)
                .unwrap(),
            before
        );
    }

    #[tokio::test]
    #[serial]
    async fn standalone_check_recheck_test() {
        let test = RedisCheckTestRunner::new("redis_to_redis/check/recheck_test")
            .await
            .unwrap();
        let config = test.base.base.get_config();
        let dt_common::config::sinker_config::SinkerConfig::Redis {
            url,
            connection_auth,
            is_cluster,
            ..
        } = config.sinker
        else {
            panic!("expected Redis")
        };
        let mut target = dt_connector::checker::redis_checker::RedisCheckEndpoint::new(
            &url,
            &connection_auth,
            is_cluster,
        )
        .await
        .unwrap();
        let repair = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
            target
                .query::<()>(
                    0,
                    b"temporary",
                    redis::cmd("SET").arg("temporary").arg("source"),
                )
                .await
                .unwrap();
        });
        test.run().await.unwrap();
        repair.await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn standalone_check_expired_source_test() {
        let test = RedisCheckTestRunner::new("redis_to_redis/check/expired_source_test")
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
        test.run().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn standalone_check_readonly_test() {
        let mut test = RedisCheckTestRunner::new("redis_to_redis/check/readonly_test")
            .await
            .unwrap();
        {
            let (source, target) = test.base.default_connections();
            for (conn, replication) in [(source, true), (target, false)] {
                let mut cmd = redis::cmd("ACL");
                cmd.arg("SETUSER")
                    .arg("apedts_checker_test")
                    .arg("reset")
                    .arg("on")
                    .arg(">readonly-test")
                    .arg("~*")
                    .arg("+@read")
                    .arg("+@connection")
                    .arg("+select")
                    .arg("+info");
                if replication {
                    cmd.arg("-@read")
                        .arg("+exists")
                        .arg("+psync")
                        .arg("+replconf");
                }
                cmd.query::<()>(conn).unwrap();
            }
        }
        let file = &test.base.base.task_config_file;
        crate::test_config_util::TestConfigUtil::update_task_config(
            file,
            file,
            &[
                (
                    "extractor".into(),
                    "username".into(),
                    "apedts_checker_test".into(),
                ),
                (
                    "extractor".into(),
                    "password".into(),
                    "readonly-test".into(),
                ),
                (
                    "sinker".into(),
                    "username".into(),
                    "apedts_checker_test".into(),
                ),
                ("sinker".into(), "password".into(), "readonly-test".into()),
            ],
        );
        let result = test.run().await;
        let (source, target) = test.base.default_connections();
        for conn in [source, target] {
            redis::cmd("ACL")
                .arg("DELUSER")
                .arg("apedts_checker_test")
                .query::<()>(conn)
                .unwrap();
        }
        result.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn standalone_check_read_error_test() {
        let mut test = RedisCheckTestRunner::new("redis_to_redis/check/read_error_test")
            .await
            .unwrap();
        {
            let (_, target) = test.base.default_connections();
            redis::cmd("ACL")
                .arg("SETUSER")
                .arg("apedts_checker_denied")
                .arg("reset")
                .arg("on")
                .arg(">readonly-test")
                .arg("~*")
                .arg("+@read")
                .arg("+@connection")
                .arg("+select")
                .arg("+info")
                .arg("-pttl")
                .query::<()>(target)
                .unwrap();
        }
        let file = &test.base.base.task_config_file;
        crate::test_config_util::TestConfigUtil::update_task_config(
            file,
            file,
            &[
                (
                    "sinker".into(),
                    "username".into(),
                    "apedts_checker_denied".into(),
                ),
                ("sinker".into(), "password".into(), "readonly-test".into()),
            ],
        );
        let result = test.run().await;
        let (_, target) = test.base.default_connections();
        redis::cmd("ACL")
            .arg("DELUSER")
            .arg("apedts_checker_denied")
            .query::<()>(target)
            .unwrap();
        let error = result.expect_err("a denied read must fail the standalone task");
        assert!(format!("{error:#}").contains("NOPERM"));
    }
}
