use dt_common::{
    config::{connection_auth_config::ConnectionAuthConfig, ssl_config::SslMode},
    utils::redis_util::RedisUtil,
};
use dt_connector::extractor::redis::redis_client::RedisClient;

use crate::test_config_util::TestConfigUtil;

tls_task_tests!(verify_ca, "redis_7_0", VerifyCa, false; snapshot_and_cdc => "snapshot_and_cdc");
tls_task_tests!(verify_full, "redis_7_0", VerifyFull, false; snapshot_and_cdc => "snapshot_and_cdc");

#[tokio::test]
#[serial_test::serial]
async fn tls_connection_validation() {
    let config =
        TestConfigUtil::load_task_config("tls/redis_7_0/snapshot_and_cdc/task_config.ini").unwrap();
    let good_url = config.extractor_basic.url;
    let mut wrong_host = url::Url::parse(&good_url).unwrap();
    wrong_host.set_host(Some("127.0.0.1")).unwrap();
    for mode in [SslMode::Require, SslMode::VerifyCa, SslMode::VerifyFull] {
        for allow in [false, true] {
            let mut ssl = config
                .extractor_basic
                .connection_auth
                .ssl_config()
                .unwrap()
                .clone();
            ssl.ssl_mode = mode.clone();
            ssl.ssl_allow_invalid_hostnames = allow;
            let auth = |ssl_config| ConnectionAuthConfig::BasicSsl {
                username: None,
                password: None,
                ssl_config,
            };
            for (url, expected) in [
                (&good_url, true),
                (&wrong_host.to_string(), mode == SslMode::Require || allow),
            ] {
                let auth = auth(ssl.clone());
                let ordinary = RedisUtil::create_redis_conn(url, &auth).await;
                assert_eq!(
                    ordinary.is_ok(),
                    expected,
                    "redis-rs {mode}/allow={allow}: {:?}",
                    ordinary.err()
                );
                let stream = RedisClient::new(url, &auth).await;
                assert_eq!(
                    stream.is_ok(),
                    expected,
                    "PSYNC client {mode}/allow={allow}: {:?}",
                    stream.err()
                );
            }
            ssl.ssl_ca_path = "./docker/tls/client/client-ca.crt".into();
            let auth = auth(ssl);
            assert_eq!(
                RedisUtil::create_redis_conn(&good_url, &auth).await.is_ok(),
                mode == SslMode::Require
            );
            assert_eq!(
                RedisClient::new(&good_url, &auth).await.is_ok(),
                mode == SslMode::Require
            );
        }
    }
    let mut ssl = config
        .extractor_basic
        .connection_auth
        .ssl_config()
        .unwrap()
        .clone();
    ssl.ssl_mode = SslMode::VerifyFull;
    ssl.ssl_client_cert_path.clear();
    ssl.ssl_client_key_path.clear();
    let auth = ConnectionAuthConfig::BasicSsl {
        username: None,
        password: None,
        ssl_config: ssl,
    };
    assert!(RedisUtil::create_redis_conn(&good_url, &auth)
        .await
        .is_err());
    assert!(RedisClient::new(&good_url, &auth).await.is_err());
}
